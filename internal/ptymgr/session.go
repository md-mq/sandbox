package ptymgr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/creack/pty"
)

const (
	StateRunning = "running"
	StateExited  = "exited"

	defaultShell = "/bin/sh"
	defaultTERM  = "xterm-256color"
	defaultCols  = 80
	defaultRows  = 24

	inputQueueSize = 64
	ptyReadBufSize = 4 << 10
	killGrace      = 5 * time.Second
	drainTimeout   = 2 * time.Second
)

type finishCause string

const (
	CauseChildExited finishCause = "child_exited"
	CauseDelete      finishCause = "delete"
	CauseIdle        finishCause = "idle"
	CauseShutdown    finishCause = "shutdown"
)

type SessionConfig struct {
	ReplayBytes int
}

type AllocateRequest struct {
	Command []string
	Tag     string
	Env     map[string]*string
	Workdir string
	Cols    uint16
	Rows    uint16
}

type PTYSessionStatus struct {
	PTYID              string
	Tag                string
	PID                int
	State              string
	StartedAt          time.Time
	FinishedAt         *time.Time
	ExitCode           *int
	Signal             string
	DurationMS         int64
	Cols               uint16
	Rows               uint16
	LastActivity       time.Time
	LastClientActivity time.Time
	DetachedSince      *time.Time
	Attached           bool
}

type PTYSession struct {
	ID        string
	Tag       string
	Dir       string
	StartedAt time.Time
	PID       int

	cmd      *exec.Cmd
	master   *os.File
	masterMu sync.Mutex
	pgid     int

	inCh chan []byte
	ring *RingBuffer

	runningCounter *atomic.Int64
	log            *slog.Logger

	sessionMu sync.RWMutex

	command []string
	env     map[string]*string
	workdir string
	cols    uint16
	rows    uint16

	state      string
	finishedAt *time.Time
	exitCode   *int
	signal     string
	durationMS int64

	lastActivity       time.Time
	lastClientActivity time.Time
	detachedSince      *time.Time
	attached           bool

	deleting    bool
	deleteCause finishCause
	removed     bool
	sub         *Subscriber

	finishOnce sync.Once
	removeOnce sync.Once
	killOnce   sync.Once

	done       chan struct{}
	outputDone chan struct{}
}

func newSession(id, stateDir string, req AllocateRequest, cfg SessionConfig, runningCounter *atomic.Int64, log *slog.Logger) (*PTYSession, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: pty id required", ErrInvalidRequest)
	}
	if stateDir == "" {
		return nil, fmt.Errorf("%w: state dir required", ErrInvalidRequest)
	}
	if cfg.ReplayBytes < 0 {
		cfg.ReplayBytes = 0
	}
	if log == nil {
		log = slog.Default()
	}

	command := normalizeCommand(req.Command)
	cols, rows := normalizeSize(req.Cols, req.Rows)
	env := envWithDefaultTERM(req.Env)
	started := time.Now().UTC()
	dir := filepath.Join(stateDir, "pty-"+id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir pty dir: %w", err)
	}

	cmd := exec.Command(command[0], command[1:]...)
	if req.Workdir != "" {
		cmd.Dir = req.Workdir
	}
	cmd.Env = mergeEnv(os.Environ(), env)

	master, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: cols, Rows: rows})
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("start pty: %w", err)
	}

	pgid, err := syscall.Getpgid(cmd.Process.Pid)
	if err != nil {
		pgid = cmd.Process.Pid
		log.Warn("getpgid failed, falling back to pid", "pid", cmd.Process.Pid, "err", err)
	}

	meta := &Meta{
		PTYID:     id,
		Tag:       req.Tag,
		Command:   command,
		Env:       cloneEnv(env),
		Workdir:   req.Workdir,
		PID:       cmd.Process.Pid,
		PGID:      pgid,
		Cols:      cols,
		Rows:      rows,
		StartedAt: started,
	}
	if err := WriteMeta(dir, meta); err != nil {
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		_ = cmd.Wait()
		_ = master.Close()
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("write meta: %w", err)
	}

	detachedSince := started
	s := &PTYSession{
		ID:                 id,
		Tag:                req.Tag,
		Dir:                dir,
		StartedAt:          started,
		PID:                cmd.Process.Pid,
		cmd:                cmd,
		master:             master,
		pgid:               pgid,
		inCh:               make(chan []byte, inputQueueSize),
		ring:               NewRing(cfg.ReplayBytes),
		runningCounter:     runningCounter,
		log:                log.With("pty_id", id),
		command:            command,
		env:                cloneEnv(env),
		workdir:            req.Workdir,
		cols:               cols,
		rows:               rows,
		state:              StateRunning,
		lastActivity:       started,
		lastClientActivity: started,
		detachedSince:      &detachedSince,
		done:               make(chan struct{}),
		outputDone:         make(chan struct{}),
	}

	go s.inputPump()
	go s.outputPump()
	go s.reap()
	return s, nil
}

func (s *PTYSession) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	if s.isDone() {
		return ErrGone
	}
	cp := append([]byte(nil), b...)
	now := time.Now().UTC()
	s.sessionMu.Lock()
	s.lastActivity = now
	s.lastClientActivity = now
	s.sessionMu.Unlock()

	select {
	case s.inCh <- cp:
		return nil
	case <-s.done:
		return ErrGone
	}
}

func (s *PTYSession) Resize(cols, rows uint16) error {
	if cols == 0 || rows == 0 {
		return fmt.Errorf("%w: cols and rows must be > 0", ErrInvalidRequest)
	}
	if s.isDone() {
		return ErrGone
	}
	if err := s.resizeMaster(cols, rows); err != nil {
		return err
	}
	now := time.Now().UTC()
	s.sessionMu.Lock()
	s.cols = cols
	s.rows = rows
	s.lastActivity = now
	s.lastClientActivity = now
	s.sessionMu.Unlock()
	return nil
}

func (s *PTYSession) Signal(sig string) error {
	switch sig {
	case "SIGINT":
		return s.Write([]byte{0x03})
	case "SIGQUIT":
		return s.Write([]byte{0x1c})
	}
	parsed, ok := processGroupSignal(sig)
	if !ok {
		return fmt.Errorf("%w: unsupported signal %q", ErrInvalidRequest, sig)
	}
	if s.isDone() {
		return nil
	}
	now := time.Now().UTC()
	s.sessionMu.Lock()
	s.lastActivity = now
	s.lastClientActivity = now
	s.sessionMu.Unlock()
	return s.killPGID(parsed)
}

func (s *PTYSession) Kill() error {
	return s.killFor(CauseDelete)
}

func (s *PTYSession) Done() <-chan struct{} { return s.done }

func (s *PTYSession) Wait() { <-s.done }

func (s *PTYSession) Status() PTYSessionStatus {
	s.sessionMu.RLock()
	defer s.sessionMu.RUnlock()

	st := PTYSessionStatus{
		PTYID:              s.ID,
		Tag:                s.Tag,
		PID:                s.PID,
		State:              s.state,
		StartedAt:          s.StartedAt,
		Signal:             s.signal,
		DurationMS:         s.durationMS,
		Cols:               s.cols,
		Rows:               s.rows,
		LastActivity:       s.lastActivity,
		LastClientActivity: s.lastClientActivity,
		Attached:           s.attached,
	}
	if s.finishedAt != nil {
		finished := *s.finishedAt
		st.FinishedAt = &finished
	}
	if s.exitCode != nil {
		ec := *s.exitCode
		st.ExitCode = &ec
	}
	if s.detachedSince != nil {
		detached := *s.detachedSince
		st.DetachedSince = &detached
	}
	return st
}

func (s *PTYSession) Replay(n int) ([][]byte, error) {
	s.sessionMu.RLock()
	defer s.sessionMu.RUnlock()
	if s.ring == nil {
		return nil, nil
	}
	if n > s.ring.Size() {
		return nil, ErrReplayTooLarge
	}
	return s.ring.Snapshot(n), nil
}

func (s *PTYSession) inputPump() {
	for {
		select {
		case b := <-s.inCh:
			if len(b) == 0 {
				continue
			}
			if _, err := s.writeMaster(b); err != nil {
				return
			}
		case <-s.done:
			return
		}
	}
}

func (s *PTYSession) outputPump() {
	defer close(s.outputDone)
	defer s.closeMaster()

	buf := make([]byte, ptyReadBufSize)
	for {
		n, err := s.master.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			now := time.Now().UTC()
			s.sessionMu.Lock()
			if s.ring != nil {
				s.ring.Append(chunk)
			}
			if s.sub != nil {
				if !s.sub.TryPush(outFrame{kind: frameBinary, data: chunk}) {
					s.sub.Close()
					s.sub = nil
				}
			}
			s.lastActivity = now
			s.sessionMu.Unlock()
		}
		if err != nil {
			if !isPTYClosedRead(err) {
				s.log.Debug("pty read failed", "err", err)
			}
			return
		}
	}
}

func (s *PTYSession) reap() {
	waitErr := s.cmd.Wait()
	meta := buildExitMetadata(s.cmd.ProcessState, waitErr, s.StartedAt)

	select {
	case <-s.outputDone:
	case <-time.After(drainTimeout):
		_ = s.closeMaster()
		<-s.outputDone
	}

	s.finish(s.selectedFinishCause(), meta)
}

type exitMetadata struct {
	finishedAt time.Time
	exitCode   *int
	signal     string
	durationMS int64
}

func buildExitMetadata(ps *os.ProcessState, waitErr error, startedAt time.Time) exitMetadata {
	now := time.Now().UTC()
	meta := exitMetadata{
		finishedAt: now,
		durationMS: now.Sub(startedAt).Milliseconds(),
	}

	var exitCode int
	if ps != nil {
		exitCode = ps.ExitCode()
		if ws, ok := ps.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			meta.signal = signalName(ws.Signal())
		}
	} else if waitErr != nil {
		exitCode = -1
	}
	meta.exitCode = &exitCode
	return meta
}

func (s *PTYSession) finish(cause finishCause, meta exitMetadata) {
	s.finishOnce.Do(func() {
		var sub *Subscriber
		shouldRemove := false

		s.sessionMu.Lock()
		if s.deleting && !causeRemoves(cause) {
			cause = s.deleteCause
		}
		s.state = StateExited
		s.finishedAt = &meta.finishedAt
		s.exitCode = meta.exitCode
		s.signal = meta.signal
		s.durationMS = meta.durationMS
		if s.ring != nil {
			s.ring.Reset()
			s.ring = nil
		}
		sub = s.sub
		s.sub = nil
		shouldRemove = causeRemoves(cause) || s.deleting
		s.sessionMu.Unlock()

		if sub != nil {
			if cause == CauseChildExited {
				sub.TryPush(outFrame{kind: frameText, data: exitFrame(meta)})
			}
			sub.Close()
		}
		if s.runningCounter != nil {
			s.runningCounter.Add(-1)
		}
		close(s.done)
		if shouldRemove {
			s.remove()
		}
	})
}

func (s *PTYSession) killFor(cause finishCause) error {
	s.markDeleting(cause)
	if s.isDone() {
		s.remove()
		return nil
	}

	var err error
	s.killOnce.Do(func() {
		if s.isDone() {
			return
		}
		_ = s.killPGID(syscall.SIGTERM)
		select {
		case <-s.done:
		case <-time.After(killGrace):
			err = s.killPGID(syscall.SIGKILL)
			<-s.done
		}
	})
	s.remove()
	return err
}

func (s *PTYSession) markDeleting(cause finishCause) {
	s.sessionMu.Lock()
	if !s.deleting {
		s.deleting = true
		s.deleteCause = cause
	}
	s.sessionMu.Unlock()
}

func (s *PTYSession) selectedFinishCause() finishCause {
	s.sessionMu.RLock()
	defer s.sessionMu.RUnlock()
	if s.deleting {
		return s.deleteCause
	}
	return CauseChildExited
}

func (s *PTYSession) remove() {
	s.removeOnce.Do(func() {
		s.sessionMu.Lock()
		s.removed = true
		s.ring = nil
		s.sessionMu.Unlock()
		if err := os.RemoveAll(s.Dir); err != nil {
			s.log.Warn("remove pty dir", "err", err)
		}
	})
}

func (s *PTYSession) killPGID(sig syscall.Signal) error {
	if s.pgid == 0 {
		return errors.New("no pgid")
	}
	if err := syscall.Kill(-s.pgid, sig); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	return nil
}

func (s *PTYSession) writeMaster(b []byte) (int, error) {
	s.masterMu.Lock()
	defer s.masterMu.Unlock()
	return s.master.Write(b)
}

func (s *PTYSession) resizeMaster(cols, rows uint16) error {
	s.masterMu.Lock()
	defer s.masterMu.Unlock()
	return pty.Setsize(s.master, &pty.Winsize{Cols: cols, Rows: rows})
}

func (s *PTYSession) closeMaster() error {
	s.masterMu.Lock()
	defer s.masterMu.Unlock()
	return s.master.Close()
}

func (s *PTYSession) isDone() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func normalizeCommand(command []string) []string {
	if len(command) == 0 || command[0] == "" {
		return []string{defaultShell}
	}
	out := make([]string, len(command))
	copy(out, command)
	return out
}

func normalizeSize(cols, rows uint16) (uint16, uint16) {
	if cols == 0 {
		cols = defaultCols
	}
	if rows == 0 {
		rows = defaultRows
	}
	return cols, rows
}

func envWithDefaultTERM(env map[string]*string) map[string]*string {
	out := cloneEnv(env)
	if _, ok := out["TERM"]; !ok {
		term := defaultTERM
		out["TERM"] = &term
	}
	return out
}

func cloneEnv(env map[string]*string) map[string]*string {
	if len(env) == 0 {
		return map[string]*string{}
	}
	out := make(map[string]*string, len(env))
	for k, v := range env {
		if v == nil {
			out[k] = nil
			continue
		}
		cp := *v
		out[k] = &cp
	}
	return out
}

func mergeEnv(base []string, edits map[string]*string) []string {
	out := make(map[string]string, len(base))
	for _, kv := range base {
		for i := 0; i < len(kv); i++ {
			if kv[i] == '=' {
				out[kv[:i]] = kv[i+1:]
				break
			}
		}
	}
	for k, v := range edits {
		if v == nil {
			delete(out, k)
			continue
		}
		out[k] = *v
	}
	merged := make([]string, 0, len(out))
	for k, v := range out {
		merged = append(merged, k+"="+v)
	}
	return merged
}

func processGroupSignal(sig string) (syscall.Signal, bool) {
	switch sig {
	case "SIGHUP":
		return syscall.SIGHUP, true
	case "SIGTERM":
		return syscall.SIGTERM, true
	case "SIGKILL":
		return syscall.SIGKILL, true
	case "SIGUSR1":
		return syscall.SIGUSR1, true
	case "SIGUSR2":
		return syscall.SIGUSR2, true
	default:
		return 0, false
	}
}

func causeRemoves(cause finishCause) bool {
	switch cause {
	case CauseDelete, CauseIdle, CauseShutdown:
		return true
	default:
		return false
	}
}

func isPTYClosedRead(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) || errors.Is(err, syscall.EIO) {
		return true
	}
	return false
}

func signalName(sig syscall.Signal) string {
	switch sig {
	case syscall.SIGHUP:
		return "SIGHUP"
	case syscall.SIGINT:
		return "SIGINT"
	case syscall.SIGQUIT:
		return "SIGQUIT"
	case syscall.SIGKILL:
		return "SIGKILL"
	case syscall.SIGTERM:
		return "SIGTERM"
	case syscall.SIGUSR1:
		return "SIGUSR1"
	case syscall.SIGUSR2:
		return "SIGUSR2"
	}
	return fmt.Sprintf("SIG(%d)", sig)
}

func exitFrame(meta exitMetadata) []byte {
	body := struct {
		Type       string `json:"type"`
		ExitCode   *int   `json:"exit_code"`
		Signal     string `json:"signal,omitempty"`
		FinishedAt string `json:"finished_at"`
		DurationMS int64  `json:"duration_ms"`
	}{
		Type:       "exited",
		ExitCode:   meta.exitCode,
		Signal:     meta.signal,
		FinishedAt: meta.finishedAt.Format(time.RFC3339Nano),
		DurationMS: meta.durationMS,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return []byte(`{"type":"exited"}`)
	}
	return data
}
