package execmgr

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// StartRequest is the parsed body for any POST /exec* endpoint. The manager
// trusts its inputs — HTTP-level validation happens in the server package.
type StartRequest struct {
	Command   []string
	Tag       string
	Env       map[string]*string // nil value = unset; *"" = keep key empty
	Workdir   string
	Stdin     []byte
	TimeoutMS int
}

// Exec represents a single child process. Lifecycle states transition monotonically:
//
//	failed_to_start  (terminal, never ran)
//	running → exited | signaled | timed_out
//	orphaned (terminal, recovered from a prior plx-exec crash)
type Exec struct {
	ID        string
	Tag       string
	Dir       string
	StartedAt time.Time
	TimeoutMS int
	PID       int // 0 until the child starts; unchanged by reap / recovery

	cmd      *exec.Cmd
	pgid     int
	stdinBuf *bytes.Reader

	stdoutPath string
	stderrPath string

	done   chan struct{} // closed by the reaper
	doneMu sync.Mutex    // guards double-close of done
	state  atomic.Value  // string

	statusMu sync.RWMutex
	status   *Status

	killOnce sync.Once
	log      *slog.Logger
}

// newExec prepares the on-disk layout for a launch. The caller starts the process.
func newExec(ctx context.Context, id, stateDir string, req StartRequest, log *slog.Logger) (*Exec, error) {
	dir := filepath.Join(stateDir, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir exec dir: %w", err)
	}

	e := &Exec{
		ID:         id,
		Tag:        req.Tag,
		Dir:        dir,
		StartedAt:  time.Now().UTC(),
		TimeoutMS:  req.TimeoutMS,
		stdoutPath: filepath.Join(dir, stdoutLog),
		stderrPath: filepath.Join(dir, stderrLog),
		done:       make(chan struct{}),
		log:        log.With("exec_id", id),
	}

	env := mergeEnv(os.Environ(), req.Env)

	// Meta is persisted before the fork so recovery can see the intent even if
	// plx-exec crashes between MkdirAll and cmd.Start. Env preserves nil
	// pointers so the "unset" decision is auditable.
	meta := &Meta{
		ExecID:    id,
		Tag:       req.Tag,
		Command:   req.Command,
		Env:       req.Env,
		Workdir:   req.Workdir,
		TimeoutMS: req.TimeoutMS,
		StartedAt: e.StartedAt,
	}
	if err := WriteMeta(dir, meta); err != nil {
		return nil, fmt.Errorf("write meta: %w", err)
	}

	stdoutF, err := os.OpenFile(e.stdoutPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open stdout log: %w", err)
	}
	stderrF, err := os.OpenFile(e.stderrPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		_ = stdoutF.Close()
		return nil, fmt.Errorf("open stderr log: %w", err)
	}

	cmd := exec.CommandContext(ctx, req.Command[0], req.Command[1:]...)
	if req.Workdir != "" {
		cmd.Dir = req.Workdir
	}
	cmd.Env = env
	cmd.Stdout = stdoutF
	cmd.Stderr = stderrF
	if len(req.Stdin) > 0 {
		e.stdinBuf = bytes.NewReader(req.Stdin)
		cmd.Stdin = e.stdinBuf
	} else {
		// Empty reader so the child sees EOF immediately instead of blocking on stdin.
		cmd.Stdin = bytes.NewReader(nil)
	}
	cmd.SysProcAttr = setpgid()

	e.cmd = cmd
	e.setState(StateRunning)
	return e, nil
}

// start launches the child and spawns the reaper. On failure the exec is
// marked failed_to_start and `done` is closed.
func (e *Exec) start() error {
	if err := e.cmd.Start(); err != nil {
		e.finalize(&Status{
			State:      StateFailedToStart,
			ExitCode:   nil,
			FinishedAt: time.Now().UTC(),
			DurationMS: 0,
		})
		e.closeLogs()
		return fmt.Errorf("start: %w", err)
	}

	e.PID = e.cmd.Process.Pid
	pgid, err := syscall.Getpgid(e.cmd.Process.Pid)
	if err != nil {
		// Fall back to the pid so signalling still targets something.
		pgid = e.cmd.Process.Pid
		e.log.Warn("getpgid failed, falling back to pid", "pid", e.cmd.Process.Pid, "err", err)
	}
	e.pgid = pgid

	if err := WritePid(e.Dir, pgid); err != nil {
		// Non-fatal: recovery after a crash just won't find this exec.
		e.log.Error("write pid file", "err", err)
	}

	go e.reap()
	return nil
}

// reap waits for the child, writes status.json, then closes done. Exactly one
// caller of finalize wins via doneMu.
func (e *Exec) reap() {
	defer e.closeLogs()

	deadline := time.Duration(0)
	if e.TimeoutMS > 0 {
		deadline = time.Duration(e.TimeoutMS) * time.Millisecond
	}

	type waitResult struct {
		err error
	}
	waitCh := make(chan waitResult, 1)
	go func() { waitCh <- waitResult{err: e.cmd.Wait()} }()

	var (
		timedOut bool
		timer    *time.Timer
		timerC   <-chan time.Time
	)
	if deadline > 0 {
		timer = time.NewTimer(deadline)
		timerC = timer.C
		defer timer.Stop()
	}

	var wres waitResult
	select {
	case wres = <-waitCh:
	case <-timerC:
		timedOut = true
		// Hard-kill the process group; then drain wait so cmd.ProcessState is set.
		_ = e.killPGID(syscall.SIGKILL)
		wres = <-waitCh
	}

	status := buildStatus(e.cmd.ProcessState, wres.err, timedOut, e.StartedAt)
	e.finalize(status)
}

func buildStatus(ps *os.ProcessState, waitErr error, timedOut bool, startedAt time.Time) *Status {
	now := time.Now().UTC()
	s := &Status{
		FinishedAt: now,
		DurationMS: now.Sub(startedAt).Milliseconds(),
	}

	var exitCode int
	var sigName string
	var signaled bool

	if ps != nil {
		exitCode = ps.ExitCode()
		if ws, ok := ps.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			signaled = true
			sigName = signalName(ws.Signal())
		}
	} else if waitErr != nil {
		// Extremely unlikely: wait returned an error with no ProcessState.
		exitCode = -1
	}

	switch {
	case timedOut:
		s.State = StateTimedOut
		s.Signal = "SIGKILL"
		ec := exitCode
		s.ExitCode = &ec
	case signaled:
		s.State = StateSignaled
		s.Signal = sigName
		ec := exitCode
		s.ExitCode = &ec
	default:
		s.State = StateExited
		ec := exitCode
		s.ExitCode = &ec
	}
	return s
}

func (e *Exec) finalize(s *Status) {
	// Write on-disk status first so crash recovery sees the terminal record
	// even if we crash between these steps.
	if err := WriteStatusAtomic(e.Dir, s); err != nil {
		e.log.Error("write status", "err", err)
	}

	// Publish status and close `done` BEFORE setting the terminal state. That
	// way any external caller that observes a terminal state via State() is
	// guaranteed to also see isDone()==true and status!=nil on the next read.
	// Reversed ordering caused flaky test failures where a client polled
	// state=exited but then saw EOF=false in a subsequent /logs call because
	// done had not yet closed.
	e.statusMu.Lock()
	e.status = s
	e.statusMu.Unlock()

	e.doneMu.Lock()
	select {
	case <-e.done:
	default:
		close(e.done)
	}
	e.doneMu.Unlock()

	e.setState(s.State)
}

func (e *Exec) closeLogs() {
	if e.cmd != nil {
		if f, ok := e.cmd.Stdout.(*os.File); ok {
			_ = f.Close()
		}
		if f, ok := e.cmd.Stderr.(*os.File); ok {
			_ = f.Close()
		}
	}
}

func (e *Exec) setState(s string) { e.state.Store(s) }

// State returns the current lifecycle state; unset storage is treated as running.
func (e *Exec) State() string {
	v := e.state.Load()
	if v == nil {
		return StateRunning
	}
	return v.(string)
}

// Status returns the terminal Status, or nil if the exec is still running.
func (e *Exec) Status() *Status {
	e.statusMu.RLock()
	defer e.statusMu.RUnlock()
	return e.status
}

// Done is closed when the exec has terminated.
func (e *Exec) Done() <-chan struct{} { return e.done }

func (e *Exec) Wait() { <-e.done }

// Signal delivers sig to the child's process group, or returns nil if already exited.
func (e *Exec) Signal(sig syscall.Signal) error {
	if e.isDone() {
		return nil
	}
	return e.killPGID(sig)
}

// Kill sends SIGTERM, then SIGKILL after 5s if the child is still alive.
func (e *Exec) Kill() error {
	var err error
	e.killOnce.Do(func() {
		if e.isDone() {
			return
		}
		_ = e.killPGID(syscall.SIGTERM)
		select {
		case <-e.done:
		case <-time.After(5 * time.Second):
			err = e.killPGID(syscall.SIGKILL)
			<-e.done
		}
	})
	return err
}

func (e *Exec) killPGID(sig syscall.Signal) error {
	if e.pgid == 0 {
		return errors.New("no pgid")
	}
	if err := syscall.Kill(-e.pgid, sig); err != nil {
		// ESRCH just means the group already died.
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
	return nil
}

func (e *Exec) isDone() bool {
	select {
	case <-e.done:
		return true
	default:
		return false
	}
}

func (e *Exec) StdoutPath() string { return e.stdoutPath }
func (e *Exec) StderrPath() string { return e.stderrPath }

// signalName returns the POSIX name for common signals, or "SIG(<n>)" for
// anything we don't have a constant for.
func signalName(sig syscall.Signal) string {
	switch sig {
	case syscall.SIGHUP:
		return "SIGHUP"
	case syscall.SIGINT:
		return "SIGINT"
	case syscall.SIGQUIT:
		return "SIGQUIT"
	case syscall.SIGILL:
		return "SIGILL"
	case syscall.SIGABRT:
		return "SIGABRT"
	case syscall.SIGFPE:
		return "SIGFPE"
	case syscall.SIGKILL:
		return "SIGKILL"
	case syscall.SIGSEGV:
		return "SIGSEGV"
	case syscall.SIGPIPE:
		return "SIGPIPE"
	case syscall.SIGALRM:
		return "SIGALRM"
	case syscall.SIGTERM:
		return "SIGTERM"
	case syscall.SIGUSR1:
		return "SIGUSR1"
	case syscall.SIGUSR2:
		return "SIGUSR2"
	case syscall.SIGCHLD:
		return "SIGCHLD"
	case syscall.SIGSTOP:
		return "SIGSTOP"
	}
	return fmt.Sprintf("SIG(%d)", sig)
}

// mergeEnv applies edits on top of base. A nil value in edits deletes the key.
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
