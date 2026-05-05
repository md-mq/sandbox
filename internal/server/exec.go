package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/polyaxon/sandbox/internal/execmgr"
)

const (
	defaultTimeoutMS = 30_000
	maxTimeoutMS     = 300_000
	maxStdoutBytes   = 1 << 20 // 1 MiB, matches API spec
	defaultLogMax    = 64 << 10
	maxLogMax        = 1 << 20
	followDeadline   = 30 * time.Second
	sseMaxPerPoll    = 1 << 20
	ssePollInterval  = 100 * time.Millisecond

	maxExecBodyBytes     = 16 << 20
	maxCommandTotalBytes = 4096
	maxCommandElements   = 256
	maxEnvKeys           = 256
	maxEnvKeyBytes       = 256
	maxEnvValueBytes     = 4096
	maxWorkdirBytes      = 4096
	maxStdinBytes        = 8 << 20
)

var allowedSignals = map[string]syscall.Signal{
	"SIGINT":  syscall.SIGINT,
	"SIGTERM": syscall.SIGTERM,
	"SIGKILL": syscall.SIGKILL,
	"SIGHUP":  syscall.SIGHUP,
	"SIGQUIT": syscall.SIGQUIT,
	"SIGUSR1": syscall.SIGUSR1,
	"SIGUSR2": syscall.SIGUSR2,
}

// execRequest is the wire shape for POST /exec, /exec/stream, /exec/bg.
type execRequest struct {
	Command   []string           `json:"command"`
	Tag       string             `json:"tag"`
	Env       map[string]*string `json:"env"` // null value = unset
	Workdir   string             `json:"workdir"`
	Stdin     string             `json:"stdin"` // base64
	TimeoutMS int                `json:"timeout_ms"`
}

func (r *execRequest) toStartRequest(allowTag bool) (execmgr.StartRequest, error) {
	if len(r.Command) == 0 || r.Command[0] == "" {
		return execmgr.StartRequest{}, fmt.Errorf("%w: command required", execmgr.ErrInvalidRequest)
	}
	if r.Tag != "" && !allowTag {
		return execmgr.StartRequest{}, fmt.Errorf("%w: tag is only supported for background exec", execmgr.ErrInvalidRequest)
	}
	if allowTag {
		if err := execmgr.ValidateTag(r.Tag); err != nil {
			return execmgr.StartRequest{}, err
		}
	}
	if len(r.Command) > maxCommandElements {
		return execmgr.StartRequest{}, fmt.Errorf("%w: command has %d elements, max %d",
			execmgr.ErrInvalidRequest, len(r.Command), maxCommandElements)
	}
	total := 0
	for i, arg := range r.Command {
		if strings.ContainsRune(arg, 0) {
			return execmgr.StartRequest{}, fmt.Errorf("%w: command[%d] contains NUL", execmgr.ErrInvalidRequest, i)
		}
		total += len(arg)
	}
	if total > maxCommandTotalBytes {
		return execmgr.StartRequest{}, fmt.Errorf("%w: command total length %d bytes, max %d",
			execmgr.ErrInvalidRequest, total, maxCommandTotalBytes)
	}
	if len(r.Env) > maxEnvKeys {
		return execmgr.StartRequest{}, fmt.Errorf("%w: env has %d keys, max %d",
			execmgr.ErrInvalidRequest, len(r.Env), maxEnvKeys)
	}
	for k, v := range r.Env {
		if k == "" {
			return execmgr.StartRequest{}, fmt.Errorf("%w: empty env key", execmgr.ErrInvalidRequest)
		}
		if len(k) > maxEnvKeyBytes {
			return execmgr.StartRequest{}, fmt.Errorf("%w: env key exceeds %d bytes",
				execmgr.ErrInvalidRequest, maxEnvKeyBytes)
		}
		if strings.ContainsAny(k, "=\x00") {
			return execmgr.StartRequest{}, fmt.Errorf("%w: env key contains '=' or NUL", execmgr.ErrInvalidRequest)
		}
		if strings.HasPrefix(k, "POLYAXON_") {
			return execmgr.StartRequest{}, fmt.Errorf("%w: %s", execmgr.ErrReservedEnvKey, k)
		}
		if v != nil {
			if len(*v) > maxEnvValueBytes {
				return execmgr.StartRequest{}, fmt.Errorf("%w: env[%s] value exceeds %d bytes",
					execmgr.ErrInvalidRequest, k, maxEnvValueBytes)
			}
			if strings.ContainsRune(*v, 0) {
				return execmgr.StartRequest{}, fmt.Errorf("%w: env[%s] value contains NUL",
					execmgr.ErrInvalidRequest, k)
			}
		}
	}
	if r.Workdir != "" {
		if len(r.Workdir) > maxWorkdirBytes {
			return execmgr.StartRequest{}, fmt.Errorf("%w: workdir exceeds %d bytes",
				execmgr.ErrInvalidRequest, maxWorkdirBytes)
		}
		if strings.ContainsRune(r.Workdir, 0) {
			return execmgr.StartRequest{}, fmt.Errorf("%w: workdir contains NUL", execmgr.ErrInvalidRequest)
		}
		if !filepath.IsAbs(r.Workdir) {
			return execmgr.StartRequest{}, fmt.Errorf("%w: workdir must be absolute", execmgr.ErrInvalidRequest)
		}
		info, err := os.Stat(r.Workdir)
		if err != nil || !info.IsDir() {
			return execmgr.StartRequest{}, fmt.Errorf("%w: workdir does not exist or is not a directory", execmgr.ErrInvalidRequest)
		}
	}
	var stdin []byte
	if r.Stdin != "" {
		decoded, err := base64.StdEncoding.DecodeString(r.Stdin)
		if err != nil {
			return execmgr.StartRequest{}, fmt.Errorf("%w: stdin must be valid base64", execmgr.ErrInvalidRequest)
		}
		if len(decoded) > maxStdinBytes {
			return execmgr.StartRequest{}, fmt.Errorf("%w: stdin decodes to %d bytes, max %d",
				execmgr.ErrInvalidRequest, len(decoded), maxStdinBytes)
		}
		stdin = decoded
	}
	timeout := r.TimeoutMS
	if timeout == 0 {
		timeout = defaultTimeoutMS
	}
	if timeout < 0 || timeout > maxTimeoutMS {
		return execmgr.StartRequest{}, fmt.Errorf("%w: timeout_ms must be within [0, %d]", execmgr.ErrInvalidRequest, maxTimeoutMS)
	}
	return execmgr.StartRequest{
		Command:   r.Command,
		Tag:       r.Tag,
		Env:       r.Env,
		Workdir:   r.Workdir,
		Stdin:     stdin,
		TimeoutMS: timeout,
	}, nil
}

// decodeExecRequest writes an error response and returns ok=false on failure.
func (s *Server) decodeExecRequest(w http.ResponseWriter, r *http.Request, allowTag bool) (execmgr.StartRequest, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxExecBodyBytes)
	var body execRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, s.log, http.StatusRequestEntityTooLarge, CodePayloadTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", maxExecBodyBytes))
			return execmgr.StartRequest{}, false
		}
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, "malformed JSON body")
		return execmgr.StartRequest{}, false
	}
	var extra struct{}
	if err := dec.Decode(&extra); err != io.EOF {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, s.log, http.StatusRequestEntityTooLarge, CodePayloadTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", maxExecBodyBytes))
			return execmgr.StartRequest{}, false
		}
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, "body contains trailing data after JSON")
		return execmgr.StartRequest{}, false
	}
	req, err := body.toStartRequest(allowTag)
	if err != nil {
		switch {
		case errors.Is(err, execmgr.ErrReservedEnvKey):
			writeError(w, s.log, http.StatusBadRequest, CodeReservedEnvKey, err.Error())
		case errors.Is(err, execmgr.ErrInvalidRequest):
			writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		default:
			writeError(w, s.log, http.StatusInternalServerError, CodeInternal, "validation failed")
		}
		return execmgr.StartRequest{}, false
	}
	return req, true
}

// startAndMap returns the exec, or nil if an error was already written.
func (s *Server) startAndMap(ctx context.Context, w http.ResponseWriter, req execmgr.StartRequest) *execmgr.Exec {
	e, err := s.mgr.Start(ctx, req)
	if err != nil {
		var tagErr *execmgr.ErrTagConflict
		switch {
		case errors.As(err, &tagErr):
			details := map[string]any{
				"tag":   tagErr.Tag,
				"state": tagErr.State,
			}
			if tagErr.ExistingID != "" {
				details["existing_id"] = tagErr.ExistingID
			}
			writeErrorWithDetails(w, s.log, http.StatusConflict, CodeConflict,
				"tag already exists", details)
		case errors.Is(err, execmgr.ErrCapExceeded):
			w.Header().Set("Retry-After", "1")
			// retry_after_ms duplicates Retry-After in the body for structured clients.
			writeErrorWithDetails(w, s.log, http.StatusTooManyRequests, "rate_limited",
				"concurrent exec cap exceeded", map[string]any{"retry_after_ms": 1000})
		case errors.Is(err, execmgr.ErrInvalidRequest):
			writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		default:
			s.log.Error("start exec", "err", err)
			writeError(w, s.log, http.StatusInternalServerError, CodeInternal, "failed to start exec")
		}
		return nil
	}
	return e
}

type execSyncResponse struct {
	ExecID          string `json:"exec_id"`
	ExitCode        *int   `json:"exit_code"`
	Signal          string `json:"signal,omitempty"`
	Stdout          string `json:"stdout"`
	Stderr          string `json:"stderr"`
	DurationMS      int64  `json:"duration_ms"`
	TimedOut        bool   `json:"timed_out"`
	StdoutTruncated bool   `json:"stdout_truncated,omitempty"`
	StderrTruncated bool   `json:"stderr_truncated,omitempty"`
}

func (s *Server) handleExec(w http.ResponseWriter, r *http.Request) {
	req, ok := s.decodeExecRequest(w, r, false)
	if !ok {
		return
	}
	e := s.startAndMap(context.Background(), w, req)
	if e == nil {
		return
	}

	select {
	case <-e.Done():
	case <-r.Context().Done():
		// Client disconnected. The child uses context.Background(), so this is
		// the single cleanup path.
		_ = e.Kill()
		return
	}

	status := e.Status()
	stdoutData, stdoutTrunc := readCapped(e.StdoutPath(), maxStdoutBytes)
	stderrData, stderrTrunc := readCapped(e.StderrPath(), maxStdoutBytes)

	resp := execSyncResponse{
		ExecID:          e.ID,
		Stdout:          string(stdoutData),
		Stderr:          string(stderrData),
		StdoutTruncated: stdoutTrunc,
		StderrTruncated: stderrTrunc,
	}
	if status != nil {
		resp.ExitCode = status.ExitCode
		resp.Signal = status.Signal
		resp.DurationMS = status.DurationMS
		resp.TimedOut = status.State == execmgr.StateTimedOut
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleExecStream(w http.ResponseWriter, r *http.Request) {
	req, ok := s.decodeExecRequest(w, r, false)
	if !ok {
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, s.log, http.StatusInternalServerError, CodeInternal, "streaming not supported")
		return
	}

	// Start before flipping into SSE mode so 429/invalid can still be a JSON
	// error envelope. After headers are flushed we can't switch back.
	e := s.startAndMap(context.Background(), w, req)
	if e == nil {
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	writeSSE(w, flusher, "start", map[string]any{
		"exec_id":    e.ID,
		"pid":        e.PID,
		"started_at": e.StartedAt,
	})

	ctx := r.Context()
	ping := time.NewTicker(3 * time.Second)
	defer ping.Stop()

	// Reusable idle-poll timer. time.After allocated per loop iteration leaked
	// 10 timers/sec during idle execs — tiny, but avoidable.
	idle := time.NewTimer(ssePollInterval)
	defer idle.Stop()

	var stdoutOff, stderrOff int64
	for {
		progressed := false

		if data, next, err := readChunk(e.StdoutPath(), stdoutOff, sseMaxPerPoll); err == nil && len(data) > 0 {
			writeSSE(w, flusher, "stdout", map[string]any{
				"text":   string(data),
				"offset": next,
			})
			stdoutOff = next
			progressed = true
		}
		if data, next, err := readChunk(e.StderrPath(), stderrOff, sseMaxPerPoll); err == nil && len(data) > 0 {
			writeSSE(w, flusher, "stderr", map[string]any{
				"text":   string(data),
				"offset": next,
			})
			stderrOff = next
			progressed = true
		}

		if progressed {
			// Stay hot — there may be more bytes queued up.
			continue
		}

		resetTimer(idle, ssePollInterval)
		select {
		case <-ctx.Done():
			return
		case <-e.Done():
			// Drain any remaining bytes once more before emitting completion.
			if data, next, err := readChunk(e.StdoutPath(), stdoutOff, sseMaxPerPoll); err == nil && len(data) > 0 {
				writeSSE(w, flusher, "stdout", map[string]any{"text": string(data), "offset": next})
				stdoutOff = next
			}
			if data, next, err := readChunk(e.StderrPath(), stderrOff, sseMaxPerPoll); err == nil && len(data) > 0 {
				writeSSE(w, flusher, "stderr", map[string]any{"text": string(data), "offset": next})
				stderrOff = next
			}
			status := e.Status()
			if status != nil {
				writeSSE(w, flusher, "execution_complete", map[string]any{
					"exit_code":   status.ExitCode,
					"signal":      nullIfEmpty(status.Signal),
					"duration_ms": status.DurationMS,
					"timed_out":   status.State == execmgr.StateTimedOut,
				})
			}
			return
		case <-ping.C:
			writeSSE(w, flusher, "ping", map[string]any{})
		case <-idle.C:
		}
	}
}

// resetTimer drains any pending firing then resets to d. Correct reset idiom
// per the time.Timer docs.
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

type execBgResponse struct {
	ExecID    string    `json:"exec_id"`
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"started_at"`
	Tag       string    `json:"tag,omitempty"`
}

func (s *Server) handleExecBg(w http.ResponseWriter, r *http.Request) {
	req, ok := s.decodeExecRequest(w, r, true)
	if !ok {
		return
	}
	// Background context: the exec outlives the request.
	e := s.startAndMap(context.Background(), w, req)
	if e == nil {
		return
	}
	writeJSON(w, http.StatusAccepted, execBgResponse{
		ExecID:    e.ID,
		PID:       e.PID,
		StartedAt: e.StartedAt,
		Tag:       e.Tag,
	})
}

type bgListResponse struct {
	Execs []bgListItem `json:"execs"`
}

type bgListItem struct {
	ExecID      string     `json:"exec_id"`
	PID         int        `json:"pid"`
	State       string     `json:"state"`
	StartedAt   time.Time  `json:"started_at"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	DurationMS  int64      `json:"duration_ms,omitempty"`
	ExitCode    *int       `json:"exit_code,omitempty"`
	Signal      string     `json:"signal,omitempty"`
	StdoutBytes int64      `json:"stdout_bytes"`
	StderrBytes int64      `json:"stderr_bytes"`
	Tag         string     `json:"tag,omitempty"`
}

func (s *Server) handleExecBgList(w http.ResponseWriter, r *http.Request) {
	tagFilter := r.URL.Query().Get("tag")
	if tagFilter != "" {
		if err := execmgr.ValidateTag(tagFilter); err != nil {
			writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
			return
		}
	}
	summaries := s.mgr.List(tagFilter)
	resp := bgListResponse{Execs: make([]bgListItem, 0, len(summaries))}
	for _, summary := range summaries {
		resp.Execs = append(resp.Execs, bgListItem{
			ExecID:      summary.ID,
			PID:         summary.PID,
			State:       summary.State,
			StartedAt:   summary.StartedAt,
			FinishedAt:  summary.FinishedAt,
			DurationMS:  summary.DurationMS,
			ExitCode:    summary.ExitCode,
			Signal:      summary.Signal,
			StdoutBytes: summary.StdoutBytes,
			StderrBytes: summary.StderrBytes,
			Tag:         summary.Tag,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

type bgStatusResponse struct {
	ExecID      string     `json:"exec_id"`
	PID         int        `json:"pid"`
	State       string     `json:"state"`
	StartedAt   time.Time  `json:"started_at"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	DurationMS  int64      `json:"duration_ms,omitempty"`
	ExitCode    *int       `json:"exit_code"`
	Signal      string     `json:"signal,omitempty"`
	StdoutBytes int64      `json:"stdout_bytes"`
	StderrBytes int64      `json:"stderr_bytes"`
	Tag         string     `json:"tag,omitempty"`
}

func (s *Server) handleExecBgStatus(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	e, ok := s.mgr.Get(id)
	if !ok {
		writeError(w, s.log, http.StatusNotFound, CodeNotFound, "exec not found")
		return
	}
	resp := bgStatusResponse{
		ExecID:      e.ID,
		PID:         e.PID,
		State:       e.State(),
		StartedAt:   e.StartedAt,
		StdoutBytes: fileSize(e.StdoutPath()),
		StderrBytes: fileSize(e.StderrPath()),
		Tag:         e.Tag,
	}
	if st := e.Status(); st != nil {
		finished := st.FinishedAt
		resp.FinishedAt = &finished
		resp.DurationMS = st.DurationMS
		resp.ExitCode = st.ExitCode
		resp.Signal = st.Signal
	}
	writeJSON(w, http.StatusOK, resp)
}

type bgLogsResponse struct {
	ExecID     string `json:"exec_id"`
	Stream     string `json:"stream"`
	Offset     int64  `json:"offset"`
	NextOffset int64  `json:"next_offset"`
	Bytes      int    `json:"bytes"`
	Data       string `json:"data"`
	EOF        bool   `json:"eof"`
	State      string `json:"state"`
}

func (s *Server) handleExecBgLogs(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	e, ok := s.mgr.Get(id)
	if !ok {
		writeError(w, s.log, http.StatusNotFound, CodeNotFound, "exec not found")
		return
	}

	streamStr := r.URL.Query().Get("stream")
	if streamStr == "" {
		streamStr = "stdout"
	}
	var stream execmgr.LogStream
	switch streamStr {
	case "stdout":
		stream = execmgr.LogStdout
	case "stderr":
		stream = execmgr.LogStderr
	case "combined":
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest,
			"combined log stream is deferred; request stdout or stderr")
		return
	default:
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest,
			"stream must be 'stdout' or 'stderr'")
		return
	}

	offset, err := parseInt64(r.URL.Query().Get("offset"), 0)
	if err != nil || offset < 0 {
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, "offset must be >= 0")
		return
	}
	maxBytes, err := parseInt64(r.URL.Query().Get("max_bytes"), defaultLogMax)
	if err != nil || maxBytes <= 0 {
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, "max_bytes must be > 0")
		return
	}
	if maxBytes > maxLogMax {
		maxBytes = maxLogMax
	}

	follow := r.URL.Query().Get("follow") == "true"

	res, err := e.ReadLog(r.Context(), stream, offset, maxBytes, follow, followDeadline)
	if err != nil && !errors.Is(err, context.Canceled) {
		writeError(w, s.log, http.StatusInternalServerError, CodeInternal, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, bgLogsResponse{
		ExecID:     e.ID,
		Stream:     streamStr,
		Offset:     res.Offset,
		NextOffset: res.NextOffset,
		Bytes:      len(res.Data),
		Data:       string(res.Data),
		EOF:        res.EOF,
		State:      e.State(),
	})
}

type signalRequest struct {
	Signal string `json:"signal"`
}

func (s *Server) handleExecBgSignal(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	e, ok := s.mgr.Get(id)
	if !ok {
		writeError(w, s.log, http.StatusNotFound, CodeNotFound, "exec not found")
		return
	}
	var body signalRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, "malformed JSON body")
		return
	}
	sig, ok := allowedSignals[body.Signal]
	if !ok {
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest,
			"signal must be one of SIGINT/SIGTERM/SIGKILL/SIGHUP/SIGQUIT/SIGUSR1/SIGUSR2")
		return
	}
	if err := e.Signal(sig); err != nil {
		writeError(w, s.log, http.StatusInternalServerError, CodeInternal, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleExecBgDelete(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if err := s.mgr.Delete(id); err != nil {
		if errors.Is(err, execmgr.ErrNotFound) {
			writeError(w, s.log, http.StatusNotFound, CodeNotFound, "exec not found")
			return
		}
		writeError(w, s.log, http.StatusInternalServerError, CodeInternal, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeJSON emits a JSON body with the given status code. Encoding errors are silently dropped.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeErrorWithDetails mirrors writeError but includes a details map in the envelope.
func writeErrorWithDetails(w http.ResponseWriter, log interface{ Error(string, ...any) }, status int, code, message string, details map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	env := errorEnvelope{Error: errorBody{Code: code, Message: message, Details: details}}
	if err := json.NewEncoder(w).Encode(env); err != nil {
		log.Error("write error response", "err", err)
	}
}

func writeSSE(w http.ResponseWriter, flusher http.Flusher, event string, data any) {
	buf, _ := json.Marshal(data)
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, buf)
	flusher.Flush()
}

// readCapped returns (data, truncated). Reads up to max bytes; truncated is
// true if the file was longer than max. Thin adapter over execmgr.ReadAt so we
// have one source of truth for file-offset reads.
func readCapped(path string, max int64) ([]byte, bool) {
	data, size, err := execmgr.ReadAt(path, 0, max)
	if err != nil {
		return nil, false
	}
	return data, size > int64(len(data))
}

// readChunk returns (data, nextOffset, err) — the SSE handler wants cursor
// semantics, so we adapt ReadAt's (data, fileSize) shape.
func readChunk(path string, offset, max int64) ([]byte, int64, error) {
	data, _, err := execmgr.ReadAt(path, offset, max)
	return data, offset + int64(len(data)), err
}

func fileSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}

func parseInt64(s string, def int64) (int64, error) {
	if s == "" {
		return def, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
