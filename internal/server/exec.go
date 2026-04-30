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
	Env       map[string]*string `json:"env"` // null value = unset
	Workdir   string             `json:"workdir"`
	Stdin     string             `json:"stdin"` // base64
	TimeoutMS int                `json:"timeout_ms"`
}

func (r *execRequest) toStartRequest() (execmgr.StartRequest, error) {
	if len(r.Command) == 0 || r.Command[0] == "" {
		return execmgr.StartRequest{}, fmt.Errorf("%w: command required", execmgr.ErrInvalidRequest)
	}
	for k := range r.Env {
		if strings.HasPrefix(k, "POLYAXON_") {
			return execmgr.StartRequest{}, fmt.Errorf("%w: %s", execmgr.ErrReservedEnvKey, k)
		}
	}
	if r.Workdir != "" {
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
		Env:       r.Env,
		Workdir:   r.Workdir,
		Stdin:     stdin,
		TimeoutMS: timeout,
	}, nil
}

// decodeExecRequest writes an error response and returns ok=false on failure.
func (s *Server) decodeExecRequest(w http.ResponseWriter, r *http.Request) (execmgr.StartRequest, bool) {
	var body execRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, "malformed JSON body")
		return execmgr.StartRequest{}, false
	}
	req, err := body.toStartRequest()
	if err != nil {
		switch {
		case errors.Is(err, execmgr.ErrReservedEnvKey):
			writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
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
		switch {
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
	req, ok := s.decodeExecRequest(w, r)
	if !ok {
		return
	}
	e := s.startAndMap(r.Context(), w, req)
	if e == nil {
		return
	}

	select {
	case <-e.Done():
	case <-r.Context().Done():
		// Caller left before the exec finished — best-effort kill.
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
	req, ok := s.decodeExecRequest(w, r)
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
	e := s.startAndMap(r.Context(), w, req)
	if e == nil {
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	writeSSE(w, flusher, "start", map[string]any{
		"exec_id":    e.ID,
		"pid":        0, // raw pid is not exposed over the wire
		"started_at": e.StartedAt.Format(time.RFC3339Nano),
	})

	ctx := r.Context()
	ping := time.NewTicker(3 * time.Second)
	defer ping.Stop()

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
		case <-time.After(100 * time.Millisecond):
		}
	}
}

type execBgResponse struct {
	ExecID    string `json:"exec_id"`
	PID       int    `json:"pid"`
	StartedAt string `json:"started_at"`
}

func (s *Server) handleExecBg(w http.ResponseWriter, r *http.Request) {
	req, ok := s.decodeExecRequest(w, r)
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
		PID:       0,
		StartedAt: e.StartedAt.Format(time.RFC3339Nano),
	})
}

type bgStatusResponse struct {
	ExecID      string `json:"exec_id"`
	State       string `json:"state"`
	StartedAt   string `json:"started_at"`
	FinishedAt  string `json:"finished_at,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"`
	ExitCode    *int   `json:"exit_code"`
	Signal      string `json:"signal,omitempty"`
	StdoutBytes int64  `json:"stdout_bytes"`
	StderrBytes int64  `json:"stderr_bytes"`
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
		State:       e.State(),
		StartedAt:   e.StartedAt.Format(time.RFC3339Nano),
		StdoutBytes: fileSize(e.StdoutPath()),
		StderrBytes: fileSize(e.StderrPath()),
	}
	if st := e.Status(); st != nil {
		resp.FinishedAt = st.FinishedAt.Format(time.RFC3339Nano)
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
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
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

// readCapped reads up to max bytes from path and reports whether the file was longer.
func readCapped(path string, max int64) ([]byte, bool) {
	f, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, false
	}
	if fi.Size() > max {
		buf := make([]byte, max)
		_, _ = io.ReadFull(f, buf)
		return buf, true
	}
	data, _ := io.ReadAll(f)
	return data, false
}

// readChunk reads up to max bytes starting at offset, returning the data and the next offset.
func readChunk(path string, offset, max int64) ([]byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, offset, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, offset, err
	}
	if offset >= fi.Size() {
		return nil, offset, nil
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, offset, err
	}
	toRead := fi.Size() - offset
	if toRead > max {
		toRead = max
	}
	buf := make([]byte, toRead)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, offset, err
	}
	return buf[:n], offset + int64(n), nil
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
