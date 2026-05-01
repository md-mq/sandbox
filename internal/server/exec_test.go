package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/polyaxon/sandbox/internal/config"
	"github.com/polyaxon/sandbox/internal/execmgr"
)

// runHTTPServer starts the given Server on a test listener.
// Returns the base URL and a cleanup.
func runHTTPServer(t *testing.T, s *Server) string {
	t.Helper()
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts.URL
}

func doJSON(t *testing.T, method, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(headerSandboxToken, testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	return resp
}

func decodeJSON(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatalf("decode: %v", err)
	}
}

func TestExec_SyncHappyPath(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	body := `{"command":["sh","-c","echo hi"],"timeout_ms":5000}`
	resp := doJSON(t, http.MethodPost, base+"/exec", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		ExecID   string `json:"exec_id"`
		ExitCode *int   `json:"exit_code"`
		Stdout   string `json:"stdout"`
	}
	decodeJSON(t, resp, &got)
	if got.ExecID == "" {
		t.Errorf("exec_id empty")
	}
	if got.ExitCode == nil || *got.ExitCode != 0 {
		t.Errorf("exit_code = %v, want 0", got.ExitCode)
	}
	if !strings.Contains(got.Stdout, "hi") {
		t.Errorf("stdout = %q", got.Stdout)
	}
}

func TestExec_RejectsPolyaxonPrefixedEnv(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	body := `{"command":["echo","hi"],"env":{"POLYAXON_FOO":"bar"}}`
	resp := doJSON(t, http.MethodPost, base+"/exec", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var env errorEnvelope
	decodeJSON(t, resp, &env)
	if env.Error.Code != CodeInvalidRequest {
		t.Errorf("code = %q, want invalid_request", env.Error.Code)
	}
}

func TestExec_InvalidWorkdir(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	body := `{"command":["echo","hi"],"workdir":"/this/path/does/not/exist"}`
	resp := doJSON(t, http.MethodPost, base+"/exec", body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestExec_StreamEmitsEvents(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	body := `{"command":["sh","-c","printf one; printf two"],"timeout_ms":5000}`
	req, _ := http.NewRequest(http.MethodPost, base+"/exec/stream", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerSandboxToken, testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q, want text/event-stream*", ct)
	}

	events := parseSSE(t, resp)
	saw := map[string]bool{}
	for _, ev := range events {
		saw[ev.event] = true
	}
	for _, want := range []string{"start", "execution_complete"} {
		if !saw[want] {
			t.Errorf("missing %s event; got %v", want, events)
		}
	}
	// There should be at least one stdout event containing some of the output.
	foundStdout := false
	for _, ev := range events {
		if ev.event == "stdout" && strings.Contains(ev.data, "one") {
			foundStdout = true
		}
	}
	if !foundStdout {
		t.Errorf("no stdout event with expected text; got %v", events)
	}
}

func TestExec_StreamDisconnectDoesNotKillChild(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	dir := t.TempDir()
	marker := filepath.Join(dir, "finished")

	body := fmt.Sprintf(
		`{"command":["sh","-c","printf started; sleep 0.3; : > finished"],"workdir":%q,"timeout_ms":5000}`,
		dir,
	)
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/exec/stream", strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequestWithContext: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerSandboxToken, testToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	cancel()
	resp.Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("marker file was not written; stream disconnect likely killed child")
}

func TestExec_BodyTooLarge(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	body := `{"command":["true"],"stdin":"` + strings.Repeat("A", maxExecBodyBytes) + `"}`
	resp := doJSON(t, http.MethodPost, base+"/exec", body)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
	var env errorEnvelope
	decodeJSON(t, resp, &env)
	if env.Error.Code != CodePayloadTooLarge {
		t.Errorf("error.code = %q, want %q", env.Error.Code, CodePayloadTooLarge)
	}
}

func TestExec_TrailingJSONRejected(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	resp := doJSON(t, http.MethodPost, base+"/exec", `{"command":["true"]}{}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	var env errorEnvelope
	decodeJSON(t, resp, &env)
	if env.Error.Code != CodeInvalidRequest {
		t.Errorf("error.code = %q, want %q", env.Error.Code, CodeInvalidRequest)
	}
}

func TestExec_PerFieldCaps(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	env := make([]string, 0, maxEnvKeys+1)
	for i := 0; i < maxEnvKeys+1; i++ {
		env = append(env, fmt.Sprintf("%q:%q", fmt.Sprintf("K%d", i), "v"))
	}
	args := make([]string, 0, maxCommandElements+1)
	for i := 0; i < maxCommandElements+1; i++ {
		args = append(args, `"x"`)
	}

	tests := []struct {
		name string
		body string
	}{
		{
			name: "too many command elements",
			body: `{"command":[` + strings.Join(args, ",") + `]}`,
		},
		{
			name: "too many env keys",
			body: `{"command":["true"],"env":{` + strings.Join(env, ",") + `}}`,
		},
		{
			name: "oversized stdin decoded",
			body: `{"command":["true"],"stdin":"` + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte("x"), maxStdinBytes+1)) + `"}`,
		},
		{
			name: "invalid env key",
			body: `{"command":["true"],"env":{"BAD=KEY":"v"}}`,
		},
		{
			name: "command arg nul",
			body: `{"command":["sh","-c","printf \u0000"]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doJSON(t, http.MethodPost, base+"/exec", tc.body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			resp.Body.Close()
		})
	}
}

func TestExec_BgLifecycle(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	body := `{"command":["sh","-c","printf hi; sleep 0.1"]}`
	resp := doJSON(t, http.MethodPost, base+"/exec/bg", body)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("bg status = %d, want 202", resp.StatusCode)
	}
	var start struct {
		ExecID string `json:"exec_id"`
	}
	decodeJSON(t, resp, &start)

	// Poll status until it transitions to exited.
	var statusResp bgStatusResponse
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r := doJSON(t, http.MethodGet, base+"/exec/bg/"+start.ExecID, "")
		decodeJSON(t, r, &statusResp)
		if statusResp.State == execmgr.StateExited {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if statusResp.State != execmgr.StateExited {
		t.Fatalf("state = %q, want exited", statusResp.State)
	}
	if statusResp.ExitCode == nil || *statusResp.ExitCode != 0 {
		t.Errorf("exit_code = %v", statusResp.ExitCode)
	}

	// Fetch the logs.
	r := doJSON(t, http.MethodGet,
		base+"/exec/bg/"+start.ExecID+"/logs?stream=stdout&offset=0&max_bytes=1024", "")
	var logs bgLogsResponse
	decodeJSON(t, r, &logs)
	if !strings.Contains(logs.Data, "hi") {
		t.Errorf("logs data = %q, want to contain 'hi'", logs.Data)
	}
	if !logs.EOF {
		t.Errorf("EOF = false, want true after process exit")
	}
}

func TestExec_BgDeleteRunning(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	resp := doJSON(t, http.MethodPost, base+"/exec/bg",
		`{"command":["sleep","30"],"timeout_ms":60000}`)
	var start struct {
		ExecID string `json:"exec_id"`
	}
	decodeJSON(t, resp, &start)

	r := doJSON(t, http.MethodDelete, base+"/exec/bg/"+start.ExecID, "")
	if r.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", r.StatusCode)
	}
	r.Body.Close()

	// Exec should be gone from the map.
	r = doJSON(t, http.MethodGet, base+"/exec/bg/"+start.ExecID, "")
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("post-delete status = %d, want 404", r.StatusCode)
	}
	r.Body.Close()
}

func TestExec_BgSignalInvalid(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	resp := doJSON(t, http.MethodPost, base+"/exec/bg",
		`{"command":["sleep","5"],"timeout_ms":10000}`)
	var start struct {
		ExecID string `json:"exec_id"`
	}
	decodeJSON(t, resp, &start)
	t.Cleanup(func() {
		doJSON(t, http.MethodDelete, base+"/exec/bg/"+start.ExecID, "").Body.Close()
	})

	r := doJSON(t, http.MethodPost, base+"/exec/bg/"+start.ExecID+"/signal",
		`{"signal":"SIGFOO"}`)
	if r.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", r.StatusCode)
	}
	r.Body.Close()

	del := doJSON(t, http.MethodDelete, base+"/exec/bg/"+start.ExecID, "")
	if del.StatusCode != http.StatusNoContent {
		t.Errorf("cleanup DELETE status = %d, want 204", del.StatusCode)
	}
	del.Body.Close()
}

func TestExec_CapExceeded(t *testing.T) {
	// newTestServer sets MaxExecs=4. Saturate with long-running sleeps, then verify 429.
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	var ids []string
	for i := 0; i < 4; i++ {
		resp := doJSON(t, http.MethodPost, base+"/exec/bg",
			`{"command":["sleep","30"],"timeout_ms":60000}`)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("saturate #%d: status = %d", i, resp.StatusCode)
		}
		var start struct {
			ExecID string `json:"exec_id"`
		}
		decodeJSON(t, resp, &start)
		ids = append(ids, start.ExecID)
	}
	t.Cleanup(func() {
		for _, id := range ids {
			doJSON(t, http.MethodDelete, base+"/exec/bg/"+id, "").Body.Close()
		}
	})

	resp := doJSON(t, http.MethodPost, base+"/exec/bg",
		`{"command":["true"]}`)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	// Retry-After header must be present so HTTP clients honoring it can back off.
	if ra := resp.Header.Get("Retry-After"); ra == "" {
		t.Errorf("Retry-After header missing on 429")
	}
	// Body should carry a machine-readable hint too.
	var env errorEnvelope
	decodeJSON(t, resp, &env)
	if env.Error.Code != "rate_limited" {
		t.Errorf("error.code = %q, want rate_limited", env.Error.Code)
	}
	if hint, ok := env.Error.Details["retry_after_ms"]; !ok || hint == nil {
		t.Errorf("error.details.retry_after_ms missing: %v", env.Error.Details)
	}
}

func TestExec_PingReflectsRunningCount(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	// Kick off two long sleeps.
	for i := 0; i < 2; i++ {
		resp := doJSON(t, http.MethodPost, base+"/exec/bg",
			`{"command":["sleep","30"],"timeout_ms":60000}`)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("bg status = %d", resp.StatusCode)
		}
		resp.Body.Close()
	}

	r := doJSON(t, http.MethodGet, base+"/ping", "")
	var p pingResponse
	decodeJSON(t, r, &p)
	if p.ExecsRunning != 2 {
		t.Errorf("execs_running = %d, want 2", p.ExecsRunning)
	}
}

// TestExec_RestartRecovery verifies a terminal bg exec remains visible via
// HTTP after plx-exec restarts. Starts a bg exec, waits for it to exit, tears
// down the server, spins a fresh Server against the same state dir, and asserts
// the exec is still queryable with its final status.
func TestExec_RestartRecovery(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte(testToken), 0o400); err != nil {
		t.Fatalf("write token: %v", err)
	}
	stateDir := filepath.Join(dir, "state")

	newSrv := func() *Server {
		cfg := &config.Config{
			ListenAddr: ":0",
			LogFormat:  "json",
			TokenFile:  tokenPath,
			StateDir:   stateDir,
			MaxExecs:   4,
		}
		log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
		s, err := New(cfg, log, "test")
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return s
	}

	// First server instance: kick off a quick bg exec and wait for it to exit.
	s1 := newSrv()
	ts1 := httptest.NewServer(s1.Handler())
	resp := doJSON(t, http.MethodPost, ts1.URL+"/exec/bg",
		`{"command":["sh","-c","printf persistent-output"]}`)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("bg start status = %d", resp.StatusCode)
	}
	var start struct {
		ExecID string `json:"exec_id"`
	}
	decodeJSON(t, resp, &start)

	// Wait for the exec to exit so status.json is on disk.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r := doJSON(t, http.MethodGet, ts1.URL+"/exec/bg/"+start.ExecID, "")
		var st bgStatusResponse
		decodeJSON(t, r, &st)
		if st.State == execmgr.StateExited {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Tear down the first server, wait for shutdown to complete.
	ts1.Close()
	_ = s1.Shutdown(context.Background())

	// Second server instance: same state dir, fresh manager. Recovery should
	// pick the terminal record up.
	s2 := newSrv()
	ts2 := httptest.NewServer(s2.Handler())
	t.Cleanup(ts2.Close)

	r := doJSON(t, http.MethodGet, ts2.URL+"/exec/bg/"+start.ExecID, "")
	if r.StatusCode != http.StatusOK {
		t.Fatalf("status after restart = %d, want 200", r.StatusCode)
	}
	var st bgStatusResponse
	decodeJSON(t, r, &st)
	if st.State != execmgr.StateExited {
		t.Errorf("state after restart = %q, want exited", st.State)
	}
	if st.ExitCode == nil || *st.ExitCode != 0 {
		t.Errorf("exit_code after restart = %v, want 0", st.ExitCode)
	}

	// Logs must still be readable — same offset semantics.
	r = doJSON(t, http.MethodGet,
		ts2.URL+"/exec/bg/"+start.ExecID+"/logs?stream=stdout&offset=0&max_bytes=1024", "")
	var logs bgLogsResponse
	decodeJSON(t, r, &logs)
	if !strings.Contains(logs.Data, "persistent-output") {
		t.Errorf("logs data = %q, want to contain persistent-output", logs.Data)
	}
}

func TestExec_CombinedStreamRejected(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	resp := doJSON(t, http.MethodPost, base+"/exec/bg",
		`{"command":["true"]}`)
	var start struct {
		ExecID string `json:"exec_id"`
	}
	decodeJSON(t, resp, &start)

	r := doJSON(t, http.MethodGet,
		base+"/exec/bg/"+start.ExecID+"/logs?stream=combined", "")
	if r.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", r.StatusCode)
	}
	r.Body.Close()
}

// sseEvent is a parsed SSE record.
type sseEvent struct {
	event string
	data  string
}

func parseSSE(t *testing.T, resp *http.Response) []sseEvent {
	t.Helper()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read SSE body: %v", err)
	}
	var events []sseEvent
	scanner := bufio.NewScanner(&buf)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)

	var cur sseEvent
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if cur.event != "" {
				events = append(events, cur)
			}
			cur = sseEvent{}
			continue
		}
		switch {
		case strings.HasPrefix(line, "event: "):
			cur.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			cur.data = strings.TrimPrefix(line, "data: ")
		}
	}
	if cur.event != "" {
		events = append(events, cur)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan SSE: %v", err)
	}
	return events
}
