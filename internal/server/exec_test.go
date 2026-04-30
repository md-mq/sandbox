package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	resp.Body.Close()
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

