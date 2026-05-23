package server

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/polyaxon/sandbox/internal/ptymgr"
)

func TestPTY_CreateStatusListDelete(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	resp := doJSON(t, http.MethodPost, base+"/pty",
		`{"command":["sleep","30"],"cols":100,"rows":40,"tag":"shell"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); !strings.HasPrefix(loc, "/pty/") {
		t.Fatalf("Location = %q, want /pty/{id}", loc)
	}
	var created ptyCreateResponse
	decodeJSON(t, resp, &created)
	if created.PTYID == "" || created.PID == 0 {
		t.Fatalf("created = %#v", created)
	}
	if created.Cols != 100 || created.Rows != 40 || created.Tag != "shell" {
		t.Fatalf("created size/tag = %#v", created)
	}

	status := getPTYStatus(t, base, created.PTYID)
	if status.State != ptymgr.StateRunning || status.Tag != "shell" {
		t.Fatalf("status = %#v", status)
	}
	if status.Cols != 100 || status.Rows != 40 {
		t.Fatalf("status size = %dx%d, want 100x40", status.Cols, status.Rows)
	}

	list := getPTYList(t, base, "")
	if len(list.Sessions) != 1 || list.Sessions[0].PTYID != created.PTYID {
		t.Fatalf("list = %#v", list)
	}
	filtered := getPTYList(t, base, "?tag=shell")
	if len(filtered.Sessions) != 1 || filtered.Sessions[0].PTYID != created.PTYID {
		t.Fatalf("filtered list = %#v", filtered)
	}
	filtered = getPTYList(t, base, "?tag=missing")
	if len(filtered.Sessions) != 0 {
		t.Fatalf("missing tag list = %#v, want empty", filtered)
	}

	del := doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "")
	if del.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", del.StatusCode)
	}
	_ = del.Body.Close()

	r := doJSON(t, http.MethodGet, base+"/pty/"+created.PTYID, "")
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("status after delete = %d, want 404", r.StatusCode)
	}
	_ = r.Body.Close()
}

func TestPTY_DefaultsAndPingCounter(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	resp := doJSON(t, http.MethodPost, base+"/pty", `{}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", resp.StatusCode)
	}
	var created ptyCreateResponse
	decodeJSON(t, resp, &created)
	t.Cleanup(func() {
		_ = doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
	})
	if created.Cols != 80 || created.Rows != 24 {
		t.Fatalf("default size = %dx%d, want 80x24", created.Cols, created.Rows)
	}

	r := doJSON(t, http.MethodGet, base+"/ping", "")
	var p pingResponse
	decodeJSON(t, r, &p)
	if p.PTYsRunning != 1 {
		t.Fatalf("ptys_running = %d, want 1", p.PTYsRunning)
	}
}

func TestPTY_NaturalExitRetainedAndTagConflict(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	resp := doJSON(t, http.MethodPost, base+"/pty",
		`{"command":["sh","-c","exit 7"],"tag":"once"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", resp.StatusCode)
	}
	var created ptyCreateResponse
	decodeJSON(t, resp, &created)

	status := waitPTYState(t, base, created.PTYID)
	if status.ExitCode == nil || *status.ExitCode != 7 {
		t.Fatalf("exit_code = %v, want 7", status.ExitCode)
	}

	resp = doJSON(t, http.MethodPost, base+"/pty",
		`{"command":["sleep","30"],"tag":"once"}`)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate tag status = %d, want 409", resp.StatusCode)
	}
	var env errorEnvelope
	decodeJSON(t, resp, &env)
	if env.Error.Code != CodeConflict {
		t.Fatalf("code = %q, want conflict", env.Error.Code)
	}
	if env.Error.Details["state"] != ptymgr.StateExited || env.Error.Details["existing_id"] != created.PTYID {
		t.Fatalf("details = %#v", env.Error.Details)
	}

	del := doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "")
	if del.StatusCode != http.StatusNoContent {
		t.Fatalf("delete exited status = %d, want 204", del.StatusCode)
	}
	_ = del.Body.Close()

	resp = doJSON(t, http.MethodPost, base+"/pty",
		`{"command":["sleep","30"],"tag":"once"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("tag reuse status = %d, want 201", resp.StatusCode)
	}
	decodeJSON(t, resp, &created)
	_ = doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
}

func TestPTY_CapExceeded(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	var ids []string
	for i := 0; i < 4; i++ {
		resp := doJSON(t, http.MethodPost, base+"/pty", `{"command":["sleep","30"]}`)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("saturate #%d status = %d, want 201", i, resp.StatusCode)
		}
		var created ptyCreateResponse
		decodeJSON(t, resp, &created)
		ids = append(ids, created.PTYID)
	}
	t.Cleanup(func() {
		for _, id := range ids {
			_ = doJSON(t, http.MethodDelete, base+"/pty/"+id, "").Body.Close()
		}
	})

	resp := doJSON(t, http.MethodPost, base+"/pty", `{"command":["true"]}`)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if ra := resp.Header.Get("Retry-After"); ra == "" {
		t.Fatal("Retry-After header missing")
	}
	var env errorEnvelope
	decodeJSON(t, resp, &env)
	if env.Error.Code != "rate_limited" {
		t.Fatalf("code = %q, want rate_limited", env.Error.Code)
	}
}

func TestPTY_Resize(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	created := createPTY(t, base, `{"command":["sleep","30"]}`)
	t.Cleanup(func() {
		_ = doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
	})

	resp := doJSON(t, http.MethodPost, base+"/pty/"+created.PTYID+"/resize",
		`{"cols":120,"rows":50}`)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("resize status = %d, want 204", resp.StatusCode)
	}
	_ = resp.Body.Close()

	status := getPTYStatus(t, base, created.PTYID)
	if status.Cols != 120 || status.Rows != 50 {
		t.Fatalf("status size = %dx%d, want 120x50", status.Cols, status.Rows)
	}
}

func TestPTY_SignalTERM(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	dir := t.TempDir()

	body := `{"command":["sh","-c","trap 'printf term > term.txt; exit 0' TERM; printf ready > ready.txt; while :; do sleep 1; done"],"workdir":` + strconv.Quote(dir) + `}`
	created := createPTY(t, base, body)
	t.Cleanup(func() {
		_ = doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
	})
	waitForFileContent(t, filepath.Join(dir, "ready.txt"), "ready", 3*time.Second)

	resp := doJSON(t, http.MethodPost, base+"/pty/"+created.PTYID+"/signal",
		`{"signal":"SIGTERM"}`)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("signal status = %d, want 204", resp.StatusCode)
	}
	_ = resp.Body.Close()

	waitForFileContent(t, filepath.Join(dir, "term.txt"), "term", 3*time.Second)
	waitPTYState(t, base, created.PTYID)
}

func TestPTY_ExitedResizeAndSignalReturnGone(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	created := createPTY(t, base, `{"command":["sh","-c","exit 0"]}`)
	waitPTYState(t, base, created.PTYID)
	t.Cleanup(func() {
		_ = doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
	})

	resp := doJSON(t, http.MethodPost, base+"/pty/"+created.PTYID+"/resize",
		`{"cols":80,"rows":24}`)
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("resize exited status = %d, want 410", resp.StatusCode)
	}
	_ = resp.Body.Close()

	resp = doJSON(t, http.MethodPost, base+"/pty/"+created.PTYID+"/signal",
		`{"signal":"SIGTERM"}`)
	if resp.StatusCode != http.StatusGone {
		t.Fatalf("signal exited status = %d, want 410", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func TestPTY_ValidationFailures(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)

	longBody := `{"` + strings.Repeat("x", maxPTYBodyBytes+1) + `":true}`
	tests := []struct {
		name     string
		body     string
		want     int
		wantCode string
	}{
		{name: "bad tag", body: `{"tag":"bad/tag"}`, want: http.StatusBadRequest},
		{name: "bad workdir", body: `{"workdir":"relative"}`, want: http.StatusBadRequest},
		{name: "reserved env", body: `{"env":{"POLYAXON_FOO":"bar"}}`, want: http.StatusBadRequest, wantCode: CodeReservedEnvKey},
		{name: "cols zero", body: `{"cols":0}`, want: http.StatusBadRequest},
		{name: "rows too large", body: `{"rows":1001}`, want: http.StatusBadRequest},
		{name: "trailing json", body: `{} {}`, want: http.StatusBadRequest},
		{name: "body too large", body: longBody, want: http.StatusRequestEntityTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := doJSON(t, http.MethodPost, base+"/pty", tt.body)
			if resp.StatusCode != tt.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.want)
			}
			if tt.wantCode != "" {
				var env errorEnvelope
				decodeJSON(t, resp, &env)
				if env.Error.Code != tt.wantCode {
					t.Fatalf("code = %q, want %q", env.Error.Code, tt.wantCode)
				}
				return
			}
			_ = resp.Body.Close()
		})
	}

	created := createPTY(t, base, `{"command":["sleep","30"]}`)
	t.Cleanup(func() {
		_ = doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
	})
	resp := doJSON(t, http.MethodPost, base+"/pty/"+created.PTYID+"/signal",
		`{"signal":"SIGWINCH"}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid signal status = %d, want 400", resp.StatusCode)
	}
	_ = resp.Body.Close()

	resp = doJSON(t, http.MethodGet, base+"/pty?tag=bad/tag", "")
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad tag filter status = %d, want 400", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

func createPTY(t *testing.T, base, body string) ptyCreateResponse {
	t.Helper()
	resp := doJSON(t, http.MethodPost, base+"/pty", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", resp.StatusCode)
	}
	var created ptyCreateResponse
	decodeJSON(t, resp, &created)
	return created
}

func getPTYStatus(t *testing.T, base, id string) ptyStatusResponse {
	t.Helper()
	resp := doJSON(t, http.MethodGet, base+"/pty/"+id, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var status ptyStatusResponse
	decodeJSON(t, resp, &status)
	return status
}

func getPTYList(t *testing.T, base, query string) ptyListResponse {
	t.Helper()
	resp := doJSON(t, http.MethodGet, base+"/pty"+query, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want 200", resp.StatusCode)
	}
	var list ptyListResponse
	decodeJSON(t, resp, &list)
	return list
}

func waitPTYState(t *testing.T, base, id string) ptyStatusResponse {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var last ptyStatusResponse
	for time.Now().Before(deadline) {
		resp := doJSON(t, http.MethodGet, base+"/pty/"+id, "")
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			time.Sleep(20 * time.Millisecond)
			continue
		}
		decodeJSON(t, resp, &last)
		if last.State == ptymgr.StateExited {
			return last
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("pty %s state = %q, want %q", id, last.State, ptymgr.StateExited)
	return last
}

func waitForFileContent(t *testing.T, path, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil && string(raw) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	raw, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read %s: %v", path, err)
	}
	t.Fatalf("%s = %q, want %q", path, string(raw), want)
}
