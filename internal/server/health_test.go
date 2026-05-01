package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/polyaxon/sandbox/internal/config"
)

const testToken = "test-token-abcd"

func newTestServer(t *testing.T, pingOnly bool) *Server {
	t.Helper()

	cfg := &config.Config{
		ListenAddr: ":0",
		LogFormat:  "json",
		PingOnly:   pingOnly,
		MaxExecs:   4,
	}
	if !pingOnly {
		dir := t.TempDir()
		path := filepath.Join(dir, "token")
		if err := os.WriteFile(path, []byte(testToken), 0o400); err != nil {
			t.Fatalf("write token: %v", err)
		}
		cfg.TokenFile = path
		cfg.StateDir = filepath.Join(dir, "state")
	}

	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	s, err := New(cfg, log, "test")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestPing_NoAuthRequired(t *testing.T) {
	s := newTestServer(t, false)

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var body pingResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "ok" {
		t.Errorf("Status = %q, want ok", body.Status)
	}
	if body.Version != "test" {
		t.Errorf("Version = %q, want test", body.Version)
	}
	if body.UptimeMS < 0 {
		t.Errorf("UptimeMS = %d, want >= 0", body.UptimeMS)
	}
	if body.LastActivity == "" {
		t.Errorf("LastActivity is empty")
	}
	if body.PTYsRunning != 0 {
		t.Errorf("PTYsRunning = %d, want 0", body.PTYsRunning)
	}
}

func TestAuth_MissingTokenReturns401(t *testing.T) {
	s := newTestServer(t, false)

	req := httptest.NewRequest(http.MethodGet, "/exec", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	assertErrorCode(t, rec.Body.Bytes(), CodeUnauthorized)
}

func TestAuth_WrongTokenReturns401(t *testing.T) {
	s := newTestServer(t, false)

	req := httptest.NewRequest(http.MethodGet, "/exec", nil)
	req.Header.Set(headerSandboxToken, "wrong-token")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	assertErrorCode(t, rec.Body.Bytes(), CodeUnauthorized)
}

func TestAuth_TouchOnlyAfterValidToken(t *testing.T) {
	s := newTestServer(t, false)

	req := httptest.NewRequest(http.MethodGet, "/exec", nil)
	req.Header.Set(headerSandboxToken, "wrong-token")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong-token status = %d, want 401", rec.Code)
	}
	if !s.counters.LastActivity().IsZero() {
		t.Fatal("wrong token should not update last activity")
	}

	req = httptest.NewRequest(http.MethodGet, "/nope", nil)
	req.Header.Set(headerSandboxToken, testToken)
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("valid-token status = %d, want 404", rec.Code)
	}
	if s.counters.LastActivity().IsZero() {
		t.Fatal("valid authenticated request should update last activity")
	}
}

func TestAuth_CorrectTokenFallsThroughTo404(t *testing.T) {
	s := newTestServer(t, false)

	// Use an unknown path so auth passes and the catch-all returns 404
	// (avoids coupling this test to whichever real routes happen to be registered).
	req := httptest.NewRequest(http.MethodGet, "/nope", nil)
	req.Header.Set(headerSandboxToken, testToken)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (auth should have passed)", rec.Code)
	}
}

func TestPingOnly_RejectsRoutes_UsesPath(t *testing.T) {
	// sanity: keep the existing PingOnly coverage on a non-real path too
	s := newTestServer(t, true)

	req := httptest.NewRequest(http.MethodGet, "/nope", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 in PingOnly mode", rec.Code)
	}
}

func TestPingOnly_RejectsOtherRoutes(t *testing.T) {
	s := newTestServer(t, true)

	req := httptest.NewRequest(http.MethodGet, "/exec", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 in PingOnly mode", rec.Code)
	}
}

func TestPingOnly_PingStillWorks(t *testing.T) {
	s := newTestServer(t, true)

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 in PingOnly mode", rec.Code)
	}
}

func TestRequestIDHeaderSet(t *testing.T) {
	s := newTestServer(t, false)

	req := httptest.NewRequest(http.MethodGet, "/exec", nil)
	req.Header.Set(headerSandboxToken, testToken)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)

	if id := rec.Header().Get(headerRequestID); id == "" {
		t.Error("expected X-Request-Id header to be set by logging middleware")
	}
}

func assertErrorCode(t *testing.T, body []byte, wantCode string) {
	t.Helper()
	var env errorEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatalf("decode error envelope: %v — body=%s", err, string(body))
	}
	if env.Error.Code != wantCode {
		t.Errorf("error.code = %q, want %q", env.Error.Code, wantCode)
	}
}
