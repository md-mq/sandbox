package server

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"

	"github.com/polyaxon/sandbox/internal/auth"
)

const (
	headerSandboxToken = "X-Polyaxon-Sandbox-Token"
	headerRequestID    = "X-Request-Id"
)

// recoverMiddleware catches panics, logs them with a stack trace,
// and returns a 500 with the standard error envelope.
func recoverMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("panic recovered",
						"panic", rec,
						"stack", string(debug.Stack()),
						"path", r.URL.Path,
						"method", r.Method,
					)
					writeError(w, log, http.StatusInternalServerError, CodeInternal, "internal server error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// statusRecorder captures the response status so loggingMiddleware can report it.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// loggingMiddleware logs each request as one structured line. /ping is skipped
// to avoid log spam from liveness probes.
func loggingMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/ping" {
				next.ServeHTTP(w, r)
				return
			}

			reqID, err := uuid.NewV7()
			if err != nil {
				// Extremely unlikely; fall back to v4 so a broken clock can't kill requests.
				reqID = uuid.New()
			}
			w.Header().Set(headerRequestID, reqID.String())

			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()
			next.ServeHTTP(rec, r)

			log.Info("request",
				"request_id", reqID.String(),
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		})
	}
}

// authMiddleware enforces the X-Polyaxon-Sandbox-Token header on every wrapped route.
// Handlers mounted outside this middleware (e.g. /ping) are unauthenticated.
func authMiddleware(a *auth.Authenticator, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := r.Header.Get(headerSandboxToken)
			if got == "" || !a.Check(got) {
				writeError(w, log, http.StatusUnauthorized, CodeUnauthorized,
					"invalid or missing X-Polyaxon-Sandbox-Token")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
