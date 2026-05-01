package server

import (
	"bufio"
	"errors"
	"log/slog"
	"net"
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

// recoverMiddleware logs panics with a stack trace and returns a 500 envelope.
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

// statusRecorder captures the response status for logging and forwards Flush
// so SSE handlers still drain through the middleware chain.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}

func (s *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := s.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	return h.Hijack()
}

func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// loggingMiddleware emits one structured log line per request, skipping /ping.
func loggingMiddleware(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/ping" {
				next.ServeHTTP(w, r)
				return
			}

			reqID, err := uuid.NewV7()
			if err != nil {
				// Fall back so a broken clock can't kill requests.
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

// authMiddleware enforces X-Polyaxon-Sandbox-Token on wrapped routes. /ping is
// mounted outside the group so probes don't need a token.
func authMiddleware(a *auth.Authenticator, log *slog.Logger, counters *Counters) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got := r.Header.Get(headerSandboxToken)
			if got == "" || !a.Check(got) {
				writeError(w, log, http.StatusUnauthorized, CodeUnauthorized,
					"invalid or missing X-Polyaxon-Sandbox-Token")
				return
			}
			counters.Touch(time.Now())
			next.ServeHTTP(w, r)
		})
	}
}
