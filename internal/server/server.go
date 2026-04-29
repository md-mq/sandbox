package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/polyaxon/sandbox/internal/auth"
	"github.com/polyaxon/sandbox/internal/config"
)

// Server wraps the HTTP server, auth, and runtime counters for plx-exec.
type Server struct {
	cfg      *config.Config
	log      *slog.Logger
	auth     *auth.Authenticator
	router   *chi.Mux
	http     *http.Server
	start    time.Time
	version  string
	counters *Counters
}

// New constructs a Server from config. It loads the auth token unless PingOnly
// is set (useful for smoke tests and local dev).
func New(cfg *config.Config, log *slog.Logger, version string) (*Server, error) {
	s := &Server{
		cfg:      cfg,
		log:      log,
		start:    time.Now().UTC(),
		version:  version,
		counters: &Counters{},
	}

	if !cfg.PingOnly {
		a, err := auth.LoadFromFile(cfg.TokenFile)
		if err != nil {
			return nil, err
		}
		s.auth = a
	}

	s.router = s.buildRouter()
	s.http = &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           s.router,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s, nil
}

func (s *Server) buildRouter() *chi.Mux {
	r := chi.NewRouter()
	r.Use(recoverMiddleware(s.log))
	r.Use(loggingMiddleware(s.log))

	// /ping is unauthenticated: k8s liveness probes, sidecar pings, smoke tests.
	r.Get("/ping", s.handlePing)

	// Everything else sits behind auth. Individual endpoints land in later phases.
	r.Group(func(r chi.Router) {
		if s.auth != nil {
			r.Use(authMiddleware(s.auth, s.log))
		} else {
			// PingOnly mode: any non-/ping route returns 401 so callers see a
			// consistent signal regardless of daemon configuration.
			r.Use(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					writeError(w, s.log, http.StatusUnauthorized, CodeUnauthorized,
						"server running in ping-only mode")
				})
			})
		}
		// Phase 2B–2D will attach /exec, /pty, /fs handlers here.
	})

	return r
}

// Start runs the HTTP server until Shutdown is called. It returns nil on a
// graceful shutdown and the underlying error otherwise.
func (s *Server) Start() error {
	s.log.Info("plx-exec starting",
		"addr", s.cfg.ListenAddr,
		"version", s.version,
		"ping_only", s.cfg.PingOnly,
	)
	err := s.http.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully stops the HTTP server, respecting the given context's deadline.
func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Info("plx-exec shutting down")
	return s.http.Shutdown(ctx)
}

// Handler exposes the router for use in tests via httptest.
func (s *Server) Handler() http.Handler {
	return s.router
}
