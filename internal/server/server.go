package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/polyaxon/sandbox/internal/auth"
	"github.com/polyaxon/sandbox/internal/config"
	"github.com/polyaxon/sandbox/internal/execmgr"
)

type Server struct {
	cfg      *config.Config
	log      *slog.Logger
	auth     *auth.Authenticator
	router   *chi.Mux
	http     *http.Server
	start    time.Time
	version  string
	counters *Counters
	mgr      *execmgr.Manager
}

// New loads the auth token and builds the exec manager. In PingOnly mode both
// are skipped, which is useful for smoke tests and local dev.
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

		mgr, err := execmgr.NewManager(cfg.StateDir, cfg.MaxExecs, &s.counters.ExecsRunning, log)
		if err != nil {
			return nil, fmt.Errorf("build exec manager: %w", err)
		}
		if err := mgr.Recover(context.Background()); err != nil {
			return nil, fmt.Errorf("recover execs: %w", err)
		}
		s.mgr = mgr
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

	// /ping is unauthenticated for liveness probes.
	r.Get("/ping", s.handlePing)

	// The catch-all inside the auth group ensures unknown paths 401 rather than
	// 404 when no token is presented.
	r.Group(func(r chi.Router) {
		if s.auth != nil {
			r.Use(authMiddleware(s.auth, s.log))
		} else {
			r.Use(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					writeError(w, s.log, http.StatusUnauthorized, CodeUnauthorized,
						"server running in ping-only mode")
				})
			})
		}

		if s.mgr != nil {
			r.Post("/exec", s.handleExec)
			r.Post("/exec/stream", s.handleExecStream)
			r.Post("/exec/bg", s.handleExecBg)
			r.Get("/exec/bg/{id}", s.handleExecBgStatus)
			r.Get("/exec/bg/{id}/logs", s.handleExecBgLogs)
			r.Post("/exec/bg/{id}/signal", s.handleExecBgSignal)
			r.Delete("/exec/bg/{id}", s.handleExecBgDelete)
		}

		r.Handle("/*", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			writeError(w, s.log, http.StatusNotFound, CodeNotFound, "route not found")
		}))
	})

	return r
}

// Start returns nil on graceful shutdown, or the underlying error otherwise.
func (s *Server) Start() error {
	s.log.Info("plx-exec starting",
		"addr", s.cfg.ListenAddr,
		"ping_only", s.cfg.PingOnly,
	)
	err := s.http.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Info("plx-exec shutting down")
	if s.mgr != nil {
		s.mgr.Shutdown(ctx)
	}
	return s.http.Shutdown(ctx)
}

// Handler is exported for httptest-based tests.
func (s *Server) Handler() http.Handler {
	return s.router
}
