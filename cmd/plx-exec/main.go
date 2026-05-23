package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/polyaxon/sandbox/internal/config"
	"github.com/polyaxon/sandbox/internal/server"
)

// version is overridden at build time via:
//
//	go build -ldflags "-X main.version=<sha>"
var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "plx-exec:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := newLogger(cfg.LogFormat).With("component", "plx-exec", "version", version)
	slog.SetDefault(log)

	s, err := server.New(cfg, log, version)
	if err != nil {
		return fmt.Errorf("build server: %w", err)
	}

	// Serve in a goroutine; block on signal in the main goroutine.
	errCh := make(chan error, 1)
	go func() { errCh <- s.Start() }()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("server exited: %w", err)
		}
	case sig := <-sigCh:
		log.Info("signal received, shutting down", "signal", sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
	}
	return nil
}

func newLogger(format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	var h slog.Handler
	if format == "text" {
		h = slog.NewTextHandler(os.Stderr, opts)
	} else {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.New(h)
}
