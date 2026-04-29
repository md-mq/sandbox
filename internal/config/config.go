package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	envListenAddr      = "POLYAXON_SANDBOX_LISTEN_ADDR"
	envTokenFile       = "POLYAXON_SANDBOX_TOKEN_FILE"
	envStateDir        = "POLYAXON_SANDBOX_STATE_DIR"
	envLogFormat       = "POLYAXON_SANDBOX_LOG_FORMAT"
	envShutdownTimeout = "POLYAXON_SANDBOX_SHUTDOWN_TIMEOUT"
	envPingOnly        = "POLYAXON_SANDBOX_PING_ONLY"

	defaultListenAddr      = ":9090"
	defaultTokenFile       = "/opt/polyaxon/sandbox-token"
	defaultStateDir        = "/tmp/plx-exec"
	defaultLogFormat       = "json"
	defaultShutdownTimeout = 10 * time.Second
)

type Config struct {
	ListenAddr      string
	TokenFile       string
	StateDir        string
	LogFormat       string
	ShutdownTimeout time.Duration
	PingOnly        bool
}

func Load() (*Config, error) {
	cfg := &Config{
		ListenAddr:      envOr(envListenAddr, defaultListenAddr),
		TokenFile:       envOr(envTokenFile, defaultTokenFile),
		StateDir:        envOr(envStateDir, defaultStateDir),
		LogFormat:       envOr(envLogFormat, defaultLogFormat),
		ShutdownTimeout: defaultShutdownTimeout,
		PingOnly:        envBool(envPingOnly),
	}

	if raw := os.Getenv(envShutdownTimeout); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", envShutdownTimeout, err)
		}
		cfg.ShutdownTimeout = d
	}

	if cfg.LogFormat != "json" && cfg.LogFormat != "text" {
		return nil, fmt.Errorf("%s: must be 'json' or 'text', got %q", envLogFormat, cfg.LogFormat)
	}

	if !cfg.PingOnly {
		if _, err := os.Stat(cfg.TokenFile); err != nil {
			return nil, fmt.Errorf("%s %q: %w", envTokenFile, cfg.TokenFile, err)
		}
	}

	return cfg, nil
}

func envOr(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envBool(key string) bool {
	v, ok := os.LookupEnv(key)
	if !ok {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}
