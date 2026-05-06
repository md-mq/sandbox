package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	envListenAddr      = "POLYAXON_SANDBOX_LISTEN_ADDR"
	envToken           = "POLYAXON_SANDBOX_TOKEN"
	envTokenFile       = "POLYAXON_SANDBOX_TOKEN_FILE"
	envStateDir        = "POLYAXON_SANDBOX_STATE_DIR"
	envLogFormat       = "POLYAXON_SANDBOX_LOG_FORMAT"
	envShutdownTimeout = "POLYAXON_SANDBOX_SHUTDOWN_TIMEOUT"
	envPingOnly        = "POLYAXON_SANDBOX_PING_ONLY"
	envMaxExecs        = "POLYAXON_SANDBOX_MAX_EXECS"
	envMaxPTYs         = "POLYAXON_SANDBOX_MAX_PTYS"
	envPTYIdleTTL      = "POLYAXON_SANDBOX_PTY_IDLE_TTL"
	envPTYTerminalTTL  = "POLYAXON_SANDBOX_PTY_TERMINAL_TTL"
	envPTYHeartbeat    = "POLYAXON_SANDBOX_PTY_HEARTBEAT_INTERVAL"
	envPTYPongTimeout  = "POLYAXON_SANDBOX_PTY_PONG_TIMEOUT"
	envPTYReplayBytes  = "POLYAXON_SANDBOX_PTY_REPLAY_BYTES"

	defaultListenAddr      = ":9090"
	defaultTokenFile       = "/opt/polyaxon/sandbox-token"
	defaultStateDir        = "/tmp/plx-exec"
	defaultLogFormat       = "json"
	defaultShutdownTimeout = 10 * time.Second
	defaultMaxExecs        = 64
	defaultMaxPTYs         = 16
	defaultPTYIdleTTL      = 30 * time.Minute
	defaultPTYTerminalTTL  = 10 * time.Minute
	defaultPTYHeartbeat    = 30 * time.Second
	defaultPTYPongTimeout  = 60 * time.Second
	defaultPTYReplayBytes  = 256 << 10
	maxPTYReplayBytes      = 4 << 20
	maxMaxPTYs             = 256
)

type Config struct {
	ListenAddr      string
	Token           string
	TokenFile       string
	StateDir        string
	LogFormat       string
	ShutdownTimeout time.Duration
	PingOnly        bool
	MaxExecs        int
	MaxPTYs         int
	PTYIdleTTL      time.Duration
	PTYTerminalTTL  time.Duration
	PTYHeartbeat    time.Duration
	PTYPongTimeout  time.Duration
	PTYReplayBytes  int
}

func Load() (*Config, error) {
	cfg := &Config{
		ListenAddr:      envOr(envListenAddr, defaultListenAddr),
		Token:           os.Getenv(envToken),
		TokenFile:       envOr(envTokenFile, defaultTokenFile),
		StateDir:        envOr(envStateDir, defaultStateDir),
		LogFormat:       envOr(envLogFormat, defaultLogFormat),
		ShutdownTimeout: defaultShutdownTimeout,
		PingOnly:        envBool(envPingOnly),
		MaxExecs:        defaultMaxExecs,
		MaxPTYs:         defaultMaxPTYs,
		PTYIdleTTL:      defaultPTYIdleTTL,
		PTYTerminalTTL:  defaultPTYTerminalTTL,
		PTYHeartbeat:    defaultPTYHeartbeat,
		PTYPongTimeout:  defaultPTYPongTimeout,
		PTYReplayBytes:  defaultPTYReplayBytes,
	}

	if err := parseDuration(envShutdownTimeout, &cfg.ShutdownTimeout); err != nil {
		return nil, err
	}
	if err := parseDuration(envPTYIdleTTL, &cfg.PTYIdleTTL); err != nil {
		return nil, err
	}
	if err := parseDuration(envPTYTerminalTTL, &cfg.PTYTerminalTTL); err != nil {
		return nil, err
	}
	if err := parseDuration(envPTYHeartbeat, &cfg.PTYHeartbeat); err != nil {
		return nil, err
	}
	if err := parseDuration(envPTYPongTimeout, &cfg.PTYPongTimeout); err != nil {
		return nil, err
	}

	if err := parsePositiveInt(envMaxExecs, &cfg.MaxExecs); err != nil {
		return nil, err
	}
	if err := parseBoundedPositiveInt(envMaxPTYs, &cfg.MaxPTYs, maxMaxPTYs); err != nil {
		return nil, err
	}
	if err := parseBoundedNonNegativeInt(envPTYReplayBytes, &cfg.PTYReplayBytes, maxPTYReplayBytes); err != nil {
		return nil, err
	}
	if cfg.ShutdownTimeout <= 0 {
		return nil, fmt.Errorf("%s: must be > 0, got %s", envShutdownTimeout, cfg.ShutdownTimeout)
	}
	for key, value := range map[string]time.Duration{
		envPTYIdleTTL:     cfg.PTYIdleTTL,
		envPTYTerminalTTL: cfg.PTYTerminalTTL,
		envPTYHeartbeat:   cfg.PTYHeartbeat,
		envPTYPongTimeout: cfg.PTYPongTimeout,
	} {
		if value <= 0 {
			return nil, fmt.Errorf("%s: must be > 0, got %s", key, value)
		}
	}

	if cfg.LogFormat != "json" && cfg.LogFormat != "text" {
		return nil, fmt.Errorf("%s: must be 'json' or 'text', got %q", envLogFormat, cfg.LogFormat)
	}

	if !cfg.PingOnly && cfg.Token == "" {
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

func parseDuration(key string, dst *time.Duration) error {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	*dst = d
	return nil
}

func parsePositiveInt(key string, dst *int) error {
	return parseBoundedPositiveInt(key, dst, 0)
}

func parseBoundedPositiveInt(key string, dst *int, max int) error {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	if n < 1 {
		return fmt.Errorf("%s: must be >= 1, got %d", key, n)
	}
	if max > 0 && n > max {
		return fmt.Errorf("%s: must be <= %d, got %d", key, max, n)
	}
	*dst = n
	return nil
}

func parseBoundedNonNegativeInt(key string, dst *int, max int) error {
	raw := os.Getenv(key)
	if raw == "" {
		return nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	if n < 0 {
		return fmt.Errorf("%s: must be >= 0, got %d", key, n)
	}
	if n > max {
		return fmt.Errorf("%s: must be <= %d, got %d", key, max, n)
	}
	*dst = n
	return nil
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
