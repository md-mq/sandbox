package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	clearEnv(t)
	t.Setenv(envPingOnly, "1")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ListenAddr != defaultListenAddr {
		t.Errorf("ListenAddr = %q, want %q", cfg.ListenAddr, defaultListenAddr)
	}
	if cfg.Token != "" {
		t.Errorf("Token = %q, want empty", cfg.Token)
	}
	if cfg.TokenFile != defaultTokenFile {
		t.Errorf("TokenFile = %q, want %q", cfg.TokenFile, defaultTokenFile)
	}
	if cfg.StateDir != defaultStateDir {
		t.Errorf("StateDir = %q, want %q", cfg.StateDir, defaultStateDir)
	}
	if cfg.LogFormat != defaultLogFormat {
		t.Errorf("LogFormat = %q, want %q", cfg.LogFormat, defaultLogFormat)
	}
	if cfg.ShutdownTimeout != defaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %v, want %v", cfg.ShutdownTimeout, defaultShutdownTimeout)
	}
	if !cfg.PingOnly {
		t.Errorf("PingOnly = false, want true")
	}
	if cfg.MaxExecs != defaultMaxExecs {
		t.Errorf("MaxExecs = %d, want %d", cfg.MaxExecs, defaultMaxExecs)
	}
	if cfg.MaxPTYs != defaultMaxPTYs {
		t.Errorf("MaxPTYs = %d, want %d", cfg.MaxPTYs, defaultMaxPTYs)
	}
	if cfg.PTYIdleTTL != defaultPTYIdleTTL {
		t.Errorf("PTYIdleTTL = %v, want %v", cfg.PTYIdleTTL, defaultPTYIdleTTL)
	}
	if cfg.PTYTerminalTTL != defaultPTYTerminalTTL {
		t.Errorf("PTYTerminalTTL = %v, want %v", cfg.PTYTerminalTTL, defaultPTYTerminalTTL)
	}
	if cfg.PTYHeartbeat != defaultPTYHeartbeat {
		t.Errorf("PTYHeartbeat = %v, want %v", cfg.PTYHeartbeat, defaultPTYHeartbeat)
	}
	if cfg.PTYPongTimeout != defaultPTYPongTimeout {
		t.Errorf("PTYPongTimeout = %v, want %v", cfg.PTYPongTimeout, defaultPTYPongTimeout)
	}
	if cfg.PTYReplayBytes != defaultPTYReplayBytes {
		t.Errorf("PTYReplayBytes = %d, want %d", cfg.PTYReplayBytes, defaultPTYReplayBytes)
	}
}

func TestLoad_EnvOverrides(t *testing.T) {
	clearEnv(t)
	tokenFile := writeTempToken(t, "secret")

	t.Setenv(envListenAddr, ":8080")
	t.Setenv(envToken, "env-secret")
	t.Setenv(envTokenFile, tokenFile)
	t.Setenv(envStateDir, "/var/lib/plx")
	t.Setenv(envLogFormat, "text")
	t.Setenv(envShutdownTimeout, "30s")
	t.Setenv(envMaxExecs, "8")
	t.Setenv(envMaxPTYs, "12")
	t.Setenv(envPTYIdleTTL, "20m")
	t.Setenv(envPTYTerminalTTL, "90s")
	t.Setenv(envPTYHeartbeat, "5s")
	t.Setenv(envPTYPongTimeout, "15s")
	t.Setenv(envPTYReplayBytes, "131072")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ListenAddr != ":8080" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
	}
	if cfg.Token != "env-secret" {
		t.Errorf("Token = %q", cfg.Token)
	}
	if cfg.TokenFile != tokenFile {
		t.Errorf("TokenFile = %q", cfg.TokenFile)
	}
	if cfg.StateDir != "/var/lib/plx" {
		t.Errorf("StateDir = %q", cfg.StateDir)
	}
	if cfg.LogFormat != "text" {
		t.Errorf("LogFormat = %q", cfg.LogFormat)
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %v", cfg.ShutdownTimeout)
	}
	if cfg.MaxExecs != 8 {
		t.Errorf("MaxExecs = %d", cfg.MaxExecs)
	}
	if cfg.MaxPTYs != 12 {
		t.Errorf("MaxPTYs = %d", cfg.MaxPTYs)
	}
	if cfg.PTYIdleTTL != 20*time.Minute {
		t.Errorf("PTYIdleTTL = %v", cfg.PTYIdleTTL)
	}
	if cfg.PTYTerminalTTL != 90*time.Second {
		t.Errorf("PTYTerminalTTL = %v", cfg.PTYTerminalTTL)
	}
	if cfg.PTYHeartbeat != 5*time.Second {
		t.Errorf("PTYHeartbeat = %v", cfg.PTYHeartbeat)
	}
	if cfg.PTYPongTimeout != 15*time.Second {
		t.Errorf("PTYPongTimeout = %v", cfg.PTYPongTimeout)
	}
	if cfg.PTYReplayBytes != 131072 {
		t.Errorf("PTYReplayBytes = %d", cfg.PTYReplayBytes)
	}
}

func TestLoad_MissingTokenFileErrors(t *testing.T) {
	clearEnv(t)
	t.Setenv(envTokenFile, "/does/not/exist/sandbox-token")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for missing token file, got nil")
	}
}

func TestLoad_TokenEnvSkipsTokenFileStat(t *testing.T) {
	clearEnv(t)
	t.Setenv(envToken, "env-secret")
	t.Setenv(envTokenFile, "/does/not/exist/sandbox-token")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Token != "env-secret" {
		t.Errorf("Token = %q, want env-secret", cfg.Token)
	}
	if cfg.TokenFile != "/does/not/exist/sandbox-token" {
		t.Errorf("TokenFile = %q", cfg.TokenFile)
	}
}

func TestLoad_InvalidLogFormatErrors(t *testing.T) {
	clearEnv(t)
	t.Setenv(envPingOnly, "1")
	t.Setenv(envLogFormat, "yaml")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for invalid log format, got nil")
	}
}

func TestLoad_InvalidShutdownTimeoutErrors(t *testing.T) {
	clearEnv(t)
	t.Setenv(envPingOnly, "1")
	t.Setenv(envShutdownTimeout, "not-a-duration")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for invalid shutdown timeout, got nil")
	}
}

func TestLoad_MaxExecsOverride(t *testing.T) {
	clearEnv(t)
	t.Setenv(envPingOnly, "1")
	t.Setenv(envMaxExecs, "8")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MaxExecs != 8 {
		t.Errorf("MaxExecs = %d, want 8", cfg.MaxExecs)
	}
}

func TestLoad_MaxExecsInvalid(t *testing.T) {
	cases := []string{"0", "-1", "abc"}
	for _, v := range cases {
		t.Run(v, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(envPingOnly, "1")
			t.Setenv(envMaxExecs, v)

			_, err := Load()
			if err == nil {
				t.Fatalf("expected error for MaxExecs=%q, got nil", v)
			}
		})
	}
}

func TestLoad_PTYConfigInvalid(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  string
	}{
		{"max ptys zero", envMaxPTYs, "0"},
		{"max ptys too large", envMaxPTYs, "257"},
		{"max ptys invalid", envMaxPTYs, "abc"},
		{"idle ttl zero", envPTYIdleTTL, "0s"},
		{"idle ttl invalid", envPTYIdleTTL, "abc"},
		{"terminal ttl zero", envPTYTerminalTTL, "0s"},
		{"heartbeat zero", envPTYHeartbeat, "0s"},
		{"pong timeout zero", envPTYPongTimeout, "0s"},
		{"replay negative", envPTYReplayBytes, "-1"},
		{"replay too large", envPTYReplayBytes, "4194305"},
		{"replay invalid", envPTYReplayBytes, "abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(envPingOnly, "1")
			t.Setenv(tt.key, tt.val)

			_, err := Load()
			if err == nil {
				t.Fatalf("expected error for %s=%q, got nil", tt.key, tt.val)
			}
		})
	}
}

func TestLoad_PTYReplayCanBeDisabled(t *testing.T) {
	clearEnv(t)
	t.Setenv(envPingOnly, "1")
	t.Setenv(envPTYReplayBytes, "0")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.PTYReplayBytes != 0 {
		t.Errorf("PTYReplayBytes = %d, want 0", cfg.PTYReplayBytes)
	}
}

func writeTempToken(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "sandbox-token")
	if err := os.WriteFile(path, []byte(content), 0o400); err != nil {
		t.Fatalf("write temp token: %v", err)
	}
	return path
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		envListenAddr, envToken, envTokenFile, envStateDir,
		envLogFormat, envShutdownTimeout, envPingOnly, envMaxExecs,
		envMaxPTYs, envPTYIdleTTL, envPTYTerminalTTL,
		envPTYHeartbeat, envPTYPongTimeout, envPTYReplayBytes,
	} {
		t.Setenv(k, "")
		_ = os.Unsetenv(k)
	}
}
