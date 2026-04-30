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
}

func TestLoad_EnvOverrides(t *testing.T) {
	clearEnv(t)
	tokenFile := writeTempToken(t, "secret")

	t.Setenv(envListenAddr, ":8080")
	t.Setenv(envTokenFile, tokenFile)
	t.Setenv(envStateDir, "/var/lib/plx")
	t.Setenv(envLogFormat, "text")
	t.Setenv(envShutdownTimeout, "30s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.ListenAddr != ":8080" {
		t.Errorf("ListenAddr = %q", cfg.ListenAddr)
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
}

func TestLoad_MissingTokenFileErrors(t *testing.T) {
	clearEnv(t)
	t.Setenv(envTokenFile, "/does/not/exist/sandbox-token")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for missing token file, got nil")
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
		envListenAddr, envTokenFile, envStateDir,
		envLogFormat, envShutdownTimeout, envPingOnly, envMaxExecs,
	} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
}
