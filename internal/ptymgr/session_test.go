package ptymgr

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var nextPTYTestID atomic.Uint64

func newTestSession(t *testing.T, req AllocateRequest, replayBytes int) (*PTYSession, *atomic.Int64) {
	t.Helper()
	counter := &atomic.Int64{}
	counter.Add(1)
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	id := fmt.Sprintf("test-%d", nextPTYTestID.Add(1))
	s, err := newSession(id, t.TempDir(), req, SessionConfig{ReplayBytes: replayBytes}, counter, log)
	if err != nil {
		counter.Add(-1)
		t.Fatalf("newSession: %v", err)
	}
	t.Cleanup(func() {
		_ = s.Kill()
		waitForPTYCounter(t, counter, 0)
	})
	return s, counter
}

func TestSession_DefaultCommandStartsShell(t *testing.T) {
	s, _ := newTestSession(t, AllocateRequest{}, 4096)

	if err := s.Write([]byte("printf default-ok\\n\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	waitForReplay(t, s, "default-ok", 3*time.Second)
}

func TestSession_TERMDefaultAndOverride(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]*string
		want string
	}{
		{name: "default", want: defaultTERM},
		{name: "override", env: map[string]*string{"TERM": strPtr("vt100")}, want: "vt100"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			s, _ := newTestSession(t, AllocateRequest{
				Command: []string{"sh", "-c", "printf '%s' \"$TERM\" > term.txt; sleep 30"},
				Env:     tt.env,
				Workdir: dir,
			}, 0)
			waitForFile(t, filepath.Join(dir, "term.txt"), tt.want, 3*time.Second)
			_ = s.Kill()
		})
	}
}

func TestSession_WriteAndReplay(t *testing.T) {
	s, _ := newTestSession(t, AllocateRequest{Command: []string{"/bin/sh"}}, 4096)

	if err := s.Write([]byte("printf pty-ok\\n\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	waitForReplay(t, s, "pty-ok", 3*time.Second)
}

func TestSession_ResizeUpdatesStatus(t *testing.T) {
	s, _ := newTestSession(t, AllocateRequest{Command: []string{"sleep", "30"}}, 1024)

	if err := s.Resize(100, 40); err != nil {
		t.Fatalf("Resize: %v", err)
	}
	st := s.Status()
	if st.Cols != 100 || st.Rows != 40 {
		t.Fatalf("size = %dx%d, want 100x40", st.Cols, st.Rows)
	}
}

func TestSession_NaturalExitRetainsStatus(t *testing.T) {
	s, counter := newTestSession(t, AllocateRequest{
		Command: []string{"sh", "-c", "exit 7"},
	}, 1024)

	waitDone(t, s.Done(), 3*time.Second)
	waitForPTYCounter(t, counter, 0)

	st := s.Status()
	if st.State != StateExited {
		t.Fatalf("state = %q, want exited", st.State)
	}
	if st.ExitCode == nil || *st.ExitCode != 7 {
		t.Fatalf("exit_code = %v, want 7", st.ExitCode)
	}
	if st.FinishedAt == nil {
		t.Fatal("finished_at is nil")
	}
	if _, err := os.Stat(s.Dir); err != nil {
		t.Fatalf("natural exit should retain dir: %v", err)
	}
	if replay, err := s.Replay(1); err != nil || len(replay) != 0 {
		t.Fatalf("Replay after exit = %q, %v; want empty nil-error", string(bytes.Join(replay, nil)), err)
	}
}

func TestSession_KillRemovesRecord(t *testing.T) {
	s, counter := newTestSession(t, AllocateRequest{Command: []string{"sleep", "30"}}, 1024)

	if err := s.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	waitForPTYCounter(t, counter, 0)
	if _, err := os.Stat(s.Dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dir exists after Kill: err=%v", err)
	}
}

func TestSession_SignalINTUsesTerminalInterrupt(t *testing.T) {
	dir := t.TempDir()
	s, _ := newTestSession(t, AllocateRequest{
		Command: []string{"sh", "-c", "trap 'printf int > int.txt; exit 0' INT; printf ready > ready.txt; while :; do sleep 1; done"},
		Workdir: dir,
	}, 1024)

	waitForFile(t, filepath.Join(dir, "ready.txt"), "ready", 3*time.Second)
	if err := s.Signal("SIGINT"); err != nil {
		t.Fatalf("Signal(SIGINT): %v", err)
	}
	waitForFile(t, filepath.Join(dir, "int.txt"), "int", 3*time.Second)
	waitDone(t, s.Done(), 3*time.Second)
}

func TestSession_SignalTERMUsesProcessGroup(t *testing.T) {
	dir := t.TempDir()
	s, _ := newTestSession(t, AllocateRequest{
		Command: []string{"sh", "-c", "trap 'printf term > term.txt; exit 0' TERM; printf ready > ready.txt; while :; do sleep 1; done"},
		Workdir: dir,
	}, 1024)

	waitForFile(t, filepath.Join(dir, "ready.txt"), "ready", 3*time.Second)
	if err := s.Signal("SIGTERM"); err != nil {
		t.Fatalf("Signal(SIGTERM): %v", err)
	}
	waitForFile(t, filepath.Join(dir, "term.txt"), "term", 3*time.Second)
	waitDone(t, s.Done(), 3*time.Second)
}

func TestSession_InvalidSignalRejected(t *testing.T) {
	s, _ := newTestSession(t, AllocateRequest{Command: []string{"sleep", "30"}}, 1024)

	err := s.Signal("SIGWINCH")
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Signal invalid err = %v, want ErrInvalidRequest", err)
	}
}

func TestSession_ConcurrentChildExitAndDeleteRemoves(t *testing.T) {
	for i := 0; i < 10; i++ {
		s, counter := newTestSession(t, AllocateRequest{
			Command: []string{"sh", "-c", "sleep 0.08; exit 0"},
		}, 1024)

		time.Sleep(40 * time.Millisecond)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = s.Kill()
		}()
		go func() {
			defer wg.Done()
			<-s.Done()
		}()
		wg.Wait()

		waitForPTYCounter(t, counter, 0)
		if _, err := os.Stat(s.Dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("iteration %d: dir exists after delete race: err=%v", i, err)
		}
	}
}

func waitForReplay(t *testing.T, s *PTYSession, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		parts, err := s.Replay(4096)
		if err != nil {
			t.Fatalf("Replay: %v", err)
		}
		if bytes.Contains(bytes.Join(parts, nil), []byte(needle)) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	parts, _ := s.Replay(4096)
	t.Fatalf("replay did not contain %q before timeout; got %q", needle, string(bytes.Join(parts, nil)))
}

func waitForFile(t *testing.T, path, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path)
		if err == nil && string(raw) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	raw, _ := os.ReadFile(path)
	t.Fatalf("%s = %q, want %q before timeout", path, string(raw), want)
}

func waitDone(t *testing.T, done <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("session did not finish within %s", timeout)
	}
}

func waitForPTYCounter(t *testing.T, c *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.Load() == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("counter = %d, want %d", c.Load(), want)
}

func strPtr(v string) *string { return &v }
