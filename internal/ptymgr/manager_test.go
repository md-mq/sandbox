package ptymgr

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestManager(t *testing.T, maxRun int, cfg ManagerConfig) (*Manager, *atomic.Int64, string) {
	t.Helper()
	if cfg.IdleTTL == 0 {
		cfg.IdleTTL = time.Hour
	}
	if cfg.TerminalTTL == 0 {
		cfg.TerminalTTL = time.Hour
	}
	if cfg.ReaperInterval == 0 {
		cfg.ReaperInterval = 20 * time.Millisecond
	}
	cfg.ReplayBytes = 4096
	counter := &atomic.Int64{}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	dir := t.TempDir()
	mgr, err := NewManager(dir, cfg, maxRun, counter, &atomic.Int64{}, log)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		mgr.Shutdown(ctx)
		waitForPTYCounter(t, counter, 0)
	})
	return mgr, counter, dir
}

func TestManager_AllocateGetList(t *testing.T) {
	mgr, counter, _ := newTestManager(t, 4, ManagerConfig{})

	s, err := mgr.Allocate(AllocateRequest{
		Command: []string{"sleep", "30"},
		Tag:     "shell",
	})
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	if counter.Load() != 1 {
		t.Fatalf("counter = %d, want 1", counter.Load())
	}
	if got, ok := mgr.Get(s.ID); !ok || got != s {
		t.Fatalf("Get(%q) = %v, %v", s.ID, got, ok)
	}
	if got, ok := mgr.GetByTag("shell"); !ok || got != s {
		t.Fatalf("GetByTag(shell) = %v, %v", got, ok)
	}
	list := mgr.List("")
	if len(list) != 1 || list[0].PTYID != s.ID || list[0].Tag != "shell" {
		t.Fatalf("List = %#v", list)
	}
	filtered := mgr.List("shell")
	if len(filtered) != 1 || filtered[0].PTYID != s.ID {
		t.Fatalf("List(tag) = %#v", filtered)
	}
	if got := mgr.List("missing"); len(got) != 0 {
		t.Fatalf("List(missing) = %#v, want empty", got)
	}
}

func TestManager_CapExceededNoCounterDrift(t *testing.T) {
	mgr, counter, _ := newTestManager(t, 1, ManagerConfig{})

	s, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}})
	if err != nil {
		t.Fatalf("Allocate first: %v", err)
	}
	_, err = mgr.Allocate(AllocateRequest{Command: []string{"true"}})
	if !errors.Is(err, ErrCapExceeded) {
		t.Fatalf("Allocate over cap err = %v, want ErrCapExceeded", err)
	}
	if counter.Load() != 1 {
		t.Fatalf("counter = %d, want 1", counter.Load())
	}
	if err := mgr.Delete(s.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	waitForPTYCounter(t, counter, 0)
}

func TestManager_InvalidTagDoesNotReserveSlot(t *testing.T) {
	mgr, counter, _ := newTestManager(t, 1, ManagerConfig{})

	_, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}, Tag: "bad tag"})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Allocate invalid tag err = %v, want ErrInvalidRequest", err)
	}
	if counter.Load() != 0 {
		t.Fatalf("counter = %d, want 0", counter.Load())
	}
}

func TestManager_StartFailureReleasesCapAndTag(t *testing.T) {
	mgr, counter, _ := newTestManager(t, 1, ManagerConfig{})

	_, err := mgr.Allocate(AllocateRequest{
		Command: []string{"/definitely/not-a-real-command"},
		Tag:     "retry",
	})
	if err == nil {
		t.Fatal("Allocate invalid command succeeded")
	}
	if counter.Load() != 0 {
		t.Fatalf("counter = %d, want 0", counter.Load())
	}
	if _, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}, Tag: "retry"}); err != nil {
		t.Fatalf("tag should be reusable after start failure: %v", err)
	}
}

func TestManager_ExitedRetainedDoesNotHoldCap(t *testing.T) {
	mgr, counter, _ := newTestManager(t, 1, ManagerConfig{})

	exited, err := mgr.Allocate(AllocateRequest{Command: []string{"sh", "-c", "exit 0"}, Tag: "done"})
	if err != nil {
		t.Fatalf("Allocate exited: %v", err)
	}
	waitDone(t, exited.Done())
	waitForPTYCounter(t, counter, 0)

	running, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}, Tag: "next"})
	if err != nil {
		t.Fatalf("Allocate after retained exit: %v", err)
	}
	if running.ID == exited.ID {
		t.Fatalf("running id = exited id %q", running.ID)
	}
	if counter.Load() != 1 {
		t.Fatalf("counter = %d, want 1", counter.Load())
	}
	if got := mgr.List(""); len(got) != 2 {
		t.Fatalf("List length = %d, want retained exited + running", len(got))
	}
}

func TestManager_TagConflictRunningAndReuseAfterDelete(t *testing.T) {
	mgr, counter, _ := newTestManager(t, 4, ManagerConfig{})

	s, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}, Tag: "dev"})
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	_, err = mgr.Allocate(AllocateRequest{Command: []string{"true"}, Tag: "dev"})
	var conflict *ErrTagConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("duplicate tag err = %v, want ErrTagConflict", err)
	}
	if conflict.Tag != "dev" || conflict.State != StateRunning || conflict.ExistingID != s.ID {
		t.Fatalf("conflict = %#v", conflict)
	}

	if err := mgr.Delete(s.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	waitForPTYCounter(t, counter, 0)
	reused, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}, Tag: "dev"})
	if err != nil {
		t.Fatalf("Allocate after delete: %v", err)
	}
	if reused.ID == s.ID {
		t.Fatalf("reused id = old id %q", reused.ID)
	}
}

func TestManager_TagConflictExitedRetained(t *testing.T) {
	mgr, counter, _ := newTestManager(t, 4, ManagerConfig{})

	s, err := mgr.Allocate(AllocateRequest{Command: []string{"sh", "-c", "exit 0"}, Tag: "once"})
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	waitDone(t, s.Done())
	waitForPTYCounter(t, counter, 0)

	_, err = mgr.Allocate(AllocateRequest{Command: []string{"true"}, Tag: "once"})
	var conflict *ErrTagConflict
	if !errors.As(err, &conflict) {
		t.Fatalf("duplicate retained tag err = %v, want ErrTagConflict", err)
	}
	if conflict.Tag != "once" || conflict.State != StateExited || conflict.ExistingID != s.ID {
		t.Fatalf("conflict = %#v", conflict)
	}
}

func TestManager_ConcurrentSameTagAllocation(t *testing.T) {
	for i := 0; i < 20; i++ {
		mgr, counter, _ := newTestManager(t, 4, ManagerConfig{})

		var wg sync.WaitGroup
		wg.Add(2)
		results := make(chan error, 2)
		sessions := make(chan *PTYSession, 2)
		for j := 0; j < 2; j++ {
			go func() {
				defer wg.Done()
				s, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}, Tag: "race"})
				if err == nil {
					sessions <- s
				}
				results <- err
			}()
		}
		wg.Wait()
		close(results)
		close(sessions)

		var wins, conflicts int
		for err := range results {
			if err == nil {
				wins++
				continue
			}
			var conflict *ErrTagConflict
			if !errors.As(err, &conflict) {
				t.Fatalf("iteration %d: err = %v, want ErrTagConflict", i, err)
			}
			if conflict.State != "allocating" && conflict.State != StateRunning {
				t.Fatalf("iteration %d: conflict state = %q", i, conflict.State)
			}
			conflicts++
		}
		if wins != 1 || conflicts != 1 {
			t.Fatalf("iteration %d: wins=%d conflicts=%d", i, wins, conflicts)
		}
		if counter.Load() != 1 {
			t.Fatalf("iteration %d: counter = %d, want 1", i, counter.Load())
		}
		for s := range sessions {
			if err := mgr.Delete(s.ID); err != nil {
				t.Fatalf("Delete winner: %v", err)
			}
		}
	}
}

func TestManager_DeleteRemovesDirAndFreesTag(t *testing.T) {
	mgr, counter, _ := newTestManager(t, 4, ManagerConfig{})

	s, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}, Tag: "tmp"})
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	dir := s.Dir
	if err := mgr.Delete(s.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	waitForPTYCounter(t, counter, 0)
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dir exists after Delete: err=%v", err)
	}
	if _, ok := mgr.Get(s.ID); ok {
		t.Fatalf("Get(%q) should be missing after Delete", s.ID)
	}
	if _, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}, Tag: "tmp"}); err != nil {
		t.Fatalf("tag should be reusable after Delete: %v", err)
	}
}

func TestManager_TerminalTTLRemovesExitedRecord(t *testing.T) {
	mgr, counter, _ := newTestManager(t, 4, ManagerConfig{
		IdleTTL:     time.Hour,
		TerminalTTL: 80 * time.Millisecond,
	})

	s, err := mgr.Allocate(AllocateRequest{Command: []string{"sh", "-c", "exit 0"}, Tag: "short"})
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	waitDone(t, s.Done())
	waitForPTYCounter(t, counter, 0)
	if _, ok := mgr.Get(s.ID); !ok {
		t.Fatalf("exited record should be retained before terminal TTL")
	}
	waitForCondition(t, 3*time.Second, func() bool {
		_, ok := mgr.Get(s.ID)
		return !ok
	})
	if _, err := os.Stat(s.Dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dir exists after terminal TTL: err=%v", err)
	}
	if _, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}, Tag: "short"}); err != nil {
		t.Fatalf("tag should be reusable after terminal TTL: %v", err)
	}
}

func TestManager_DetachedIdleRemovesRunningSession(t *testing.T) {
	mgr, counter, _ := newTestManager(t, 4, ManagerConfig{
		IdleTTL:     80 * time.Millisecond,
		TerminalTTL: time.Hour,
	})

	s, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}, Tag: "idle"})
	if err != nil {
		t.Fatalf("Allocate: %v", err)
	}
	waitForCondition(t, 3*time.Second, func() bool {
		_, ok := mgr.Get(s.ID)
		return !ok
	})
	waitForPTYCounter(t, counter, 0)
	if _, err := os.Stat(s.Dir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dir exists after idle reap: err=%v", err)
	}
	if _, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}, Tag: "idle"}); err != nil {
		t.Fatalf("tag should be reusable after idle reap: %v", err)
	}
}

func TestManager_CleanupRemovesOnlyPTYDirs(t *testing.T) {
	mgr, _, stateDir := newTestManager(t, 4, ManagerConfig{})
	ptyDir := filepath.Join(stateDir, "pty-stale")
	execDir := filepath.Join(stateDir, "01999999-0000-7000-8000-000000000001")
	if err := os.MkdirAll(ptyDir, 0o700); err != nil {
		t.Fatalf("mkdir pty dir: %v", err)
	}
	if err := os.MkdirAll(execDir, 0o700); err != nil {
		t.Fatalf("mkdir exec dir: %v", err)
	}
	if err := mgr.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if _, err := os.Stat(ptyDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pty dir exists after Cleanup: err=%v", err)
	}
	if _, err := os.Stat(execDir); err != nil {
		t.Fatalf("exec dir should remain after Cleanup: %v", err)
	}
}

func TestManager_ShutdownKillsAllSessions(t *testing.T) {
	mgr, counter, _ := newTestManager(t, 4, ManagerConfig{})

	for i := 0; i < 3; i++ {
		if _, err := mgr.Allocate(AllocateRequest{Command: []string{"sleep", "30"}}); err != nil {
			t.Fatalf("Allocate %d: %v", i, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	mgr.Shutdown(ctx)
	waitForPTYCounter(t, counter, 0)
	if got := mgr.List(""); len(got) != 0 {
		t.Fatalf("List after Shutdown = %#v, want empty", got)
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
