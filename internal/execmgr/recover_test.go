package execmgr

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func newRecoveryManager(t *testing.T, stateDir string) *Manager {
	t.Helper()
	counter := &atomic.Int64{}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	mgr, err := NewManager(stateDir, 4, counter, log)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { mgr.Shutdown(context.Background()) })
	return mgr
}

func writeExecDir(t *testing.T, root, id string, meta *Meta, pid *int, status *Status) {
	t.Helper()
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if meta != nil {
		if err := WriteMeta(dir, meta); err != nil {
			t.Fatalf("WriteMeta: %v", err)
		}
	}
	if pid != nil {
		if err := WritePid(dir, *pid); err != nil {
			t.Fatalf("WritePid: %v", err)
		}
	}
	if status != nil {
		if err := WriteStatusAtomic(dir, status); err != nil {
			t.Fatalf("WriteStatus: %v", err)
		}
	}
}

func TestRecover_TerminalExitedRecord(t *testing.T) {
	root := t.TempDir()
	id := "01999999-9999-7999-8999-999999999999"
	ec := 0
	writeExecDir(t, root, id,
		&Meta{ExecID: id, Command: []string{"true"}, StartedAt: time.Now().Add(-time.Minute)},
		intPtr(1),
		&Status{State: StateExited, ExitCode: &ec, FinishedAt: time.Now(), DurationMS: 10},
	)

	mgr := newRecoveryManager(t, root)
	if err := mgr.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	e, ok := mgr.Get(id)
	if !ok {
		t.Fatal("exec missing after recovery")
	}
	if e.State() != StateExited {
		t.Errorf("state = %q, want exited", e.State())
	}
	// Done channel should already be closed.
	select {
	case <-e.Done():
	default:
		t.Error("Done channel should be closed for terminal record")
	}
}

func TestRecover_DeadPidWithoutStatusSynthesizesOrphaned(t *testing.T) {
	root := t.TempDir()
	id := "01999999-0000-7000-8000-000000000001"
	// Use a PID that's very unlikely to exist. 2147483646 = INT_MAX - 1.
	writeExecDir(t, root, id,
		&Meta{ExecID: id, Command: []string{"true"}, StartedAt: time.Now().Add(-time.Minute)},
		intPtr(2147483646),
		nil,
	)

	mgr := newRecoveryManager(t, root)
	if err := mgr.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	e, ok := mgr.Get(id)
	if !ok {
		t.Fatal("exec missing after recovery")
	}
	if e.State() != StateOrphaned {
		t.Errorf("state = %q, want orphaned", e.State())
	}

	// Verify a status.json was synthesized.
	st, err := ReadStatus(filepath.Join(root, id))
	if err != nil {
		t.Fatalf("ReadStatus: %v", err)
	}
	if st.State != StateOrphaned {
		t.Errorf("synthesized status.state = %q, want orphaned", st.State)
	}
	if st.ExitCode != nil {
		t.Errorf("orphaned ExitCode = %v, want nil", st.ExitCode)
	}
}

func TestRecover_IncompleteStartRemovesDir(t *testing.T) {
	root := t.TempDir()
	id := "01999999-1111-7000-8000-000000000002"
	// meta only, no pid, no status = crashed between MkdirAll and Start.
	writeExecDir(t, root, id,
		&Meta{ExecID: id, Command: []string{"true"}, StartedAt: time.Now().Add(-time.Minute)},
		nil, nil,
	)

	mgr := newRecoveryManager(t, root)
	if err := mgr.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	if _, ok := mgr.Get(id); ok {
		t.Error("incomplete-start dir should not register an exec")
	}
	if _, err := os.Stat(filepath.Join(root, id)); !os.IsNotExist(err) {
		t.Errorf("dir should have been removed, err = %v", err)
	}
}

func TestRecover_AliveOrphanTransitionsOnDeath(t *testing.T) {
	// Start a real child process, record its pgid, then spin up a fresh manager
	// pointing at the same state dir. The manager should pick the child up as
	// `running`, poll /proc, and flip to `orphaned` when the child dies.
	root := t.TempDir()
	id := "01999999-2222-7000-8000-000000000003"
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	// We launch via sh so Setpgid semantics are the same as the real path,
	// but the child is NOT owned by our manager — it's a pure unrelated process.
	cmd := exec.Command("sh", "-c", "sleep 2")
	cmd.SysProcAttr = setpgid()
	if err := cmd.Start(); err != nil {
		t.Skipf("sh unavailable: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	if err := WriteMeta(dir, &Meta{ExecID: id, Command: []string{"sh"}, StartedAt: time.Now()}); err != nil {
		t.Fatalf("WriteMeta: %v", err)
	}
	if err := WritePid(dir, cmd.Process.Pid); err != nil {
		t.Fatalf("WritePid: %v", err)
	}

	// Reap our tracked handle in the background so the OS doesn't leak a zombie.
	go func() { _ = cmd.Wait() }()

	mgr := newRecoveryManager(t, root)
	if err := mgr.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	e, ok := mgr.Get(id)
	if !ok {
		t.Fatal("alive exec not picked up by recovery")
	}
	if e.State() != StateRunning {
		t.Errorf("state = %q, want running", e.State())
	}

	// Wait for the sleep to finish. Orphan reaper polls at 1s, so give it ~4s.
	select {
	case <-e.Done():
	case <-time.After(6 * time.Second):
		t.Fatal("orphan reaper did not finalize within 6s")
	}
	if e.State() != StateOrphaned {
		t.Errorf("state = %q, want orphaned", e.State())
	}
}

func TestRecover_SkipsPTYDirs(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "pty-01999999-3333-7000-8000-000000000004")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := WriteMeta(dir, &Meta{
		ExecID:    "pty-01999999-3333-7000-8000-000000000004",
		Command:   []string{"should-not-register"},
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("WriteMeta: %v", err)
	}

	mgr := newRecoveryManager(t, root)
	if err := mgr.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, ok := mgr.Get("pty-01999999-3333-7000-8000-000000000004"); ok {
		t.Fatal("pty-* dir should not register as an exec")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("pty-* dir should be left alone, stat err = %v", err)
	}
}

func TestRecover_OrphanDoesNotHoldSlot(t *testing.T) {
	root := t.TempDir()
	id := "01999999-4444-7000-8000-000000000005"
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	cmd := exec.Command("sh", "-c", "sleep 5")
	cmd.SysProcAttr = setpgid()
	if err := cmd.Start(); err != nil {
		t.Skipf("sh unavailable: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	go func() { _ = cmd.Wait() }()

	if err := WriteMeta(dir, &Meta{ExecID: id, Command: []string{"sh"}, StartedAt: time.Now()}); err != nil {
		t.Fatalf("WriteMeta: %v", err)
	}
	if err := WritePid(dir, cmd.Process.Pid); err != nil {
		t.Fatalf("WritePid: %v", err)
	}

	counter := &atomic.Int64{}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	mgr, err := NewManager(root, 2, counter, log)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { mgr.Shutdown(context.Background()) })
	if err := mgr.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if counter.Load() != 0 {
		t.Fatalf("counter after orphan recovery = %d, want 0", counter.Load())
	}

	e1, err := mgr.Start(context.Background(), StartRequest{Command: []string{"sleep", "1"}, TimeoutMS: 5000})
	if err != nil {
		t.Fatalf("Start #1: %v", err)
	}
	e2, err := mgr.Start(context.Background(), StartRequest{Command: []string{"sleep", "1"}, TimeoutMS: 5000})
	if err != nil {
		t.Fatalf("Start #2: %v", err)
	}
	if counter.Load() != 2 {
		t.Fatalf("counter after two new starts = %d, want 2", counter.Load())
	}
	<-e1.Done()
	<-e2.Done()
}

func TestRecover_TagResurrectsWithRecord(t *testing.T) {
	root := t.TempDir()
	id := "01999999-5555-7000-8000-000000000006"
	ec := 0
	writeExecDir(t, root, id,
		&Meta{ExecID: id, Tag: "kept", Command: []string{"true"}, StartedAt: time.Now().Add(-time.Minute)},
		intPtr(1),
		&Status{State: StateExited, ExitCode: &ec, FinishedAt: time.Now(), DurationMS: 10},
	)

	mgr := newRecoveryManager(t, root)
	if err := mgr.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, err := mgr.Start(context.Background(), StartRequest{Command: []string{"true"}, Tag: "kept"}); err == nil {
		t.Fatal("Start with recovered tag succeeded, want conflict")
	} else {
		var tagErr *ErrTagConflict
		if !errors.As(err, &tagErr) {
			t.Fatalf("err = %v, want ErrTagConflict", err)
		}
		if tagErr.ExistingID != id || tagErr.State != StateExited {
			t.Fatalf("tag conflict = %#v, want existing %s exited", tagErr, id)
		}
	}
}

func TestRecover_DuplicateTagsFirstLexicographicWins(t *testing.T) {
	root := t.TempDir()
	first := "01999999-5555-7000-8000-000000000007"
	second := "01999999-5555-7000-8000-000000000008"
	ec := 0
	writeExecDir(t, root, first,
		&Meta{ExecID: first, Tag: "dup", Command: []string{"true"}, StartedAt: time.Now().Add(-time.Minute)},
		intPtr(1),
		&Status{State: StateExited, ExitCode: &ec, FinishedAt: time.Now(), DurationMS: 10},
	)
	writeExecDir(t, root, second,
		&Meta{ExecID: second, Tag: "dup", Command: []string{"true"}, StartedAt: time.Now().Add(-time.Minute)},
		intPtr(1),
		&Status{State: StateExited, ExitCode: &ec, FinishedAt: time.Now(), DurationMS: 10},
	)

	mgr := newRecoveryManager(t, root)
	if err := mgr.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, ok := mgr.Get(first); !ok {
		t.Fatal("first duplicate should recover")
	}
	if _, ok := mgr.Get(second); ok {
		t.Fatal("second duplicate should be skipped")
	}
}

func intPtr(n int) *int { return &n }
