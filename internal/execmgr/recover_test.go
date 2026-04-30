package execmgr

import (
	"context"
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

func writeExecDir(t *testing.T, root, id string, meta *Meta, pid *int, status *Status) string {
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
	return dir
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

func intPtr(n int) *int { return &n }
