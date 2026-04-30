package execmgr

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestExec_SuccessfulExit(t *testing.T) {
	mgr, _ := newTestManager(t, 4)
	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"sh", "-c", "echo hello"},
		TimeoutMS: 5_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	e.Wait()

	st := e.Status()
	if st == nil {
		t.Fatal("status is nil after Wait")
	}
	if st.State != StateExited {
		t.Errorf("state = %q, want exited", st.State)
	}
	if st.ExitCode == nil || *st.ExitCode != 0 {
		t.Errorf("exit code = %v, want 0", st.ExitCode)
	}
	if st.DurationMS < 0 {
		t.Errorf("duration_ms = %d, want >= 0", st.DurationMS)
	}

	out, err := os.ReadFile(filepath.Join(e.Dir, stdoutLog))
	if err != nil {
		t.Fatalf("read stdout log: %v", err)
	}
	if !strings.Contains(string(out), "hello") {
		t.Errorf("stdout = %q, want hello", string(out))
	}
}

func TestExec_NonZeroExit(t *testing.T) {
	mgr, _ := newTestManager(t, 4)
	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"sh", "-c", "exit 7"},
		TimeoutMS: 5_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	e.Wait()

	st := e.Status()
	if st.ExitCode == nil || *st.ExitCode != 7 {
		t.Errorf("exit code = %v, want 7", st.ExitCode)
	}
}

func TestExec_Timeout(t *testing.T) {
	mgr, _ := newTestManager(t, 4)
	start := time.Now()
	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"sleep", "30"},
		TimeoutMS: 200,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	e.Wait()

	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		t.Errorf("timeout took %v, want near 200ms", elapsed)
	}
	st := e.Status()
	if st.State != StateTimedOut {
		t.Errorf("state = %q, want timed_out", st.State)
	}
}

func TestExec_ProcessGroupCleanup(t *testing.T) {
	mgr, _ := newTestManager(t, 4)

	tmp := t.TempDir()
	marker := filepath.Join(tmp, "gc.pid")
	// Script: launch a grandchild that records its PID, then block.
	// `wait` in the parent shell keeps the session alive.
	script := "sh -c 'echo $$ > " + marker + "; sleep 300' &\nwait\n"

	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"sh", "-c", script},
		TimeoutMS: 60_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait for the grandchild to write its pid.
	var gcPid int
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(marker)
		if err == nil {
			n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
			if err == nil && n > 0 {
				gcPid = n
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if gcPid == 0 {
		t.Skip("grandchild did not start — environment lacks sh or is very slow")
	}

	if err := mgr.Delete(e.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	e.Wait()

	// Poll for the grandchild to disappear.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if !pidAlive(gcPid) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Errorf("grandchild pid %d is still alive after Delete — process group signalling failed", gcPid)
}

func TestExec_EnvMerging(t *testing.T) {
	mgr, _ := newTestManager(t, 4)

	val := "hello-from-user"
	env := map[string]*string{"PLX_TEST_VAL": &val}

	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"sh", "-c", "echo ${PLX_TEST_VAL}"},
		Env:       env,
		TimeoutMS: 5_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	e.Wait()

	out, _ := os.ReadFile(filepath.Join(e.Dir, stdoutLog))
	if !strings.Contains(string(out), "hello-from-user") {
		t.Errorf("env merge failed: stdout = %q", string(out))
	}
}

func TestExec_EnvUnset(t *testing.T) {
	t.Setenv("PLX_PARENT_VAR", "should-be-gone")

	mgr, _ := newTestManager(t, 4)
	env := map[string]*string{"PLX_PARENT_VAR": nil}

	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"sh", "-c", "echo ${PLX_PARENT_VAR-unset}"},
		Env:       env,
		TimeoutMS: 5_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	e.Wait()

	out, _ := os.ReadFile(filepath.Join(e.Dir, stdoutLog))
	if !strings.Contains(string(out), "unset") {
		t.Errorf("expected env var to be unset, got %q", string(out))
	}
}

func TestExec_EmptyStdinClosesImmediately(t *testing.T) {
	mgr, _ := newTestManager(t, 4)
	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"cat"},
		TimeoutMS: 5_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	select {
	case <-e.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("cat did not exit on empty stdin EOF")
	}
}

func TestExec_StdinPassed(t *testing.T) {
	mgr, _ := newTestManager(t, 4)
	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"cat"},
		Stdin:     []byte("hello stdin\n"),
		TimeoutMS: 5_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	e.Wait()

	out, _ := os.ReadFile(filepath.Join(e.Dir, stdoutLog))
	if !strings.Contains(string(out), "hello stdin") {
		t.Errorf("expected stdin echo, got %q", string(out))
	}
}

func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}
