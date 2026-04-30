package execmgr

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func newTestManager(t *testing.T, maxRun int) (*Manager, *atomic.Int64) {
	t.Helper()
	counter := &atomic.Int64{}
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	mgr, err := NewManager(t.TempDir(), maxRun, counter, log)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { mgr.Shutdown(context.Background()) })
	return mgr, counter
}

func TestManager_StartAndLookup(t *testing.T) {
	mgr, counter := newTestManager(t, 4)

	e, err := mgr.Start(context.Background(), StartRequest{
		Command: []string{"true"},
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if counter.Load() != 1 {
		t.Errorf("counter = %d, want 1", counter.Load())
	}
	if _, ok := mgr.Get(e.ID); !ok {
		t.Errorf("Get(%q) missing", e.ID)
	}

	<-e.Done()
	// Counter drops back to 0 after the done-watcher releases.
	waitForCounter(t, counter, 0)
}

func TestManager_CapExceeded(t *testing.T) {
	mgr, counter := newTestManager(t, 2)

	for i := 0; i < 2; i++ {
		if _, err := mgr.Start(context.Background(), StartRequest{
			Command:   []string{"sleep", "5"},
			TimeoutMS: 10_000,
		}); err != nil {
			t.Fatalf("Start %d: %v", i, err)
		}
	}
	if counter.Load() != 2 {
		t.Fatalf("counter = %d, want 2", counter.Load())
	}

	_, err := mgr.Start(context.Background(), StartRequest{Command: []string{"true"}})
	if !errors.Is(err, ErrCapExceeded) {
		t.Fatalf("Start beyond cap: err = %v, want ErrCapExceeded", err)
	}
	if counter.Load() != 2 {
		t.Errorf("counter = %d after cap rejection, want 2 unchanged", counter.Load())
	}
}

func TestManager_GetMissing(t *testing.T) {
	mgr, _ := newTestManager(t, 4)
	if _, ok := mgr.Get("does-not-exist"); ok {
		t.Errorf("Get unknown id returned ok=true")
	}
}

func TestManager_DeleteTerminatesRunning(t *testing.T) {
	mgr, counter := newTestManager(t, 4)

	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"sleep", "30"},
		TimeoutMS: 60_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if counter.Load() != 1 {
		t.Fatalf("counter = %d, want 1", counter.Load())
	}

	if err := mgr.Delete(e.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	select {
	case <-e.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("exec did not terminate after Delete")
	}

	if state := e.State(); state != StateSignaled && state != StateExited {
		// sleep usually gets SIGTERM/SIGKILL → signaled, but some shells catch and exit normally.
		t.Errorf("state = %q, want signaled or exited", state)
	}

	waitForCounter(t, counter, 0)
	if _, ok := mgr.Get(e.ID); ok {
		t.Errorf("Delete should have removed the exec from the map")
	}
}

func TestManager_DeleteMissing(t *testing.T) {
	mgr, _ := newTestManager(t, 4)
	if err := mgr.Delete("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete unknown: err = %v, want ErrNotFound", err)
	}
}

func TestManager_EmptyCommandRejected(t *testing.T) {
	mgr, _ := newTestManager(t, 4)
	_, err := mgr.Start(context.Background(), StartRequest{Command: nil})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("Start empty command: err = %v, want ErrInvalidRequest", err)
	}
}

func waitForCounter(t *testing.T, c *atomic.Int64, want int64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.Load() == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("counter = %d, want %d after 3s", c.Load(), want)
}
