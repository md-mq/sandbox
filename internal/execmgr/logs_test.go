package execmgr

import (
	"context"
	"testing"
	"time"
)

func TestReadLog_ReturnsWrittenBytes(t *testing.T) {
	mgr, _ := newTestManager(t, 4)
	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"sh", "-c", "printf abcdefghij"},
		TimeoutMS: 5_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	e.Wait()

	res, err := e.ReadLog(context.Background(), LogStdout, 0, 100, false, 0)
	if err != nil {
		t.Fatalf("ReadLog: %v", err)
	}
	if string(res.Data) != "abcdefghij" {
		t.Errorf("data = %q, want abcdefghij", string(res.Data))
	}
	if res.NextOffset != 10 {
		t.Errorf("NextOffset = %d, want 10", res.NextOffset)
	}
	if !res.EOF {
		t.Errorf("EOF = false, want true (process done, all bytes read)")
	}
}

func TestReadLog_ResumeFromOffset(t *testing.T) {
	mgr, _ := newTestManager(t, 4)
	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"sh", "-c", "printf abcdefghij"},
		TimeoutMS: 5_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	e.Wait()

	// First chunk.
	r1, err := e.ReadLog(context.Background(), LogStdout, 0, 3, false, 0)
	if err != nil {
		t.Fatalf("ReadLog #1: %v", err)
	}
	if string(r1.Data) != "abc" {
		t.Errorf("first chunk = %q, want abc", string(r1.Data))
	}

	// Resume from where the first read ended.
	r2, err := e.ReadLog(context.Background(), LogStdout, r1.NextOffset, 100, false, 0)
	if err != nil {
		t.Fatalf("ReadLog #2: %v", err)
	}
	if string(r2.Data) != "defghij" {
		t.Errorf("second chunk = %q, want defghij", string(r2.Data))
	}
	if !r2.EOF {
		t.Errorf("EOF should be true after final chunk")
	}
}

func TestReadLog_FollowReturnsOnProcessExit(t *testing.T) {
	mgr, _ := newTestManager(t, 4)
	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"sh", "-c", "sleep 0.2 && printf done"},
		TimeoutMS: 5_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// First follow call: returns either all bytes (possibly without EOF if there
	// was a race between our read and the reaper closing done), or may return
	// early with partial data. Either is acceptable under the API contract —
	// clients chase offset until EOF=true.
	res, err := e.ReadLog(context.Background(), LogStdout, 0, 100, true, 3*time.Second)
	if err != nil {
		t.Fatalf("first ReadLog: %v", err)
	}
	got := string(res.Data)

	// Keep chasing until EOF, up to a hard deadline.
	deadline := time.Now().Add(3 * time.Second)
	for !res.EOF && time.Now().Before(deadline) {
		res, err = e.ReadLog(context.Background(), LogStdout, res.NextOffset, 100, true, 500*time.Millisecond)
		if err != nil {
			t.Fatalf("follow-up ReadLog: %v", err)
		}
		got += string(res.Data)
	}
	if !res.EOF {
		t.Fatalf("never reached EOF; got %q", got)
	}
	if got != "done" {
		t.Errorf("aggregated data = %q, want done", got)
	}
}

func TestReadLog_FollowDeadline(t *testing.T) {
	mgr, _ := newTestManager(t, 4)
	// Long-running exec that emits nothing. Follow should time out per maxWait.
	e, err := mgr.Start(context.Background(), StartRequest{
		Command:   []string{"sleep", "30"},
		TimeoutMS: 60_000,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = mgr.Delete(e.ID) })

	start := time.Now()
	res, err := e.ReadLog(context.Background(), LogStdout, 0, 100, true, 200*time.Millisecond)
	if err != nil {
		t.Fatalf("ReadLog: %v", err)
	}
	if len(res.Data) != 0 {
		t.Errorf("expected empty data, got %q", string(res.Data))
	}
	if res.EOF {
		t.Errorf("EOF=true while process still running")
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond || elapsed > 2*time.Second {
		t.Errorf("follow elapsed %v, expected ~200ms", elapsed)
	}
}
