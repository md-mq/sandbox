package execmgr

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Manager owns the registry of running execs and the on-disk state dir.
type Manager struct {
	stateDir string
	maxRun   int
	counter  *atomic.Int64
	log      *slog.Logger

	mu    sync.RWMutex
	execs map[string]*Exec

	tagReservations map[string]struct{}
}

// NewManager creates stateDir if it does not exist.
func NewManager(stateDir string, maxConcurrent int, counter *atomic.Int64, log *slog.Logger) (*Manager, error) {
	if stateDir == "" {
		return nil, fmt.Errorf("stateDir required")
	}
	if maxConcurrent < 1 {
		return nil, fmt.Errorf("maxConcurrent must be >= 1, got %d", maxConcurrent)
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	return &Manager{
		stateDir:        stateDir,
		maxRun:          maxConcurrent,
		counter:         counter,
		log:             log.With("component", "execmgr"),
		execs:           make(map[string]*Exec),
		tagReservations: make(map[string]struct{}),
	}, nil
}

// Start reserves a slot, launches the process, and registers the Exec.
// Returns ErrCapExceeded if the concurrent cap is reached.
func (m *Manager) Start(ctx context.Context, req StartRequest) (*Exec, error) {
	if len(req.Command) == 0 {
		return nil, fmt.Errorf("%w: command required", ErrInvalidRequest)
	}
	if err := ValidateTag(req.Tag); err != nil {
		return nil, err
	}
	if !m.tryReserve() {
		return nil, ErrCapExceeded
	}
	tagReserved := false
	if req.Tag != "" {
		if err := m.reserveTag(req.Tag); err != nil {
			m.release()
			return nil, err
		}
		tagReserved = true
	}
	releaseTag := func() {
		if tagReserved {
			m.releaseTagReservation(req.Tag)
		}
	}

	id := uuid.Must(uuid.NewV7()).String()
	e, err := newExec(ctx, id, m.stateDir, req, m.log)
	if err != nil {
		m.release()
		releaseTag()
		_ = os.RemoveAll(filepath.Join(m.stateDir, id))
		return nil, err
	}
	if err := e.start(); err != nil {
		m.release()
		releaseTag()
		_ = os.RemoveAll(e.Dir)
		return nil, err
	}

	m.mu.Lock()
	m.execs[id] = e
	if tagReserved {
		delete(m.tagReservations, req.Tag)
	}
	m.mu.Unlock()

	// The counter is released exactly once, when the exec finishes.
	go func() {
		<-e.Done()
		m.release()
	}()

	return e, nil
}

func (m *Manager) reserveTag(tag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tagReservations[tag]; ok {
		return &ErrTagConflict{Tag: tag, State: "allocating"}
	}
	for _, e := range m.execs {
		if e.Tag == tag {
			return &ErrTagConflict{Tag: tag, State: e.State(), ExistingID: e.ID}
		}
	}
	m.tagReservations[tag] = struct{}{}
	return nil
}

func (m *Manager) releaseTagReservation(tag string) {
	m.mu.Lock()
	delete(m.tagReservations, tag)
	m.mu.Unlock()
}

// Get returns the Exec with the given id, or (nil, false) if unknown.
func (m *Manager) Get(id string) (*Exec, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.execs[id]
	return e, ok
}

type ExecSummary struct {
	ID          string
	Tag         string
	PID         int
	State       string
	StartedAt   time.Time
	FinishedAt  *time.Time
	DurationMS  int64
	ExitCode    *int
	Signal      string
	StdoutBytes int64
	StderrBytes int64
}

func (m *Manager) List(tagFilter string) []ExecSummary {
	m.mu.RLock()
	execs := make([]*Exec, 0, len(m.execs))
	for _, e := range m.execs {
		if tagFilter == "" || e.Tag == tagFilter {
			execs = append(execs, e)
		}
	}
	m.mu.RUnlock()

	sort.Slice(execs, func(i, j int) bool {
		if execs[i].StartedAt.Equal(execs[j].StartedAt) {
			return execs[i].ID < execs[j].ID
		}
		return execs[i].StartedAt.Before(execs[j].StartedAt)
	})

	out := make([]ExecSummary, 0, len(execs))
	for _, e := range execs {
		summary := ExecSummary{
			ID:          e.ID,
			Tag:         e.Tag,
			PID:         e.PID,
			State:       e.State(),
			StartedAt:   e.StartedAt,
			StdoutBytes: logSize(e.StdoutPath()),
			StderrBytes: logSize(e.StderrPath()),
		}
		if st := e.Status(); st != nil {
			finished := st.FinishedAt
			summary.FinishedAt = &finished
			summary.DurationMS = st.DurationMS
			if st.ExitCode != nil {
				ec := *st.ExitCode
				summary.ExitCode = &ec
			}
			summary.Signal = st.Signal
		}
		out = append(out, summary)
	}
	return out
}

// Delete terminates the exec and removes it from the registry. The counter is
// released by the done-watcher spawned in Start.
func (m *Manager) Delete(id string) error {
	m.mu.RLock()
	e, ok := m.execs[id]
	if !ok {
		m.mu.RUnlock()
		return ErrNotFound
	}
	m.mu.RUnlock()

	_ = e.Kill()
	if err := os.RemoveAll(e.Dir); err != nil {
		return fmt.Errorf("remove exec dir: %w", err)
	}

	m.mu.Lock()
	if cur, ok := m.execs[id]; ok && cur == e {
		delete(m.execs, id)
	}
	m.mu.Unlock()
	return nil
}

// Shutdown best-effort kills every running exec so the pod can terminate cleanly.
// Kills run in parallel and are bounded by ctx's deadline — Exec.Kill does a
// SIGTERM → 5s → SIGKILL escalation, so sequential cleanup would stall the
// shutdown for N × 5s with a saturated cap.
func (m *Manager) Shutdown(ctx context.Context) {
	m.mu.Lock()
	execs := make([]*Exec, 0, len(m.execs))
	for _, e := range m.execs {
		execs = append(execs, e)
	}
	m.mu.Unlock()

	if len(execs) == 0 {
		return
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(execs))
	for _, e := range execs {
		go func(ex *Exec) {
			defer wg.Done()
			_ = ex.Kill()
		}(e)
	}
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-ctx.Done():
		m.log.Warn("shutdown deadline reached; some execs may still be draining")
	}
}

func (m *Manager) tryReserve() bool {
	for {
		cur := m.counter.Load()
		if int(cur) >= m.maxRun {
			return false
		}
		if m.counter.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (m *Manager) release() {
	m.counter.Add(-1)
}

func logSize(path string) int64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return fi.Size()
}
