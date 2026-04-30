package execmgr

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

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
		stateDir: stateDir,
		maxRun:   maxConcurrent,
		counter:  counter,
		log:      log.With("component", "execmgr"),
		execs:    make(map[string]*Exec),
	}, nil
}

// Start reserves a slot, launches the process, and registers the Exec.
// Returns ErrCapExceeded if the concurrent cap is reached.
func (m *Manager) Start(ctx context.Context, req StartRequest) (*Exec, error) {
	if len(req.Command) == 0 {
		return nil, fmt.Errorf("%w: command required", ErrInvalidRequest)
	}
	if !m.tryReserve() {
		return nil, ErrCapExceeded
	}

	id := uuid.Must(uuid.NewV7()).String()
	e, err := newExec(ctx, id, m.stateDir, req, m.log)
	if err != nil {
		m.release()
		return nil, err
	}
	if err := e.start(); err != nil {
		m.release()
		return nil, err
	}

	m.mu.Lock()
	m.execs[id] = e
	m.mu.Unlock()

	// The counter is released exactly once, when the exec finishes.
	go func() {
		<-e.Done()
		m.release()
	}()

	return e, nil
}

// Get returns the Exec with the given id, or (nil, false) if unknown.
func (m *Manager) Get(id string) (*Exec, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.execs[id]
	return e, ok
}

// Delete terminates the exec and removes it from the registry. The counter is
// released by the done-watcher spawned in Start.
func (m *Manager) Delete(id string) error {
	m.mu.Lock()
	e, ok := m.execs[id]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	delete(m.execs, id)
	m.mu.Unlock()

	_ = e.Kill()
	return nil
}

// Shutdown best-effort kills every running exec so the pod can terminate cleanly.
func (m *Manager) Shutdown(ctx context.Context) {
	m.mu.Lock()
	execs := make([]*Exec, 0, len(m.execs))
	for _, e := range m.execs {
		execs = append(execs, e)
	}
	m.mu.Unlock()

	for _, e := range execs {
		_ = e.Kill()
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
