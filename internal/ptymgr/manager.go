package ptymgr

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

const defaultReaperInterval = 30 * time.Second

type ManagerConfig struct {
	IdleTTL        time.Duration
	TerminalTTL    time.Duration
	ReplayBytes    int
	ReaperInterval time.Duration
}

// Manager owns durable PTY session records for the lifetime of the sandbox pod.
type Manager struct {
	stateDir string
	cfg      ManagerConfig
	maxRun   int

	runningCounter *atomic.Int64
	attachCounter  *atomic.Int64
	log            *slog.Logger

	mu              sync.RWMutex
	sessions        map[string]*PTYSession
	tagReservations map[string]struct{}

	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	shutdownOnce sync.Once
}

func NewManager(stateDir string, cfg ManagerConfig, maxConcurrent int, runningCounter, attachCounter *atomic.Int64, log *slog.Logger) (*Manager, error) {
	if stateDir == "" {
		return nil, fmt.Errorf("stateDir required")
	}
	if maxConcurrent < 1 {
		return nil, fmt.Errorf("maxConcurrent must be >= 1, got %d", maxConcurrent)
	}
	if cfg.IdleTTL <= 0 {
		return nil, fmt.Errorf("IdleTTL must be > 0, got %s", cfg.IdleTTL)
	}
	if cfg.TerminalTTL <= 0 {
		return nil, fmt.Errorf("TerminalTTL must be > 0, got %s", cfg.TerminalTTL)
	}
	if cfg.ReplayBytes < 0 {
		return nil, fmt.Errorf("ReplayBytes must be >= 0, got %d", cfg.ReplayBytes)
	}
	if cfg.ReaperInterval <= 0 {
		cfg.ReaperInterval = defaultReaperInterval
	}
	if runningCounter == nil {
		runningCounter = &atomic.Int64{}
	}
	if attachCounter == nil {
		attachCounter = &atomic.Int64{}
	}
	if log == nil {
		log = slog.Default()
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	m := &Manager{
		stateDir:        stateDir,
		cfg:             cfg,
		maxRun:          maxConcurrent,
		runningCounter:  runningCounter,
		attachCounter:   attachCounter,
		log:             log.With("component", "ptymgr"),
		sessions:        make(map[string]*PTYSession),
		tagReservations: make(map[string]struct{}),
		ctx:             ctx,
		cancel:          cancel,
	}
	m.wg.Add(2)
	go m.reaperLoop(m.reapDetachedIdle)
	go m.reaperLoop(m.reapTerminalRetained)
	return m, nil
}

// Cleanup removes stale PTY directories from a previous plx-exec process.
// It must only be called during startup, before new allocations are accepted.
func (m *Manager) Cleanup() error {
	entries, err := os.ReadDir(m.stateDir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "pty-") {
			continue
		}
		if err := os.RemoveAll(filepath.Join(m.stateDir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Allocate(req AllocateRequest) (*PTYSession, error) {
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
	s, err := newSession(id, m.stateDir, req, SessionConfig{ReplayBytes: m.cfg.ReplayBytes}, m.runningCounter, m.log)
	if err != nil {
		m.release()
		releaseTag()
		return nil, err
	}

	m.mu.Lock()
	m.sessions[id] = s
	if tagReserved {
		delete(m.tagReservations, req.Tag)
	}
	m.mu.Unlock()
	return s, nil
}

func (m *Manager) Get(id string) (*PTYSession, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[id]
	return s, ok
}

func (m *Manager) GetByTag(tag string) (*PTYSession, bool) {
	if tag == "" {
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.sessions {
		if s.Tag == tag {
			return s, true
		}
	}
	return nil, false
}

func (m *Manager) List(tagFilter string) []PTYSessionStatus {
	m.mu.RLock()
	sessions := make([]*PTYSession, 0, len(m.sessions))
	for _, s := range m.sessions {
		if tagFilter == "" || s.Tag == tagFilter {
			sessions = append(sessions, s)
		}
	}
	m.mu.RUnlock()

	sort.Slice(sessions, func(i, j int) bool {
		if sessions[i].StartedAt.Equal(sessions[j].StartedAt) {
			return sessions[i].ID < sessions[j].ID
		}
		return sessions[i].StartedAt.Before(sessions[j].StartedAt)
	})

	out := make([]PTYSessionStatus, 0, len(sessions))
	for _, s := range sessions {
		out = append(out, s.Status())
	}
	return out
}

func (m *Manager) Delete(id string) error {
	m.mu.RLock()
	s, ok := m.sessions[id]
	m.mu.RUnlock()
	if !ok {
		return ErrNotFound
	}

	if err := s.Kill(); err != nil {
		return err
	}
	m.removeSession(id, s)
	return nil
}

func (m *Manager) Shutdown(ctx context.Context) {
	m.shutdownOnce.Do(func() {
		m.cancel()
	})

	m.mu.RLock()
	sessions := make([]*PTYSession, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.RUnlock()

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(len(sessions))
	for _, s := range sessions {
		go func(session *PTYSession) {
			defer wg.Done()
			_ = session.killFor(CauseShutdown)
			m.removeSession(session.ID, session)
		}(s)
	}
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		m.log.Warn("shutdown deadline reached; some ptys may still be draining")
	}

	reapersDone := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(reapersDone)
	}()
	select {
	case <-reapersDone:
	case <-ctx.Done():
		m.log.Warn("shutdown deadline reached; pty reapers still stopping")
	}
}

func (m *Manager) reserveTag(tag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tagReservations[tag]; ok {
		return &ErrTagConflict{Tag: tag, State: "allocating"}
	}
	for _, s := range m.sessions {
		if s.Tag == tag {
			return &ErrTagConflict{Tag: tag, State: s.Status().State, ExistingID: s.ID}
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

func (m *Manager) tryReserve() bool {
	for {
		cur := m.runningCounter.Load()
		if int(cur) >= m.maxRun {
			return false
		}
		if m.runningCounter.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (m *Manager) release() {
	m.runningCounter.Add(-1)
}

func (m *Manager) removeSession(id string, s *PTYSession) {
	m.mu.RLock()
	cur, ok := m.sessions[id]
	m.mu.RUnlock()
	if !ok || cur != s {
		return
	}
	s.remove()

	m.mu.Lock()
	if cur, ok := m.sessions[id]; ok && cur == s {
		delete(m.sessions, id)
	}
	m.mu.Unlock()
}

func (m *Manager) reaperLoop(fn func(time.Time)) {
	defer m.wg.Done()
	ticker := time.NewTicker(m.cfg.ReaperInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			fn(now.UTC())
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Manager) reapDetachedIdle(now time.Time) {
	for _, s := range m.snapshotSessions() {
		st := s.Status()
		if st.State != StateRunning || st.DetachedSince == nil {
			continue
		}
		if now.Sub(*st.DetachedSince) < m.cfg.IdleTTL {
			continue
		}
		_ = s.killFor(CauseIdle)
		m.removeSession(s.ID, s)
	}
}

func (m *Manager) reapTerminalRetained(now time.Time) {
	for _, s := range m.snapshotSessions() {
		st := s.Status()
		if st.State != StateExited || st.FinishedAt == nil {
			continue
		}
		if now.Sub(*st.FinishedAt) < m.cfg.TerminalTTL {
			continue
		}
		m.removeSession(s.ID, s)
	}
}

func (m *Manager) snapshotSessions() []*PTYSession {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*PTYSession, 0, len(m.sessions))
	for _, s := range m.sessions {
		out = append(out, s)
	}
	return out
}
