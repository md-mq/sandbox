package execmgr

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Recover scans stateDir and rebuilds the in-memory map. Call once before
// serving traffic.
func (m *Manager) Recover(ctx context.Context) error {
	entries, err := os.ReadDir(m.stateDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read state dir: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), "pty-") {
			continue
		}
		if err := m.recoverOne(ctx, entry.Name()); err != nil {
			m.log.Warn("recover exec", "exec_id", entry.Name(), "err", err)
		}
	}
	return nil
}

// recoverOne rebuilds a single exec from its on-disk state.
//
//	meta missing              → skip; partial dir, future GC
//	meta + no pid + no status → incomplete start; delete the dir
//	meta + pid + status       → terminal, load stored status
//	meta + pid + no status    → probe pgid; alive → orphan-reaper; dead → synthesize orphaned
func (m *Manager) recoverOne(ctx context.Context, id string) error {
	dir := filepath.Join(m.stateDir, id)

	meta, err := ReadMeta(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read meta: %w", err)
	}

	pgid, pidErr := ReadPid(dir)
	hasPid := pidErr == nil
	status, statusErr := ReadStatus(dir)
	hasStatus := statusErr == nil

	switch {
	case !hasPid && !hasStatus:
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove incomplete dir: %w", err)
		}
		return nil

	case hasStatus:
		e := m.shellExec(id, dir, meta)
		e.setState(status.State)
		e.statusMu.Lock()
		e.status = status
		e.statusMu.Unlock()
		close(e.done)
		m.mu.Lock()
		m.execs[id] = e
		m.mu.Unlock()
		return nil

	case hasPid && !hasStatus:
		if isAlive(pgid) {
			e := m.shellExec(id, dir, meta)
			e.pgid = pgid
			e.setState(StateRunning)
			// Orphans never hold a cap slot. New launches can briefly push the
			// process total above MaxExecs, but blocking fresh work on recovered
			// processes we can no longer wait(2) on is worse.
			// We can't wait(2) on a child we don't own.
			go m.orphanReaper(ctx, e, pgid)
			m.mu.Lock()
			m.execs[id] = e
			m.mu.Unlock()
			return nil
		}
		// Dead without a status.json — exit code is unknowable.
		s := &Status{
			State:      StateOrphaned,
			ExitCode:   nil,
			FinishedAt: time.Now().UTC(),
			DurationMS: 0,
		}
		if err := WriteStatusAtomic(dir, s); err != nil {
			return fmt.Errorf("write orphan status: %w", err)
		}
		e := m.shellExec(id, dir, meta)
		e.setState(StateOrphaned)
		e.statusMu.Lock()
		e.status = s
		e.statusMu.Unlock()
		close(e.done)
		m.mu.Lock()
		m.execs[id] = e
		m.mu.Unlock()
		return nil
	}

	return fmt.Errorf("unexpected recovery state")
}

// shellExec builds an Exec record from on-disk meta without a live cmd.
func (m *Manager) shellExec(id, dir string, meta *Meta) *Exec {
	return &Exec{
		ID:         id,
		Dir:        dir,
		StartedAt:  meta.StartedAt,
		TimeoutMS:  meta.TimeoutMS,
		stdoutPath: filepath.Join(dir, stdoutLog),
		stderrPath: filepath.Join(dir, stderrLog),
		done:       make(chan struct{}),
		log:        m.log.With("exec_id", id),
	}
}

// orphanReaper polls the pgid every second and finalizes the exec as orphaned
// when the group disappears. Used when plx-exec restarts and loses the
// parent/child relationship — wait(2) is no longer available.
func (m *Manager) orphanReaper(ctx context.Context, e *Exec, pgid int) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if isAlive(pgid) {
				continue
			}
			s := &Status{
				State:      StateOrphaned,
				ExitCode:   nil,
				FinishedAt: time.Now().UTC(),
				DurationMS: time.Since(e.StartedAt).Milliseconds(),
			}
			e.finalize(s)
			return
		}
	}
}

// isAlive returns true if any process in the group exists. kill(-pgid, 0) sends
// no signal but returns ESRCH when the target is gone.
func isAlive(pgid int) bool {
	if pgid <= 0 {
		return false
	}
	err := syscall.Kill(-pgid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
