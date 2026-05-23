package execmgr

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	StateRunning       = "running"
	StateExited        = "exited"
	StateSignaled      = "signaled"
	StateTimedOut      = "timed_out"
	StateFailedToStart = "failed_to_start"
	StateOrphaned      = "orphaned"
)

// Meta is the on-disk launch record. Written once, never mutated.
// Env values are nullable: a nil pointer records "unset this key in the child",
// distinct from an empty string meaning "set the key but with no value".
type Meta struct {
	ExecID    string             `json:"exec_id"`
	Tag       string             `json:"tag,omitempty"`
	Command   []string           `json:"command"`
	Env       map[string]*string `json:"env,omitempty"`
	Workdir   string             `json:"workdir"`
	TimeoutMS int                `json:"timeout_ms"`
	StartedAt time.Time          `json:"started_at"`
}

// Status is the on-disk termination record. ExitCode is nullable for orphaned execs.
type Status struct {
	State      string    `json:"state"`
	ExitCode   *int      `json:"exit_code"`
	Signal     string    `json:"signal,omitempty"`
	FinishedAt time.Time `json:"finished_at"`
	DurationMS int64     `json:"duration_ms"`
}

const (
	metaFile   = "meta.json"
	statusFile = "status.json"
	pidFile    = "pid"
	stdoutLog  = "stdout.log"
	stderrLog  = "stderr.log"
)

func WriteMeta(dir string, m *Meta) error {
	return writeJSONAtomic(filepath.Join(dir, metaFile), m)
}

func ReadMeta(dir string) (*Meta, error) {
	var m Meta
	if err := readJSON(filepath.Join(dir, metaFile), &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// WriteStatusAtomic uses tmp + fsync + rename so a concurrent reader never
// sees a partially-written status.
func WriteStatusAtomic(dir string, s *Status) error {
	return writeJSONAtomic(filepath.Join(dir, statusFile), s)
}

func ReadStatus(dir string) (*Status, error) {
	var s Status
	if err := readJSON(filepath.Join(dir, statusFile), &s); err != nil {
		return nil, err
	}
	return &s, nil
}

func WritePid(dir string, pgid int) error {
	path := filepath.Join(dir, pidFile)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(f, "%d\n", pgid); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func ReadPid(dir string) (int, error) {
	raw, err := os.ReadFile(filepath.Join(dir, pidFile))
	if err != nil {
		return 0, err
	}
	var pgid int
	if _, err := fmt.Sscanf(string(raw), "%d", &pgid); err != nil {
		return 0, fmt.Errorf("parse pid: %w", err)
	}
	return pgid, nil
}

// writeJSONAtomic writes v to path via tmp + fsync + rename.
func writeJSONAtomic(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func readJSON(path string, v any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}
