package ptymgr

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const metaFile = "meta.json"

type Meta struct {
	PTYID     string             `json:"pty_id"`
	Tag       string             `json:"tag,omitempty"`
	Command   []string           `json:"command"`
	Env       map[string]*string `json:"env,omitempty"`
	Workdir   string             `json:"workdir"`
	PID       int                `json:"pid"`
	PGID      int                `json:"pgid"`
	Cols      uint16             `json:"cols"`
	Rows      uint16             `json:"rows"`
	StartedAt time.Time          `json:"started_at"`
}

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
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename atomic json: %w", err)
	}
	return nil
}

func readJSON(path string, v any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}
