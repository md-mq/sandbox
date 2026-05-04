package ptymgr

import (
	"errors"
	"os"
	"testing"
	"time"
)

func TestMetaRoundTrip(t *testing.T) {
	dir := t.TempDir()
	envValue := "xterm-256color"
	started := time.Now().UTC().Truncate(time.Nanosecond)
	meta := &Meta{
		PTYID:     "01999999-0000-7000-8000-000000000001",
		Tag:       "shell",
		Command:   []string{"/bin/sh"},
		Env:       map[string]*string{"TERM": &envValue, "EMPTY": nil},
		Workdir:   "/",
		PID:       123,
		PGID:      123,
		Cols:      80,
		Rows:      24,
		StartedAt: started,
	}

	if err := WriteMeta(dir, meta); err != nil {
		t.Fatalf("WriteMeta: %v", err)
	}
	got, err := ReadMeta(dir)
	if err != nil {
		t.Fatalf("ReadMeta: %v", err)
	}
	if got.PTYID != meta.PTYID || got.Tag != meta.Tag || got.Command[0] != "/bin/sh" {
		t.Fatalf("roundtrip meta = %#v", got)
	}
	if got.Env["TERM"] == nil || *got.Env["TERM"] != envValue {
		t.Fatalf("TERM env = %#v", got.Env["TERM"])
	}
	if _, ok := got.Env["EMPTY"]; !ok || got.Env["EMPTY"] != nil {
		t.Fatalf("nil env not preserved: %#v", got.Env)
	}
	if !got.StartedAt.Equal(started) {
		t.Fatalf("StartedAt = %s, want %s", got.StartedAt, started)
	}
}

func TestReadMetaMissing(t *testing.T) {
	_, err := ReadMeta(t.TempDir())
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ReadMeta missing err = %v, want os.ErrNotExist", err)
	}
}
