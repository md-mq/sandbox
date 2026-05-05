package fsapi

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestMkdir(t *testing.T) {
	dir := t.TempDir()

	one := filepath.Join(dir, "one")
	if err := Mkdir(MkdirRequest{Path: one, Mode: 0o700}); err != nil {
		t.Fatalf("Mkdir one: %v", err)
	}
	if info, err := os.Stat(one); err != nil || !info.IsDir() {
		t.Fatalf("one was not created as dir: info=%v err=%v", info, err)
	}

	nested := filepath.Join(dir, "a", "b")
	if err := Mkdir(MkdirRequest{Path: nested, Parents: true, Mode: 0o755}); err != nil {
		t.Fatalf("Mkdir parents: %v", err)
	}
	if info, err := os.Stat(nested); err != nil || !info.IsDir() {
		t.Fatalf("nested was not created as dir: info=%v err=%v", info, err)
	}

	if err := Mkdir(MkdirRequest{Path: nested, Parents: true, Mode: 0o755}); err != nil {
		t.Fatalf("Mkdir existing parents=true: %v", err)
	}
	if err := Mkdir(MkdirRequest{Path: nested, Mode: 0o755}); !errors.Is(err, ErrConflict) {
		t.Fatalf("Mkdir existing parents=false err = %v, want ErrConflict", err)
	}
}

func TestMkdirErrors(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	if err := Mkdir(MkdirRequest{Path: file, Parents: true, Mode: 0o755}); !errors.Is(err, ErrConflict) {
		t.Fatalf("existing file err = %v, want ErrConflict", err)
	}
	if err := Mkdir(MkdirRequest{Path: filepath.Join(dir, "missing", "child"), Mode: 0o755}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing parent err = %v, want ErrNotFound", err)
	}
	if err := Mkdir(MkdirRequest{Path: filepath.Join(file, "child"), Parents: true, Mode: 0o755}); !errors.Is(err, ErrConflict) {
		t.Fatalf("file parent err = %v, want ErrConflict", err)
	}
}
