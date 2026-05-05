package fsapi

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRemove(t *testing.T) {
	dir := t.TempDir()

	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := Remove(file, false); err != nil {
		t.Fatalf("Remove file: %v", err)
	}
	if _, err := os.Stat(file); !os.IsNotExist(err) {
		t.Fatalf("file still exists after remove: %v", err)
	}

	emptyDir := filepath.Join(dir, "empty")
	if err := os.Mkdir(emptyDir, 0o755); err != nil {
		t.Fatalf("mkdir empty: %v", err)
	}
	if err := Remove(emptyDir, false); err != nil {
		t.Fatalf("Remove empty dir: %v", err)
	}

	tree := filepath.Join(dir, "tree")
	if err := os.Mkdir(tree, 0o755); err != nil {
		t.Fatalf("mkdir tree: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tree, "child"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write child: %v", err)
	}
	if err := Remove(tree, false); !errors.Is(err, ErrConflict) {
		t.Fatalf("Remove non-empty err = %v, want ErrConflict", err)
	}
	if err := Remove(tree, true); err != nil {
		t.Fatalf("Remove recursive: %v", err)
	}
	if _, err := os.Stat(tree); !os.IsNotExist(err) {
		t.Fatalf("tree still exists after recursive remove: %v", err)
	}
}

func TestRemoveMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")
	if err := Remove(path, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing non-recursive err = %v, want ErrNotFound", err)
	}
	if err := Remove(path, true); err != nil {
		t.Fatalf("missing recursive err = %v, want nil", err)
	}
}
