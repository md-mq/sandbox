package fsapi

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReadAt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.txt")
	if err := os.WriteFile(path, []byte("hello world"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	res, err := ReadAt(path, 0, 5)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if string(res.Data) != "hello" {
		t.Fatalf("data = %q, want hello", string(res.Data))
	}
	if res.NextOffset != 5 {
		t.Fatalf("next_offset = %d, want 5", res.NextOffset)
	}
	if res.EOF {
		t.Fatal("EOF = true, want false")
	}
	if res.Size != 11 {
		t.Fatalf("size = %d, want 11", res.Size)
	}

	res, err = ReadAt(path, 6, 64)
	if err != nil {
		t.Fatalf("ReadAt tail: %v", err)
	}
	if string(res.Data) != "world" || res.NextOffset != 11 || !res.EOF {
		t.Fatalf("tail = (%q, %d, %v), want (world, 11, true)", string(res.Data), res.NextOffset, res.EOF)
	}

	res, err = ReadAt(path, 99, 64)
	if err != nil {
		t.Fatalf("ReadAt beyond EOF: %v", err)
	}
	if len(res.Data) != 0 || res.NextOffset != 99 || !res.EOF {
		t.Fatalf("beyond EOF = (%d bytes, %d, %v), want empty, 99, true", len(res.Data), res.NextOffset, res.EOF)
	}
}

func TestReadAtErrors(t *testing.T) {
	dir := t.TempDir()

	if _, err := ReadAt(filepath.Join(dir, "missing"), 0, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing err = %v, want ErrNotFound", err)
	}
	if _, err := ReadAt(dir, 0, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("dir err = %v, want ErrConflict", err)
	}
	if _, err := ReadAt(filepath.Join(dir, "missing"), -1, 1); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("negative offset err = %v, want ErrInvalidRequest", err)
	}
	if _, err := ReadAt(filepath.Join(dir, "missing"), 0, 0); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("zero max err = %v, want ErrInvalidRequest", err)
	}
}

func TestReadAtForbidden(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; chmod 000 does not enforce")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "secret")
	if err := os.WriteFile(path, []byte("secret"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := os.Chmod(path, 0); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })

	_, err := ReadAt(path, 0, 1)
	if err == nil {
		t.Skip("chmod 000 did not block read on this filesystem")
	}
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("err = %v, want ErrForbidden", err)
	}
}
