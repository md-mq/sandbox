package fsapi

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteCreateOverwriteAppend(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.txt")

	res, err := Write(path, []byte("one"), 0o600, true, false)
	if err != nil {
		t.Fatalf("Write create: %v", err)
	}
	if !res.Created || res.BytesWritten != 3 {
		t.Fatalf("create result = %+v, want created with 3 bytes", res)
	}
	assertFileContent(t, path, "one")
	if mode := filePerm(t, path); mode != 0o600 {
		t.Fatalf("mode = %04o, want 0600", mode)
	}

	res, err = Write(path, []byte("two"), 0o644, false, false)
	if err != nil {
		t.Fatalf("Write overwrite: %v", err)
	}
	if res.Created {
		t.Fatal("overwrite marked Created=true")
	}
	assertFileContent(t, path, "two")
	if mode := filePerm(t, path); mode != 0o600 {
		t.Fatalf("mode after overwrite = %04o, want 0600", mode)
	}

	res, err = Write(path, []byte("-three"), 0o644, false, true)
	if err != nil {
		t.Fatalf("Write append: %v", err)
	}
	if res.BytesWritten != 6 {
		t.Fatalf("append bytes = %d, want 6", res.BytesWritten)
	}
	assertFileContent(t, path, "two-three")
}

func TestWriteErrors(t *testing.T) {
	dir := t.TempDir()

	if _, err := Write(filepath.Join(dir, "missing"), []byte("x"), 0o644, false, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing create=false err = %v, want ErrNotFound", err)
	}
	if _, err := Write(dir, []byte("x"), 0o644, true, false); !errors.Is(err, ErrConflict) {
		t.Fatalf("dir err = %v, want ErrConflict", err)
	}
	if _, err := Write(filepath.Join(dir, "no-parent", "file"), []byte("x"), 0o644, true, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing parent err = %v, want ErrNotFound", err)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(raw) != want {
		t.Fatalf("file content = %q, want %q", string(raw), want)
	}
}

func filePerm(t *testing.T, path string) os.FileMode {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}
	return info.Mode().Perm()
}
