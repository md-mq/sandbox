package fsapi

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStat(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(file, []byte("abc"), 0o640); err != nil {
		t.Fatalf("write file: %v", err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink("file.txt", link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	fileStat, err := Stat(file)
	if err != nil {
		t.Fatalf("Stat file: %v", err)
	}
	if fileStat.Path != file || fileStat.Type != "file" || fileStat.Size != 3 || fileStat.Mode != "0640" {
		t.Fatalf("file stat = %+v, want file size 3 mode 0640", fileStat)
	}
	if fileStat.SymlinkTarget != nil {
		t.Fatalf("file symlink target = %v, want nil", *fileStat.SymlinkTarget)
	}

	dirStat, err := Stat(dir)
	if err != nil {
		t.Fatalf("Stat dir: %v", err)
	}
	if dirStat.Type != "dir" {
		t.Fatalf("dir type = %q, want dir", dirStat.Type)
	}

	linkStat, err := Stat(link)
	if err != nil {
		t.Fatalf("Stat link: %v", err)
	}
	if linkStat.Type != "symlink" || linkStat.SymlinkTarget == nil || *linkStat.SymlinkTarget != "file.txt" {
		t.Fatalf("link stat = %+v, want symlink target file.txt", linkStat)
	}
}

func TestStatMissing(t *testing.T) {
	if _, err := Stat(filepath.Join(t.TempDir(), "missing")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing err = %v, want ErrNotFound", err)
	}
}
