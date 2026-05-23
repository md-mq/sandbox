package fsapi

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestListNonRecursive(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "file.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.Symlink("file.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	res, err := List(dir, false, 10)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if res.Path != dir || res.Truncated {
		t.Fatalf("result path/truncated = %q/%v, want %q/false", res.Path, res.Truncated, dir)
	}

	byName := entriesByName(res.Entries)
	if byName["file.txt"].Type != entryTypeFile {
		t.Fatalf("file type = %q, want file", byName["file.txt"].Type)
	}
	if byName["sub"].Type != entryTypeDir {
		t.Fatalf("sub type = %q, want dir", byName["sub"].Type)
	}
	if byName["link"].Type != entryTypeSymlink || byName["link"].SymlinkTarget != "file.txt" {
		t.Fatalf("link entry = %+v, want symlink to file.txt", byName["link"])
	}
}

func TestListRecursive(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "a", "b"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a", "b", "leaf.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write leaf: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "root.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write root: %v", err)
	}

	res, err := List(dir, true, 10)
	if err != nil {
		t.Fatalf("List recursive: %v", err)
	}
	got := entryNames(res.Entries)
	want := []string{"a", filepath.Join("a", "b"), filepath.Join("a", "b", "leaf.txt"), "root.txt"}
	if !sameStrings(got, want) {
		t.Fatalf("names = %v, want %v", got, want)
	}
}

func TestListTruncation(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 12; i++ {
		if err := os.WriteFile(filepath.Join(dir, string(rune('a'+i))), nil, 0o644); err != nil {
			t.Fatalf("write file %d: %v", i, err)
		}
	}

	res, err := List(dir, false, 10)
	if err != nil {
		t.Fatalf("List truncated: %v", err)
	}
	if len(res.Entries) != 10 || !res.Truncated {
		t.Fatalf("truncated result = %d/%v, want 10/true", len(res.Entries), res.Truncated)
	}

	res, err = List(dir, false, 12)
	if err != nil {
		t.Fatalf("List exact: %v", err)
	}
	if len(res.Entries) != 12 || res.Truncated {
		t.Fatalf("exact result = %d/%v, want 12/false", len(res.Entries), res.Truncated)
	}
}

func TestListRootSymlinkToDir(t *testing.T) {
	parent := t.TempDir()
	realDir := filepath.Join(parent, "real")
	if err := os.Mkdir(realDir, 0o755); err != nil {
		t.Fatalf("mkdir real: %v", err)
	}
	if err := os.WriteFile(filepath.Join(realDir, "child"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write child: %v", err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(realDir, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	res, err := List(link, false, 10)
	if err != nil {
		t.Fatalf("List root symlink: %v", err)
	}
	if _, ok := entriesByName(res.Entries)["child"]; !ok {
		t.Fatalf("entries = %v, want child through root symlink", entryNames(res.Entries))
	}
}

func TestListErrors(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	tests := []struct {
		name    string
		path    string
		max     int
		wantErr error
	}{
		{name: "missing", path: filepath.Join(dir, "missing"), max: 1, wantErr: ErrNotFound},
		{name: "file", path: file, max: 1, wantErr: ErrConflict},
		{name: "bad max", path: dir, max: 0, wantErr: ErrInvalidRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := List(tc.path, false, tc.max)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func entriesByName(entries []Entry) map[string]Entry {
	out := make(map[string]Entry, len(entries))
	for _, entry := range entries {
		out[entry.Name] = entry
	}
	return out
}

func entryNames(entries []Entry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.Name)
	}
	sort.Strings(out)
	return out
}

func sameStrings(a, b []string) bool {
	sort.Strings(a)
	sort.Strings(b)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
