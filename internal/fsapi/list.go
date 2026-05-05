package fsapi

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Entry struct {
	Name          string    `json:"name"`
	Type          string    `json:"type"`
	Size          int64     `json:"size"`
	Mtime         time.Time `json:"mtime"`
	Mode          string    `json:"mode"`
	SymlinkTarget string    `json:"symlink_target,omitempty"`
}

type ListResult struct {
	Path      string  `json:"path"`
	Entries   []Entry `json:"entries"`
	Truncated bool    `json:"truncated"`
}

var errListTruncated = errors.New("list truncated")

func List(path string, recursive bool, maxEntries int) (ListResult, error) {
	if maxEntries <= 0 {
		return ListResult{}, fmt.Errorf("%w: max entries must be > 0", ErrInvalidRequest)
	}

	info, err := os.Stat(path)
	if err != nil {
		return ListResult{}, mapOSError(err, "stat directory")
	}
	if !info.IsDir() {
		return ListResult{}, fmt.Errorf("%w: path is not a directory; use /fs/stat for files", ErrConflict)
	}

	out := make([]Entry, 0, maxEntries+1)
	if recursive {
		err = listRecursive(path, path, "", maxEntries, &out)
	} else {
		err = listDir(path, "", maxEntries, &out)
	}
	if err != nil && !errors.Is(err, errListTruncated) {
		return ListResult{}, err
	}

	truncated := len(out) > maxEntries
	if truncated {
		out = out[:maxEntries]
	}
	return ListResult{Path: path, Entries: out, Truncated: truncated}, nil
}

func listRecursive(root, dir, rel string, maxEntries int, out *[]Entry) error {
	start := len(*out)
	if err := listDir(dir, rel, maxEntries, out); err != nil {
		return err
	}
	end := len(*out)
	for i := start; i < end; i++ {
		entry := (*out)[i]
		if entry.Type != "dir" {
			continue
		}
		if err := listRecursive(root, filepath.Join(root, entry.Name), entry.Name, maxEntries, out); err != nil {
			return err
		}
	}
	return nil
}

func listDir(dir, rel string, maxEntries int, out *[]Entry) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return mapOSError(err, "read directory")
	}
	for _, de := range entries {
		name := de.Name()
		entryName := name
		if rel != "" {
			entryName = filepath.Join(rel, name)
		}
		entry, err := entryFromPath(filepath.Join(dir, name), entryName)
		if err != nil {
			return err
		}
		*out = append(*out, entry)
		if len(*out) > maxEntries {
			return errListTruncated
		}
	}
	return nil
}
