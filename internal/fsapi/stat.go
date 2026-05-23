package fsapi

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"
)

const (
	entryTypeDir     = "dir"
	entryTypeFile    = "file"
	entryTypeSymlink = "symlink"
	entryTypeOther   = "other"
)

type StatResult struct {
	Path          string    `json:"path"`
	Type          string    `json:"type"`
	Size          int64     `json:"size"`
	Mtime         time.Time `json:"mtime"`
	Mode          string    `json:"mode"`
	UID           uint32    `json:"uid"`
	GID           uint32    `json:"gid"`
	SymlinkTarget *string   `json:"symlink_target"`
}

func Stat(path string) (StatResult, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return StatResult{}, mapOSError(err, "stat path")
	}
	entry, err := entryFromInfo(path, info)
	if err != nil {
		return StatResult{}, err
	}
	uid, gid := uidGid(info)
	return StatResult{
		Path:          path,
		Type:          entry.Type,
		Size:          entry.Size,
		Mtime:         entry.Mtime,
		Mode:          entry.Mode,
		UID:           uid,
		GID:           gid,
		SymlinkTarget: symlinkPointer(entry.SymlinkTarget),
	}, nil
}

func entryFromPath(path, name string) (Entry, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return Entry{}, mapOSError(err, "stat path")
	}
	entry, err := entryFromInfo(path, info)
	if err != nil {
		return Entry{}, err
	}
	entry.Name = name
	return entry, nil
}

func entryFromInfo(path string, info os.FileInfo) (Entry, error) {
	entry := Entry{
		Name:  info.Name(),
		Type:  entryType(info),
		Size:  info.Size(),
		Mtime: info.ModTime().UTC(),
		Mode:  fmt.Sprintf("%04o", info.Mode().Perm()),
	}
	if entry.Type == entryTypeSymlink {
		target, err := os.Readlink(path)
		if err != nil {
			return Entry{}, mapOSError(err, "read symlink")
		}
		entry.SymlinkTarget = target
	}
	return entry, nil
}

func entryType(info os.FileInfo) string {
	mode := info.Mode()
	switch {
	case mode.IsDir():
		return entryTypeDir
	case mode&os.ModeSymlink != 0:
		return entryTypeSymlink
	case mode.IsRegular():
		return entryTypeFile
	default:
		return entryTypeOther
	}
}

func uidGid(info os.FileInfo) (uint32, uint32) {
	st := info.Sys().(*syscall.Stat_t)
	return st.Uid, st.Gid
}

func symlinkPointer(target string) *string {
	if target == "" {
		return nil
	}
	return &target
}

func mapOSError(err error, op string) error {
	switch {
	case err == nil:
		return nil
	case os.IsNotExist(err):
		return fmt.Errorf("%w: %s: %v", ErrNotFound, op, err)
	case os.IsPermission(err):
		return fmt.Errorf("%w: %s: %v", ErrForbidden, op, err)
	case errors.Is(err, syscall.EISDIR), errors.Is(err, syscall.ENOTDIR), errors.Is(err, syscall.EEXIST):
		return fmt.Errorf("%w: %s: %v", ErrConflict, op, err)
	case isDirectoryNotEmpty(err):
		return fmt.Errorf("%w: %s: %v", ErrConflict, op, err)
	default:
		return err
	}
}

func isDirectoryNotEmpty(err error) bool {
	return errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST)
}
