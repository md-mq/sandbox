package fsapi

import (
	"fmt"
	"os"
)

func Remove(path string, recursive bool) error {
	if recursive {
		if _, err := os.Lstat(path); err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return mapOSError(err, "stat path")
		}
		if err := os.RemoveAll(path); err != nil {
			return mapOSError(err, "remove tree")
		}
		return nil
	}

	if err := os.Remove(path); err != nil {
		if isDirectoryNotEmpty(err) {
			return fmt.Errorf("%w: directory not empty; pass recursive=true to remove tree", ErrConflict)
		}
		return mapOSError(err, "remove path")
	}
	return nil
}
