package fsapi

import (
	"fmt"
	"os"
)

type MkdirRequest struct {
	Path    string
	Parents bool
	Mode    os.FileMode
}

func Mkdir(req MkdirRequest) error {
	info, err := os.Stat(req.Path)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%w: path exists and is not a directory", ErrConflict)
		}
		if req.Parents {
			return nil
		}
		return fmt.Errorf("%w: directory already exists", ErrConflict)
	}
	if err != nil && !os.IsNotExist(err) {
		return mapOSError(err, "stat directory")
	}

	if req.Parents {
		err = os.MkdirAll(req.Path, req.Mode)
	} else {
		err = os.Mkdir(req.Path, req.Mode)
	}
	if err != nil {
		return mapOSError(err, "make directory")
	}
	return nil
}
