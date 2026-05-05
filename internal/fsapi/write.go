package fsapi

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

type WriteResult struct {
	Path         string `json:"path"`
	BytesWritten int64  `json:"bytes_written"`
	Created      bool   `json:"created"`
}

func Write(path string, data []byte, mode os.FileMode, create, appendMode bool) (WriteResult, error) {
	info, statErr := os.Stat(path)
	if statErr != nil && !os.IsNotExist(statErr) {
		return WriteResult{}, mapOSError(statErr, "stat file")
	}
	if statErr == nil && info.IsDir() {
		return WriteResult{}, fmt.Errorf("%w: path is a directory", ErrConflict)
	}
	if os.IsNotExist(statErr) && !create {
		return WriteResult{}, fmt.Errorf("%w: file does not exist; pass create=true to create", ErrNotFound)
	}

	flags := os.O_WRONLY
	if create {
		flags |= os.O_CREATE
	}
	if appendMode {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}

	f, err := os.OpenFile(path, flags, mode)
	if err != nil {
		return WriteResult{}, mapOSError(err, "open file")
	}

	n, writeErr := io.Copy(f, bytes.NewReader(data))
	closeErr := f.Close()
	res := WriteResult{
		Path:         path,
		BytesWritten: n,
		Created:      os.IsNotExist(statErr),
	}
	if writeErr != nil {
		return res, mapOSError(writeErr, "write file")
	}
	if closeErr != nil {
		return res, mapOSError(closeErr, "close file")
	}
	return res, nil
}
