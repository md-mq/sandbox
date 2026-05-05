package fsapi

import (
	"errors"
	"fmt"
	"io"
	"os"
)

type ReadResult struct {
	Data       []byte
	NextOffset int64
	EOF        bool
	Size       int64
}

func ReadAt(path string, offset, max int64) (ReadResult, error) {
	if offset < 0 {
		return ReadResult{}, fmt.Errorf("%w: offset must be >= 0", ErrInvalidRequest)
	}
	if max <= 0 {
		return ReadResult{}, fmt.Errorf("%w: max must be > 0", ErrInvalidRequest)
	}

	f, err := os.Open(path)
	if err != nil {
		return ReadResult{}, mapOSError(err, "open file")
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return ReadResult{}, mapOSError(err, "stat file")
	}
	if info.IsDir() {
		return ReadResult{}, fmt.Errorf("%w: path is a directory", ErrConflict)
	}

	size := info.Size()
	if offset >= size {
		return ReadResult{NextOffset: offset, EOF: true, Size: size}, nil
	}

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return ReadResult{}, mapOSError(err, "seek file")
	}

	toRead := size - offset
	if toRead > max {
		toRead = max
	}
	buf := make([]byte, toRead)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return ReadResult{}, mapOSError(err, "read file")
	}

	next := offset + int64(n)
	eof := next >= size
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		eof = true
	}
	return ReadResult{
		Data:       buf[:n],
		NextOffset: next,
		EOF:        eof,
		Size:       size,
	}, nil
}
