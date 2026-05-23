package execmgr

import (
	"context"
	"errors"
	"io"
	"os"
	"time"
)

type LogStream int

const (
	LogStdout LogStream = iota
	LogStderr
)

// followPollInterval is how often ReadLog re-checks the log file when
// follow=true and there's nothing new to read. Exported-ish (lowercase but
// addressable from tests in the same package) so test runs can shorten it.
var followPollInterval = 100 * time.Millisecond

type LogRead struct {
	Data       []byte
	Offset     int64
	NextOffset int64
	EOF        bool // process done AND all bytes consumed
}

// ReadLog reads up to max bytes from stream starting at offset.
// When follow=true and nothing is available, it polls (100ms cadence) until
// one of:
//   - data arrives
//   - the exec terminates (one final read drains any remainder; EOF=true)
//   - maxWait elapses (returns empty; client should reconnect)
//   - ctx is cancelled
func (e *Exec) ReadLog(ctx context.Context, stream LogStream, offset, max int64, follow bool, maxWait time.Duration) (*LogRead, error) {
	if offset < 0 {
		return nil, errors.New("offset must be >= 0")
	}
	if max <= 0 {
		return nil, errors.New("max must be > 0")
	}

	path, err := e.streamPath(stream)
	if err != nil {
		return nil, err
	}

	deadline := time.Time{}
	if follow {
		deadline = time.Now().Add(maxWait)
	}

	for {
		data, fileSize, err := readAt(path, offset, max)
		if err != nil {
			return nil, err
		}
		res := &LogRead{
			Data:       data,
			Offset:     offset,
			NextOffset: offset + int64(len(data)),
		}
		if len(res.Data) > 0 {
			if e.isDone() && res.NextOffset == fileSize {
				res.EOF = true
			}
			return res, nil
		}

		if e.isDone() {
			res.EOF = true
			return res, nil
		}
		if !follow {
			return res, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return res, nil
		}
		sleep := followPollInterval
		if remaining < sleep {
			sleep = remaining
		}
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-e.done:
			// Drain any final bytes on the next iteration.
			continue
		case <-time.After(sleep):
		}
	}
}

// ReadAt reads up to max bytes from path starting at offset. Returns the data,
// the file size at read time (so callers can tell "at end of file" without
// re-statting), and any error. Used by both ReadLog's follow-poll and the
// server's one-shot and SSE handlers.
func ReadAt(path string, offset, max int64) ([]byte, int64, error) {
	return readAt(path, offset, max)
}

func readAt(path string, offset, max int64) ([]byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := fi.Size()

	if offset >= size {
		return nil, size, nil
	}

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, size, err
	}

	remaining := size - offset
	toRead := remaining
	if toRead > max {
		toRead = max
	}
	buf := make([]byte, toRead)
	n, err := io.ReadFull(f, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return nil, size, err
	}

	return buf[:n], size, nil
}

func (e *Exec) streamPath(s LogStream) (string, error) {
	switch s {
	case LogStdout:
		return e.stdoutPath, nil
	case LogStderr:
		return e.stderrPath, nil
	default:
		return "", errors.New("unsupported log stream")
	}
}
