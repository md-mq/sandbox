package server

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"

	"github.com/polyaxon/sandbox/internal/fsapi"
)

const (
	defaultFSReadBytes = 1 << 20
	maxFSReadBytes     = 16 << 20
	maxFSWriteBytes    = 16 << 20
	defaultFSLsEntries = 1000
	maxFSLsEntries     = 10000
)

func (s *Server) handleFSRead(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	path, err := fsapi.ValidatePath(q.Get("path"))
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	offset, err := parseFSInt64(q.Get("offset"), 0)
	if err != nil || offset < 0 {
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, "offset must be >= 0")
		return
	}
	length, err := parseFSInt64(q.Get("length"), defaultFSReadBytes)
	if err != nil || length <= 0 || length > maxFSReadBytes {
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest,
			fmt.Sprintf("length must be within [1, %d]", maxFSReadBytes))
		return
	}

	res, err := fsapi.ReadAt(path, offset, length)
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(res.Data)))
	w.Header().Set("X-Polyaxon-Next-Offset", strconv.FormatInt(res.NextOffset, 10))
	w.Header().Set("X-Polyaxon-Eof", strconv.FormatBool(res.EOF))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(res.Data)
}

func (s *Server) handleFSWrite(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	path, err := fsapi.ValidatePath(q.Get("path"))
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	mode, err := parseMode(q.Get("mode"), 0o644)
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	create, err := parseFSBool(q.Get("create"), true)
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	appendMode, err := parseFSBool(q.Get("append"), false)
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxFSWriteBytes)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, s.log, http.StatusRequestEntityTooLarge, CodePayloadTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", maxFSWriteBytes))
			return
		}
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, "read body failed")
		return
	}

	res, err := fsapi.Write(path, data, mode, create, appendMode)
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleFSList(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	path, err := fsapi.ValidatePath(q.Get("path"))
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	recursive, err := parseFSBool(q.Get("recursive"), false)
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	maxEntries, err := parseFSInt(q.Get("max_entries"), defaultFSLsEntries)
	if err != nil || maxEntries <= 0 || maxEntries > maxFSLsEntries {
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest,
			fmt.Sprintf("max_entries must be within [1, %d]", maxFSLsEntries))
		return
	}

	res, err := fsapi.List(path, recursive, maxEntries)
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

type fsMkdirRequest struct {
	Path    string `json:"path"`
	Parents bool   `json:"parents"`
	Mode    string `json:"mode"`
}

type fsPathResponse struct {
	Path string `json:"path"`
}

func (s *Server) handleFSMkdir(w http.ResponseWriter, r *http.Request) {
	var body fsMkdirRequest
	if !s.decodeJSONBody(w, r, &body) {
		return
	}
	path, err := fsapi.ValidatePath(body.Path)
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	mode, err := parseMode(body.Mode, 0o755)
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	if err := fsapi.Mkdir(fsapi.MkdirRequest{
		Path:    path,
		Parents: body.Parents,
		Mode:    mode,
	}); err != nil {
		writeFSError(w, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, fsPathResponse{Path: path})
}

func (s *Server) handleFSRemove(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	path, err := fsapi.ValidatePath(q.Get("path"))
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	recursive, err := parseFSBool(q.Get("recursive"), false)
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	if err := fsapi.Remove(path, recursive); err != nil {
		writeFSError(w, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, fsPathResponse{Path: path})
}

func (s *Server) handleFSStat(w http.ResponseWriter, r *http.Request) {
	path, err := fsapi.ValidatePath(r.URL.Query().Get("path"))
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	res, err := fsapi.Stat(path)
	if err != nil {
		writeFSError(w, s.log, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func writeFSError(w http.ResponseWriter, log *slog.Logger, err error) {
	switch {
	case errors.Is(err, fsapi.ErrInvalidRequest):
		writeError(w, log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
	case errors.Is(err, fsapi.ErrNotFound):
		writeError(w, log, http.StatusNotFound, CodeNotFound, err.Error())
	case errors.Is(err, fsapi.ErrForbidden):
		writeError(w, log, http.StatusForbidden, CodeForbidden, err.Error())
	case errors.Is(err, fsapi.ErrConflict):
		writeError(w, log, http.StatusConflict, CodeConflict, err.Error())
	default:
		log.Error("fs operation failed", "err", err)
		writeError(w, log, http.StatusInternalServerError, CodeInternal, err.Error())
	}
}

func parseFSBool(raw string, def bool) (bool, error) {
	if raw == "" {
		return def, nil
	}
	switch raw {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("%w: boolean query parameter must be true or false", fsapi.ErrInvalidRequest)
	}
}

func parseFSInt64(raw string, def int64) (int64, error) {
	if raw == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: integer query parameter required", fsapi.ErrInvalidRequest)
	}
	return n, nil
}

func parseFSInt(raw string, def int) (int, error) {
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: integer query parameter required", fsapi.ErrInvalidRequest)
	}
	return n, nil
}

func parseMode(raw string, defaultMode os.FileMode) (os.FileMode, error) {
	if raw == "" {
		return defaultMode, nil
	}
	n, err := strconv.ParseUint(raw, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("%w: mode must be octal", fsapi.ErrInvalidRequest)
	}
	if n > 0o777 {
		return 0, fmt.Errorf("%w: mode must be within [0, 0777]", fsapi.ErrInvalidRequest)
	}
	return os.FileMode(n), nil
}
