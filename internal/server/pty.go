package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/polyaxon/sandbox/internal/ptymgr"
)

const (
	maxPTYBodyBytes = 1 << 20
	maxPTYDimension = 1000
)

type ptyCreateRequest struct {
	Command []string           `json:"command"`
	Tag     string             `json:"tag"`
	Env     map[string]*string `json:"env"`
	Workdir string             `json:"workdir"`
	Cols    *int               `json:"cols"`
	Rows    *int               `json:"rows"`
}

func (r *ptyCreateRequest) toAllocateRequest() (ptymgr.AllocateRequest, error) {
	if err := ptymgr.ValidateTag(r.Tag); err != nil {
		return ptymgr.AllocateRequest{}, err
	}
	if len(r.Command) > 0 && r.Command[0] == "" {
		return ptymgr.AllocateRequest{}, fmt.Errorf("%w: command[0] required", ptymgr.ErrInvalidRequest)
	}
	if len(r.Command) > maxCommandElements {
		return ptymgr.AllocateRequest{}, fmt.Errorf("%w: command has %d elements, max %d",
			ptymgr.ErrInvalidRequest, len(r.Command), maxCommandElements)
	}
	total := 0
	for i, arg := range r.Command {
		if strings.ContainsRune(arg, 0) {
			return ptymgr.AllocateRequest{}, fmt.Errorf("%w: command[%d] contains NUL", ptymgr.ErrInvalidRequest, i)
		}
		total += len(arg)
	}
	if total > maxCommandTotalBytes {
		return ptymgr.AllocateRequest{}, fmt.Errorf("%w: command total length %d bytes, max %d",
			ptymgr.ErrInvalidRequest, total, maxCommandTotalBytes)
	}
	if len(r.Env) > maxEnvKeys {
		return ptymgr.AllocateRequest{}, fmt.Errorf("%w: env has %d keys, max %d",
			ptymgr.ErrInvalidRequest, len(r.Env), maxEnvKeys)
	}
	for k, v := range r.Env {
		if k == "" {
			return ptymgr.AllocateRequest{}, fmt.Errorf("%w: empty env key", ptymgr.ErrInvalidRequest)
		}
		if len(k) > maxEnvKeyBytes {
			return ptymgr.AllocateRequest{}, fmt.Errorf("%w: env key exceeds %d bytes",
				ptymgr.ErrInvalidRequest, maxEnvKeyBytes)
		}
		if strings.ContainsAny(k, "=\x00") {
			return ptymgr.AllocateRequest{}, fmt.Errorf("%w: env key contains '=' or NUL", ptymgr.ErrInvalidRequest)
		}
		if strings.HasPrefix(k, "POLYAXON_") {
			return ptymgr.AllocateRequest{}, fmt.Errorf("%w: %s", ptymgr.ErrReservedEnvKey, k)
		}
		if v != nil {
			if len(*v) > maxEnvValueBytes {
				return ptymgr.AllocateRequest{}, fmt.Errorf("%w: env[%s] value exceeds %d bytes",
					ptymgr.ErrInvalidRequest, k, maxEnvValueBytes)
			}
			if strings.ContainsRune(*v, 0) {
				return ptymgr.AllocateRequest{}, fmt.Errorf("%w: env[%s] value contains NUL",
					ptymgr.ErrInvalidRequest, k)
			}
		}
	}
	if r.Workdir != "" {
		if len(r.Workdir) > maxWorkdirBytes {
			return ptymgr.AllocateRequest{}, fmt.Errorf("%w: workdir exceeds %d bytes",
				ptymgr.ErrInvalidRequest, maxWorkdirBytes)
		}
		if strings.ContainsRune(r.Workdir, 0) {
			return ptymgr.AllocateRequest{}, fmt.Errorf("%w: workdir contains NUL", ptymgr.ErrInvalidRequest)
		}
		if !filepath.IsAbs(r.Workdir) {
			return ptymgr.AllocateRequest{}, fmt.Errorf("%w: workdir must be absolute", ptymgr.ErrInvalidRequest)
		}
		info, err := os.Stat(r.Workdir)
		if err != nil || !info.IsDir() {
			return ptymgr.AllocateRequest{}, fmt.Errorf("%w: workdir does not exist or is not a directory", ptymgr.ErrInvalidRequest)
		}
	}

	cols, err := ptyDimension(r.Cols, "cols")
	if err != nil {
		return ptymgr.AllocateRequest{}, err
	}
	rows, err := ptyDimension(r.Rows, "rows")
	if err != nil {
		return ptymgr.AllocateRequest{}, err
	}
	return ptymgr.AllocateRequest{
		Command: r.Command,
		Tag:     r.Tag,
		Env:     r.Env,
		Workdir: r.Workdir,
		Cols:    cols,
		Rows:    rows,
	}, nil
}

type ptyCreateResponse struct {
	PTYID     string    `json:"pty_id"`
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"started_at"`
	Cols      uint16    `json:"cols"`
	Rows      uint16    `json:"rows"`
	Tag       string    `json:"tag,omitempty"`
}

func (s *Server) handlePTYCreate(w http.ResponseWriter, r *http.Request) {
	var body ptyCreateRequest
	if !s.decodeJSONBody(w, r, maxPTYBodyBytes, &body) {
		return
	}
	req, err := body.toAllocateRequest()
	if err != nil {
		writePTYValidationError(w, s.log, err)
		return
	}
	session, err := s.ptyMgr.Allocate(req)
	if err != nil {
		s.writePTYStartError(w, err)
		return
	}
	st := session.Status()
	w.Header().Set("Location", "/pty/"+session.ID)
	writeJSON(w, http.StatusCreated, ptyCreateResponse{
		PTYID:     session.ID,
		PID:       session.PID,
		StartedAt: session.StartedAt,
		Cols:      st.Cols,
		Rows:      st.Rows,
		Tag:       session.Tag,
	})
}

type ptyListResponse struct {
	Sessions []ptyStatusResponse `json:"sessions"`
}

type ptyStatusResponse struct {
	PTYID              string     `json:"pty_id"`
	PID                int        `json:"pid,omitempty"`
	State              string     `json:"state"`
	StartedAt          time.Time  `json:"started_at"`
	FinishedAt         *time.Time `json:"finished_at,omitempty"`
	DurationMS         int64      `json:"duration_ms,omitempty"`
	ExitCode           *int       `json:"exit_code,omitempty"`
	Signal             string     `json:"signal,omitempty"`
	LastActivity       time.Time  `json:"last_activity"`
	LastClientActivity time.Time  `json:"last_client_activity"`
	DetachedSince      *time.Time `json:"detached_since"`
	Attached           bool       `json:"attached"`
	Cols               uint16     `json:"cols"`
	Rows               uint16     `json:"rows"`
	Tag                string     `json:"tag,omitempty"`
}

func (s *Server) handlePTYList(w http.ResponseWriter, r *http.Request) {
	tagFilter := r.URL.Query().Get("tag")
	if tagFilter != "" {
		if err := ptymgr.ValidateTag(tagFilter); err != nil {
			writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
			return
		}
	}
	statuses := s.ptyMgr.List(tagFilter)
	resp := ptyListResponse{Sessions: make([]ptyStatusResponse, 0, len(statuses))}
	for _, st := range statuses {
		resp.Sessions = append(resp.Sessions, ptyStatusFromStatus(st))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handlePTYStatus(w http.ResponseWriter, r *http.Request) {
	session, ok := s.ptyMgr.Get(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, s.log, http.StatusNotFound, CodeNotFound, "pty not found")
		return
	}
	writeJSON(w, http.StatusOK, ptyStatusFromStatus(session.Status()))
}

type ptyResizeRequest struct {
	Cols *int `json:"cols"`
	Rows *int `json:"rows"`
}

func (s *Server) handlePTYResize(w http.ResponseWriter, r *http.Request) {
	session, ok := s.ptyMgr.Get(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, s.log, http.StatusNotFound, CodeNotFound, "pty not found")
		return
	}
	if session.Status().State != ptymgr.StateRunning {
		writePTYGone(w, s.log)
		return
	}
	var body ptyResizeRequest
	if !s.decodeJSONBody(w, r, maxPTYBodyBytes, &body) {
		return
	}
	cols, err := requiredPTYDimension(body.Cols, "cols")
	if err != nil {
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	rows, err := requiredPTYDimension(body.Rows, "rows")
	if err != nil {
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
		return
	}
	if err := session.Resize(cols, rows); err != nil {
		if errors.Is(err, ptymgr.ErrGone) {
			writePTYGone(w, s.log)
			return
		}
		if errors.Is(err, ptymgr.ErrInvalidRequest) {
			writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
			return
		}
		writeError(w, s.log, http.StatusInternalServerError, CodeInternal, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type ptySignalRequest struct {
	Signal string `json:"signal"`
}

func (s *Server) handlePTYSignal(w http.ResponseWriter, r *http.Request) {
	session, ok := s.ptyMgr.Get(chi.URLParam(r, "id"))
	if !ok {
		writeError(w, s.log, http.StatusNotFound, CodeNotFound, "pty not found")
		return
	}
	if session.Status().State != ptymgr.StateRunning {
		writePTYGone(w, s.log)
		return
	}
	var body ptySignalRequest
	if !s.decodeJSONBody(w, r, maxPTYBodyBytes, &body) {
		return
	}
	if !validPTYSignal(body.Signal) {
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest,
			"signal must be one of SIGINT/SIGTERM/SIGKILL/SIGHUP/SIGQUIT/SIGUSR1/SIGUSR2")
		return
	}
	if err := session.Signal(body.Signal); err != nil {
		if errors.Is(err, ptymgr.ErrGone) {
			writePTYGone(w, s.log)
			return
		}
		if errors.Is(err, ptymgr.ErrInvalidRequest) {
			writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
			return
		}
		writeError(w, s.log, http.StatusInternalServerError, CodeInternal, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePTYDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.ptyMgr.Delete(chi.URLParam(r, "id")); err != nil {
		if errors.Is(err, ptymgr.ErrNotFound) {
			writeError(w, s.log, http.StatusNotFound, CodeNotFound, "pty not found")
			return
		}
		writeError(w, s.log, http.StatusInternalServerError, CodeInternal, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) decodeJSONBody(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, s.log, http.StatusRequestEntityTooLarge, CodePayloadTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", maxBytes))
			return false
		}
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, "malformed JSON body")
		return false
	}
	var extra struct{}
	if err := dec.Decode(&extra); err != io.EOF {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, s.log, http.StatusRequestEntityTooLarge, CodePayloadTooLarge,
				fmt.Sprintf("request body exceeds %d bytes", maxBytes))
			return false
		}
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, "body contains trailing data after JSON")
		return false
	}
	return true
}

func (s *Server) writePTYStartError(w http.ResponseWriter, err error) {
	var tagErr *ptymgr.ErrTagConflict
	switch {
	case errors.As(err, &tagErr):
		details := map[string]any{
			"tag":   tagErr.Tag,
			"state": tagErr.State,
		}
		if tagErr.ExistingID != "" {
			details["existing_id"] = tagErr.ExistingID
		}
		writeErrorWithDetails(w, s.log, http.StatusConflict, CodeConflict,
			"tag already exists", details)
	case errors.Is(err, ptymgr.ErrCapExceeded):
		w.Header().Set("Retry-After", "1")
		writeErrorWithDetails(w, s.log, http.StatusTooManyRequests, "rate_limited",
			"concurrent pty cap exceeded", map[string]any{"retry_after_ms": 1000})
	case errors.Is(err, ptymgr.ErrInvalidRequest):
		writeError(w, s.log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
	default:
		s.log.Error("start pty", "err", err)
		writeError(w, s.log, http.StatusInternalServerError, CodeInternal, "failed to start pty")
	}
}

func ptyStatusFromStatus(st ptymgr.PTYSessionStatus) ptyStatusResponse {
	return ptyStatusResponse{
		PTYID:              st.PTYID,
		PID:                st.PID,
		State:              st.State,
		StartedAt:          st.StartedAt,
		FinishedAt:         st.FinishedAt,
		DurationMS:         st.DurationMS,
		ExitCode:           st.ExitCode,
		Signal:             st.Signal,
		LastActivity:       st.LastActivity,
		LastClientActivity: st.LastClientActivity,
		DetachedSince:      st.DetachedSince,
		Attached:           st.Attached,
		Cols:               st.Cols,
		Rows:               st.Rows,
		Tag:                st.Tag,
	}
}

func ptyDimension(v *int, name string) (uint16, error) {
	if v == nil {
		return 0, nil
	}
	return requiredPTYDimension(v, name)
}

func requiredPTYDimension(v *int, name string) (uint16, error) {
	if v == nil {
		return 0, fmt.Errorf("%w: %s required", ptymgr.ErrInvalidRequest, name)
	}
	if *v < 1 || *v > maxPTYDimension {
		return 0, fmt.Errorf("%w: %s must be within [1, %d]", ptymgr.ErrInvalidRequest, name, maxPTYDimension)
	}
	return uint16(*v), nil
}

func validPTYSignal(sig string) bool {
	switch sig {
	case "SIGINT", "SIGTERM", "SIGKILL", "SIGHUP", "SIGQUIT", "SIGUSR1", "SIGUSR2":
		return true
	default:
		return false
	}
}

func writePTYValidationError(w http.ResponseWriter, log *slog.Logger, err error) {
	switch {
	case errors.Is(err, ptymgr.ErrReservedEnvKey):
		writeError(w, log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
	case errors.Is(err, ptymgr.ErrInvalidRequest):
		writeError(w, log, http.StatusBadRequest, CodeInvalidRequest, err.Error())
	default:
		writeError(w, log, http.StatusInternalServerError, CodeInternal, "validation failed")
	}
}

func writePTYGone(w http.ResponseWriter, log *slog.Logger) {
	writeError(w, log, http.StatusGone, CodeGone, "pty exited")
}
