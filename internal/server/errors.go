package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// ErrorCode enumerates the stable error codes returned in JSON envelopes.
// Keep in sync with memos/sandbox/plx-exec-api.md.
const (
	CodeInvalidRequest  = "invalid_request"
	CodeUnauthorized    = "unauthorized"
	CodeNotFound        = "not_found"
	CodeConflict        = "conflict"
	CodeGone            = "gone"
	CodeTimeout         = "timeout"
	CodePayloadTooLarge = "payload_too_large"
	CodeInternal        = "internal"
)

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func writeError(w http.ResponseWriter, log *slog.Logger, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(errorEnvelope{
		Error: errorBody{Code: code, Message: message},
	}); err != nil {
		log.Error("write error response", "err", err)
	}
}
