package fsapi

import "errors"

var (
	ErrInvalidRequest = errors.New("invalid request")
	ErrNotFound       = errors.New("not found")
	ErrForbidden      = errors.New("permission denied")
	ErrConflict       = errors.New("conflict")
)
