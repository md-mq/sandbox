package execmgr

import "errors"

// Sentinels mapped to HTTP status codes by the server layer.
var (
	ErrNotFound       = errors.New("exec not found")
	ErrCapExceeded    = errors.New("concurrent exec cap exceeded")
	ErrInvalidRequest = errors.New("invalid request")
	ErrReservedEnvKey = errors.New("env key reserved for platform")
	ErrTimeout        = errors.New("exec timed out")
)

type ErrTagConflict struct {
	Tag        string
	State      string
	ExistingID string
}

func (e *ErrTagConflict) Error() string {
	if e.ExistingID == "" {
		return "tag " + e.Tag + " conflicts with " + e.State + " exec"
	}
	return "tag " + e.Tag + " conflicts with " + e.State + " exec " + e.ExistingID
}
