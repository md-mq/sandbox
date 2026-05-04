package ptymgr

import "errors"

var (
	ErrNotFound        = errors.New("pty not found")
	ErrGone            = errors.New("pty exited")
	ErrAlreadyAttached = errors.New("pty already attached")
	ErrCapExceeded     = errors.New("concurrent pty cap exceeded")
	ErrInvalidRequest  = errors.New("invalid request")
	ErrReservedEnvKey  = errors.New("env key reserved for platform")
	ErrReplayTooLarge  = errors.New("replay_bytes exceeds ring size")
)

type ErrTagConflict struct {
	Tag        string
	State      string
	ExistingID string
}

func (e *ErrTagConflict) Error() string {
	if e.ExistingID == "" {
		return "tag " + e.Tag + " conflicts with " + e.State + " pty"
	}
	return "tag " + e.Tag + " conflicts with " + e.State + " pty " + e.ExistingID
}
