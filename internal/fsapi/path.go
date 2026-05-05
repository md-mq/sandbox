package fsapi

import (
	"fmt"
	"path/filepath"
	"strings"
)

const MaxPathBytes = 4096

func ValidatePath(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("%w: path required", ErrInvalidRequest)
	}
	if !filepath.IsAbs(raw) {
		return "", fmt.Errorf("%w: path must be absolute", ErrInvalidRequest)
	}
	if strings.ContainsRune(raw, 0) {
		return "", fmt.Errorf("%w: path contains NUL", ErrInvalidRequest)
	}
	if len(raw) > MaxPathBytes {
		return "", fmt.Errorf("%w: path exceeds %d bytes", ErrInvalidRequest, MaxPathBytes)
	}
	return filepath.Clean(raw), nil
}
