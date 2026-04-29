package auth

import (
	"bytes"
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
)

// Authenticator compares incoming X-POLYAXON-SANDBOX-TOKEN headers
// against a pre-loaded secret in constant time.
type Authenticator struct {
	expected []byte
}

// LoadFromFile reads a token file from disk, trims trailing whitespace,
// and returns an Authenticator. The file must contain a non-empty token.
func LoadFromFile(path string) (*Authenticator, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read token file %q: %w", path, err)
	}
	token := bytes.TrimSpace(raw)
	if len(token) == 0 {
		return nil, errors.New("token file is empty")
	}
	return &Authenticator{expected: token}, nil
}

// Check returns true iff got matches the loaded token. The comparison
// is constant-time to avoid timing attacks on token length or content.
func (a *Authenticator) Check(got string) bool {
	if a == nil {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), a.expected) == 1
}
