package fsapi

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidatePath(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr error
	}{
		{name: "empty", raw: "", wantErr: ErrInvalidRequest},
		{name: "relative", raw: "tmp/file", wantErr: ErrInvalidRequest},
		{name: "nul", raw: "/tmp/a\x00b", wantErr: ErrInvalidRequest},
		{name: "overlong", raw: "/" + strings.Repeat("a", MaxPathBytes), wantErr: ErrInvalidRequest},
		{name: "clean absolute", raw: "/tmp/../var//x", want: filepath.Clean("/var/x")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ValidatePath(tc.raw)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidatePath: %v", err)
			}
			if got != tc.want {
				t.Fatalf("path = %q, want %q", got, tc.want)
			}
		})
	}
}
