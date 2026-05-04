package ptymgr

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateTag(t *testing.T) {
	for _, tag := range []string{"", "shell", "a.b_c-d:1", strings.Repeat("a", 128)} {
		if err := ValidateTag(tag); err != nil {
			t.Fatalf("ValidateTag(%q): %v", tag, err)
		}
	}

	for _, tag := range []string{"bad tag", "bad/tag", strings.Repeat("a", 129)} {
		err := ValidateTag(tag)
		if !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("ValidateTag(%q) err = %v, want ErrInvalidRequest", tag, err)
		}
	}
}
