package auth

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFromFile(t *testing.T) {
	path := writeToken(t, "my-secret-token\n")

	a, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}
	if !a.Check("my-secret-token") {
		t.Errorf("expected token to match after whitespace trim")
	}
}

func TestLoadFromFile_Missing(t *testing.T) {
	_, err := LoadFromFile("/nope/does-not-exist")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadFromFile_Empty(t *testing.T) {
	path := writeToken(t, "   \n\t ")
	_, err := LoadFromFile(path)
	if err == nil {
		t.Fatal("expected error for empty token")
	}
}

func TestCheck(t *testing.T) {
	path := writeToken(t, "abcd1234")
	a, err := LoadFromFile(path)
	if err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}

	cases := []struct {
		name string
		got  string
		want bool
	}{
		{"exact match", "abcd1234", true},
		{"empty", "", false},
		{"wrong", "wrong", false},
		{"prefix only", "abcd", false},
		{"suffix only", "1234", false},
		{"trailing extra", "abcd1234x", false},
		{"leading extra", "xabcd1234", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.Check(tc.got); got != tc.want {
				t.Errorf("Check(%q) = %v, want %v", tc.got, got, tc.want)
			}
		})
	}
}

func TestCheck_NilAuthenticator(t *testing.T) {
	var a *Authenticator
	if a.Check("anything") {
		t.Errorf("nil Authenticator.Check should return false")
	}
}

func writeToken(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte(content), 0o400); err != nil {
		t.Fatalf("write token: %v", err)
	}
	return path
}
