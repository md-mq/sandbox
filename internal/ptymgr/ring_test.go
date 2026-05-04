package ptymgr

import (
	"bytes"
	"testing"
)

func TestRingBufferSnapshot(t *testing.T) {
	tests := []struct {
		name    string
		size    int
		appends []string
		n       int
		want    string
	}{
		{name: "empty", size: 4, n: 2, want: ""},
		{name: "partial", size: 8, appends: []string{"abc"}, n: 8, want: "abc"},
		{name: "exact", size: 3, appends: []string{"abc"}, n: 3, want: "abc"},
		{name: "wrap", size: 5, appends: []string{"abc", "def"}, n: 5, want: "bcdef"},
		{name: "last n", size: 8, appends: []string{"abcdefgh"}, n: 3, want: "fgh"},
		{name: "oversized append", size: 5, appends: []string{"0123456789"}, n: 5, want: "56789"},
		{name: "disabled", size: 0, appends: []string{"abc"}, n: 5, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRing(tt.size)
			for _, b := range tt.appends {
				r.Append([]byte(b))
			}
			if got := join(r.Snapshot(tt.n)); string(got) != tt.want {
				t.Fatalf("snapshot = %q, want %q", string(got), tt.want)
			}
		})
	}
}

func TestRingBufferSnapshotCopies(t *testing.T) {
	r := NewRing(4)
	r.Append([]byte("abcd"))
	snap := r.Snapshot(4)
	if got := string(join(snap)); got != "abcd" {
		t.Fatalf("snapshot = %q, want abcd", got)
	}
	r.Append([]byte("WXYZ"))
	if got := string(join(snap)); got != "abcd" {
		t.Fatalf("snapshot mutated after append: %q", got)
	}
}

func TestRingBufferReset(t *testing.T) {
	r := NewRing(4)
	r.Append([]byte("abcd"))
	if r.Used() != 4 {
		t.Fatalf("Used = %d, want 4", r.Used())
	}
	r.Reset()
	if r.Used() != 0 {
		t.Fatalf("Used after reset = %d, want 0", r.Used())
	}
	if got := join(r.Snapshot(4)); len(got) != 0 {
		t.Fatalf("snapshot after reset = %q, want empty", string(got))
	}
}

func join(parts [][]byte) []byte {
	return bytes.Join(parts, nil)
}
