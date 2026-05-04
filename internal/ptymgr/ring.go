package ptymgr

type RingBuffer struct {
	buf  []byte
	head int
	used int
}

func NewRing(size int) *RingBuffer {
	if size < 0 {
		size = 0
	}
	return &RingBuffer{buf: make([]byte, size)}
}

func (r *RingBuffer) Append(b []byte) {
	if r == nil || len(r.buf) == 0 || len(b) == 0 {
		return
	}
	if len(b) >= len(r.buf) {
		b = b[len(b)-len(r.buf):]
		copy(r.buf, b)
		r.head = 0
		r.used = len(r.buf)
		return
	}
	for len(b) > 0 {
		n := copy(r.buf[r.head:], b)
		r.head = (r.head + n) % len(r.buf)
		if r.used < len(r.buf) {
			r.used += n
			if r.used > len(r.buf) {
				r.used = len(r.buf)
			}
		}
		b = b[n:]
	}
}

func (r *RingBuffer) Snapshot(n int) [][]byte {
	if r == nil || n <= 0 || r.used == 0 {
		return nil
	}
	if n > r.used {
		n = r.used
	}
	start := r.head - n
	if start < 0 {
		start += len(r.buf)
	}
	if start+n <= len(r.buf) {
		out := make([]byte, n)
		copy(out, r.buf[start:start+n])
		return [][]byte{out}
	}
	firstLen := len(r.buf) - start
	first := make([]byte, firstLen)
	copy(first, r.buf[start:])
	secondLen := n - firstLen
	second := make([]byte, secondLen)
	copy(second, r.buf[:secondLen])
	return [][]byte{first, second}
}

func (r *RingBuffer) Reset() {
	if r == nil {
		return
	}
	r.head = 0
	r.used = 0
}

func (r *RingBuffer) Used() int {
	if r == nil {
		return 0
	}
	return r.used
}

func (r *RingBuffer) Size() int {
	if r == nil {
		return 0
	}
	return len(r.buf)
}
