package ptymgr

import "sync/atomic"

const subscriberQueueSize = 256

type FrameKind int

const (
	FrameText FrameKind = iota + 1
	FrameBinary
)

type Frame struct {
	Kind FrameKind
	Data []byte
}

type Subscriber struct {
	out    chan Frame
	closed atomic.Bool
}

func newSubscriber() *Subscriber {
	return &Subscriber{out: make(chan Frame, subscriberQueueSize)}
}

func (s *Subscriber) TryPush(frame Frame) (ok bool) {
	if s == nil || s.closed.Load() {
		return false
	}
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	select {
	case s.out <- frame:
		return true
	default:
		return false
	}
}

func (s *Subscriber) Frames() <-chan Frame {
	if s == nil {
		return nil
	}
	return s.out
}

func (s *Subscriber) Close() {
	if s == nil {
		return
	}
	if s.closed.CompareAndSwap(false, true) {
		close(s.out)
	}
}
