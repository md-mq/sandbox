package ptymgr

import "sync/atomic"

const subscriberQueueSize = 256

const (
	frameText = iota + 1
	frameBinary
)

type outFrame struct {
	kind int
	data []byte
}

type Subscriber struct {
	out    chan outFrame
	closed atomic.Bool
}

func newSubscriber() *Subscriber {
	return &Subscriber{out: make(chan outFrame, subscriberQueueSize)}
}

func (s *Subscriber) TryPush(frame outFrame) (ok bool) {
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

func (s *Subscriber) Close() {
	if s == nil {
		return
	}
	if s.closed.CompareAndSwap(false, true) {
		close(s.out)
	}
}
