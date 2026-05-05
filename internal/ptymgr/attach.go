package ptymgr

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

type AttachReservation struct {
	session   *PTYSession
	released  atomic.Bool
	finalized atomic.Bool
}

type Attachment struct {
	session *PTYSession
	sub     *Subscriber
	once    sync.Once
}

func (s *PTYSession) TryReserveAttach() (*AttachReservation, error) {
	now := time.Now().UTC()
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	if s.state != StateRunning || s.removed {
		return nil, ErrGone
	}
	if s.attached || s.attaching {
		return nil, ErrAlreadyAttached
	}
	s.attaching = true
	s.lastClientActivity = now
	return &AttachReservation{session: s}, nil
}

func (r *AttachReservation) Finalize(replayBytes int) (*Attachment, error) {
	if r == nil || r.session == nil {
		return nil, ErrNotFound
	}
	if replayBytes < 0 {
		return nil, ErrInvalidRequest
	}
	if !r.finalized.CompareAndSwap(false, true) {
		return nil, ErrAlreadyAttached
	}

	s := r.session
	sub := newSubscriber()
	now := time.Now().UTC()

	s.sessionMu.Lock()
	if !s.attaching {
		s.sessionMu.Unlock()
		return nil, ErrAlreadyAttached
	}
	if s.removed {
		s.attaching = false
		s.sessionMu.Unlock()
		return nil, ErrGone
	}
	if s.ring != nil && replayBytes > s.ring.Size() {
		s.attaching = false
		s.sessionMu.Unlock()
		return nil, ErrReplayTooLarge
	}

	st := s.statusLocked()
	sub.TryPush(Frame{Kind: FrameText, Data: attachedFrame(st)})
	if s.ring != nil && replayBytes > 0 {
		for _, chunk := range s.ring.Snapshot(replayBytes) {
			sub.TryPush(Frame{Kind: FrameBinary, Data: chunk})
		}
	}

	s.attaching = false
	if s.state == StateRunning {
		s.attached = true
		s.detachedSince = nil
		s.sub = sub
		s.lastClientActivity = now
		if s.attachCounter != nil {
			s.attachCounter.Add(1)
		}
	} else {
		sub.TryPush(Frame{Kind: FrameText, Data: exitFrameFromStatus(st)})
		sub.Close()
	}
	s.sessionMu.Unlock()

	return &Attachment{session: s, sub: sub}, nil
}

func (r *AttachReservation) Release() {
	if r == nil || r.session == nil || !r.released.CompareAndSwap(false, true) {
		return
	}
	if r.finalized.Load() {
		return
	}
	s := r.session
	s.sessionMu.Lock()
	if s.attaching {
		s.attaching = false
	}
	s.sessionMu.Unlock()
}

func (a *Attachment) Frames() <-chan Frame {
	if a == nil || a.sub == nil {
		return nil
	}
	return a.sub.Frames()
}

func (a *Attachment) Release() {
	if a == nil || a.session == nil {
		return
	}
	a.once.Do(func() {
		a.session.detach(a.sub)
	})
}

func (a *Attachment) SendText(data []byte) bool {
	if a == nil || a.sub == nil {
		return false
	}
	return a.sub.TryPush(Frame{Kind: FrameText, Data: append([]byte(nil), data...)})
}

func (s *PTYSession) detach(sub *Subscriber) {
	now := time.Now().UTC()
	var closeSub *Subscriber
	decAttach := false

	s.sessionMu.Lock()
	closeSub, decAttach = s.detachLocked(sub, now)
	s.sessionMu.Unlock()

	if closeSub != nil {
		closeSub.Close()
	}
	if decAttach && s.attachCounter != nil {
		s.attachCounter.Add(-1)
	}
}

func (s *PTYSession) evictSubscriberLocked(sub *Subscriber, now time.Time) (*Subscriber, bool) {
	return s.detachLocked(sub, now)
}

func (s *PTYSession) detachLocked(sub *Subscriber, now time.Time) (*Subscriber, bool) {
	if sub == nil || s.sub != sub {
		return nil, false
	}
	s.sub = nil
	decAttach := false
	if s.attached {
		s.attached = false
		decAttach = true
	}
	if s.state == StateRunning {
		detached := now
		s.detachedSince = &detached
	}
	s.lastClientActivity = now
	return sub, decAttach
}

func (s *PTYSession) statusLocked() PTYSessionStatus {
	st := PTYSessionStatus{
		PTYID:              s.ID,
		Tag:                s.Tag,
		PID:                s.PID,
		State:              s.state,
		StartedAt:          s.StartedAt,
		Signal:             s.signal,
		DurationMS:         s.durationMS,
		Cols:               s.cols,
		Rows:               s.rows,
		LastActivity:       s.lastActivity,
		LastClientActivity: s.lastClientActivity,
		Attached:           s.attached,
	}
	if s.finishedAt != nil {
		finished := *s.finishedAt
		st.FinishedAt = &finished
	}
	if s.exitCode != nil {
		ec := *s.exitCode
		st.ExitCode = &ec
	}
	if s.detachedSince != nil {
		detached := *s.detachedSince
		st.DetachedSince = &detached
	}
	return st
}

func attachedFrame(st PTYSessionStatus) []byte {
	body := struct {
		Type  string `json:"type"`
		PTYID string `json:"pty_id"`
		PID   int    `json:"pid"`
		State string `json:"state"`
		Cols  uint16 `json:"cols"`
		Rows  uint16 `json:"rows"`
	}{
		Type:  "attached",
		PTYID: st.PTYID,
		PID:   st.PID,
		State: st.State,
		Cols:  st.Cols,
		Rows:  st.Rows,
	}
	data, err := json.Marshal(body)
	if err != nil {
		return []byte(`{"type":"attached"}`)
	}
	return data
}

func exitFrameFromStatus(st PTYSessionStatus) []byte {
	meta := exitMetadata{
		finishedAt: st.StartedAt,
		exitCode:   st.ExitCode,
		signal:     st.Signal,
		durationMS: st.DurationMS,
	}
	if st.FinishedAt != nil {
		meta.finishedAt = *st.FinishedAt
	}
	return exitFrame(meta)
}
