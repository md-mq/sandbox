package server

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
	"time"
)

// Counters holds runtime activity counters exposed by /ping.
// Fields are swapped to real exec/PTY managers in later phases; for 2A they stay at zero.
type Counters struct {
	ExecsRunning atomic.Int64
	PTYsAttached atomic.Int64
	lastActivity atomic.Int64 // unix nanoseconds
}

// Touch records the last time a non-/ping request served.
func (c *Counters) Touch(t time.Time) {
	c.lastActivity.Store(t.UnixNano())
}

func (c *Counters) LastActivity() time.Time {
	ns := c.lastActivity.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}

type pingResponse struct {
	Status       string `json:"status"`
	Version      string `json:"version"`
	UptimeMS     int64  `json:"uptime_ms"`
	LastActivity string `json:"last_activity"`
	ExecsRunning int64  `json:"execs_running"`
	PTYsAttached int64  `json:"ptys_attached"`
}

func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	last := s.counters.LastActivity()
	if last.IsZero() {
		last = s.start
	}
	resp := pingResponse{
		Status:       "ok",
		Version:      s.version,
		UptimeMS:     time.Since(s.start).Milliseconds(),
		LastActivity: last.Format(time.RFC3339Nano),
		ExecsRunning: s.counters.ExecsRunning.Load(),
		PTYsAttached: s.counters.PTYsAttached.Load(),
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.log.Error("encode ping response", "err", err)
	}
}
