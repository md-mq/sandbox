package server

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/polyaxon/sandbox/internal/ptymgr"
)

func TestPTYWS_AttachWriteDetach(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	created := createPTY(t, base, `{"command":["/bin/sh"]}`)
	t.Cleanup(func() {
		doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
	})

	conn, resp, err := dialPTYWS(base, created.PTYID, "")
	if err != nil {
		t.Fatalf("dial: %v status=%v", err, responseStatus(resp))
	}
	attached := readWSText(t, conn, 3*time.Second)
	if attached["type"] != "attached" || attached["pty_id"] != created.PTYID {
		t.Fatalf("attached frame = %#v", attached)
	}
	if st := getPTYStatus(t, base, created.PTYID); !st.Attached || st.DetachedSince != nil {
		t.Fatalf("status after attach = %#v", st)
	}

	if err := conn.WriteMessage(websocket.BinaryMessage, []byte("printf ws-ok\\n\n")); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	readWSBinaryContains(t, conn, "ws-ok", 3*time.Second)
	if err := conn.Close(); err != nil {
		t.Fatalf("close ws: %v", err)
	}
	waitPTYDetached(t, base, created.PTYID)
	if st := getPTYStatus(t, base, created.PTYID); st.State != ptymgr.StateRunning {
		t.Fatalf("state after detach = %q, want running", st.State)
	}
}

func TestPTYWS_ReplayBytes(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	created := createPTY(t, base, `{"command":["sh","-c","printf before; sleep 30"]}`)
	t.Cleanup(func() {
		doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
	})
	waitPTYReplayReady(t, s, created.PTYID, 6)

	conn, resp, err := dialPTYWS(base, created.PTYID, "replay_bytes=6")
	if err != nil {
		t.Fatalf("dial: %v status=%v", err, responseStatus(resp))
	}
	defer conn.Close()
	attached := readWSText(t, conn, 3*time.Second)
	if attached["type"] != "attached" {
		t.Fatalf("first frame = %#v, want attached", attached)
	}
	mt, data, err := readWSMessage(t, conn, 3*time.Second)
	if err != nil {
		t.Fatalf("read replay: %v", err)
	}
	if mt != websocket.BinaryMessage || string(data) != "before" {
		t.Fatalf("replay = type %d %q, want binary before", mt, string(data))
	}
}

func TestPTYWS_AlreadyAttachedConflict(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	created := createPTY(t, base, `{"command":["sleep","30"]}`)
	t.Cleanup(func() {
		doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
	})

	conn, resp, err := dialPTYWS(base, created.PTYID, "")
	if err != nil {
		t.Fatalf("first dial: %v status=%v", err, responseStatus(resp))
	}
	defer conn.Close()
	readWSText(t, conn, 3*time.Second)

	second, resp, err := dialPTYWS(base, created.PTYID, "")
	if err == nil {
		second.Close()
		t.Fatal("second dial succeeded, want 409")
	}
	if resp == nil || resp.StatusCode != http.StatusConflict {
		t.Fatalf("second dial status = %v, want 409", responseStatus(resp))
	}
}

func TestPTYWS_ControlResizeAndSignalError(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	created := createPTY(t, base, `{"command":["sleep","30"]}`)
	t.Cleanup(func() {
		doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
	})

	conn, resp, err := dialPTYWS(base, created.PTYID, "")
	if err != nil {
		t.Fatalf("dial: %v status=%v", err, responseStatus(resp))
	}
	defer conn.Close()
	readWSText(t, conn, 3*time.Second)

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"resize","cols":111,"rows":33}`)); err != nil {
		t.Fatalf("write resize: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := getPTYStatus(t, base, created.PTYID)
		if st.Cols == 111 && st.Rows == 33 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	st := getPTYStatus(t, base, created.PTYID)
	if st.Cols != 111 || st.Rows != 33 {
		t.Fatalf("size = %dx%d, want 111x33", st.Cols, st.Rows)
	}

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"signal","signal":"SIGWINCH"}`)); err != nil {
		t.Fatalf("write invalid signal: %v", err)
	}
	msg := readWSText(t, conn, 3*time.Second)
	if msg["type"] != "error" {
		t.Fatalf("control error frame = %#v", msg)
	}
}

func TestPTYWS_AttachExitedReturnsGone(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	created := createPTY(t, base, `{"command":["sh","-c","exit 0"]}`)
	waitPTYState(t, base, created.PTYID, ptymgr.StateExited)
	t.Cleanup(func() {
		doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
	})

	conn, resp, err := dialPTYWS(base, created.PTYID, "")
	if err == nil {
		conn.Close()
		t.Fatal("dial exited succeeded, want 410")
	}
	if resp == nil || resp.StatusCode != http.StatusGone {
		t.Fatalf("dial exited status = %v, want 410", responseStatus(resp))
	}
}

func TestPTYWS_ExitWhileAttachedSendsExited(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	created := createPTY(t, base, `{"command":["sh","-c","sleep 0.1; printf done; exit 0"]}`)
	t.Cleanup(func() {
		doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
	})

	conn, resp, err := dialPTYWS(base, created.PTYID, "")
	if err != nil {
		t.Fatalf("dial: %v status=%v", err, responseStatus(resp))
	}
	defer conn.Close()
	attached := readWSText(t, conn, 3*time.Second)
	if attached["type"] != "attached" {
		t.Fatalf("first frame = %#v, want attached", attached)
	}

	sawDone := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mt, data, err := readWSMessage(t, conn, time.Until(deadline))
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		switch mt {
		case websocket.BinaryMessage:
			if strings.Contains(string(data), "done") {
				sawDone = true
			}
		case websocket.TextMessage:
			var msg map[string]any
			if err := json.Unmarshal(data, &msg); err != nil {
				t.Fatalf("decode text frame %q: %v", string(data), err)
			}
			if msg["type"] == "exited" {
				if !sawDone {
					t.Fatal("exited arrived before final output")
				}
				return
			}
		}
	}
	t.Fatal("did not receive exited frame")
}

func TestPTYWS_InvalidReplay(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	created := createPTY(t, base, `{"command":["sleep","30"]}`)
	t.Cleanup(func() {
		doJSON(t, http.MethodDelete, base+"/pty/"+created.PTYID, "").Body.Close()
	})

	conn, resp, err := dialPTYWS(base, created.PTYID, "replay_bytes=4097")
	if err == nil {
		conn.Close()
		t.Fatal("dial invalid replay succeeded, want 400")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %v, want 400", responseStatus(resp))
	}
}

func dialPTYWS(base, id, rawQuery string) (*websocket.Conn, *http.Response, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, nil, err
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}
	u.Path = "/pty/" + id + "/ws"
	u.RawQuery = rawQuery
	header := http.Header{}
	header.Set(headerSandboxToken, testToken)
	return websocket.DefaultDialer.Dial(u.String(), header)
}

func readWSText(t *testing.T, conn *websocket.Conn, timeout time.Duration) map[string]any {
	t.Helper()
	mt, data, err := readWSMessage(t, conn, timeout)
	if err != nil {
		t.Fatalf("read text: %v", err)
	}
	if mt != websocket.TextMessage {
		t.Fatalf("message type = %d, want text", mt)
	}
	var msg map[string]any
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("decode text frame %q: %v", string(data), err)
	}
	return msg
}

func readWSBinaryContains(t *testing.T, conn *websocket.Conn, needle string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got strings.Builder
	for time.Now().Before(deadline) {
		mt, data, err := readWSMessage(t, conn, time.Until(deadline))
		if err != nil {
			t.Fatalf("read binary: %v", err)
		}
		if mt != websocket.BinaryMessage {
			continue
		}
		got.Write(data)
		if strings.Contains(got.String(), needle) {
			return
		}
	}
	t.Fatalf("binary stream = %q, want to contain %q", got.String(), needle)
}

func readWSMessage(t *testing.T, conn *websocket.Conn, timeout time.Duration) (int, []byte, error) {
	t.Helper()
	if timeout <= 0 {
		timeout = time.Millisecond
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return 0, nil, err
	}
	return conn.ReadMessage()
}

func waitPTYDetached(t *testing.T, base, id string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := getPTYStatus(t, base, id)
		if !st.Attached && st.DetachedSince != nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("pty %s did not detach", id)
}

func waitPTYReplayReady(t *testing.T, s *Server, id string, n int) {
	t.Helper()
	session, ok := s.ptyMgr.Get(id)
	if !ok {
		t.Fatalf("pty %s missing", id)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		parts, err := session.Replay(n)
		if err != nil {
			t.Fatalf("Replay: %v", err)
		}
		total := 0
		for _, part := range parts {
			total += len(part)
		}
		if total >= n {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("pty %s replay did not reach %d bytes", id, n)
}

func responseStatus(resp *http.Response) any {
	if resp == nil {
		return nil
	}
	return resp.StatusCode
}
