package server

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestSSHTunnel_BinaryRoundTrip(t *testing.T) {
	target, stop := runEchoTCPServer(t)
	defer stop()

	s := newTestServer(t, false)
	s.cfg.SSHTarget = target
	base := runHTTPServer(t, s)

	conn, resp, err := dialSSHTunnelWS(base, true)
	if err != nil {
		t.Fatalf("dial: %v status=%v", err, responseStatus(resp))
	}
	defer func() { _ = conn.Close() }()

	payload := []byte("SSH-2.0-test\r\n")
	if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	mt, data, err := readWSMessage(t, conn, 3*time.Second)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if mt != websocket.BinaryMessage || !bytes.Equal(data, payload) {
		t.Fatalf("message = type %d %q, want binary %q", mt, string(data), string(payload))
	}
}

func TestSSHTunnel_RequiresAuth(t *testing.T) {
	target, stop := runEchoTCPServer(t)
	defer stop()

	s := newTestServer(t, false)
	s.cfg.SSHTarget = target
	base := runHTTPServer(t, s)

	conn, resp, err := dialSSHTunnelWS(base, false)
	if err == nil {
		_ = conn.Close()
		t.Fatal("dial without auth succeeded, want 401")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %v, want 401", responseStatus(resp))
	}
}

func TestSSHTunnel_TextFrameCloses(t *testing.T) {
	target, stop := runEchoTCPServer(t)
	defer stop()

	s := newTestServer(t, false)
	s.cfg.SSHTarget = target
	base := runHTTPServer(t, s)

	conn, resp, err := dialSSHTunnelWS(base, true)
	if err != nil {
		t.Fatalf("dial: %v status=%v", err, responseStatus(resp))
	}
	defer func() { _ = conn.Close() }()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("nope")); err != nil {
		t.Fatalf("write text: %v", err)
	}
	_, _, err = readWSMessage(t, conn, 3*time.Second)
	if err == nil {
		t.Fatal("read after text frame succeeded, want close")
	}
	var closeErr *websocket.CloseError
	if !websocket.IsCloseError(err, websocket.CloseUnsupportedData) {
		if !asCloseError(err, &closeErr) {
			t.Fatalf("error = %v, want close error", err)
		}
		t.Fatalf("close code = %d, want %d", closeErr.Code, websocket.CloseUnsupportedData)
	}
}

func TestSSHTunnel_DialFailureClosesWebsocket(t *testing.T) {
	target := closedTCPAddress(t)
	s := newTestServer(t, false)
	s.cfg.SSHTarget = target
	base := runHTTPServer(t, s)

	conn, resp, err := dialSSHTunnelWS(base, true)
	if err != nil {
		t.Fatalf("dial: %v status=%v", err, responseStatus(resp))
	}
	defer func() { _ = conn.Close() }()

	_, _, err = readWSMessage(t, conn, 3*time.Second)
	if err == nil {
		t.Fatal("read after failed target dial succeeded, want close")
	}
	if !websocket.IsCloseError(err, websocket.CloseTryAgainLater) {
		t.Fatalf("error = %v, want close code %d", err, websocket.CloseTryAgainLater)
	}
}

func TestSSHTunnel_UpstreamCloseClosesWebsocket(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	accepted := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		close(accepted)
		_ = conn.Close()
	}()

	s := newTestServer(t, false)
	s.cfg.SSHTarget = ln.Addr().String()
	base := runHTTPServer(t, s)

	conn, resp, err := dialSSHTunnelWS(base, true)
	if err != nil {
		t.Fatalf("dial: %v status=%v", err, responseStatus(resp))
	}
	defer func() { _ = conn.Close() }()
	<-accepted

	_, _, err = readWSMessage(t, conn, 3*time.Second)
	if err == nil {
		t.Fatal("read after upstream close succeeded, want close")
	}
	if !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
		t.Fatalf("error = %v, want normal close", err)
	}
}

func TestSSHTunnel_ClientCloseClosesUpstream(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	closed := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, conn)
		_ = conn.Close()
		close(closed)
	}()

	s := newTestServer(t, false)
	s.cfg.SSHTarget = ln.Addr().String()
	base := runHTTPServer(t, s)

	conn, resp, err := dialSSHTunnelWS(base, true)
	if err != nil {
		t.Fatalf("dial: %v status=%v", err, responseStatus(resp))
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close websocket: %v", err)
	}

	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream TCP connection did not close")
	}
}

func dialSSHTunnelWS(base string, authenticated bool) (*websocket.Conn, *http.Response, error) {
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
	u.Path = "/ssh/tunnel"
	header := http.Header{}
	if authenticated {
		header.Set(headerSandboxToken, testToken)
	}
	return websocket.DefaultDialer.Dial(u.String(), header)
}

func runEchoTCPServer(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}

func closedTCPAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

func asCloseError(err error, target **websocket.CloseError) bool {
	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		return false
	}
	*target = closeErr
	return true
}
