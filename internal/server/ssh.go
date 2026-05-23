package server

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	sshDialTimeout = 5 * time.Second
	sshFrameLimit  = 1 << 20
)

var sshWSUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (s *Server) handleSSHTunnel(w http.ResponseWriter, r *http.Request) {
	wsConn, err := sshWSUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	tcpConn, err := net.DialTimeout("tcp", s.cfg.SSHTarget, sshDialTimeout)
	if err != nil {
		s.closeSSHTunnelWithError(wsConn, "ssh target unavailable")
		return
	}

	s.runSSHTunnel(wsConn, tcpConn)
}

func (s *Server) runSSHTunnel(wsConn *websocket.Conn, tcpConn net.Conn) {
	wsConn.SetReadLimit(sshFrameLimit)

	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			_ = tcpConn.Close()
			_ = wsConn.Close()
		})
	}
	defer closeBoth()

	errCh := make(chan error, 2)
	go func() {
		errCh <- s.copySSHTCPToWS(wsConn, tcpConn)
	}()
	go func() {
		errCh <- s.copySSHWebsocketToTCP(wsConn, tcpConn)
	}()

	err := <-errCh
	closeBoth()
	<-errCh
	if err != nil && s.log != nil {
		s.log.Debug("ssh tunnel closed", "err", err)
	}
}

func (s *Server) copySSHWebsocketToTCP(wsConn *websocket.Conn, tcpConn net.Conn) error {
	for {
		messageType, reader, err := wsConn.NextReader()
		if err != nil {
			return err
		}
		if messageType != websocket.BinaryMessage {
			_ = wsConn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseUnsupportedData, "ssh tunnel accepts binary frames only"),
				time.Now().Add(wsWriteTimeout),
			)
			return fmt.Errorf("ssh tunnel received non-binary websocket frame")
		}
		if _, err := io.Copy(tcpConn, reader); err != nil {
			return err
		}
		s.touchActivity()
	}
}

func (s *Server) copySSHTCPToWS(wsConn *websocket.Conn, tcpConn net.Conn) error {
	buf := make([]byte, 32<<10)
	for {
		n, err := tcpConn.Read(buf)
		if n > 0 {
			if writeErr := writeSSHWSBinary(wsConn, buf[:n]); writeErr != nil {
				return writeErr
			}
			s.touchActivity()
		}
		if err != nil {
			_ = wsConn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "ssh target closed"),
				time.Now().Add(wsWriteTimeout),
			)
			return err
		}
	}
}

func writeSSHWSBinary(wsConn *websocket.Conn, data []byte) error {
	writer, err := wsConn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}

func (s *Server) closeSSHTunnelWithError(wsConn *websocket.Conn, message string) {
	defer func() { _ = wsConn.Close() }()
	_ = wsConn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseTryAgainLater, message),
		time.Now().Add(wsWriteTimeout),
	)
	if s.log != nil {
		s.log.Debug("ssh tunnel unavailable", "target", s.cfg.SSHTarget)
	}
}
