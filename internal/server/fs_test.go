package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFS_RoundTrip(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")

	resp := doFS(t, http.MethodPost, base+"/fs/write?path="+url.QueryEscape(path), strings.NewReader("hello"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("write status = %d, want 200", resp.StatusCode)
	}
	var writeBody struct {
		Path         string `json:"path"`
		BytesWritten int64  `json:"bytes_written"`
		Created      bool   `json:"created"`
	}
	decodeJSON(t, resp, &writeBody)
	if writeBody.Path != path || writeBody.BytesWritten != 5 || !writeBody.Created {
		t.Fatalf("write body = %+v, want path/%d/created", writeBody, 5)
	}

	resp = doFS(t, http.MethodGet, base+"/fs/read?path="+url.QueryEscape(path), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("read status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("content-type = %q, want application/octet-stream", ct)
	}
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(raw) != "hello" {
		t.Fatalf("read body = %q, want hello", string(raw))
	}

	resp = doFS(t, http.MethodGet, base+"/fs/ls?path="+url.QueryEscape(dir), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ls status = %d, want 200", resp.StatusCode)
	}
	var listBody struct {
		Path    string `json:"path"`
		Entries []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"entries"`
		Truncated bool `json:"truncated"`
	}
	decodeJSON(t, resp, &listBody)
	if listBody.Path != dir || listBody.Truncated || len(listBody.Entries) != 1 {
		t.Fatalf("ls body = %+v, want one untruncated entry", listBody)
	}
	if listBody.Entries[0].Name != "file.txt" || listBody.Entries[0].Type != "file" {
		t.Fatalf("ls entry = %+v, want file.txt/file", listBody.Entries[0])
	}

	resp = doFS(t, http.MethodGet, base+"/fs/stat?path="+url.QueryEscape(path), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stat status = %d, want 200", resp.StatusCode)
	}
	var statBody struct {
		Path   string  `json:"path"`
		Type   string  `json:"type"`
		Size   int64   `json:"size"`
		Mode   string  `json:"mode"`
		Target *string `json:"symlink_target"`
	}
	decodeJSON(t, resp, &statBody)
	if statBody.Path != path || statBody.Type != "file" || statBody.Size != 5 || statBody.Target != nil {
		t.Fatalf("stat body = %+v, want file size 5", statBody)
	}

	resp = doFS(t, http.MethodDelete, base+"/fs/rm?path="+url.QueryEscape(path), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rm status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("file still exists after rm: %v", err)
	}
}

func TestFS_ReadOffsetLengthHeaders(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	path := filepath.Join(t.TempDir(), "data")
	if err := os.WriteFile(path, []byte("abcdef"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	resp := doFS(t, http.MethodGet,
		base+"/fs/read?path="+url.QueryEscape(path)+"&offset=2&length=3", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(raw) != "cde" {
		t.Fatalf("body = %q, want cde", string(raw))
	}
	if got := resp.Header.Get("X-Polyaxon-Next-Offset"); got != "5" {
		t.Fatalf("next offset = %q, want 5", got)
	}
	if got := resp.Header.Get("X-Polyaxon-Eof"); got != "false" {
		t.Fatalf("eof = %q, want false", got)
	}
	if got := resp.Header.Get("Content-Length"); got != "3" {
		t.Fatalf("content-length = %q, want 3", got)
	}
}

func TestFS_WriteBodyCap(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	dir := t.TempDir()

	exactPath := filepath.Join(dir, "exact")
	resp := doFS(t, http.MethodPost, base+"/fs/write?path="+url.QueryEscape(exactPath),
		bytes.NewReader(bytes.Repeat([]byte("x"), maxFSWriteBytes)))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("exact cap status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	if info, err := os.Stat(exactPath); err != nil || info.Size() != maxFSWriteBytes {
		t.Fatalf("exact file size = %v/%v, want %d", info, err, maxFSWriteBytes)
	}

	largePath := filepath.Join(dir, "large")
	resp = doFS(t, http.MethodPost, base+"/fs/write?path="+url.QueryEscape(largePath),
		bytes.NewReader(bytes.Repeat([]byte("x"), maxFSWriteBytes+1)))
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("large status = %d, want 413", resp.StatusCode)
	}
	var env errorEnvelope
	decodeJSON(t, resp, &env)
	if env.Error.Code != CodePayloadTooLarge {
		t.Fatalf("code = %q, want %q", env.Error.Code, CodePayloadTooLarge)
	}
}

func TestFS_PathValidation(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	tests := []struct {
		name string
		path string
	}{
		{name: "empty", path: ""},
		{name: "relative", path: "tmp/file"},
		{name: "nul", path: "/tmp/a\x00b"},
		{name: "overlong", path: "/" + strings.Repeat("a", 4097)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp := doFS(t, http.MethodGet, base+"/fs/stat?path="+url.QueryEscape(tc.path), nil)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			var env errorEnvelope
			decodeJSON(t, resp, &env)
			if env.Error.Code != CodeInvalidRequest {
				t.Fatalf("code = %q, want invalid_request", env.Error.Code)
			}
		})
	}
}

func TestFS_QueryValidation(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	dir := t.TempDir()

	tests := []struct {
		name string
		path string
	}{
		{
			name: "invalid boolean",
			path: "/fs/ls?path=" + url.QueryEscape(dir) + "&recursive=1",
		},
		{
			name: "invalid integer",
			path: "/fs/ls?path=" + url.QueryEscape(dir) + "&max_entries=nope",
		},
		{
			name: "invalid mode",
			path: "/fs/write?path=" + url.QueryEscape(filepath.Join(dir, "x")) + "&mode=0999",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			method := http.MethodGet
			var body io.Reader
			if strings.HasPrefix(tc.path, "/fs/write") {
				method = http.MethodPost
				body = strings.NewReader("x")
			}
			resp := doFS(t, method, base+tc.path, body)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			var env errorEnvelope
			decodeJSON(t, resp, &env)
			if env.Error.Code != CodeInvalidRequest {
				t.Fatalf("code = %q, want invalid_request", env.Error.Code)
			}
		})
	}
}

func TestFS_ListTruncation(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	dir := t.TempDir()
	for i := 0; i < 1500; i++ {
		path := filepath.Join(dir, fmt.Sprintf("%04d", i))
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatalf("write file %d: %v", i, err)
		}
	}

	resp := doFS(t, http.MethodGet,
		base+"/fs/ls?path="+url.QueryEscape(dir)+"&max_entries=1000", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Entries   []json.RawMessage `json:"entries"`
		Truncated bool              `json:"truncated"`
	}
	decodeJSON(t, resp, &body)
	if len(body.Entries) != 1000 || !body.Truncated {
		t.Fatalf("entries/truncated = %d/%v, want 1000/true", len(body.Entries), body.Truncated)
	}
}

func TestFS_MkdirParents(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	path := filepath.Join(t.TempDir(), "a", "b")

	resp := doJSON(t, http.MethodPost, base+"/fs/mkdir",
		fmt.Sprintf(`{"path":%q,"parents":true,"mode":"0750"}`, path))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	if info, err := os.Stat(path); err != nil || !info.IsDir() {
		t.Fatalf("path was not created as directory: info=%v err=%v", info, err)
	}
}

func TestFS_RemoveRecursiveFlag(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	dir := t.TempDir()
	tree := filepath.Join(dir, "tree")
	if err := os.Mkdir(tree, 0o755); err != nil {
		t.Fatalf("mkdir tree: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tree, "child"), nil, 0o644); err != nil {
		t.Fatalf("write child: %v", err)
	}

	resp := doFS(t, http.MethodDelete, base+"/fs/rm?path="+url.QueryEscape(tree), nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("non-recursive status = %d, want 409", resp.StatusCode)
	}
	resp.Body.Close()

	resp = doFS(t, http.MethodDelete,
		base+"/fs/rm?path="+url.QueryEscape(tree)+"&recursive=true", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("recursive status = %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()
	if _, err := os.Stat(tree); !os.IsNotExist(err) {
		t.Fatalf("tree still exists after recursive rm: %v", err)
	}
}

func TestFS_ForbiddenMapping(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root; chmod 000 does not enforce")
	}
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("secret"), 0o600); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	if err := os.Chmod(path, 0); err != nil {
		t.Fatalf("chmod secret: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })

	resp := doFS(t, http.MethodGet, base+"/fs/read?path="+url.QueryEscape(path), nil)
	if resp.StatusCode == http.StatusOK {
		resp.Body.Close()
		t.Skip("chmod 000 did not block read on this filesystem")
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	var env errorEnvelope
	decodeJSON(t, resp, &env)
	if env.Error.Code != CodeForbidden {
		t.Fatalf("code = %q, want %q", env.Error.Code, CodeForbidden)
	}
}

func TestFS_StatSymlink(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "target"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write target: %v", err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink("target", link); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	resp := doFS(t, http.MethodGet, base+"/fs/stat?path="+url.QueryEscape(link), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Type          string  `json:"type"`
		SymlinkTarget *string `json:"symlink_target"`
	}
	decodeJSON(t, resp, &body)
	if body.Type != "symlink" || body.SymlinkTarget == nil || *body.SymlinkTarget != "target" {
		t.Fatalf("stat body = %+v, want symlink target", body)
	}
}

func doFS(t *testing.T, method, rawURL string, body io.Reader) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, rawURL, body)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set(headerSandboxToken, testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	return resp
}

func TestFS_ActivityTouched(t *testing.T) {
	s := newTestServer(t, false)
	base := runHTTPServer(t, s)
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}
	before := s.counters.LastActivity()
	time.Sleep(time.Millisecond)
	resp := doFS(t, http.MethodGet, base+"/fs/stat?path="+url.QueryEscape(path), nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if !s.counters.LastActivity().After(before) {
		t.Fatal("authenticated fs request did not update last activity")
	}
}
