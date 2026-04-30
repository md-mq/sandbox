//go:build linux

package execmgr

import "syscall"

// setpgid returns the SysProcAttr needed to start a child in its own process group.
// Used by tests. The production path sets this inline in exec.go.
func setpgid() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
