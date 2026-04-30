//go:build !linux

package execmgr

import "syscall"

// setpgid on non-Linux (dev machines: darwin) just returns Setpgid as well.
// Same field exists on BSDs and macOS.
func setpgid() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true}
}
