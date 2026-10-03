//go:build darwin

package system

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

func foundationProcessStartIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("invalid pid")
	}
	bootSession, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil || strings.TrimSpace(bootSession) == "" {
		return "", fmt.Errorf("read native boot session identity")
	}
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return "", fmt.Errorf("read native process start identity: %w", err)
	}
	if int(info.Proc.P_pid) != pid || info.Proc.P_starttime.Sec <= 0 || info.Proc.P_starttime.Usec < 0 {
		return "", fmt.Errorf("native process start identity unavailable")
	}
	return fmt.Sprintf("darwin-start:%s:%d:%d", strings.TrimSpace(bootSession), info.Proc.P_starttime.Sec, info.Proc.P_starttime.Usec), nil
}
