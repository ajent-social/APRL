//go:build darwin

package supervisor

import (
	"fmt"
	"strings"

	"golang.org/x/sys/unix"
)

func processStartIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", ErrInvalid
	}
	bootSession, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil || strings.TrimSpace(bootSession) == "" {
		return "", fmt.Errorf("read native boot session identity: %w", ErrInvalid)
	}
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return "", fmt.Errorf("read native process start identity: %w", err)
	}
	if int(info.Proc.P_pid) != pid || info.Proc.P_starttime.Sec <= 0 || info.Proc.P_starttime.Usec < 0 {
		return "", fmt.Errorf("native process start identity unavailable: %w", ErrInvalid)
	}
	return fmt.Sprintf("darwin-start:%s:%d:%d", strings.TrimSpace(bootSession), info.Proc.P_starttime.Sec, info.Proc.P_starttime.Usec), nil
}
