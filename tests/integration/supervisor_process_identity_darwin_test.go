//go:build darwin

package integration

import (
	"fmt"
	"strings"

	"github.com/ajent-social/APRL/internal/supervisor"
	"golang.org/x/sys/unix"
)

func supervisorNativeProcessStartIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", supervisor.ErrInvalid
	}
	bootSession, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil || strings.TrimSpace(bootSession) == "" {
		return "", supervisor.ErrInvalid
	}
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return "", err
	}
	if int(info.Proc.P_pid) != pid || info.Proc.P_starttime.Sec <= 0 || info.Proc.P_starttime.Usec < 0 {
		return "", supervisor.ErrInvalid
	}
	return fmt.Sprintf("darwin-start:%s:%d:%d", strings.TrimSpace(bootSession), info.Proc.P_starttime.Sec, info.Proc.P_starttime.Usec), nil
}
