//go:build linux

package integration

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ajent-social/APRL/internal/supervisor"
)

func supervisorNativeProcessStartIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", supervisor.ErrInvalid
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", err
	}
	closeParen := strings.LastIndexByte(string(stat), ')')
	if closeParen < 0 {
		return "", supervisor.ErrInvalid
	}
	fields := strings.Fields(string(stat[closeParen+1:]))
	if len(fields) <= 19 {
		return "", supervisor.ErrInvalid
	}
	startTicks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || startTicks == 0 {
		return "", supervisor.ErrInvalid
	}
	bootID, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(bootID)) == "" {
		return "", supervisor.ErrInvalid
	}
	return "linux-start:" + strings.TrimSpace(string(bootID)) + ":" + strconv.FormatUint(startTicks, 10), nil
}
