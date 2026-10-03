//go:build linux

package system

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func foundationProcessStartIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", fmt.Errorf("invalid pid")
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", fmt.Errorf("read native process start identity: %w", err)
	}
	closeParen := strings.LastIndexByte(string(stat), ')')
	if closeParen < 0 {
		return "", fmt.Errorf("parse native process stat")
	}
	fields := strings.Fields(string(stat[closeParen+1:]))
	if len(fields) <= 19 {
		return "", fmt.Errorf("parse native process start time")
	}
	startTicks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || startTicks == 0 {
		return "", fmt.Errorf("parse native process start time")
	}
	bootID, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(bootID)) == "" {
		return "", fmt.Errorf("read native boot identity")
	}
	return "linux-start:" + strings.TrimSpace(string(bootID)) + ":" + strconv.FormatUint(startTicks, 10), nil
}
