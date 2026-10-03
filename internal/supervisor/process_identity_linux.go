//go:build linux

package supervisor

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func processStartIdentity(pid int) (string, error) {
	if pid <= 0 {
		return "", ErrInvalid
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", fmt.Errorf("read native process start identity: %w", err)
	}
	close := strings.LastIndexByte(string(stat), ')')
	if close < 0 {
		return "", fmt.Errorf("parse native process stat: %w", ErrInvalid)
	}
	fields := strings.Fields(string(stat[close+1:]))
	if len(fields) <= 19 {
		return "", fmt.Errorf("parse native process start time: %w", ErrInvalid)
	}
	startTicks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || startTicks == 0 {
		return "", fmt.Errorf("parse native process start time: %w", ErrInvalid)
	}
	bootID, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil || strings.TrimSpace(string(bootID)) == "" {
		return "", fmt.Errorf("read native boot identity: %w", ErrInvalid)
	}
	return "linux-start:" + strings.TrimSpace(string(bootID)) + ":" + strconv.FormatUint(startTicks, 10), nil
}
