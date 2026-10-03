//go:build !darwin && !linux

package supervisor

import "fmt"

func processStartIdentity(int) (string, error) {
	return "", fmt.Errorf("native process start identity is unsupported on this platform: %w", ErrInvalid)
}
