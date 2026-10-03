//go:build !darwin && !linux

package system

import "fmt"

func foundationProcessStartIdentity(int) (string, error) {
	return "", fmt.Errorf("native process start identity is unsupported")
}
