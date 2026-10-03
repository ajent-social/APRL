//go:build !darwin && !linux

package integration

import "github.com/ajent-social/APRL/internal/supervisor"

func supervisorNativeProcessStartIdentity(int) (string, error) {
	return "", supervisor.ErrInvalid
}
