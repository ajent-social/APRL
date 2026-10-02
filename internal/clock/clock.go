// Package clock provides injectable wall and manual clocks for APRL services.
package clock

import (
	"sync"
	"time"
)

// Clock supplies timestamps to persistence and service code.
type Clock interface {
	Now() time.Time
}

// System returns the current wall-clock time.
type System struct{}

// Now returns the current wall-clock time in UTC.
func (System) Now() time.Time {
	return time.Now().UTC()
}

// Manual is a mutex-safe deterministic clock for tests and simulations.
type Manual struct {
	mu      sync.RWMutex
	current time.Time
}

// NewManual constructs a clock at the supplied instant.
func NewManual(start time.Time) *Manual {
	return &Manual{current: start.UTC()}
}

// Now returns the currently configured instant.
func (m *Manual) Now() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.current
}

// Set replaces the current instant.
func (m *Manual) Set(instant time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.current = instant.UTC()
}

// Advance moves the clock by delta and returns the new instant. It does not
// sleep; callers can deterministically move time forward or backward.
func (m *Manual) Advance(delta time.Duration) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.current = m.current.Add(delta)
	return m.current
}
