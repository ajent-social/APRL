// Package migrations embeds APRL's authoritative SQL migration sources.
package migrations

import "embed"

// CoreVersion is the identifier recorded for the initial core schema.
const CoreVersion = "001_core"

// source embeds SQL migrations without duplicating their contents in Go.
//
//go:embed 001_core.sql
var source embed.FS

// ReadCore returns the authoritative SQL source for the core schema.
func ReadCore() ([]byte, error) {
	return source.ReadFile("001_core.sql")
}
