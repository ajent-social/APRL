// Package migrations embeds APRL's authoritative SQL migration sources.
package migrations

import "embed"

// CoreVersion is the identifier recorded for the initial core schema.
const CoreVersion = "001_core"

// source embeds SQL migrations without duplicating their contents in Go.
//
//go:embed *.sql
var source embed.FS

// ReadCore returns the authoritative SQL source for the core schema.
func ReadCore() ([]byte, error) {
	return source.ReadFile("001_core.sql")
}

// ReadPlanTasks returns the embedded additive plan task migration.
func ReadPlanTasks() ([]byte, error) { return source.ReadFile("002_plan_tasks.sql") }

// ReadProcessHolds returns the durable process reservation migration.
func ReadProcessHolds() ([]byte, error) { return source.ReadFile("003_process_holds.sql") }
