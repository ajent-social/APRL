# ADR 006: Durable supervisor process evidence

Status: Accepted for the unreleased foundation schema

Execution admission creates a RUNNING run before a process starts. The supervisor records a complete process tuple only while that admission is running: positive process/group IDs, a unique workspace UUID, process start, heartbeat and deadline. Identity and deadline are immutable after registration; heartbeat cannot move backward. A null tuple means no process registration evidence.

The supervisor must validate current lease and reservation immediately before launch, use an owned process registry and workspace marker, and record actual bounded execution. Persisted OS IDs alone are never permission to kill a process after restart: PID reuse requires verified ownership. E1's injected native fixture runner does not prove OCI isolation or crash-safe provider admission; those remain E2 prerequisites.

The core schema is not installed outside disposable owned fixtures. Once released, schema changes require forward migrations. A real-Postgres process-record regression failed before these fields/guards; 24 migration and lease race cases pass afterward, zero skips, together with affected vet/lint gates.
