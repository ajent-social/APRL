# ADR 001: Go service roles and Redis Streams transport

## Status
Accepted

## Date
2026 10 01

## Context
The user explicitly requires APRL in Go. The draft RFC's original FastAPI/AsyncPG/BullMQ stack must therefore change without weakening durable delivery, snapshot gating, cancellation, or financial admission. APRL must remain available while ephemeral Codex agents exit or wait on CI.

## Decision
Use one Go module, github.com/ajent-social/APRL, with standard-library net/http, encoding/json, flag, os/exec and structured slog. Use pgx/v5 for Postgres 16 and go-redis/v9 for Redis Streams. YAML and JSON Schema validation libraries are narrow justified dependencies chosen and pinned during T1.3, not replacements for domain validation. Avoid web/CLI frameworks, Cobra, Viper and assertion frameworks. Tests use the standard testing/httptest packages, table-driven cases, gofmt/goimports, go vet and golangci-lint. Pin the toolchain during T1.1; the planning host reports Go 1.27.1.

The module exposes separately supervised api, control and worker roles through cmd/aprl. API handles signed webhook receipt and authenticated results; control handles routing, outbox dispatch, reconciliation, policy and broker/budget admission; worker supervises one-shot OCI containers. Every goroutine exits through a context or joined lifecycle. State and execution leases remain in Postgres.

Replace BullMQ with Redis Streams consumer groups. XADD carries a logical job UUID independent of the stream entry ID; XREADGROUP/XACK/XAUTOCLAIM implement transport delivery/recovery. Redis does not own scheduling, delayed retries, task serialization or cancellation authority: Postgres due outbox records, leases and generations provide them. A duplicate stream entry cannot duplicate a confirmed logical operation. This avoids introducing a non-Go queue bridge.

Foundation verification uses test doubles in _test.go or explicit testutil packages and owned fixture subprocesses. No production adapter may return fake success: unavailable GitHub/provider/OCI integration fails closed. Live OCI Linux containers are built/run with Podman; the RFC's Docker terminology denotes isolation. DevSpace is the development orchestration option if a Kubernetes target is selected. Production placement/provisioning remains E5 until the operator supplies an owned target; this plan provisions nothing.

## Consequences
Task/file boundaries and concurrency tests follow the Go skill. Redis delivery IDs differ from logical job IDs, so recovery must check Postgres before execution and before acknowledging stale work. Stream reclaim is not a lease transfer. Deadline/backoff scheduling is additional control-plane code, covered by fault tests. Real OCI isolation and metered provider compatibility remain rollout prerequisites. The user-directed stack change is reflected in RFC-0001, plan rows, and task contracts together.

## Evidence
[Redis Go documentation](https://redis.io/docs/latest/develop/clients/go/) documents go-redis/v9. [XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/) documents pending-entry reclaim. [pgx/v5](https://pkg.go.dev/github.com/jackc/pgx/v5) supplies the PostgreSQL adapter. Local codex-cli 0.159.3 help exposes JSONL/schema/ephemeral options, which do not prove cost bounds.
