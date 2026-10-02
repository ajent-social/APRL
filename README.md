# APRL

APRL is a Go control plane for a guarded author, reviewer, and fixer pull request lifecycle. PostgreSQL owns durable state and execution authority; Redis Streams transports delivery hints. See [the RFC](docs/rfc/rfc-0001.md) and [implementation plan](docs/plan.md).

Implementation is in progress. The foundation uses owned test services and injected test workers. Real paid execution and autonomous GitHub mutations remain unavailable until isolation, pricing, and runtime admission are proven. Production worker startup must fail closed when a supported adapter is unavailable.

## Development

The supported toolchain is Go 1.27.1. Tools are pinned to `golang.org/x/tools/cmd/goimports` v0.50.0 and golangci-lint v2.14.0. Use an owned scratch directory on a volume with enough space for module, build, temporary, and tool artifacts:

```sh
export APRL_SCRATCH=/path/to/owned-volume/aprl-scratch
mkdir -p "$APRL_SCRATCH/cache" "$APRL_SCRATCH/modules" "$APRL_SCRATCH/tmp" "$APRL_SCRATCH/tools"
export GOCACHE="$APRL_SCRATCH/cache"
export GOMODCACHE="$APRL_SCRATCH/modules"
export GOTMPDIR="$APRL_SCRATCH/tmp"
export TMPDIR="$GOTMPDIR"
export GOBIN="$APRL_SCRATCH/tools"
export PATH="$GOBIN:$PATH"
go mod download
```

Run `go test ./tests/unit -run '^TestBootstrap' -count=1` for configuration validation. It does not require backing services and explicitly exercises missing/malformed service inputs. Format with gofmt/goimports and run go vet and golangci-lint on changed packages.

Integration tests require explicitly owned PostgreSQL 16 and/or Redis 7 fixtures. Set `TEST_RESOURCE_OWNER` to a nonempty ownership marker and configure `TEST_DATABASE_URL` and `TEST_REDIS_URL` only for the services each test requires. Missing service configuration or connectivity is a test failure. Database fixtures generate an isolated schema and set every pooled connection's search path; Redis fixtures generate an isolated key prefix. Cleanup removes only those generated resources. Never point tests at an unowned database or Redis instance.

[Verification evidence](docs/verification.md) and [current progress](docs/roadmap.md) distinguish foundation acceptance from live rollout.
