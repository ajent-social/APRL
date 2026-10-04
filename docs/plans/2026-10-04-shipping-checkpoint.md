# APRL production shipping checkpoint

Authoritative task graph: docs/plan.md and its linked epic files. This checkpoint is an execution snapshot, not another scheduler or authority source.

## Current state

- User requested execution of the merged full-SDLC plan. Production destination remains https://aprl.sire.run on AWS; T9.16 is open.
- Baseline main: d97a89d8d97e998901721a7060d7e1e9ab29200d. Exact baseline hosted CI37192937020 succeeded. Prior E1/E6/E7/E8 delivery receipts are source/fixture qualification, not production activation.
- T9.1, T2.0 and T2.1 audited/groomed; T2.2 hostauth and T2.3 resultclient implementation dispatched on disjoint owned worktrees. Coordinator owns actual TLS/Postgres integration, shared docs and verification. Implementation claims are coordinator-held; no worker may release another holder.
- Local compiler/testing held while one-minute load exceeds 10, respecting ADR 004. Existing hosted Linux CI supplies exact candidate build/test verification; no local lease bypass.
- CLI intentionally refuses all roles pending production factory, host recovery, OCI, provider and broker bindings. These new transport adapters must not activate workers or infer billing/delivery authority.

## Unresolved inputs

AWS identity probe works, but selected account/profile, region and finite AWS allowance await operator response. GitHub Apps/owned sandbox and supported subscription-only or bounded paid-provider execution/grant remain unspecified. aprl.sire.run currently resolves NXDOMAIN; authoritative DNS is Cloudflare and no configured Cloudflare operations MCP is available. A documented authorized tool binding is required before DNS changes.

## Resume and invalidation

Finish T2.2/T2.3, integrate their owned commits, run candidate CI and real result-boundary acceptance, independently review exact base/head, resolve findings through explicit fix/verification/re-review tasks, merge and verify actual landing. Then T2.8 expands only reachable real-runtime work; live pilot and deployment stay gated on specific unresolved inputs. Changes to source/head/base/auth contract invalidate affected verification/review. No release or cloud resource has been created by this run.

## Verified candidate — 2026 10 04

PR24 source6b9d5137facbb26725d6fdc94dd70e04d13fa05f passed hosted CI37195830606: 612 regular and 612 race terminal tests, zero failures/skips, plus formatting/vet/lint. T2.2/T2.3 author handoff and T2.4 verification complete; T2.5 independent review, T2.6 merge and T2.7 landed verification remain open. Test source and production source are distinct from runtime deployment evidence. GitHub repository secret/variable metadata is empty; no live App/provider/AWS grant discovered.
