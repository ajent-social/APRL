# APRL Roadmap

Updated: 2026-10-03

## Shipped to current main

- Durable control-plane foundation PR1 is present on main at `6feaf92` (the current main head observed during this documentation reconciliation). This is foundation code and task evidence, not complete E1, live paid execution, lifecycle adapter readiness, or production rollout.
- T1.1-T1.12 and T1.14 have accepted task evidence. The completed-slice checkpoint records local integration/review evidence; its historical merge checklist is superseded by PR1's presence on main.

## Historical evidence

- 2026-10-02 completed-slice checkpoint: candidate `b4255df` recorded 136 regular and 136 race pass events each, zero skips, plus build, vet, lint, formatting, and independent review evidence. Broker/supervisor coding was then in flight. This describes the earlier candidate and does not complete E1; current pending rows are authoritative for status.

## In progress

- E1 durable control-plane foundation remains incomplete. T1.13 (broker), T1.15 (recovery), T1.16 (supervisor), T1.17 (service roles), T1.18 (end-to-end foundation evidence), and T1.19 (quality gates/handoff) remain pending in [the plan](plan.md). No E1-complete claim is made.
- Whole standalone/delegated lifecycle integration semantics are accepted in [ADR 007](adr/007-whole-code-change-lifecycle.md) and [the lifecycle contract](contracts.md#whole-code-change-lifecycle-integration). External protocol schema/version is pending architecture freeze; no adapter readiness is claimed.

## Planned

- E2 review-only runtime and metering/isolation proof remains gated on E1 and real supported adapter evidence.
- E3 bounded remediation, E4 authoring/guarded merge, and E5 operations/rollout remain future scope with the dependencies and acceptance gates in [the plan](plan.md).
- E6 protocol adoption and generic task-tooling integration is tracked in the plan. Its executable implementation waits on E1 handoff and exact protocol freeze. Every code-changing PR must use an independent executable review task, guarded merge, and verified landing.

## Blocked or not yet qualified

- Live paid execution remains disabled pending proven isolation, finite billing admission, and supported runtime adapters.
- Autonomous GitHub mutation remains unavailable until broker policy, recovery, supervision, and runtime admission are implemented and qualified.
- Production rollout needs a qualified runtime plus an operator-selected target and production evidence.
- Exact delegated protocol wire schema/version is pending its architecture lane.

[Implementation plan](plan.md)
