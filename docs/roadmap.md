# APRL Roadmap

Updated: 2026-10-03

## Shipped to current main

- Durable control-plane foundation PR1 is present on main at `6feaf92` (the current main head observed during this documentation reconciliation). This is foundation code and task evidence, not complete E1, live paid execution, lifecycle adapter readiness, or production rollout.
- T1.1-T1.12 and T1.14 have accepted task evidence. The completed-slice checkpoint records local integration/review evidence; its historical merge checklist is superseded by PR1's presence on main.

## Historical evidence

- 2026-10-02 completed-slice checkpoint: candidate `b4255df` recorded 136 regular and 136 race pass events each, zero skips, plus build, vet, lint, formatting, and independent review evidence. Broker/supervisor coding was then in flight. This describes the earlier candidate and does not complete E1; current pending rows are authoritative for status.

## In progress

- E1 durable control-plane foundation remains incomplete. T1.13 (broker), T1.15 (recovery), T1.16 (supervisor), T1.17 (service roles), T1.18 (end-to-end foundation evidence), and T1.19 (quality gates/handoff) remain pending in [the plan](plan.md). No E1-complete claim is made.
- Whole standalone/delegated lifecycle integration semantics are accepted in [ADR 007](adr/007-whole-code-change-lifecycle.md) and [the lifecycle contract](contracts.md#whole-code-change-lifecycle-integration). The [code-delivery/v1 schema](contracts/code-delivery-v1.md) is frozen; no adapter readiness is claimed.

## Planned

- E2 review-only runtime and metering/isolation proof remains gated on E1 and real supported adapter evidence.
- E3 bounded remediation, E4 authoring/guarded merge, and E5 operations/rollout remain future scope with the dependencies and acceptance gates in [the plan](plan.md).
- E6 protocol adoption and generic task-tooling integration is tracked in the plan. Its executable implementation waits on E1 handoff and reviewed implementation mapping to the frozen code-delivery/v1 protocol. Every code-changing PR must use an independent executable review task, guarded merge, and verified landing.

## Blocked or not yet qualified

- Live paid execution remains disabled pending proven isolation, finite billing admission, and supported runtime adapters.
- Autonomous GitHub mutation remains unavailable until broker policy, recovery, supervision, and runtime admission are implemented and qualified.
- Production rollout needs a qualified runtime plus an operator-selected target and production evidence.
- The delegated code-delivery/v1 wire schema/version is frozen; service adapter and interoperability qualification remain pending.

[Implementation plan](plan.md)

## Current execution reconciliation — 2026 10 04

Earlier snapshots above are historical. Main d97a89d includes E1 foundation and E6/E7/E8 source/fixture delivery, plus full AWS production SDLC gates. T9.1 preflight audit and T2.0/T2.1 grooming/source preflight completed with explicit deployment blocks. E2 result authentication and HTTPS submission are in progress on disjoint lanes; real runtime/provider/factory/GitHub and AWS deployment remain unqualified. T9.16 remains the completion boundary.

2026 10 04 delivery: PR24 independently approved corrected head e3e68fc; CI37196546731 passed 613 regular/613 race tests; guarded rebase landed af9f106 and whole candidate tree matched remote main. Result transport first horizon delivered. Runtime/model/GitHub App and AWS account/region/cost inputs remain unresolved; production not deployed.

Resume2026-10-05: source-consumer horizon proposed at exact APRL83c7ae6. Three Luna audit turns429 before evidence; no audits accepted. Local compilation held by load>10. UpCloud read-only API access and raw50000credit balance observed, no qualified subscription image/auth, no cloud workers launched or spend. T2.11 proposal and T2.15 structural checks in progress; independent review/merge/landing remain separate gates. Live runtime and production unqualified.
