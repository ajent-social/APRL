# APRL Roadmap

Updated: 2026 10 01

## Shipped
No implementation shipped. RFC review/revision and planning artifacts exist locally.

## In progress
- E1 Foundation - coordinator - 2026 10 01 - T1.1-T1.12 and T1.14 locally accepted; 13/19 engineering tasks. No release or production rollout.

## In flight
- Completed-slice merge - coordinator - 2026 10 02 - 136 regular/race pass events each, no skips; reviewed and ready for PR/rebase merge.
- T1.13 broker and T1.16 supervisor - GPT-6-Luna - 2026 10 02 - two owned SSD coding lanes active.

## Planned
- E1 Go durable control-plane foundation - coordinator/pool - 2026 10 01 - PR: none - executable; wave 7 locally accepted; two-worker broker/supervisor wave active after integrated gates.
- E2 Review-only B and real metering/isolation proof - coordinator - 2026 10 01 - PR: none - outline; after E1 acceptance.
- E3 B/C remediation - coordinator/pool - 2026 10 01 - PR: none - outline; after E2 acceptance.
- E4 Authoring and guarded merge - coordinator/pool - 2026 10 01 - PR: none - outline; after E3 acceptance.
- E5 Production operations and phased rollout - operator/coordinator - 2026 10 01 - PR: none - outline; after review-only evidence and target selection.

## Blocked
No foundation task is externally blocked today. Live agent/production gates require unproven metering compatibility, owned sandbox/App credentials, and an operator-selected production target. These do not prevent fixture-based foundation work.

[Implementation plan](plan.md)
