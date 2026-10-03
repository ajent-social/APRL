# ADR 007: Whole code-change lifecycle ownership and integration

## Status
Accepted semantic contract; neutral code-delivery/v1 wire schema frozen; adapter qualification pending

## Context
APRL must remain useful as a complete standalone author/review/fix/merge lifecycle while also participating in a larger product workflow. Generic task tooling needs executable review work, and delegated integration must not create two schedulers or treat developer claims as runtime authority. The neutral code-delivery/v1 protocol is frozen in docs/contracts/code-delivery-v1.md; its adapter, foundation handoff and interoperability proof remain pending.

## Decision
APRL owns one canonical scheduler, canonical lifecycle child IDs, readiness, stage routing, and authoritative admission/result adapter for each enrolled code-change lifecycle in both standalone and delegated modes. A neutral product workflow controller owns its approved product graph, aggregate admission, product qualification, and deployment decisions. Shared generic task representation/readiness/stage routing remains reusable; lifecycle-specific authority is supplied by the APRL adapter. A claim is pickup only.

Author/fix code-change tasks complete at PR URL and exact-head handoff. Every code-changing PR has an executable independent review task handled through ordinary apply+claim. Review checks exact head/base; approval proceeds to guarded merge and verified landing under the reviewer lane. Blocking findings create bounded fix and dependent re-review tasks. Ordinary descendants wait for merged and verified landing. Explicit speculative dependencies may begin earlier only with their inputs and speculative status recorded and confer no release authority. No review exemption exists.

## Consequences
Standalone APRL operation is preserved. Delegated execution shares APRL's lifecycle scheduler rather than adding a competing stage scheduler. Review, fix, and re-review are first-class executable rows. Product readiness remains separate from code-change landing. Blocked correction paths remain visible and bounded.

## Pending
The external code-delivery/v1 schema and version are frozen in docs/contracts/code-delivery-v1.md. Its implementation mapping and independent interoperability qualification remain pending. The implementation and publication contract must be reconciled against that frozen protocol. This ADR does not claim an adapter, runtime qualification, provider execution, or production readiness.
