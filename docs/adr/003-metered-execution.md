# ADR 003: Fail-closed metered inference admission

## Status
Accepted

## Date
2026 10 01

## Context
APRL requires per-task and rolling organizational ceilings. Codex can make multiple model requests; killing a process does not cancel charges already accepted upstream. Authoring occurs before a PR exists. Supported provider paths, token classes and response bounds have not been proven for this deployment.

## Decision
Create the task ledger before authoring. Reserve a worst-case per-run envelope atomically against task and organization balances before inference; lock org then task. An external gateway checks every request, pinned pricing version, finite call/output/input bounds, and coverage for all billable token classes and outstanding requests. Provider and GitHub credentials stay outside untrusted containers. Unknown envelopes deny admission; unsupported runtime/provider combinations remain disabled, even if fixture tests pass.

Settlement uses unique request IDs and retains coverage for cancelled/unknown usage. Unexpected charges fence organizational work and require reconciliation. Phase E2 must demonstrate the real container/gateway/provider contract before any unattended paid run. No model or pricing is hardcoded by this plan; operator policy supplies the supported model, pricing source, and maximum reservation. A successful compatibility probe is evidence only for that pinned combination.

## Consequences
Foundation work can proceed with fake inference. Runtime integration is a launch gate, not a claimed CLI feature. Experimental rollout-token tracking is not accepted as a financial guarantee. If the gateway contract cannot be demonstrated, live autonomy remains blocked; changing the product's hard-ceiling promise requires an explicit RFC/ADR decision.

## Evidence
The [official Codex configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference#configtoml) documents custom provider configuration and describes rollout-budget tracking as under development. It does not establish APRL's USD envelope guarantee. RFC-0001 section 7.2 defines that additional contract.
