# ADR 010: Host-scoped authenticated result transport

## Status
Accepted source boundary; production activation remains gated.

## Context
The foundation already requires an independently authenticated supervisor principal and checks that principal against durable task/run admission. The CLI deliberately lacks production host/OCI/provider factories. A host authenticator and bounded result submitter are necessary adapters but do not prove the missing factories or containment.

## Decision
Use host-only HMAC-SHA256 credentials with a fixed worker-results audience and trusted configured issuer. Use a fixed custom v1 token framing with canonical JSON/base64url and Unix-nanosecond issue/not-before/expiry times (not JWT NumericDate). Bind each short-lived credential to exact task, run, supervisor identity and credential reference; pin key IDs with explicit rotation and bound validity to at most 15 minutes. Authenticated HTTPS POST /internal/results supplies identity independently of result JSON; durable results admission remains authoritative for lease/generation/replay and supervisor ownership. Signing keys and bearer tokens never enter worker payloads or environment. Production secret provisioning and rotation remain part of the later host/deployment contract.

The trusted host result client uses explicit TLS roots and a pinned HTTPS endpoint, bounds request/response size and duration, refuses redirects and automatic retries, and requires exact matching accepted operation ID. Any missing, malformed, ambiguous or rejected response is an error; it cannot create a durable successful completion or settlement. No ambient credentials, fixture fallback or runtime CLI enablement are added.

## Consequences
Ephemeral TLS and signing keys plus real Postgres can qualify this adapter boundary locally or in hosted CI. That evidence does not qualify real OCI execution, persisted host inventory/recovery, GitHub App identity, provider financial bounds, release or AWS operation. Remaining production bindings stay disabled until their own delivery and live acceptance gates pass.
