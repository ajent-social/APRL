# APRL source consumer verification

Status: proposal; not service admission, worker dispatch or a completed check.

The first consumer subject is canonical task T2.12, bound to public APRL source83c7ae670b07e00bc0d4b51e72879ca8f6ace71a. Its JSON proposal is `docs/contracts/fixtures/source-consumer-check-v1.json`. It checks the frozen code-delivery wire, portable lifecycle projection, signed host credentials and bounded result client. Existing producer tasks remain complete; this is a new verification task, not reopened producer work.

This source-check proposal is not a code-delivery/v1 request. That protocol delegates a bounded code change through a trusted canonical service; a source-only check cannot fabricate such enrollment. The proposal has no authenticated caller, live grant or expiry. Those nulls are explicit missing prerequisites, not default authority. The owner must supply immutable request/result subject, actual caller/scope/grant bindings, credential references, absolute expiry and stop deadline before external execution. Channel labels do not authenticate any of them.

The commands use credential-free packages only. No external Postgres/Redis fixtures, OCI runtime, GitHub App, service endpoint or model call is exercised. The source SHA is mandatory; a different source requires a new qualification subject. The portable contract remains Wazi0.0.1 with its existing exact digest; a delegating consumer's native schema revision does not mutate this owner contract or imply compatibility.

Execute only under the local shared load/lease rules or a separately qualified bounded executor. Local compiler is held while one-minute load exceeds10. Enforce each command's180second timeout and one normal/race lane; caches, temporary files and logs live on external SSD for local runs. Capture every package terminal and test terminal from JSON; require positive test count, zero failures and zero skips in each lane. No hosted check or source result is claimed until observed. Include exact source/tree/toolchain and actual commands, process outcomes, environment and timestamps in the receipt. Missing accounting is unknown, never a success.

No spending or live authority is granted by this proposal. `max_paid_model_cost_cents=0`, cloud allocation and live service calls false. Existing subscription-authenticated workers require their own qualified installation/auth/finite cloud envelope before use. Paid fallback is prohibited. This check can unblock source-consumer discovery; it cannot release the live review pilot or production gates.
