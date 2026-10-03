# ADR 009: Outbox ownership and exact-run cancellation

Status: Accepted for local foundation integration, pending source review and qualification.

LABEL_SYNC belongs to bounded reconciliation. The general dispatcher must exclude these rows and reject label callbacks, because its external callbacks execute under an outbox row lock while reconciliation later acquires task/PR and outbox locks. Combining them would deadlock or invert lock order.

Router and control cancellation intents capture task, job and the exact active run before revocation. A delayed callback can cancel only that run. Pending jobs without a process retain an explicit empty run identity and do not cancel any replacement. A trusted worker cancellation adapter may invoke in-memory CancelRun for that immutable tuple. It must not signal a persisted numeric PID or infer a new run from task identity. An unavailable authenticated cross-process transport remains retryable and unready.

Startup process recovery owns uncertain processes after restart. Durable cancellation and logical lease fencing never constitute physical reaping evidence. No production worker/provider adapter is enabled by this correction.

Cancellation also clears the cancelled job's lease token and deadline in the same transaction that terminates the captured run and retains its budget reservation as UNKNOWN. An unresolved process hold remains charged. A confirmed GitHub push fact does not authorize a final result after that execution is revoked: a paused-webhook late result returns the existing 409 stale_result response, keeps the run terminated and creates no replies. The directly affected API result regression is included in this bounded routing correction; the result service's lease checks are not weakened.
