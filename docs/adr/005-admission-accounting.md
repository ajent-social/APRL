# ADR 005: Separate admission limits from billing facts

Status: Accepted for the unreleased foundation
Date: 2026 10 01

A task admission ceiling must block new spending authority, while actual charges must remain recorded even after an unexpected overage or late provider response. A database check that rejects cached spend above the ceiling prevents faithful accounting.

New reservations enforce task and rolling organization limits under organization/task locks. Money remains nonnegative integer micro-USD. Unexpected charges are appended, conservative unknown coverage is retained and emergency mode stops further admission. Cost entries cannot be updated or deleted. Reservations are unique per immutable run; their identity, amount, pricing and complete admission envelope cannot change or be deleted.

The host supplies validated execution bounds when the queued job has no provider-specific envelope; a queued nonzero envelope must match exactly. The complete admitted envelope is stored on the reservation. This preserves stable queued payloads and makes replay compare token/call/pricing bounds as well as the amount.

The core migration is refined before its first release because it has only been applied to owned disposable test schemas. Released migrations must remain immutable and later installations require forward migrations. These foundation checks do not prove provider isolation or financial request bounds; paid execution stays disabled until E2 establishes that boundary.
