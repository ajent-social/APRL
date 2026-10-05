# APRL production shipping checkpoint

Authoritative task graph: docs/plan.md and linked epic files. This is an execution snapshot, not another scheduler or authority source.

## Delivered source

PR24 merged by guarded rebase into remote main af9f10647b564fcc9e1783af69173df0df37dd06. Independent reviewer /root/plan_review approved exact head e3e68fcf92bd7d3c044fdd964802694daedeb20c against d97a89d8d97e998901721a7060d7e1e9ab29200d after F1 UUID-normalization correction. Required hosted CI37196546731 passed formatting/vet/lint, 613 regular and 613 race terminal tests, zero failures/skips. Whole candidate tree equals landed remote tree; candidate evidence remains applicable. T2.0-T2.7 and correction exits complete; E2 live pilot is not qualified.

Host-only signed run credentials, bounded TLS result submission and actual Postgres result-boundary acceptance are delivered. CLI still refuses production roles without trusted factories. No runtime/provider/GitHub App activation, release artifact, cloud provisioning or deployment occurred. T9.16 remains open. Local compiler/testing held because one-minute load exceeded 10; hosted Linux CI supplied verification.

## Specific unresolved inputs

T2.8 requires the supported model execution path (subscription-only or bounded paid provider), owned GitHub Apps/sandbox and runtime credential bindings. T9.2 requires selected AWS account/profile, region and finite AWS cost limit. Existing AWS authentication does not select deployment authority. The operator questions remain unanswered; time elapsed is not approval.

Read-only preflight observed aprl.sire.run NXDOMAIN, Cloudflare authoritative DNS, empty repository Actions secret/variable metadata, and no configured Cloudflare operations MCP. DNS changes require an authorized available tool binding. No account identifiers, credentials or private endpoints are stored here.

## Resume

Resolve the specific operator bindings; T2.8 and T9.2 then expand actual reachable runtime and production work with owned files and stage-linked implementation, verification, independent review, guarded merge and landed evidence. Add real verified-landed exit IDs to downstream gates. Continue through release, staging, production, observation and handoff; never close T9.16 from source or fixture evidence. Keep original checkout and other agents' work preserved.

## Source-consumer continuation (2026 10 05)

PR26 delivered the explicit offline source-check proposal in docs/contracts/fixtures/source-consumer-check-v1.json and its canonical task T2.12. Independent exact-head approval3341945/base83c7ae6, CI37314527888 succeeded613regular/613race with zero failures/skips, guarded rebase landed2dd2ff3 and full approved/remote tree equality verified. E2 now16/19 explicit rows complete; T2.8 real-runtime expansion, T2.9 live pilot and T2.12 consumer execution remain open. Added bookkeeping does not change runtime completion.

Three Luna audit turns and one later bounded Luna review retry exhausted429 before evidence. Independent proposal review used the existing coordinator model without a cloud worker or paid-provider fallback. No failed Luna result accepted. No self-hosted GitHub runners were returned. Local compiler held by load>10; this candidate's actual hosted check succeeded, so no billing-unavailable status is claimed for it.

Read-only UpCloud account access succeeded: raw total credit50000, CPU6/memory12288/publicIPv4 two limits, accessible servers0 and43 templates with no worker-named match. This inventory does not qualify subscription image/auth or bootstrap; no cloud resource/reservation/spend. Shared Foundry/Zatiti bootstrap priority retained rather than launching competing controllers. Exact source-consumer commands/results are a proposal, not delegated service admission; caller/grant/expiry and executor subject await qualified owner bindings before execution.
