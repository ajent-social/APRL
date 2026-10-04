# Experimental portable plan observation

APRL consumes Wazi experimental contract 0.0.1, frozen source `16b66e5eedf20d52e72928bb56a0c19e391e8ce9`, manifest digest `sha256:7582512f122d2f2a9c4461facc7541c9887053f137260d6ebe9c6dea611d039d`. The coordinator independently verified every one of the 66 manifest paths and the sorted-path aggregate digest before mapping. Public owner: https://github.com/kazi-org/wazi. Retrieve the pinned bundle offline; APRL does not fetch schemas or dispatch from portable JSON.

The bounded adapter represents one actual authored parent task and one canonical APRL lifecycle execution unit. Native children have no authored title or acceptance text, so they are not invented portable authored tasks. Native task identity, dependency kinds, stable delivery gate, correction/finding lineage, PR candidate binding, contributor provenance and terminal observations belong to APRL namespaced metadata. Portable consumers must return to native authenticated admission APIs for claim and execution decisions.

A completed author handoff releases review work. A completed negative review releases bounded correction work, while the stable original delivery gate remains unsatisfied until verified landing. Execution completion alone does not qualify successful delivery. Cancellation, expiration and uncertainty never become fabricated success. Trusted late landing facts remain audit-only and cannot reopen delivery or settle a delegation.

The bundle deliberately exports empty evidence and evaluations. Native receipts and matching-shaped portable records are observations; their presence does not authenticate an issuer or establish independent review, CI, landing or domain acceptance. APRL internally requires reviewer independence from every contributor, including corrections. Frozen external code-delivery/v1 carries only one Author and cannot alone prove full contributor lineage. No v1 field is silently added by this adapter.

Offline structural and semantic conformance is separate from authenticated integration. Corrected owning validator exact source, binary digest, command and test results must be recorded in the delivery plan before review/merge. Preliminary Wazi validator builds are not final acceptance. APRL CI and local service-backed checks retain their separate qualification scopes. This slice does not activate providers, create a new scheduler, deploy production or establish Foundry-to-APRL live interoperability.

## Reproduce offline conformance

Use an externally supplied owning Wazi validator. Verify its SHA256 and `version` output before executing it. The frozen contract version is0.0.1 and expected digest is7582512f; no network fetch is performed by APRL. The landed Darwin arm64 verifier from public Wazi CI37185594744 has SHA256 `ea086c07024c3cb384e3327fbc037659878d12805660650e4d61753e97ee5f70`; source bytes under `contracts/plan/v0` and `cmd/wazi-contract` match landed Wazi47b9d91. CI artifacts are temporary verification artifacts, not releases.

```sh
"$WAZI_CONTRACT_BIN" version
"$WAZI_CONTRACT_BIN" fixtures --contract-digest sha256:7582512f122d2f2a9c4461facc7541c9887053f137260d6ebe9c6dea611d039d
"$WAZI_CONTRACT_BIN" validate --contract-digest sha256:7582512f122d2f2a9c4461facc7541c9887053f137260d6ebe9c6dea611d039d tests/portableplan/aprl-lifecycle.json
```

Successful validation returns JSON on stdout with `valid: true`, empty findings and `authorityAuthenticated: false`, exit0. Invalid input returns exit2; keep diagnostic stderr separate from JSON stdout. A wrong contract digest must reject, rather than silently select another version. The Go golden comparison binds the checked-in consumer example to actual production projection output from canonical state fixtures.
