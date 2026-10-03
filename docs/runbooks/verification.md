# Foundation verification

T1.18.R landed at0728071 with independently reviewed tree69ad59c9a10abd2c283202b350fe76eba68fbdda. T1.19 installs hosted quality gates; hosted execution is qualified only for the exact green source recorded below.

Run one quality job using Go1.27.1, PostgreSQL16, Redis7 and a nonempty unique TEST_RESOURCE_OWNER. Required integration fixtures fail when connectivity is absent. They create isolated database schemas/Redis prefixes and remove only owned fixture data. The workflow never configures production workers, credentials, providers or factory registries.

Quality order: recursively require empty gofmt/goimports diffs; go vet ./...; pinned golangci-lint; regular go test -json ./... -count=1; serialized go test -race -json ./... -count=1. Count terminal events with Test set and require positive passes, zero fail/skip, successful command status. Preserve JSON logs as evidence; test declarations are not executed counts. Locally use external SSD caches, the exact shared build lease and load threshold before heavy commands.

Hosted Ubuntu qualifies Linux-native process identity/group handling only when its actual run is green. Mac evidence qualifies Darwin only. Neither proves OCI isolation, restart-safe host inventory, provider settlement or live deployment. CLI startup remains unavailable until mandatory trusted factories exist.

Negative control: remove a required fixture URL for an explicit integration/system test; require a nonzero command and actual named failed event, no skip. Restore the exact owned environment and require that test to pass. Record separate failing/restored outputs. Never use missing environment as a skip permission.

Record exact reviewed source, hosted run URL/head, event totals, commands and remaining prerequisites after execution. Pending hosted runs are not green evidence. Independent first-class T1.19.R review blocks merge until accepted exact head and verified landing.

Local T1.19 preflight: removing `TEST_DATABASE_URL` executed `TestFoundationFaultsDuplicateAndStaleHTTPResults` and failed with a required-fixture setup error, with no skipped event. Restoring the exact owned fixture environment passed that named case. The initial formatter inventory omitted migrations; after independent review, tracked-source enumeration passed gofmt/goimports on all74tracked Go files, including migrations. The unchanged landed Go source already passed315normal/315race terminal events, zero skips, build/vet/lint; hosted Ubuntu results must be recorded separately before claiming Linux qualification.

Hosted preparation also pins the official Codex CLI0.160.0 release archive and its SHA256 digest for the local capability probe. The probe invokes only `--version`, root `--help`, and `exec --help`; it does not authenticate, launch an agent or request provider execution. Linux initially revealed builtin-shadowing local identifiers, then a missing CLI prerequisite; preserve those failed runs separately from any subsequent green qualification.

Hosted Linux qualification: [run37152786186](https://github.com/ajent-social/APRL/actions/runs/37152786186), exact functional head`b363f02f290f90cf3b48f8451642b6218f9c9fa2`, passed315regular and315race terminal test events with zero failures/skips. PostgreSQL16/Redis7, all tracked-Go formatting, vet and lint passed. The native Linux identity/TERM/KILL/reap fixture tests ran. This qualifies local hosted fixtures only; production host inventory, OCI isolation, provider accounting settlement and factory/profile activation remain pending. The final documentation head also requires a green check before review delivery.

Final reviewed-head receipt: [run37153113270](https://github.com/ajent-social/APRL/actions/runs/37153113270) on`ea6865b7e2effbc8d6809f135976d0c7d6734eca` passed315regular/315race events with zero failures/skips and all quality steps. Independent T1.19.R accepted that exact head and guarded rebase landed`5bb75568c60d26800dbfd7cac2be669f23391b25`, matching whole tree`f75288ed22d1f572423b2496311641437ae4b58b` and reviewed base ancestry.
