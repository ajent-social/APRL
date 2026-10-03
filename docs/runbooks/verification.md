# Foundation verification

T1.18.R landed at0728071 with independently reviewed tree69ad59c9a10abd2c283202b350fe76eba68fbdda. T1.19 installs hosted quality gates; hosted execution remains pending until a recorded green run.

Run one quality job using Go1.27.1, PostgreSQL16, Redis7 and a nonempty unique TEST_RESOURCE_OWNER. Required integration fixtures fail when connectivity is absent. They create isolated database schemas/Redis prefixes and remove only owned fixture data. The workflow never configures production workers, credentials, providers or factory registries.

Quality order: recursively require empty gofmt/goimports diffs; go vet ./...; pinned golangci-lint; regular go test -json ./... -count=1; serialized go test -race -json ./... -count=1. Count terminal events with Test set and require positive passes, zero fail/skip, successful command status. Preserve JSON logs as evidence; test declarations are not executed counts. Locally use external SSD caches, the exact shared build lease and load threshold before heavy commands.

Hosted Ubuntu qualifies Linux-native process identity/group handling only when its actual run is green. Mac evidence qualifies Darwin only. Neither proves OCI isolation, restart-safe host inventory, provider settlement or live deployment. CLI startup remains unavailable until mandatory trusted factories exist.

Negative control: remove a required fixture URL for an explicit integration/system test; require a nonzero command and actual named failed event, no skip. Restore the exact owned environment and require that test to pass. Record separate failing/restored outputs. Never use missing environment as a skip permission.

Record exact reviewed source, hosted run URL/head, event totals, commands and remaining prerequisites after execution. Pending hosted runs are not green evidence. Independent first-class T1.19.R review blocks merge until accepted exact head and verified landing.

Local T1.19 preflight: removing `TEST_DATABASE_URL` executed `TestFoundationFaultsDuplicateAndStaleHTTPResults` and failed with a required-fixture setup error, with no skipped event. Restoring the exact owned fixture environment passed that named case. The initial formatter inventory omitted migrations; after independent review, tracked-source enumeration passed gofmt/goimports on all74tracked Go files, including migrations. The unchanged landed Go source already passed315normal/315race terminal events, zero skips, build/vet/lint; hosted Ubuntu results must be recorded separately before claiming Linux qualification.
