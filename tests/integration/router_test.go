package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/router"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
)

const (
	routerHead0 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	routerHead1 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	routerBase0 = "cccccccccccccccccccccccccccccccccccccccc"
	routerBase1 = "dddddddddddddddddddddddddddddddddddddddd"
)

type routerTestDB struct {
	pool   *testutil.DatabaseFixture
	clock  *clock.Manual
	router *router.Router
	ctx    context.Context
	orgID  string
}

func routerRequireDB(t *testing.T, repo, org string, humanPR bool) routerTestDB {
	t.Helper()
	db := testutil.RequireDatabase(t)
	ctx := context.Background()
	if err := storage.Migrate(ctx, db.Pool); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Pool.Exec(ctx, `INSERT INTO org_budgets(org_id) VALUES($1)`, org); err != nil {
		t.Fatal(err)
	}
	clockNow := clock.NewManual(time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC))
	policy := router.RepositoryPolicy{OrgID: org, PolicyVersion: "router-test-v1", TaskBudgetLimitMicroUSD: 5_000_000, IssueEnrollmentLabel: "aprl:implement", AuthorizedIssueLabelerIDs: map[int64]struct{}{17: {}}, AuthorizedHumanPRAuthorIDs: map[int64]struct{}{}}
	if humanPR {
		policy.AuthorizedHumanPRAuthorIDs[44] = struct{}{}
	}
	r, err := router.New(db.Pool, clockNow, router.Config{Repositories: map[string]router.RepositoryPolicy{repo: policy}, TrustedAPRLActorIDs: map[int64]struct{}{81: {}}, TrustedAPRLAppIDs: map[int64]struct{}{901: {}}, TrustedCIAppIDs: map[int64]struct{}{902: {}}, TrustedCIActorIDs: map[int64]struct{}{82: {}}, TrustedCISenderLogins: map[string]struct{}{"github-actions[bot]": {}}, MaxTaskBudgetMicroUSD: 5_000_000})
	if err != nil {
		t.Fatal(err)
	}
	return routerTestDB{pool: &db, clock: clockNow, router: r, ctx: ctx, orgID: org}
}

func routerStoreDelivery(t *testing.T, f routerTestDB, id, eventType string, payload []byte) {
	t.Helper()
	err := storage.WithUnitOfWork(f.ctx, f.pool.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
		_, err := repos.StoreWebhookDelivery(ctx, storage.DeliveryReceipt{DeliveryID: id, EventType: eventType, Payload: payload})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func routerTask(t *testing.T, f routerTestDB, repo, source, owner, state string) string {
	t.Helper()
	var id string
	err := f.pool.Pool.QueryRow(f.ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,state,policy_version) VALUES($1,$2,$3,$4,$5,'router-test-v1') RETURNING id::text`, f.orgID, repo, source, owner, state).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func routerSeedPR(t *testing.T, f routerTestDB, taskID, repo string, number int, head, base string) int64 {
	t.Helper()
	var id int64
	err := f.pool.Pool.QueryRow(f.ctx, `INSERT INTO prs(task_id,repo_full_name,pr_number,head_sha,base_ref,base_sha,approved_head_sha,approved_base_sha,human_approval_id,ci_status) VALUES($1::uuid,$2,$3,$4,'main',$5,$4,$5,'approval','SUCCESS') RETURNING id`, taskID, repo, number, head, base).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func routerJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func routerDisposition(t *testing.T, f routerTestDB, id string) string {
	t.Helper()
	var value string
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT disposition FROM webhook_deliveries WHERE delivery_id=$1`, id).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}
func routerTaskState(t *testing.T, f routerTestDB, id string) (string, int64) {
	t.Helper()
	var state string
	var gen int64
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT state,generation FROM tasks WHERE id=$1::uuid`, id).Scan(&state, &gen); err != nil {
		t.Fatal(err)
	}
	return state, gen
}
func routerCounts(t *testing.T, f routerTestDB, taskID string) (int, int) {
	t.Helper()
	var jobs, outbox int
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT count(*) FROM jobs WHERE task_id=$1::uuid`, taskID).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox WHERE task_id=$1::uuid`, taskID).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	return jobs, outbox
}

func TestRouterEnrollmentAndReplay(t *testing.T) {
	f := routerRequireDB(t, "owner/enroll", "router-org", true)
	payload := []byte(`{"action":"labeled","repository":{"full_name":"owner/enroll"},"sender":{"id":17,"type":"User"},"issue":{"id":1001,"number":31,"user":{"id":44,"type":"User"}},"label":{"name":"aprl:implement"}}`)
	routerStoreDelivery(t, f, "router-issue-1", "issues", payload)
	if err := f.router.RouteDelivery(f.ctx, "router-issue-1"); err != nil {
		t.Fatal(err)
	}
	var taskID string
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT id::text FROM tasks WHERE repo_full_name='owner/enroll' AND source_key='issue:31'`).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	state, generation := routerTaskState(t, f, taskID)
	jobs, outbox := routerCounts(t, f, taskID)
	if state != "AUTHORING" || generation != 0 || jobs != 1 || outbox != 1 || routerDisposition(t, f, "router-issue-1") != router.DispositionProcessed {
		t.Fatalf("enrollment mismatch state=%s gen=%d jobs=%d outbox=%d disposition=%s", state, generation, jobs, outbox, routerDisposition(t, f, "router-issue-1"))
	}
	if err := f.router.RouteDelivery(f.ctx, "router-issue-1"); err != nil {
		t.Fatal(err)
	}
	state, generation = routerTaskState(t, f, taskID)
	jobs, outbox = routerCounts(t, f, taskID)
	if state != "AUTHORING" || generation != 0 || jobs != 1 || outbox != 1 {
		t.Fatalf("receipt replay duplicated transition/work: state=%s gen=%d jobs=%d outbox=%d", state, generation, jobs, outbox)
	}

	// Human PR enrollment is explicit per repository and does not start Agent A.
	prPayload := []byte(`{"action":"opened","repository":{"full_name":"owner/enroll"},"sender":{"id":44,"type":"User"},"pull_request":{"number":7,"user":{"id":44,"type":"User"},"head":{"sha":"` + routerHead0 + `","ref":"human/topic"},"base":{"sha":"` + routerBase0 + `","ref":"main"},"draft":false}}`)
	routerStoreDelivery(t, f, "router-human-pr", "pull_request", prPayload)
	if err := f.router.RouteDelivery(f.ctx, "router-human-pr"); err != nil {
		t.Fatal(err)
	}
	var prTaskID string
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT id::text FROM tasks WHERE repo_full_name='owner/enroll' AND source_key='github-pr:7'`).Scan(&prTaskID); err != nil {
		t.Fatal(err)
	}
	state, generation = routerTaskState(t, f, prTaskID)
	jobs, outbox = routerCounts(t, f, prTaskID)
	if state != "WAITING_CI" || generation != 0 || jobs != 1 || outbox != 1 {
		t.Fatalf("human PR enrollment mismatch state=%s gen=%d jobs=%d outbox=%d", state, generation, jobs, outbox)
	}
	var operation string
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT operation_type FROM jobs WHERE task_id=$1::uuid`, prTaskID).Scan(&operation); err != nil {
		t.Fatal(err)
	}
	if operation != router.OperationCIReconcile {
		t.Fatalf("human PR unexpectedly queued %q", operation)
	}
}

func TestRouterCITrustAndSnapshot(t *testing.T) {
	f := routerRequireDB(t, "owner/ci", "router-org-ci", true)
	taskID := routerTask(t, f, "owner/ci", "github-pr:9", "44", "WAITING_CI")
	routerSeedPR(t, f, taskID, "owner/ci", 9, routerHead0, routerBase0)
	ciPayload := []byte(`{"repository":{"full_name":"owner/ci"},"check_run":{"head_sha":"` + routerHead0 + `","app":{"id":902}}}`)
	routerStoreDelivery(t, f, "router-ci-1", "check_run", ciPayload)
	if err := f.router.RouteDelivery(f.ctx, "router-ci-1"); err != nil {
		t.Fatal(err)
	}
	jobs, outbox := routerCounts(t, f, taskID)
	if jobs != 1 || outbox != 1 {
		t.Fatalf("trusted CI should coalesce one reconcile hint; jobs=%d outbox=%d", jobs, outbox)
	}
	stale := []byte(`{"repository":{"full_name":"owner/ci"},"check_run":{"head_sha":"` + routerHead1 + `","app":{"id":902}}}`)
	routerStoreDelivery(t, f, "router-ci-stale", "check_run", stale)
	if err := f.router.RouteDelivery(f.ctx, "router-ci-stale"); err != nil {
		t.Fatal(err)
	}
	if got := routerDisposition(t, f, "router-ci-stale"); got != router.DispositionIgnored {
		t.Fatalf("old SHA CI disposition=%s", got)
	}
	state, generation := routerTaskState(t, f, taskID)
	jobs, outbox = routerCounts(t, f, taskID)
	if state != "WAITING_CI" || generation != 0 || jobs != 1 || outbox != 1 {
		t.Fatalf("old CI observation changed task: state=%s gen=%d jobs=%d outbox=%d", state, generation, jobs, outbox)
	}
	baseChange := []byte(`{"action":"edited","repository":{"full_name":"owner/ci"},"sender":{"id":44,"type":"User"},"pull_request":{"number":9,"user":{"id":44,"type":"User"},"head":{"sha":"` + routerHead0 + `","ref":"human/topic"},"base":{"sha":"` + routerBase1 + `","ref":"release"}}}`)
	routerStoreDelivery(t, f, "router-base-change", "pull_request", baseChange)
	if err := f.router.RouteDelivery(f.ctx, "router-base-change"); err != nil {
		t.Fatal(err)
	}
	state, generation = routerTaskState(t, f, taskID)
	jobs, outbox = routerCounts(t, f, taskID)
	var approvedHead, approvedBase, integration *string
	var ciStatus string
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT approved_head_sha,approved_base_sha,integration_sha,ci_status FROM prs WHERE task_id=$1::uuid`, taskID).Scan(&approvedHead, &approvedBase, &integration, &ciStatus); err != nil {
		t.Fatal(err)
	}
	if state != "WAITING_CI" || generation != 1 || jobs != 2 || outbox != 2 || approvedHead != nil || approvedBase != nil || integration != nil || ciStatus != "PENDING" {
		t.Fatalf("base invalidation failed state=%s gen=%d jobs=%d outbox=%d ci=%s", state, generation, jobs, outbox, ciStatus)
	}
}

func TestRouterStatusWebhookShapeAndTrust(t *testing.T) {
	f := routerRequireDB(t, "owner/status", "router-org-status", false)
	taskID := routerTask(t, f, "owner/status", "github-pr:16", "44", "WAITING_CI")
	routerSeedPR(t, f, taskID, "owner/status", 16, routerHead0, routerBase0)

	statusPayload := func(sender string, id int, sha string) []byte {
		return []byte(fmt.Sprintf(`{"repository":{"full_name":"owner/status"},"sha":%q,"state":"success","sender":{"id":%d,"login":%q,"type":"Bot"}}`, sha, id, sender))
	}
	routerStoreDelivery(t, f, "router-status-untrusted", "status", statusPayload("untrusted[bot]", 99, routerHead0))
	if err := f.router.RouteDelivery(f.ctx, "router-status-untrusted"); err != nil {
		t.Fatal(err)
	}
	if got := routerDisposition(t, f, "router-status-untrusted"); got != router.DispositionIgnored {
		t.Fatalf("untrusted top-level status sender disposition=%s", got)
	}
	routerStoreDelivery(t, f, "router-status-stale", "status", statusPayload("github-actions[bot]", 99, routerHead1))
	if err := f.router.RouteDelivery(f.ctx, "router-status-stale"); err != nil {
		t.Fatal(err)
	}
	if got := routerDisposition(t, f, "router-status-stale"); got != router.DispositionIgnored {
		t.Fatalf("stale top-level status SHA disposition=%s", got)
	}
	routerStoreDelivery(t, f, "router-status-current", "status", statusPayload("github-actions[bot]", 99, routerHead0))
	if err := f.router.RouteDelivery(f.ctx, "router-status-current"); err != nil {
		t.Fatal(err)
	}
	if got := routerDisposition(t, f, "router-status-current"); got != router.DispositionProcessed {
		t.Fatalf("trusted current status disposition=%s", got)
	}
	state, generation := routerTaskState(t, f, taskID)
	jobs, outbox := routerCounts(t, f, taskID)
	if state != "WAITING_CI" || generation != 0 || jobs != 1 || outbox != 1 {
		t.Fatalf("status observation became a verdict or duplicated reconciliation: state=%s gen=%d jobs=%d outbox=%d", state, generation, jobs, outbox)
	}
	routerStoreDelivery(t, f, "router-status-current-replay", "status", statusPayload("github-actions[bot]", 99, routerHead0))
	if err := f.router.RouteDelivery(f.ctx, "router-status-current-replay"); err != nil {
		t.Fatal(err)
	}
	jobs, outbox = routerCounts(t, f, taskID)
	if jobs != 1 || outbox != 1 {
		t.Fatalf("status replay duplicated reconciliation: jobs=%d outbox=%d", jobs, outbox)
	}
}

func TestRouterUnexpectedPushPauseAndTerminal(t *testing.T) {
	f := routerRequireDB(t, "owner/pause", "router-org-pause", false)
	taskID := routerTask(t, f, "owner/pause", "github-pr:12", "44", "IN_REVIEW")
	routerSeedPR(t, f, taskID, "owner/pause", 12, routerHead0, routerBase0)
	push := []byte(`{"repository":{"full_name":"owner/pause"},"ref":"refs/heads/human/topic","before":"` + routerHead0 + `","after":"` + routerHead1 + `","sender":{"id":44,"type":"User"}}`)
	routerStoreDelivery(t, f, "router-push-unexpected", "push", push)
	if err := f.router.RouteDelivery(f.ctx, "router-push-unexpected"); err != nil {
		t.Fatal(err)
	}
	state, generation := routerTaskState(t, f, taskID)
	jobs, outbox := routerCounts(t, f, taskID)
	if state != "PAUSED" || generation != 1 || jobs != 0 || outbox != 0 {
		t.Fatalf("unexpected push not fenced state=%s gen=%d jobs=%d outbox=%d", state, generation, jobs, outbox)
	}

	pausedObservation := []byte(`{"action":"synchronize","repository":{"full_name":"owner/pause"},"sender":{"id":44,"type":"User"},"pull_request":{"number":12,"user":{"id":44,"type":"User"},"head":{"sha":"` + routerHead1 + `","ref":"human/topic"},"base":{"sha":"` + routerBase1 + `","ref":"main"}}}`)
	routerStoreDelivery(t, f, "router-paused-observation", "pull_request", pausedObservation)
	if err := f.router.RouteDelivery(f.ctx, "router-paused-observation"); err != nil {
		t.Fatal(err)
	}
	state, generation = routerTaskState(t, f, taskID)
	jobs, outbox = routerCounts(t, f, taskID)
	if state != "PAUSED" || generation != 2 || jobs != 0 || outbox != 0 {
		t.Fatalf("paused observation escaped state state=%s gen=%d jobs=%d outbox=%d", state, generation, jobs, outbox)
	}

	closed := []byte(`{"action":"closed","repository":{"full_name":"owner/pause"},"pull_request":{"number":12,"merged":false}}`)
	routerStoreDelivery(t, f, "router-close", "pull_request", closed)
	if err := f.router.RouteDelivery(f.ctx, "router-close"); err != nil {
		t.Fatal(err)
	}
	state, generation = routerTaskState(t, f, taskID)
	if state != "CLOSED" || generation != 3 {
		t.Fatalf("close did not reach terminal state: %s generation=%d", state, generation)
	}
	reopenedSnapshot := []byte(`{"action":"synchronize","repository":{"full_name":"owner/pause"},"sender":{"id":44,"type":"User"},"pull_request":{"number":12,"user":{"id":44,"type":"User"},"head":{"sha":"` + routerHead1 + `","ref":"human/topic"},"base":{"sha":"` + routerBase1 + `","ref":"main"}}}`)
	routerStoreDelivery(t, f, "router-terminal-observation", "pull_request", reopenedSnapshot)
	if err := f.router.RouteDelivery(f.ctx, "router-terminal-observation"); err != nil {
		t.Fatal(err)
	}
	state, generation = routerTaskState(t, f, taskID)
	if state != "CLOSED" || generation != 3 {
		t.Fatalf("terminal task reopened: %s generation=%d", state, generation)
	}
}

func TestRouterRecordedPushHandoff(t *testing.T) {
	f := routerRequireDB(t, "owner/push", "router-org-push", false)
	taskID := routerTask(t, f, "owner/push", "github-pr:21", "44", "FIXING")
	routerSeedPR(t, f, taskID, "owner/push", 21, routerHead0, routerBase0)
	jobID := "71000000-0000-4000-8000-000000000001"
	operationID := "71000000-0000-4000-8000-000000000002"
	job := contracts.Job{Version: 1, TaskID: taskID, JobID: jobID, Generation: 0, Snapshot: contracts.Snapshot{HeadSHA: routerHead0, BaseSHA: routerBase0}, Attempt: 1, OperationID: operationID, CorrelationID: "71000000-0000-4000-8000-000000000003", Operation: "fix"}
	if _, err := f.pool.Pool.Exec(f.ctx, `INSERT INTO jobs(id,task_id,pr_id,logical_key,operation_type,generation,expected_head_sha,expected_base_sha,payload) SELECT $1::uuid,t.id,pr.id,'router-push-original','fix',0,$3,$4,$5::jsonb FROM tasks t JOIN prs pr ON pr.task_id=t.id WHERE t.id=$2::uuid`, jobID, taskID, routerHead0, routerBase0, routerJSON(t, job)); err != nil {
		t.Fatal(err)
	}
	request := fmt.Sprintf(`{"branch":"human/topic","new_head_sha":"%s","addressed_finding_ids":[],"reply_intents":[]}`, routerHead1)
	if _, err := f.pool.Pool.Exec(f.ctx, `INSERT INTO github_operations(id,job_id,task_id,generation,operation_type,identity,expected_head_sha,expected_base_sha,request,status) VALUES($1::uuid,$2::uuid,$3::uuid,0,'push','reply-push',$4,$5,$6::jsonb,'IN_FLIGHT')`, operationID, jobID, taskID, routerHead0, routerBase0, request); err != nil {
		t.Fatal(err)
	}
	push := []byte(`{"repository":{"full_name":"owner/push"},"ref":"refs/heads/human/topic","before":"` + routerHead0 + `","after":"` + routerHead1 + `"}`)
	routerStoreDelivery(t, f, "router-push-confirm", "push", push)
	if err := f.router.RouteDelivery(f.ctx, "router-push-confirm"); err != nil {
		t.Fatal(err)
	}
	state, generation := routerTaskState(t, f, taskID)
	jobs, outbox := routerCounts(t, f, taskID)
	if state != "WAITING_CI" || generation != 1 || jobs != 2 || outbox != 1 {
		t.Fatalf("push handoff failed state=%s gen=%d jobs=%d outbox=%d", state, generation, jobs, outbox)
	}
	var originalStatus string
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT status FROM jobs WHERE id=$1::uuid`, jobID).Scan(&originalStatus); err != nil {
		t.Fatal(err)
	}
	if originalStatus != "PENDING" {
		t.Fatalf("valid original push job was not preserved: status=%s", originalStatus)
	}
	var ciPayload []byte
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT payload FROM jobs WHERE task_id=$1::uuid AND generation=1 AND operation_type='ci_reconcile'`, taskID).Scan(&ciPayload); err != nil {
		t.Fatal(err)
	}
	ciJob, err := contracts.DecodeJob(ciPayload)
	if err != nil {
		t.Fatal(err)
	}
	if ciJob.Operation != router.OperationCIReconcile || ciJob.Generation != 1 || ciJob.Snapshot.HeadSHA != routerHead1 {
		t.Fatalf("wrong successor CI job: operation=%s generation=%d head=%s", ciJob.Operation, ciJob.Generation, ciJob.Snapshot.HeadSHA)
	}
	var result map[string]json.RawMessage
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT result FROM github_operations WHERE id=$1::uuid`, operationID).Scan(&result); err != nil {
		t.Fatal(err)
	}
	var markedGen int64
	var markedHead string
	if err := json.Unmarshal(result["handoff_generation"], &markedGen); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(result["head_sha"], &markedHead); err != nil {
		t.Fatal(err)
	}
	if markedGen != 1 || markedHead != routerHead1 {
		t.Fatalf("wrong handoff marker generation=%d head=%s", markedGen, markedHead)
	}
	// A distinct delivery echo sees the durable marker and cannot advance twice.
	routerStoreDelivery(t, f, "router-push-echo", "push", push)
	if err := f.router.RouteDelivery(f.ctx, "router-push-echo"); err != nil {
		t.Fatal(err)
	}
	state, generation = routerTaskState(t, f, taskID)
	jobs, outbox = routerCounts(t, f, taskID)
	if state != "WAITING_CI" || generation != 1 || jobs != 2 || outbox != 1 {
		t.Fatalf("push replay advanced twice state=%s gen=%d jobs=%d outbox=%d", state, generation, jobs, outbox)
	}
}

func TestRouterStaleCompletionFence(t *testing.T) {
	f := routerRequireDB(t, "owner/completion", "router-org-completion", false)
	payload := []byte(`{"action":"labeled","repository":{"full_name":"owner/completion"},"sender":{"id":17,"type":"User"},"issue":{"id":2001,"number":3,"user":{"id":44,"type":"User"}},"label":{"name":"aprl:implement"}}`)
	routerStoreDelivery(t, f, "router-completion-enroll", "issues", payload)
	if err := f.router.RouteDelivery(f.ctx, "router-completion-enroll"); err != nil {
		t.Fatal(err)
	}
	var taskID, jobID string
	var jobPayload []byte
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT id::text FROM tasks WHERE repo_full_name='owner/completion' AND source_key='issue:3'`).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT id::text,payload FROM jobs WHERE task_id=$1::uuid`, taskID).Scan(&jobID, &jobPayload); err != nil {
		t.Fatal(err)
	}
	job, err := contracts.DecodeJob(jobPayload)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.WithUnitOfWork(f.ctx, f.pool.Pool, f.clock, func(ctx context.Context, repos *storage.Repositories) error {
		_, task, err := repos.LockOrgBudgetAndTask(ctx, "router-org-completion", taskID)
		if err != nil {
			return err
		}
		_, err = repos.UpdateTaskState(ctx, task, 0, 1, "WAITING_CI")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	completionPayload := []byte(`{"repository":{"full_name":"owner/completion"}}`)
	routerStoreDelivery(t, f, "router-old-completion", "agent.completed", completionPayload)
	event := contracts.Event{Version: 1, EventID: "71000000-0000-4000-8000-000000000004", EventType: "completion", TaskID: taskID, JobID: jobID, Generation: 0, Snapshot: job.Snapshot, OperationID: job.OperationID, CorrelationID: job.CorrelationID}
	if err := f.router.RouteTaskEvent(f.ctx, "router-old-completion", event); err != nil {
		t.Fatal(err)
	}
	state, generation := routerTaskState(t, f, taskID)
	jobs, outbox := routerCounts(t, f, taskID)
	if state != "WAITING_CI" || generation != 1 || jobs != 1 || outbox != 1 || routerDisposition(t, f, "router-old-completion") != router.DispositionIgnored {
		t.Fatalf("stale completion was not fenced state=%s gen=%d jobs=%d outbox=%d disposition=%s", state, generation, jobs, outbox, routerDisposition(t, f, "router-old-completion"))
	}

	// The exact job tuple, not only UUID shape, binds a normalized event.
	bad := event
	bad.TaskID = "71000000-0000-4000-8000-000000000099"
	bad.EventID = "71000000-0000-4000-8000-000000000005"
	routerStoreDelivery(t, f, "router-wrong-task-completion", "agent.completed", completionPayload)
	if err := f.router.RouteTaskEvent(f.ctx, "router-wrong-task-completion", bad); err != nil {
		t.Fatal(err)
	}
	if got := routerDisposition(t, f, "router-wrong-task-completion"); got != router.DispositionRejected {
		t.Fatalf("wrong task/job binding disposition=%s", got)
	}
}

func TestRouterPolicyFailClosed(t *testing.T) {
	f := routerRequireDB(t, "owner/policy", "router-org-policy", false)
	payload := []byte(`{"action":"labeled","repository":{"full_name":"owner/policy"},"sender":{"id":999,"type":"Bot"},"issue":{"id":3001,"number":4,"user":{"id":44,"type":"User"}},"label":{"name":"aprl:implement"}}`)
	routerStoreDelivery(t, f, "router-unauthorized-label", "issues", payload)
	if err := f.router.RouteDelivery(f.ctx, "router-unauthorized-label"); err != nil {
		t.Fatal(err)
	}
	if got := routerDisposition(t, f, "router-unauthorized-label"); got != router.DispositionIgnored {
		t.Fatalf("unauthorized enrollment disposition=%s", got)
	}
	var count int
	if err := f.pool.Pool.QueryRow(f.ctx, `SELECT count(*) FROM tasks WHERE repo_full_name='owner/policy'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("unauthorized event enrolled %d tasks", count)
	}
	unknown := []byte(`{"action":"labeled","repository":{"full_name":"unconfigured/repo"},"sender":{"id":17,"type":"User"},"issue":{"id":3002,"number":5,"user":{"id":44,"type":"User"}},"label":{"name":"aprl:implement"}}`)
	routerStoreDelivery(t, f, "router-unknown-repo", "issues", unknown)
	if err := f.router.RouteDelivery(f.ctx, "router-unknown-repo"); err != nil {
		t.Fatal(err)
	}
	if got := routerDisposition(t, f, "router-unknown-repo"); got != router.DispositionIgnored {
		t.Fatalf("unknown repository disposition=%s", got)
	}
}
