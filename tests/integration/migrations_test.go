package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ajent-social/APRL/internal/storage"
	"github.com/ajent-social/APRL/tests/testutil"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMigrations(t *testing.T) {
	database := testutil.RequireDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var serverVersion int
	if err := database.Pool.QueryRow(ctx, `SELECT current_setting('server_version_num')::integer`).Scan(&serverVersion); err != nil {
		t.Fatalf("read PostgreSQL server version: %v", err)
	}
	if serverVersion < 160000 || serverVersion >= 170000 {
		t.Fatalf("PostgreSQL server_version_num = %d, want PostgreSQL 16", serverVersion)
	}
	var currentSchema string
	if err := database.Pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&currentSchema); err != nil {
		t.Fatalf("read current schema: %v", err)
	}
	if currentSchema != database.Schema {
		t.Fatalf("current schema = %q, want owned fixture schema %q", currentSchema, database.Schema)
	}

	if err := storage.Migrate(ctx, database.Pool); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	if err := storage.Migrate(ctx, database.Pool); err != nil {
		t.Fatalf("second Migrate() error = %v", err)
	}
	migrationAssertCoreTables(ctx, t, database)

	orgID := "org-migration-test"
	if _, err := database.Pool.Exec(ctx, `INSERT INTO org_budgets (org_id) VALUES ($1)`, orgID); err != nil {
		t.Fatalf("insert org budget: %v", err)
	}
	var taskID string
	if err := database.Pool.QueryRow(ctx, `
		INSERT INTO tasks (org_id, repo_full_name, source_key, owner_id, policy_version)
		VALUES ($1, 'example/repo', 'issue:17', 'actor-1', 'policy-v1') RETURNING id`, orgID).Scan(&taskID); err != nil {
		t.Fatalf("insert task: %v", err)
	}
	var prID int64
	if err := database.Pool.QueryRow(ctx, `
		INSERT INTO prs (task_id, repo_full_name, pr_number, head_sha, base_ref, base_sha)
		VALUES ($1, 'example/repo', 18, $2, 'main', $3) RETURNING id`, taskID, migrationHeadSHA, migrationBaseSHA).Scan(&prID); err != nil {
		t.Fatalf("insert PR: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `INSERT INTO webhook_deliveries (delivery_id, event_type, payload) VALUES ('delivery-1', 'issues', '{}')`); err != nil {
		t.Fatalf("insert webhook delivery: %v", err)
	}
	var jobID string
	if err := database.Pool.QueryRow(ctx, `
		INSERT INTO jobs (task_id, pr_id, source_delivery_id, logical_key, operation_type, generation,
		                  expected_head_sha, expected_base_sha, remediation_attempt, payload)
		VALUES ($1, $2, 'delivery-1', 'task-1:generation-0:author', 'author', 0,
		        $3, $4, 1, '{}') RETURNING id`, taskID, prID, migrationHeadSHA, migrationBaseSHA).Scan(&jobID); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `INSERT INTO outbox (task_id, job_id, kind, payload) VALUES ($1, $2, 'DISPATCH', '{}')`, taskID, jobID); err != nil {
		t.Fatalf("insert outbox item: %v", err)
	}
	var runID string
	if err := database.Pool.QueryRow(ctx, `
		INSERT INTO agent_runs (task_id, job_id, attempt_number, agent_type, codex_prompt_hash,
		                        generation, lease_token, expected_head_sha, expected_base_sha,
		                        supervisor_identity, supervisor_credential_id, execution_status, started_at)
		VALUES ($1, $2, 1, 'author', $3, 0, '99999999-9999-4999-8999-999999999999', $4, $5,
		        'host-supervisor-a', 'credential-key-7', 'RUNNING', CURRENT_TIMESTAMP) RETURNING id`,
		taskID, jobID, migrationPromptHash, migrationHeadSHA, migrationBaseSHA).Scan(&runID); err != nil {
		t.Fatalf("insert agent run: %v", err)
	}
	var reservationID string
	if err := database.Pool.QueryRow(ctx, `
		INSERT INTO budget_reservations (task_id, org_id, run_id, envelope_micro_usd, remaining_micro_usd,
		                                pricing_version, status)
		VALUES ($1, $2, $3, 100000, 80000, 'rate-card-v1', 'RESERVED') RETURNING id`, taskID, orgID, runID).Scan(&reservationID); err != nil {
		t.Fatalf("insert valid reservation: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `INSERT INTO cost_entries (request_id, reservation_id, model, usage, cost_micro_usd, charged_at) VALUES ('request-1', $1, 'test-model', '{}', 20000, CURRENT_TIMESTAMP)`, reservationID); err != nil {
		t.Fatalf("insert cost entry: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `INSERT INTO review_cycles (pr_id, generation, head_sha, base_sha, verdict, normalized_version, blocker_fingerprints, payload) VALUES ($1, 0, $2, $3, 'request_changes', 'normalize-v1', '[]', '{}')`, prID, migrationHeadSHA, migrationBaseSHA); err != nil {
		t.Fatalf("insert review cycle: %v", err)
	}
	var reviewID int64
	if err := database.Pool.QueryRow(ctx, `SELECT id FROM review_cycles WHERE pr_id = $1`, prID).Scan(&reviewID); err != nil {
		t.Fatalf("read review cycle: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `INSERT INTO findings (review_id, severity, body, anchor) VALUES ($1, 'blocker', 'unanchored blocker', NULL)`, reviewID); err != nil {
		t.Fatalf("insert unanchored blocker: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `INSERT INTO github_operations (job_id, task_id, generation, operation_type, identity, expected_head_sha, expected_base_sha, request, status) VALUES ($1, $2, 0, 'push', 'operation-1', $3, $4, '{}', 'INTENDED')`, jobID, taskID, migrationHeadSHA, migrationBaseSHA); err != nil {
		t.Fatalf("insert GitHub operation: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `INSERT INTO escalations (task_id, reason) VALUES ($1, 'test escalation')`, taskID); err != nil {
		t.Fatalf("insert escalation: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `INSERT INTO control_actions (task_id, actor_id, action, reason, previous_generation, new_generation) VALUES ($1, 'operator-1', 'pause', 'test action', 0, 1)`, taskID); err != nil {
		t.Fatalf("insert control action: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `
		INSERT INTO worker_result_receipts (operation_id, task_id, job_id, run_id, generation,
		    lease_token, expected_head_sha, expected_base_sha, correlation_id, supervisor_identity,
		    supervisor_credential_id, result_status, result_payload)
		VALUES ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', $1, $2, $3, 0,
		    '99999999-9999-4999-8999-999999999999', $4, $5,
		    'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', 'host-supervisor-a', 'credential-key-7',
		    'succeeded', '{"summary":"done"}')`, taskID, jobID, runID, migrationHeadSHA, migrationBaseSHA); err != nil {
		t.Fatalf("insert worker result receipt: %v", err)
	}

	t.Run("rejects duplicate source delivery and logical job keys", func(t *testing.T) {
		_, err := database.Pool.Exec(ctx, `INSERT INTO tasks (org_id, repo_full_name, source_key, owner_id, policy_version) VALUES ($1, 'example/repo', 'issue:17', 'actor-1', 'policy-v1')`, orgID)
		migrationAssertSQLState(t, err, "23505", "duplicate task source key")
		_, err = database.Pool.Exec(ctx, `INSERT INTO webhook_deliveries (delivery_id, event_type, payload) VALUES ('delivery-1', 'issues', '{}')`)
		migrationAssertSQLState(t, err, "23505", "duplicate delivery ID")
		_, err = database.Pool.Exec(ctx, `INSERT INTO jobs (task_id, logical_key, operation_type, generation, payload) VALUES ($1, 'task-1:generation-0:author', 'author', 0, '{}')`, taskID)
		migrationAssertSQLState(t, err, "23505", "duplicate logical job key")
	})

	t.Run("rejects invalid states amounts and admission mutation", func(t *testing.T) {
		_, err := database.Pool.Exec(ctx, `UPDATE tasks SET state = 'NOT_A_STATE' WHERE id = $1`, taskID)
		migrationAssertSQLState(t, err, "23514", "invalid lifecycle state")
		_, err = database.Pool.Exec(ctx, `INSERT INTO jobs (task_id, logical_key, operation_type, generation, payload, status) VALUES ($1, 'task-1:generation-0:unleased', 'author', 0, '{}', 'LEASED')`, taskID)
		migrationAssertSQLState(t, err, "23514", "leased job without a lease token")
		_, err = database.Pool.Exec(ctx, `INSERT INTO budget_reservations (task_id, org_id, run_id, envelope_micro_usd, remaining_micro_usd, pricing_version, status) VALUES ($1, $2, $3, -1, 0, 'rate-card-v1', 'RESERVED')`, taskID, orgID, runID)
		migrationAssertSQLState(t, err, "23514", "negative reservation")
		_, err = database.Pool.Exec(ctx, `UPDATE agent_runs SET generation = 1 WHERE id = $1`, runID)
		migrationAssertSQLState(t, err, "23514", "immutable run admission tuple")
		_, err = database.Pool.Exec(ctx, `UPDATE agent_runs SET agent_type = 'reviewer' WHERE id = $1`, runID)
		migrationAssertSQLState(t, err, "23514", "immutable run role")
	})

	t.Run("rejects conflicting replay operation identity", func(t *testing.T) {
		var otherTaskID string
		if err := database.Pool.QueryRow(ctx, `INSERT INTO tasks (org_id, repo_full_name, source_key, owner_id, policy_version) VALUES ($1, 'example/other-repo', 'issue:23', 'actor-2', 'policy-v1') RETURNING id`, orgID).Scan(&otherTaskID); err != nil {
			t.Fatalf("insert task for replay identity check: %v", err)
		}
		var otherJobID string
		if err := database.Pool.QueryRow(ctx, `INSERT INTO jobs (task_id, logical_key, operation_type, generation, payload) VALUES ($1, 'task-2:generation-0:author', 'author', 0, '{}') RETURNING id`, otherTaskID).Scan(&otherJobID); err != nil {
			t.Fatalf("insert job for replay identity check: %v", err)
		}
		var otherRunID string
		if err := database.Pool.QueryRow(ctx, `
			INSERT INTO agent_runs (task_id, job_id, attempt_number, agent_type, codex_prompt_hash,
			                        generation, lease_token, supervisor_identity, supervisor_credential_id,
			                        execution_status, started_at)
			VALUES ($1, $2, 1, 'author', $3, 0, 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
			        'host-supervisor-b', 'credential-key-8', 'RUNNING', CURRENT_TIMESTAMP) RETURNING id`,
			otherTaskID, otherJobID, migrationPromptHash).Scan(&otherRunID); err != nil {
			t.Fatalf("insert run for replay identity check: %v", err)
		}
		_, err := database.Pool.Exec(ctx, `
			INSERT INTO worker_result_receipts (operation_id, task_id, job_id, run_id, generation,
			    lease_token, expected_head_sha, expected_base_sha, correlation_id, supervisor_identity,
			    supervisor_credential_id, result_status, result_payload)
			VALUES ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', $1, $2, $3, 0,
			    'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', NULL, NULL,
			    'cccccccc-cccc-4ccc-8ccc-cccccccccccc', 'host-supervisor-b', 'credential-key-8',
			    'succeeded', '{"summary":"different task and run"}')`, otherTaskID, otherJobID, otherRunID)
		migrationAssertSQLState(t, err, "23505", "operation ID rebound to a different task and run")
	})

	t.Run("records actual over-budget spend and protects billing identity", func(t *testing.T) {
		if _, err := database.Pool.Exec(ctx, `UPDATE tasks SET spent_micro_usd=budget_limit_micro_usd+20000, reserved_micro_usd=100000 WHERE id=$1`, taskID); err != nil {
			t.Fatalf("actual billing must survive an admission ceiling breach: %v", err)
		}
		_, err := database.Pool.Exec(ctx, `INSERT INTO budget_reservations(task_id,org_id,run_id,envelope_micro_usd,remaining_micro_usd,pricing_version,status) VALUES($1,$2,$3,100000,0,'rate-card-v1','RESERVED')`, taskID, orgID, runID)
		migrationAssertSQLState(t, err, "23505", "duplicate run reservation")
		_, err = database.Pool.Exec(ctx, `UPDATE budget_reservations SET pricing_version='changed' WHERE id=$1`, reservationID)
		migrationAssertSQLState(t, err, "23514", "immutable reservation pricing")
		_, err = database.Pool.Exec(ctx, `UPDATE budget_reservations SET admission_envelope='{"model":"changed"}'::jsonb WHERE id=$1`, reservationID)
		migrationAssertSQLState(t, err, "23514", "immutable admission envelope")
		_, err = database.Pool.Exec(ctx, `UPDATE cost_entries SET cost_micro_usd=0 WHERE request_id='request-1'`)
		migrationAssertSQLState(t, err, "23514", "immutable recorded charge")
		_, err = database.Pool.Exec(ctx, `DELETE FROM cost_entries WHERE request_id='request-1'`)
		migrationAssertSQLState(t, err, "23514", "append-only recorded charge")
	})

	t.Run("records bounded supervisor process ownership", func(t *testing.T) {
		_, err := database.Pool.Exec(ctx, `UPDATE agent_runs SET process_id=123 WHERE id=$1`, runID)
		migrationAssertSQLState(t, err, "23514", "incomplete process ownership")
		if _, err := database.Pool.Exec(ctx, `UPDATE agent_runs SET process_id=123,process_group_id=123,workspace_id=id,process_started_at=CURRENT_TIMESTAMP,last_heartbeat_at=CURRENT_TIMESTAMP,execution_deadline_at=CURRENT_TIMESTAMP+interval '1 minute' WHERE id=$1`, runID); err != nil {
			t.Fatalf("record bounded process ownership: %v", err)
		}
		if _, err := database.Pool.Exec(ctx, `UPDATE agent_runs SET last_heartbeat_at=last_heartbeat_at+interval '1 second' WHERE id=$1`, runID); err != nil {
			t.Fatalf("record forward heartbeat: %v", err)
		}
		for _, mutation := range []string{`process_id=456`, `process_group_id=456`, `workspace_id=gen_random_uuid()`, `process_started_at=process_started_at+interval '1 second'`, `execution_deadline_at=execution_deadline_at+interval '1 minute'`, `last_heartbeat_at=last_heartbeat_at-interval '1 second'`} {
			_, err := database.Pool.Exec(ctx, "UPDATE agent_runs SET "+mutation+" WHERE id=$1", runID)
			migrationAssertSQLState(t, err, "23514", "immutable process ownership/monotonic heartbeat "+mutation)
		}
	})

	t.Run("protects immutable GitHub mutation intent", func(t *testing.T) {
		if _, err := database.Pool.Exec(ctx, `UPDATE github_operations SET status='CONFIRMED',remote_id='remote-1',result='{"head_sha":"confirmed"}' WHERE task_id=$1`, taskID); err != nil {
			t.Fatalf("update mutable transport outcome: %v", err)
		}
		for _, mutation := range []string{
			`id=gen_random_uuid()`, `job_id=NULL`, `task_id=gen_random_uuid()`,
			`generation=generation+1`, `operation_type='merge'`, `identity='different'`,
			`expected_head_sha=NULL,expected_base_sha=NULL`,
			`request='{"run_id":"different","lease_token":"different"}'::jsonb`,
			`created_at=created_at+interval '1 second'`,
		} {
			_, err := database.Pool.Exec(ctx, "UPDATE github_operations SET "+mutation+" WHERE task_id=$1", taskID)
			migrationAssertSQLState(t, err, "23514", "immutable GitHub intent "+mutation)
		}
		_, err := database.Pool.Exec(ctx, `DELETE FROM github_operations WHERE task_id=$1`, taskID)
		migrationAssertSQLState(t, err, "23514", "append-only GitHub intent")
		var original bool
		if err := database.Pool.QueryRow(ctx, `SELECT status='CONFIRMED' AND remote_id='remote-1' AND request='{}'::jsonb AND result='{"head_sha":"confirmed"}'::jsonb FROM github_operations WHERE task_id=$1`, taskID).Scan(&original); err != nil || !original {
			t.Fatalf("intent/outcome not preserved: original=%v err=%v", original, err)
		}
	})

	t.Run("retains audit rows when a task closes", func(t *testing.T) {
		if _, err := database.Pool.Exec(ctx, `UPDATE tasks SET state = 'CLOSED', generation = 1 WHERE id = $1`, taskID); err != nil {
			t.Fatalf("close task: %v", err)
		}
		for _, item := range []struct {
			table string
			query string
		}{
			{table: "budget_reservations", query: `SELECT count(*) FROM budget_reservations WHERE task_id = $1`},
			{table: "cost_entries", query: `SELECT count(*) FROM cost_entries ce JOIN budget_reservations br ON br.id = ce.reservation_id WHERE br.task_id = $1`},
			{table: "github_operations", query: `SELECT count(*) FROM github_operations WHERE task_id = $1`},
			{table: "control_actions", query: `SELECT count(*) FROM control_actions WHERE task_id = $1`},
			{table: "worker_result_receipts", query: `SELECT count(*) FROM worker_result_receipts WHERE task_id = $1`},
		} {
			var count int
			if err := database.Pool.QueryRow(ctx, item.query, taskID).Scan(&count); err != nil {
				t.Fatalf("count %s after closure: %v", item.table, err)
			}
			if count != 1 {
				t.Errorf("%s rows after task closure = %d, want 1", item.table, count)
			}
		}
	})

}

const (
	migrationHeadSHA    = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	migrationBaseSHA    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	migrationPromptHash = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func migrationAssertCoreTables(ctx context.Context, t *testing.T, database testutil.DatabaseFixture) {
	t.Helper()
	var count int
	if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'`, database.Schema).Scan(&count); err != nil {
		t.Fatalf("count tables in owned schema: %v", err)
	}
	if count != 20 {
		t.Fatalf("owned schema table count = %d, want 20", count)
	}
	var applied bool
	if err := database.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = '001_core')`).Scan(&applied); err != nil {
		t.Fatalf("read migration ledger: %v", err)
	}
	if !applied {
		t.Fatal("core schema version was not recorded")
	}
	if err := database.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = '004_delegations')`).Scan(&applied); err != nil {
		t.Fatalf("read delegation migration ledger: %v", err)
	}
	if !applied {
		t.Fatal("delegation schema version was not recorded")
	}
}

func migrationAssertSQLState(t *testing.T, err error, code, label string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s unexpectedly succeeded", label)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("%s error = %T %v, want PostgreSQL SQLSTATE %s", label, err, err, code)
	}
	if pgErr.Code != code {
		t.Fatalf("%s SQLSTATE = %s, want %s (error %v)", label, pgErr.Code, code, err)
	}
	t.Logf("expected database rejection observed: %s returned SQLSTATE %s (%s)", label, pgErr.Code, pgErr.ConstraintName)
}
