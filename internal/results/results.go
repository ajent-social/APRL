// Package results accepts authenticated worker completions and commits their
// durable disposition with any correlated push handoff.
package results

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/budget"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/router"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrInvalid reports a malformed result or authenticated principal.
	ErrInvalid = errors.New("invalid result submission")
	// ErrForbidden reports a principal that does not own the submitted run.
	ErrForbidden = errors.New("result principal is not authorized")
	// ErrConflict reports reuse of a result operation ID with different meaning.
	ErrConflict = errors.New("result operation conflicts with accepted receipt")
	// ErrStale reports a result whose execution lease or task fence is no longer current.
	ErrStale = errors.New("result execution is stale")
)

// Principal is supplied by trusted host authentication and is never decoded
// from the result request body.
type Principal struct {
	TaskID       string
	RunID        string
	Identity     string
	CredentialID string
}

// Accepted identifies a result receipt committed by Submit.
type Accepted struct {
	OperationID string
	Duplicate   bool
}

// Service commits worker results, execution disposition, usage uncertainty,
// and confirmed push handoffs in one PostgreSQL unit of work.
type Service struct {
	pool   *pgxpool.Pool
	clock  clock.Clock
	router *router.Router
}

// New constructs a result service. A router is required to finalize confirmed pushes.
func New(pool *pgxpool.Pool, c clock.Clock, pushRouter *router.Router) (*Service, error) {
	if pool == nil || c == nil {
		return nil, errors.New("results service requires PostgreSQL pool and clock")
	}
	return &Service{pool: pool, clock: c, router: pushRouter}, nil
}

// Submit authenticates the immutable run tuple, then accepts an exact replay
// or commits a currently fenced completion and any push successors atomically.
func (s *Service) Submit(ctx context.Context, principal Principal, result contracts.Result) (Accepted, error) {
	if s == nil || s.pool == nil || s.clock == nil || ctx == nil || strings.TrimSpace(principal.TaskID) == "" ||
		strings.TrimSpace(principal.RunID) == "" || strings.TrimSpace(principal.Identity) == "" || strings.TrimSpace(principal.CredentialID) == "" {
		return Accepted{}, ErrInvalid
	}
	if err := result.Validate(); err != nil {
		return Accepted{}, fmt.Errorf("validate result: %w", ErrInvalid)
	}
	result.TaskID = strings.ToLower(result.TaskID)
	result.JobID = strings.ToLower(result.JobID)
	result.RunID = strings.ToLower(result.RunID)
	result.LeaseToken = strings.ToLower(result.LeaseToken)
	result.OperationID = strings.ToLower(result.OperationID)
	result.CorrelationID = strings.ToLower(result.CorrelationID)
	principal.TaskID = strings.ToLower(principal.TaskID)
	principal.RunID = strings.ToLower(principal.RunID)
	if principal.TaskID != result.TaskID || principal.RunID != result.RunID {
		return Accepted{}, ErrForbidden
	}
	var accepted Accepted
	err := storage.WithUnitOfWork(ctx, s.pool, s.clock, func(ctx context.Context, repos *storage.Repositories) error {
		var orgID string
		if err := repos.Queries().QueryRow(ctx, `SELECT org_id FROM tasks WHERE id=$1::uuid`, result.TaskID).Scan(&orgID); errors.Is(err, pgx.ErrNoRows) {
			return ErrStale
		} else if err != nil {
			return err
		}
		_, locked, err := repos.LockOrgBudgetAndTask(ctx, orgID, result.TaskID)
		if err != nil {
			return err
		}
		var run leaseAdmission
		var payload []byte
		var jobStatus, operationType string
		var expires *time.Time
		err = repos.Queries().QueryRow(ctx, `SELECT r.id::text,r.task_id::text,r.job_id::text,r.generation,r.lease_token::text,
			r.expected_head_sha,r.expected_base_sha,r.expected_integration_sha,r.supervisor_identity,r.supervisor_credential_id,
			r.execution_status,r.agent_type,j.payload,j.status,j.operation_type,j.lease_expires_at
			FROM agent_runs r JOIN jobs j ON j.id=r.job_id AND j.task_id=r.task_id
			WHERE r.id=$1::uuid AND r.task_id=$2::uuid AND j.id=$3::uuid FOR UPDATE OF j,r`, result.RunID, result.TaskID, result.JobID).
			Scan(&run.runID, &run.taskID, &run.jobID, &run.generation, &run.token, &run.head, &run.base, &run.integration,
				&run.identity, &run.credentialID, &run.status, &run.agentType, &payload, &jobStatus, &operationType, &expires)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrForbidden
		}
		if err != nil {
			return err
		}
		job, err := contracts.DecodeJob(payload)
		if err != nil {
			return fmt.Errorf("decode durable result job: %w", err)
		}
		if run.taskID != principal.TaskID || run.runID != principal.RunID || run.identity != principal.Identity || run.credentialID != principal.CredentialID {
			return ErrForbidden
		}
		if !resultMatchesAdmission(result, run, job, operationType) {
			return ErrStale
		}
		var pushHandoff bool
		if operationType == "fix" {
			var pushExists bool
			if err := repos.Queries().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM github_operations WHERE id=$1::uuid AND job_id=$2::uuid AND task_id=$3::uuid AND operation_type='push')`,
				result.OperationID, result.JobID, result.TaskID).Scan(&pushExists); err != nil {
				return err
			}
			pushHandoff = pushExists
		}
		if pushHandoff {
			if result.Status != "succeeded" {
				return ErrStale
			}
			var opTask, opJob, opIdentity, opStatus string
			var opGeneration int64
			var expectedHead, expectedBase *string
			var request []byte
			err := repos.Queries().QueryRow(ctx, `SELECT task_id::text,job_id::text,generation,identity,status,expected_head_sha,expected_base_sha,request
				FROM github_operations WHERE id=$1::uuid AND operation_type='push' FOR UPDATE`, result.OperationID).
				Scan(&opTask, &opJob, &opGeneration, &opIdentity, &opStatus, &expectedHead, &expectedBase, &request)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrStale
			}
			if err != nil {
				return err
			}
			if opTask != result.TaskID || opJob != result.JobID || opGeneration != result.Generation || opIdentity != run.agentType ||
				!sameOptional(expectedHead, result.Snapshot.HeadSHA) || !sameOptional(expectedBase, result.Snapshot.BaseSHA) ||
				!pushIntentMatches(request, result) || opStatus != "CONFIRMED" {
				return ErrStale
			}
			if !validReplyIntents(request) {
				return ErrStale
			}
			if err := validateReplyFindings(ctx, repos, locked, request); err != nil {
				return err
			}
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return fmt.Errorf("encode result receipt: %w", err)
		}
		duplicate, err := acceptedReceipt(ctx, repos, principal, result, encoded)
		if err != nil {
			return err
		}
		if duplicate {
			accepted = Accepted{OperationID: result.OperationID, Duplicate: true}
			return nil
		}
		lease := leases.Lease{TaskID: result.TaskID, JobID: result.JobID, RunID: result.RunID, Token: result.LeaseToken,
			Generation: result.Generation, Snapshot: result.Snapshot, Attempt: result.Attempt}
		// RunAttempt is independently loaded from the immutable run row.
		if err := repos.Queries().QueryRow(ctx, `SELECT attempt_number FROM agent_runs WHERE id=$1::uuid`, result.RunID).Scan(&lease.RunAttempt); err != nil {
			return err
		}
		current := locked.Record()
		webhookHandoff := false
		if current.Generation == result.Generation && sameSnapshot(current.Snapshot, result.Snapshot) {
			if err := leases.ValidateLocked(ctx, repos, lease, s.clock); err != nil {
				if errors.Is(err, leases.ErrStale) || errors.Is(err, leases.ErrInvalid) {
					return fmt.Errorf("validate current result lease: %w", ErrStale)
				}
				return err
			}
		} else {
			webhookHandoff, err = s.validateWebhookFirst(ctx, repos, current, result, run, pushHandoff, jobStatus, expires)
			if err != nil {
				return err
			}
		}
		if err := completeRunLocked(ctx, repos, lease, result.Status, webhookHandoff, s.clock); err != nil {
			if errors.Is(err, leases.ErrStale) || errors.Is(err, leases.ErrInvalid) {
				return ErrStale
			}
			return err
		}
		if leases.RequiresInference(operationType) {
			var reservationID string
			err := repos.Queries().QueryRow(ctx, `SELECT id::text FROM budget_reservations WHERE run_id=$1::uuid`, result.RunID).Scan(&reservationID)
			if errors.Is(err, pgx.ErrNoRows) {
				return fmt.Errorf("inference result has no budget reservation: %w", ErrStale)
			}
			if err != nil {
				return err
			}
			if err := budget.MarkUnknownLocked(ctx, repos, s.clock, reservationID); err != nil {
				if errors.Is(err, budget.ErrReservationClosed) || errors.Is(err, budget.ErrReservationConflict) || errors.Is(err, budget.ErrAdmission) {
					return fmt.Errorf("inference result has invalid usage reservation: %w", ErrStale)
				}
				return fmt.Errorf("retain unverified provider usage: %w", err)
			}
		}
		if pushHandoff {
			if s.router == nil {
				return ErrStale
			}
			if current.State == "PAUSED" || current.State == "ESCALATED" {
				if err := setReplyMarker(ctx, repos, result.OperationID, false); err != nil {
					return err
				}
			} else {
				var branch, newHead string
				if err := repos.Queries().QueryRow(ctx, `SELECT request->>'branch',request->>'new_head_sha' FROM github_operations WHERE id=$1::uuid AND status='CONFIRMED'`, result.OperationID).Scan(&branch, &newHead); err != nil {
					return fmt.Errorf("load confirmed push intent: %w", err)
				}
				updated, _, err := s.router.ApplyConfirmedPush(ctx, repos, locked, result.OperationID, branch, newHead)
				if err != nil {
					if errors.Is(err, router.ErrPushFence) || errors.Is(err, storage.ErrStaleGeneration) {
						return ErrStale
					}
					return err
				}
				if err := MaterializeRepliesLocked(ctx, repos, updated, result.OperationID); err != nil {
					return err
				}
			}
		}
		if err := insertReceipt(ctx, repos, principal, result, run, encoded, s.clock.Now().UTC()); err != nil {
			return err
		}
		accepted = Accepted{OperationID: result.OperationID}
		return nil
	})
	if err != nil {
		return Accepted{}, err
	}
	return accepted, nil
}

type leaseAdmission struct {
	runID, taskID, jobID, token string
	generation                  int64
	head, base, integration     *string
	identity, credentialID      string
	status, agentType           string
}

func resultMatchesAdmission(result contracts.Result, run leaseAdmission, job contracts.Job, operation string) bool {
	mappedAgent, supported := leases.AgentTypeForOperation(operation)
	return result.RunID == run.runID && result.TaskID == run.taskID && result.JobID == run.jobID && result.Generation == run.generation &&
		result.LeaseToken == run.token && result.Attempt == job.Attempt && result.OperationID == job.OperationID && result.CorrelationID == job.CorrelationID &&
		job.TaskID == result.TaskID && job.JobID == result.JobID && job.Generation == result.Generation && job.Operation == operation && operation != "" &&
		supported && run.agentType == mappedAgent && job.RunID == "" && job.LeaseToken == "" && sameSnapshot(job.Snapshot, result.Snapshot) &&
		result.Snapshot.HeadSHA == optional(run.head) && result.Snapshot.BaseSHA == optional(run.base) && result.Snapshot.IntegrationSHA == optional(run.integration)
}

func acceptedReceipt(ctx context.Context, repos *storage.Repositories, principal Principal, result contracts.Result, encoded []byte) (bool, error) {
	var taskID, jobID, runID, token, correlationID, identity, credentialID, status string
	var generation int64
	var head, base, integration *string
	var payload []byte
	err := repos.Queries().QueryRow(ctx, `SELECT task_id::text,job_id::text,run_id::text,generation,lease_token::text,
		expected_head_sha,expected_base_sha,expected_integration_sha,correlation_id::text,supervisor_identity,supervisor_credential_id,result_status,result_payload
		FROM worker_result_receipts WHERE operation_id=$1::uuid FOR UPDATE`, result.OperationID).
		Scan(&taskID, &jobID, &runID, &generation, &token, &head, &base, &integration, &correlationID, &identity, &credentialID, &status, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if taskID != principal.TaskID || runID != principal.RunID || identity != principal.Identity || credentialID != principal.CredentialID {
		return false, ErrForbidden
	}
	if taskID != result.TaskID || jobID != result.JobID || runID != result.RunID || generation != result.Generation || token != result.LeaseToken ||
		optional(head) != result.Snapshot.HeadSHA || optional(base) != result.Snapshot.BaseSHA || optional(integration) != result.Snapshot.IntegrationSHA ||
		correlationID != result.CorrelationID || status != result.Status || !jsonEqual(payload, encoded) {
		return false, ErrConflict
	}
	return true, nil
}

func insertReceipt(ctx context.Context, repos *storage.Repositories, principal Principal, result contracts.Result, run leaseAdmission, encoded []byte, acceptedAt time.Time) error {
	tag, err := repos.Queries().Exec(ctx, `INSERT INTO worker_result_receipts
		(operation_id,task_id,job_id,run_id,generation,lease_token,expected_head_sha,expected_base_sha,expected_integration_sha,
		correlation_id,supervisor_identity,supervisor_credential_id,result_status,result_payload,accepted_at)
		VALUES($1::uuid,$2::uuid,$3::uuid,$4::uuid,$5,$6::uuid,$7,$8,$9,$10::uuid,$11,$12,$13,$14::jsonb,$15)
		ON CONFLICT(operation_id) DO NOTHING`, result.OperationID, result.TaskID, result.JobID, result.RunID, result.Generation,
		result.LeaseToken, run.head, run.base, run.integration, result.CorrelationID, principal.Identity, principal.CredentialID, result.Status, string(encoded), acceptedAt)
	if err != nil {
		return fmt.Errorf("insert immutable result receipt: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrConflict
	}
	return nil
}

func completeRunLocked(ctx context.Context, repos *storage.Repositories, lease leases.Lease, resultStatus string, handoff bool, c clock.Clock) error {
	if !handoff {
		status := map[string]string{"succeeded": "SUCCESS", "failed": "FAILED", "cancelled": "TERMINATED"}[resultStatus]
		return leases.CompleteLocked(ctx, repos, lease, status, c)
	}
	// Only the exact immediate confirmed webhook handoff may finish the original
	// still-running run after its generation advanced.
	var expiry time.Time
	var jobToken, runToken, runStatus, originalJobStatus string
	var generation int64
	err := repos.Queries().QueryRow(ctx, `SELECT j.lease_expires_at,j.lease_token::text,r.lease_token::text,r.execution_status,r.generation,j.status
		FROM jobs j JOIN agent_runs r ON r.job_id=j.id WHERE j.id=$1::uuid AND r.id=$2::uuid FOR UPDATE OF j,r`, lease.JobID, lease.RunID).
		Scan(&expiry, &jobToken, &runToken, &runStatus, &generation, &originalJobStatus)
	now := c.Now().UTC()
	if err != nil || !expiry.After(now) || jobToken != lease.Token || runToken != lease.Token || runStatus != "RUNNING" || generation != lease.Generation ||
		(originalJobStatus != "LEASED" && originalJobStatus != "CANCELLED") {
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		return ErrStale
	}
	status := map[string]string{"succeeded": "SUCCESS", "failed": "FAILED", "cancelled": "TERMINATED"}[resultStatus]
	tag, err := repos.Queries().Exec(ctx, `UPDATE agent_runs SET execution_status=$2,finished_at=$3 WHERE id=$1::uuid AND execution_status='RUNNING'`, lease.RunID, status, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrStale
	}
	jobStatus := map[string]string{"SUCCESS": "COMPLETED", "FAILED": "FAILED", "TERMINATED": "CANCELLED"}[status]
	if originalJobStatus == "LEASED" {
		tag, err = repos.Queries().Exec(ctx, `UPDATE jobs SET status=$2 WHERE id=$1::uuid AND status='LEASED' AND lease_token=$3::uuid`, lease.JobID, jobStatus, lease.Token)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return ErrStale
		}
	}
	return nil
}

func (s *Service) validateWebhookFirst(ctx context.Context, repos *storage.Repositories, current storage.Task, result contracts.Result, run leaseAdmission, pushHandoff bool, jobStatus string, expires *time.Time) (bool, error) {
	if !pushHandoff || current.Generation != result.Generation+1 || current.State == "MERGED" || current.State == "CLOSED" ||
		(jobStatus != "LEASED" && jobStatus != "CANCELLED") || expires == nil || run.status != "RUNNING" {
		return false, ErrStale
	}
	var opTask, opJob string
	var opGeneration int64
	var opStatus, opIdentity, opHead, handoffGeneration, resultHead, requestedBranch, requestedHead, expectedBase string
	var request []byte
	err := repos.Queries().QueryRow(ctx, `SELECT task_id::text,job_id::text,generation,status,identity,expected_head_sha,expected_base_sha,
		request->>'branch',request->>'new_head_sha',request,result->>'handoff_generation',result->>'head_sha'
		FROM github_operations WHERE id=$1::uuid FOR UPDATE`, result.OperationID).
		Scan(&opTask, &opJob, &opGeneration, &opStatus, &opIdentity, &opHead, &expectedBase, &requestedBranch, &requestedHead, &request, &handoffGeneration, &resultHead)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrStale
	}
	if err != nil {
		return false, err
	}
	if opTask != result.TaskID || opJob != result.JobID || opGeneration != result.Generation || opStatus != "CONFIRMED" ||
		opIdentity != run.agentType || opHead != result.Snapshot.HeadSHA || expectedBase != result.Snapshot.BaseSHA ||
		handoffGeneration != fmt.Sprint(current.Generation) || resultHead != current.Snapshot.HeadSHA || requestedHead != current.Snapshot.HeadSHA || requestedBranch == "" ||
		current.Snapshot.BaseSHA != result.Snapshot.BaseSHA || current.Snapshot.IntegrationSHA != "" {
		return false, ErrStale
	}
	if !expires.After(s.clock.Now().UTC()) {
		return false, ErrStale
	}
	var intent struct {
		RunID      string `json:"run_id"`
		LeaseToken string `json:"lease_token"`
	}
	if json.Unmarshal(request, &intent) != nil || intent.RunID != result.RunID || intent.LeaseToken != result.LeaseToken {
		return false, ErrStale
	}
	return true, nil
}

func pushIntentMatches(raw []byte, result contracts.Result) bool {
	var intent struct {
		RunID      string `json:"run_id"`
		LeaseToken string `json:"lease_token"`
	}
	return json.Unmarshal(raw, &intent) == nil && intent.RunID == result.RunID && intent.LeaseToken == result.LeaseToken
}

func sameOptional(value *string, expected string) bool {
	return optional(value) == expected
}

// MaterializeRepliesLocked turns immutable reply intents from a confirmed push
// into current-generation reply jobs. Resume controllers may call it inside
// their existing task-locked UOW when an earlier paused handoff left the marker false.
func MaterializeRepliesLocked(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, operationID string) error {
	if ctx == nil || repos == nil || !validUUID(operationID) {
		return ErrInvalid
	}
	state := locked.Record()
	if err := repos.RequireTaskGeneration(ctx, locked, state.Generation); err != nil {
		return err
	}
	var raw []byte
	var taskID, jobID, status, operationType, identity string
	var generation int64
	var branch, newHead, expectedBase *string
	var handoffGeneration, resultHead *string
	if err := repos.Queries().QueryRow(ctx, `SELECT op.request,op.task_id::text,op.job_id::text,op.status,op.operation_type,op.identity,op.generation,
		op.request->>'branch',op.request->>'new_head_sha',op.expected_base_sha,op.result->>'handoff_generation',op.result->>'head_sha'
		FROM github_operations op WHERE op.id=$1::uuid FOR UPDATE`, operationID).
		Scan(&raw, &taskID, &jobID, &status, &operationType, &identity, &generation, &branch, &newHead, &expectedBase, &handoffGeneration, &resultHead); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrStale
		}
		return err
	}
	handoff, handoffErr := strconv.ParseInt(optional(handoffGeneration), 10, 64)
	if taskID != state.ID || jobID == "" || operationType != "push" || status != "CONFIRMED" || identity != "C" ||
		branch == nil || *branch == "" || newHead == nil || *newHead != state.Snapshot.HeadSHA || expectedBase == nil || *expectedBase != state.Snapshot.BaseSHA ||
		resultHead == nil || *resultHead != state.Snapshot.HeadSHA || handoffGeneration == nil {
		return ErrStale
	}
	if handoffErr != nil || handoff != generation+1 || handoff > state.Generation {
		return ErrStale
	}
	var jobTaskID, runID, token, runRole, durableOperation, payloadOperation string
	var jobGeneration int64
	var intentBinding struct {
		RunID      string `json:"run_id"`
		LeaseToken string `json:"lease_token"`
	}
	if json.Unmarshal(raw, &intentBinding) != nil || !validUUID(intentBinding.RunID) || !validUUID(intentBinding.LeaseToken) {
		return ErrStale
	}
	if err := repos.Queries().QueryRow(ctx, `SELECT j.task_id::text,j.generation,j.operation_type,j.payload->>'operation_id',r.id::text,r.lease_token::text,r.agent_type
		FROM jobs j JOIN agent_runs r ON r.job_id=j.id WHERE j.id=$1::uuid AND j.task_id=$2::uuid AND j.generation=$3 AND r.generation=$3 AND r.id=$4::uuid FOR UPDATE OF j,r`,
		jobID, state.ID, generation, intentBinding.RunID).Scan(&jobTaskID, &jobGeneration, &durableOperation, &payloadOperation, &runID, &token, &runRole); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrStale
		}
		return err
	}
	if jobTaskID != state.ID || jobGeneration != generation || durableOperation != "fix" || payloadOperation != operationID ||
		runID != intentBinding.RunID || token != intentBinding.LeaseToken || runRole != "C" {
		return ErrStale
	}
	var request pushRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return ErrStale
	}
	if !validReplyIntents(raw) {
		return ErrStale
	}
	if err := validateReplyFindings(ctx, repos, locked, raw); err != nil {
		return err
	}
	var created bool
	if err := repos.Queries().QueryRow(ctx, `SELECT COALESCE((result->>'reply_jobs_created')::boolean,false) FROM github_operations WHERE id=$1::uuid`, operationID).Scan(&created); err != nil {
		return err
	}
	if created {
		return nil
	}
	if state.State == "PAUSED" || state.State == "ESCALATED" || state.State == "MERGED" || state.State == "CLOSED" {
		return setReplyMarker(ctx, repos, operationID, false)
	}
	for _, intent := range request.ReplyIntents {
		key := fmt.Sprintf("task:%s:generation:%d:reply:push:%s:finding:%s", state.ID, state.Generation, operationID, intent.FindingID)
		jobID, operationID, correlationID := stableUUID("job:"+key), stableUUID("operation:"+key), stableUUID("correlation:"+key)
		job := contracts.Job{Version: 1, TaskID: state.ID, JobID: jobID, Generation: state.Generation, Snapshot: state.Snapshot,
			Attempt: 1, OperationID: operationID, CorrelationID: correlationID, Operation: "reply"}
		payload, err := json.Marshal(job)
		if err != nil {
			return err
		}
		if _, _, err := repos.InsertJobAndOutbox(ctx, locked, storage.JobInput{ID: jobID, LogicalKey: key, OperationType: "reply", PRID: state.PRID, Payload: payload}); err != nil {
			return err
		}
	}
	return setReplyMarker(ctx, repos, operationID, true)
}

type pushRequest struct {
	AddressedFindingIDs []string `json:"addressed_finding_ids"`
	ReplyIntents        []struct {
		FindingID string `json:"finding_id"`
		Body      string `json:"body"`
	} `json:"reply_intents"`
}

func validReplyIntents(raw []byte) bool {
	var request pushRequest
	if json.Unmarshal(raw, &request) != nil {
		return false
	}
	intents := make(map[string]struct{}, len(request.ReplyIntents))
	for _, intent := range request.ReplyIntents {
		if !validUUID(intent.FindingID) || strings.TrimSpace(intent.Body) == "" || len(intent.Body) > 65536 {
			return false
		}
		if _, duplicate := intents[intent.FindingID]; duplicate {
			return false
		}
		intents[intent.FindingID] = struct{}{}
	}
	addressed := make(map[string]struct{}, len(request.AddressedFindingIDs))
	for _, findingID := range request.AddressedFindingIDs {
		if !validUUID(findingID) {
			return false
		}
		if _, duplicate := addressed[findingID]; duplicate {
			return false
		}
		addressed[findingID] = struct{}{}
	}
	if len(intents) != len(addressed) {
		return false
	}
	for findingID := range intents {
		if _, ok := addressed[findingID]; !ok {
			return false
		}
	}
	return true
}

func validateReplyFindings(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, raw []byte) error {
	var request pushRequest
	if err := json.Unmarshal(raw, &request); err != nil {
		return ErrStale
	}
	if len(request.AddressedFindingIDs) == 0 {
		return nil
	}
	task := locked.Record()
	if task.PRID == nil {
		return ErrStale
	}
	for _, findingID := range request.AddressedFindingIDs {
		var found bool
		if err := repos.Queries().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM findings f JOIN review_cycles rc ON rc.id=f.review_id
			WHERE f.id=$1::uuid AND rc.pr_id=$2)`, findingID, *task.PRID).Scan(&found); err != nil {
			return err
		}
		if !found {
			return ErrStale
		}
	}
	return nil
}

func setReplyMarker(ctx context.Context, repos *storage.Repositories, operationID string, created bool) error {
	_, err := repos.Queries().Exec(ctx, `UPDATE github_operations SET result=jsonb_set(COALESCE(result,'{}'::jsonb),'{reply_jobs_created}',to_jsonb($2::boolean),true) WHERE id=$1::uuid`, operationID, created)
	return err
}

func stableUUID(value string) string {
	hash := sha256.Sum256([]byte("aprl.router.v1:" + value))
	b := hash[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(b)
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}

func optional(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func sameSnapshot(a, b contracts.Snapshot) bool {
	return a.HeadSHA == b.HeadSHA && a.BaseSHA == b.BaseSHA && a.IntegrationSHA == b.IntegrationSHA
}
func jsonEqual(a, b []byte) bool {
	var left, right contracts.Result
	return json.Unmarshal(a, &left) == nil && json.Unmarshal(b, &right) == nil && left == right
}

func validUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for i, c := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
