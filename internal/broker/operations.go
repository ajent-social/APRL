package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/ci"
	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/leases"
	"github.com/ajent-social/APRL/internal/policy"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Transport performs bounded remote reads and one authorized mutation. A
// production broker must receive a configured transport; tests inject their
// fake from a _test.go file.
type Transport interface {
	// PullRequest reads the current snapshot-bound PR state.
	PullRequest(context.Context, string, int64) (RemotePullRequest, error)
	// Checks reads trusted check observations for an exact head/base/integration tuple.
	Checks(context.Context, string, int64, contracts.Snapshot) ([]ci.Observation, error)
	// ChangedFiles inspects the commit range before author or fixer publication.
	ChangedFiles(context.Context, string, string, string) ([]string, error)
	// Execute performs one host-authorized, secret-free remote mutation.
	Execute(context.Context, RemoteOperation) (RemoteReceipt, error)
	// Lookup queries whether an immutable operation ID already reached the remote.
	Lookup(context.Context, string, string) (RemoteObservation, error)
}

// BranchResolver returns a host-owned source branch for the task and remote PR.
type BranchResolver interface {
	Resolve(context.Context, BranchInput) (string, error)
}

// BranchResolverFunc adapts a trusted host policy function to BranchResolver.
type BranchResolverFunc func(context.Context, BranchInput) (string, error)

// Resolve delegates trusted branch selection to f.
func (f BranchResolverFunc) Resolve(ctx context.Context, input BranchInput) (string, error) {
	if f == nil {
		return "", ErrInvalid
	}
	return f(ctx, input)
}

// BranchInput contains durable task and remote snapshot identity for branch
// selection. Requested worker branch text is deliberately absent.
type BranchInput struct {
	TaskID     string             // TaskID is the durable task UUID.
	Repository string             // Repository is the enrolled full name.
	PRNumber   *int64             // PRNumber is nil before PR attachment.
	Snapshot   contracts.Snapshot // Snapshot is the durable task snapshot.
	Remote     *RemotePullRequest // Remote is the snapshot-bound remote PR read.
}

// TaskUUIDBranchResolver chooses a deterministic pre-PR branch or the source
// branch read from the currently attached remote PR.
type TaskUUIDBranchResolver struct{}

// Resolve derives a safe branch from the task UUID before PR attachment, or
// validates and returns the source branch from a snapshot-bound remote PR read.
func (TaskUUIDBranchResolver) Resolve(_ context.Context, input BranchInput) (string, error) {
	if input.TaskID == "" {
		return "", ErrInvalid
	}
	if input.PRNumber == nil {
		return "aprl/" + strings.ToLower(input.TaskID), nil
	}
	if input.Remote == nil || input.Remote.Number != *input.PRNumber || input.Remote.HeadSHA != input.Snapshot.HeadSHA ||
		input.Remote.BaseSHA != input.Snapshot.BaseSHA || strings.TrimSpace(input.Remote.HeadRef) == "" {
		return "", ErrStale
	}
	return input.Remote.HeadRef, nil
}

// RemotePullRequest is a bounded snapshot read from the repository host.
type RemotePullRequest struct {
	Number     int64  // Number is the remote pull request number.
	HeadRef    string // HeadRef is the host-reported source branch.
	BaseRef    string // BaseRef is the host-reported target branch.
	HeadSHA    string // HeadSHA is the current remote source commit.
	BaseSHA    string // BaseSHA is the current remote target commit.
	Draft      bool   // Draft reports whether the PR is still a draft.
	Conflicted bool   // Conflicted reports whether the host blocks merging.
	Open       bool   // Open reports whether the PR remains open.
}

// Request carries one worker mutation. Branch, force, and workflow fields are
// validation inputs only; authority is derived from the durable task/job/run.
type Request struct {
	OperationID       string            // OperationID must identify this exact durable intent.
	Action            Action            // Action selects one role-gated mutation.
	Branch            string            // Branch is checked against host-assigned authority.
	Title             string            // Title is optional mutation content.
	Body              string            // Body is content; reply bodies resolve from parent intent.
	NewHeadSHA        string            // NewHeadSHA is the proposed published commit.
	FindingID         string            // FindingID addresses a single verified finding.
	ParentOperationID string            // ParentOperationID backs a successor reply.
	AddressedFindings []string          // AddressedFindings lists findings covered by a push.
	ReplyIntents      []ReplyIntent     // ReplyIntents binds each addressed finding to response text.
	Force             bool              // Force explicitly requests a forbidden forced update.
	EditWorkflow      bool              // EditWorkflow explicitly requests a forbidden workflow edit.
	Review            *contracts.Review // Review is the run-bound reviewer output.
}

// RemoteOperation is the secret-free, host-authorized request sent to transport.
type RemoteOperation struct {
	ID                 string             // ID is the immutable operation UUID.
	Action             Action             // Action is the one authorized remote mutation.
	ApplicationRole    string             // ApplicationRole selects the configured GitHub App.
	Repository         string             // Repository is the enrolled full name.
	PRNumber           int64              // PRNumber is zero for pre-PR operations.
	Branch             string             // Branch is host-assigned source branch.
	TargetBranch       string             // TargetBranch is host-read merge target.
	Title              string             // Title is optional PR content.
	Body               string             // Body is comment or publication content.
	HeadSHA            string             // HeadSHA is the proposed commit.
	ExpectedHeadSHA    string             // ExpectedHeadSHA is the current source commit fence.
	ExpectedBaseSHA    string             // ExpectedBaseSHA is the current target commit fence.
	Snapshot           contracts.Snapshot // Snapshot is the complete admitted source tuple.
	Force              bool               // Force remains false for all broker mutations.
	FindingID          string             // FindingID identifies a resolved finding or reply target.
	Review             *RemoteReview      // Review contains content only, without lease authority.
	AddressedFindingID []string           // AddressedFindingID records findings addressed by this push.
	ReplyIntents       []ReplyIntent      // ReplyIntents are immutable push response bodies.
}

// RemoteReview contains review content only; lease credentials and execution
// identity are never sent across the GitHub transport boundary.
type RemoteReview struct {
	Verdict         contracts.Verdict   `json:"verdict"`                    // Verdict is the reviewer decision.
	Summary         string              `json:"summary"`                    // Summary is the review summary.
	RequestedChange string              `json:"requested_change,omitempty"` // RequestedChange is an explicit remediation request.
	Findings        []contracts.Finding `json:"findings"`                   // Findings are validated review findings.
}

// RemoteReceipt records the authoritative remote outcome for a mutation.
type RemoteReceipt struct {
	RemoteID string `json:"remote_id,omitempty"` // RemoteID is the host's resulting object identity.
	HeadSHA  string `json:"head_sha,omitempty"`  // HeadSHA is the commit confirmed by the host.
	Merged   bool   `json:"merged,omitempty"`    // Merged reports an authoritative successful merge.
}

// RemoteObservation is a query result for a previously admitted operation.
type RemoteObservation struct {
	Applied bool          `json:"applied"`           // Applied reports a matching remote mutation.
	Receipt RemoteReceipt `json:"receipt,omitempty"` // Receipt contains verified remote evidence.
}

// ReplyIntent preserves the immutable finding response admitted with a push.
type ReplyIntent struct {
	FindingID string `json:"finding_id"` // FindingID is the addressed finding UUID.
	Body      string `json:"body"`       // Body is the immutable reply text.
}

// Operation is the durable immutable admission and latest remote disposition.
type Operation struct {
	ID         string          `json:"id"`                  // ID is the durable operation UUID.
	TaskID     string          `json:"task_id"`             // TaskID is the immutable owner task.
	JobID      string          `json:"job_id,omitempty"`    // JobID is the durable source job, absent for host merge.
	Generation int64           `json:"generation"`          // Generation is the admitted task generation.
	Type       Action          `json:"type"`                // Type is the immutable mutation capability.
	Identity   string          `json:"identity"`            // Identity is the logical application role.
	Status     string          `json:"status"`              // Status is the latest durable remote disposition.
	Request    json.RawMessage `json:"request"`             // Request is the immutable run-bound intent.
	Result     json.RawMessage `json:"result,omitempty"`    // Result records the current remote evidence.
	RemoteID   string          `json:"remote_id,omitempty"` // RemoteID is the confirmed remote object identity.
}

// Service admits immutable operations, then performs bounded remote I/O after
// the PostgreSQL transaction has released its task, job, and run locks.
type Service struct {
	pool      *pgxpool.Pool
	clock     clock.Clock
	transport Transport
	branches  BranchResolver
	config    Config
}

// New constructs the broker and fails closed when any required authority or
// transport dependency is missing.
func New(pool *pgxpool.Pool, c clock.Clock, transport Transport, branches BranchResolver, config Config) (*Service, error) {
	if pool == nil || c == nil || transport == nil || branches == nil {
		return nil, fmt.Errorf("broker requires PostgreSQL, clock, transport, and branch resolver: %w", ErrInvalid)
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	config = cloneConfig(config)
	return &Service{pool: pool, clock: c, transport: transport, branches: branches, config: config}, nil
}

func cloneConfig(config Config) Config {
	cloned := Config{Repositories: make(map[string]RepositoryPolicy, len(config.Repositories)), RPCTimeout: config.RPCTimeout}
	for repository, rule := range config.Repositories {
		rule.AllowedTargetBranches = append([]string(nil), rule.AllowedTargetBranches...)
		rule.ProtectedTargetBranches = append([]string(nil), rule.ProtectedTargetBranches...)
		rule.CI.TrustedProducers = append([]policy.Producer(nil), rule.CI.TrustedProducers...)
		rule.CI.RequiredChecks = append([]policy.RequiredCheck(nil), rule.CI.RequiredChecks...)
		cloned.Repositories[repository] = rule
	}
	return cloned
}

type immutableRequest struct {
	OperationID       string             `json:"operation_id"`
	RunID             string             `json:"run_id"`
	LeaseToken        string             `json:"lease_token"`
	Action            Action             `json:"action"`
	Snapshot          contracts.Snapshot `json:"snapshot"`
	Branch            string             `json:"branch,omitempty"`
	Title             string             `json:"title,omitempty"`
	Body              string             `json:"body,omitempty"`
	NewHeadSHA        string             `json:"new_head_sha,omitempty"`
	FindingID         string             `json:"finding_id,omitempty"`
	ParentOperationID string             `json:"parent_operation_id,omitempty"`
	AddressedFindings []string           `json:"addressed_finding_ids,omitempty"`
	ReplyIntents      []ReplyIntent      `json:"reply_intents,omitempty"`
	Review            *contracts.Review  `json:"review,omitempty"`
	taskID            string
	jobID             string
	role              string
	generation        int64
}

type runBinding struct {
	taskID, jobID, runID, token string
	generation                  int64
	role, jobOperation          string
	job                         contracts.Job
	jobStatus                   string
}

// Execute validates a worker request against the immutable run and job, stores
// its append-only intent, then makes one bounded network call without DB locks.
func (s *Service) Execute(ctx context.Context, lease leases.Lease, request Request) (Operation, error) {
	if s == nil || s.pool == nil || s.clock == nil || s.transport == nil || s.branches == nil || ctx == nil {
		return Operation{}, ErrInvalid
	}
	replayed, found, err := s.acceptExactReplay(ctx, lease, request)
	if err != nil {
		return Operation{}, err
	}
	if found && replayed.Status == "CONFIRMED" {
		return replayed, nil
	}
	if found && (replayed.Status == "IN_FLIGHT" || replayed.Status == "UNKNOWN") {
		reconciled, reconcileErr := s.Reconcile(ctx, request.OperationID)
		if reconcileErr != nil {
			return Operation{}, reconcileErr
		}
		if reconciled.Status == "CONFIRMED" {
			return reconciled, nil
		}
		if reconciled.Status == "IN_FLIGHT" {
			return Operation{}, ErrInFlight
		}
	}
	prepared, err := s.prepare(ctx, lease, request)
	if err != nil {
		return Operation{}, err
	}
	var operation Operation
	var invoke bool
	var remoteRequest RemoteOperation
	err = storage.WithUnitOfWork(ctx, s.pool, s.clock, func(ctx context.Context, repos *storage.Repositories) error {
		binding, locked, err := s.lockAdmission(ctx, repos, lease, request)
		if err != nil {
			return err
		}
		prepared.OperationID = request.OperationID
		prepared.RunID = lease.RunID
		prepared.LeaseToken = lease.Token
		if err := s.verifyPrepared(ctx, repos, binding, locked.Record(), &prepared, request); err != nil {
			return err
		}
		remoteRequest = buildRemoteOperation(locked.Record(), prepared)
		var existing Operation
		err = scanOperation(repos.Queries().QueryRow(ctx, `SELECT id::text,task_id::text,COALESCE(job_id::text,''),generation,operation_type,identity,status,request,result,COALESCE(remote_id,'')
			FROM github_operations WHERE id=$1::uuid FOR UPDATE`, request.OperationID), &existing)
		if errors.Is(err, pgx.ErrNoRows) {
			immutable, marshalErr := json.Marshal(prepared)
			if marshalErr != nil {
				return marshalErr
			}
			_, err = repos.Queries().Exec(ctx, `INSERT INTO github_operations
				(id,job_id,task_id,generation,operation_type,identity,expected_head_sha,expected_base_sha,request,result,status,created_at)
				VALUES($1::uuid,$2::uuid,$3::uuid,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,'IN_FLIGHT',$11)`, prepared.OperationID,
				prepared.jobID, prepared.taskID, prepared.generation, string(request.Action), prepared.role,
				nullableSHA(prepared.Snapshot.HeadSHA), nullableSHA(prepared.Snapshot.BaseSHA), string(immutable), inFlightResult(s.clock.Now()), s.clock.Now().UTC())
			if err != nil {
				return fmt.Errorf("persist immutable GitHub operation intent: %w", err)
			}
			operation = Operation{ID: prepared.OperationID, TaskID: prepared.taskID, JobID: prepared.jobID, Generation: prepared.generation,
				Type: request.Action, Identity: prepared.role, Status: "IN_FLIGHT", Request: immutable}
			invoke = true
			return nil
		}
		if err != nil {
			return err
		}
		if existing.TaskID != prepared.taskID || existing.JobID != prepared.jobID || existing.Generation != prepared.generation ||
			existing.Type != request.Action || existing.Identity != prepared.role || !jsonEqual(existing.Request, prepared) {
			return ErrConflict
		}
		switch existing.Status {
		case "CONFIRMED":
			operation = existing
			return nil
		case "UNKNOWN":
			operation = existing
			return nil
		case "INTENDED", "IN_FLIGHT":
			return ErrInFlight
		default:
			return ErrStale
		}
	})
	if err != nil {
		return Operation{}, err
	}
	if operation.Status == "CONFIRMED" {
		return operation, nil
	}
	if !invoke {
		observation, reconcileErr := s.Reconcile(ctx, request.OperationID)
		if reconcileErr != nil {
			return Operation{}, reconcileErr
		}
		if observation.Status == "CONFIRMED" {
			return observation, nil
		}
		if observation.Status == "IN_FLIGHT" {
			return Operation{}, ErrInFlight
		}
		// An UNKNOWN operation can be retried only by its original still-current
		// lease, and only after Reconcile queried the remote system above.
		err = storage.WithUnitOfWork(ctx, s.pool, s.clock, func(ctx context.Context, repos *storage.Repositories) error {
			binding, locked, lockErr := s.lockAdmission(ctx, repos, lease, request)
			if lockErr != nil {
				return lockErr
			}
			if verifyErr := s.verifyPrepared(ctx, repos, binding, locked.Record(), &prepared, request); verifyErr != nil {
				return verifyErr
			}
			remoteRequest = buildRemoteOperation(locked.Record(), prepared)
			tag, updateErr := repos.Queries().Exec(ctx, `UPDATE github_operations SET status='IN_FLIGHT',result=$5::jsonb,remote_id=NULL
					WHERE id=$1::uuid AND task_id=$2::uuid AND job_id=$3::uuid AND generation=$4 AND status='UNKNOWN'`,
				prepared.OperationID, prepared.taskID, prepared.jobID, prepared.generation, inFlightResult(s.clock.Now()))
			if updateErr != nil {
				return updateErr
			}
			if tag.RowsAffected() != 1 {
				return ErrInFlight
			}
			invoke = true
			return nil
		})
		if err != nil {
			return Operation{}, err
		}
	}
	return s.perform(ctx, prepared.OperationID, remoteRequest)
}

func (s *Service) acceptExactReplay(ctx context.Context, lease leases.Lease, request Request) (Operation, bool, error) {
	if !validUUID(request.OperationID) {
		return Operation{}, false, ErrInvalid
	}
	var operation Operation
	err := scanOperation(s.pool.QueryRow(ctx, `SELECT id::text,task_id::text,COALESCE(job_id::text,''),generation,operation_type,identity,status,request,result,COALESCE(remote_id,'')
		FROM github_operations WHERE id=$1::uuid`, request.OperationID), &operation)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, false, nil
	}
	if err != nil {
		return Operation{}, false, err
	}
	var recorded immutableRequest
	if json.Unmarshal(operation.Request, &recorded) != nil {
		return Operation{}, false, ErrConflict
	}
	if operation.TaskID != lease.TaskID || operation.JobID != lease.JobID || operation.Generation != lease.Generation ||
		recorded.RunID != lease.RunID || recorded.LeaseToken != lease.Token || recorded.OperationID != request.OperationID || recorded.Action != request.Action ||
		(request.Branch != "" && request.Branch != recorded.Branch) || request.Force || request.EditWorkflow {
		return Operation{}, false, ErrConflict
	}
	var jobOperation, role string
	var payload []byte
	err = s.pool.QueryRow(ctx, `SELECT j.operation_type,j.payload,r.agent_type FROM jobs j JOIN agent_runs r ON r.job_id=j.id AND r.task_id=j.task_id
		WHERE j.id=$1::uuid AND j.task_id=$2::uuid AND r.id=$3::uuid AND r.lease_token=$4::uuid`,
		lease.JobID, lease.TaskID, lease.RunID, lease.Token).Scan(&jobOperation, &payload, &role)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, false, ErrConflict
	}
	if err != nil {
		return Operation{}, false, err
	}
	job, err := contracts.DecodeJob(payload)
	if err != nil || job.OperationID != request.OperationID || !permitsRole(role, jobOperation, request.Action) || role != operation.Identity {
		return Operation{}, false, ErrConflict
	}
	expected := immutableRequest{OperationID: request.OperationID, RunID: lease.RunID, LeaseToken: lease.Token, Action: request.Action,
		Snapshot: lease.Snapshot, Branch: recorded.Branch, Title: request.Title, Body: request.Body, NewHeadSHA: request.NewHeadSHA,
		FindingID: request.FindingID, ParentOperationID: request.ParentOperationID,
		AddressedFindings: append([]string(nil), request.AddressedFindings...), ReplyIntents: append([]ReplyIntent(nil), request.ReplyIntents...), Review: request.Review}
	if request.Action == ActionReply && request.Body == "" {
		expected.Body = recorded.Body
	}
	if !jsonEqual(operation.Request, expected) {
		return Operation{}, false, ErrConflict
	}
	return operation, true, nil
}

// Reconcile queries an existing remote operation by immutable ID without lease
// authority. It can record confirmation but never sends a mutation or rebinds
// the intent to another run.
func (s *Service) Reconcile(ctx context.Context, operationID string) (Operation, error) {
	if s == nil || s.pool == nil || s.transport == nil || ctx == nil || !validUUID(operationID) {
		return Operation{}, ErrInvalid
	}
	var intent Operation
	var operationType string
	err := s.pool.QueryRow(ctx, `SELECT id::text,task_id::text,COALESCE(job_id::text,''),generation,operation_type,identity,status,request,result,COALESCE(remote_id,'')
		FROM github_operations WHERE id=$1::uuid`, operationID).Scan(&intent.ID, &intent.TaskID, &intent.JobID, &intent.Generation,
		&operationType, &intent.Identity, &intent.Status, &intent.Request, &intent.Result, &intent.RemoteID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, storage.ErrNotFound
	}
	if err != nil {
		return Operation{}, err
	}
	intent.Type = Action(operationType)
	if intent.Status == "CONFIRMED" || intent.Status == "FAILED" {
		return intent, nil
	}
	if intent.Status == "IN_FLIGHT" {
		var state struct {
			InFlightAt time.Time `json:"in_flight_at"`
		}
		if json.Unmarshal(intent.Result, &state) != nil || state.InFlightAt.IsZero() || state.InFlightAt.Add(s.config.RPCTimeout).After(s.clock.Now().UTC()) {
			return intent, ErrInFlight
		}
	}
	ctx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
	defer cancel()
	remote, err := s.transport.Lookup(ctx, intent.ID, string(intent.Type))
	if err != nil {
		return intent, fmt.Errorf("query remote GitHub operation: %w", err)
	}
	if remote.Applied && !remoteOutcomeMatches(intent.Type, remoteExpectedHead(intent), remote.Receipt) {
		return intent, ErrUnknown
	}
	var resultJSON []byte
	status, remoteID := "UNKNOWN", ""
	if remote.Applied {
		status = "CONFIRMED"
		remoteID = remote.Receipt.RemoteID
	}
	resultJSON, err = json.Marshal(remote.Receipt)
	if err != nil {
		return Operation{}, err
	}
	updated, err := s.updateDisposition(ctx, intent.ID, intent.Status, status, remoteID, resultJSON)
	if err != nil {
		return Operation{}, err
	}
	return updated, nil
}

// Merge admits an autonomous merge from trusted host control after current CI,
// review, target, draft, conflict, and protected-target approval gates pass.
// It has no worker lease parameter and is not callable through Execute.
func (s *Service) Merge(ctx context.Context, taskID, operationID string) (Operation, error) {
	return s.merge(ctx, taskID, operationID, false)
}

func (s *Service) merge(ctx context.Context, taskID, operationID string, queriedNotApplied bool) (Operation, error) {
	if s == nil || s.pool == nil || s.transport == nil || ctx == nil || !validUUID(taskID) || !validUUID(operationID) {
		return Operation{}, ErrInvalid
	}
	if !queriedNotApplied {
		var prior Operation
		err := scanOperation(s.pool.QueryRow(ctx, `SELECT id::text,task_id::text,COALESCE(job_id::text,''),generation,operation_type,identity,status,request,result,COALESCE(remote_id,'')
			FROM github_operations WHERE id=$1::uuid`, operationID), &prior)
		if err == nil {
			if prior.TaskID != taskID || prior.JobID != "" || prior.Type != ActionMerge || prior.Identity != "A" {
				return Operation{}, ErrConflict
			}
			if prior.Status == "CONFIRMED" {
				return prior, nil
			}
			if prior.Status == "IN_FLIGHT" || prior.Status == "UNKNOWN" {
				observed, reconcileErr := s.Reconcile(ctx, operationID)
				if reconcileErr != nil {
					return Operation{}, reconcileErr
				}
				if observed.Status == "CONFIRMED" {
					return observed, nil
				}
				if observed.Status == "UNKNOWN" {
					return s.merge(ctx, taskID, operationID, true)
				}
				return Operation{}, ErrInFlight
			}
			return Operation{}, ErrStale
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return Operation{}, err
		}
	}
	var repository string
	var prNumber, prID int64
	var generation int64
	var head, base, integration string
	err := s.pool.QueryRow(ctx, `SELECT t.repo_full_name,t.generation,p.pr_number,p.id,p.head_sha,p.base_sha,COALESCE(p.integration_sha,'')
		FROM tasks t JOIN prs p ON p.task_id=t.id WHERE t.id=$1::uuid`, taskID).Scan(&repository, &generation, &prNumber, &prID, &head, &base, &integration)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, storage.ErrNotFound
	}
	if err != nil {
		return Operation{}, err
	}
	remotePR, observations, err := s.readMergeEvidence(ctx, repository, prNumber, contracts.Snapshot{HeadSHA: head, BaseSHA: base, IntegrationSHA: integration})
	if err != nil {
		return Operation{}, err
	}
	var accepted Operation
	invoke := false
	err = storage.WithUnitOfWork(ctx, s.pool, s.clock, func(ctx context.Context, repos *storage.Repositories) error {
		locked, lockErr := repos.LockTask(ctx, taskID)
		if lockErr != nil {
			return lockErr
		}
		current := locked.Record()
		rule, ok := s.config.Repositories[current.RepoFullName]
		if !ok || !rule.AutonomousMergeEnabled || current.Generation != generation || !sameSnapshot(current.Snapshot, contracts.Snapshot{HeadSHA: remotePR.HeadSHA, BaseSHA: remotePR.BaseSHA, IntegrationSHA: integration}) {
			return ErrStale
		}
		if current.State != "READY_TO_MERGE" || current.PRID == nil || *current.PRID != prID || !remotePR.Open || remotePR.Draft || remotePR.Conflicted ||
			remotePR.BaseRef == "" || !branchAllowed(remotePR.BaseRef, rule.AllowedTargetBranches) {
			return ErrForbidden
		}
		aggregate, evaluateErr := ci.Evaluate(current.Snapshot, rule.CI, observations)
		if evaluateErr != nil || aggregate.Outcome != ci.OutcomePassed {
			return ErrStale
		}
		var reviewed bool
		if err := repos.Queries().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM review_cycles rc WHERE rc.pr_id=$1 AND rc.generation=$2
			AND rc.head_sha=$3 AND rc.base_sha=$4 AND rc.verdict='approve'
			AND NOT EXISTS(SELECT 1 FROM findings f WHERE f.review_id=rc.id AND f.severity='blocker'))`,
			*current.PRID, current.Generation, current.Snapshot.HeadSHA, current.Snapshot.BaseSHA).Scan(&reviewed); err != nil {
			return err
		}
		var approvedHead, approvedBase *string
		var approvalID *string
		var ciStatus, baseRef string
		var draft bool
		if err := repos.Queries().QueryRow(ctx, `SELECT approved_head_sha,approved_base_sha,human_approval_id,ci_status,base_ref,is_draft FROM prs WHERE id=$1 FOR UPDATE`, *current.PRID).
			Scan(&approvedHead, &approvedBase, &approvalID, &ciStatus, &baseRef, &draft); err != nil {
			return err
		}
		currentApproval := approvedHead != nil && approvedBase != nil && approvalID != nil && strings.TrimSpace(*approvalID) != "" &&
			*approvedHead == current.Snapshot.HeadSHA && *approvedBase == current.Snapshot.BaseSHA
		mergePolicy := policy.MergeAdmission{TaskState: current.State, CurrentSnapshot: current.Snapshot,
			ReviewedSnapshot: current.Snapshot, CISnapshot: aggregate.Snapshot, RequiredChecks: aggregate.Required,
			ChecksPassed: aggregate.Outcome == ci.OutcomePassed && ciStatus == "SUCCESS", ReviewApproved: reviewed,
			ReviewHasBlockers: false, AutonomousMergeEnabled: rule.AutonomousMergeEnabled, TargetBranch: remotePR.BaseRef,
			AllowedTargetBranches: rule.AllowedTargetBranches, ProtectedTargetBranches: rule.ProtectedTargetBranches,
			HumanApprovalCurrent: currentApproval, Draft: remotePR.Draft, Conflicted: remotePR.Conflicted}
		if baseRef != remotePR.BaseRef || draft != remotePR.Draft || !policy.CanAutonomouslyMerge(mergePolicy) {
			return ErrForbidden
		}
		request := map[string]any{"expected_head_sha": current.Snapshot.HeadSHA, "base_sha": current.Snapshot.BaseSHA,
			"integration_sha": current.Snapshot.IntegrationSHA, "target_branch": remotePR.BaseRef, "remote_head_sha": remotePR.HeadSHA}
		requestJSON, marshalErr := json.Marshal(request)
		if marshalErr != nil {
			return marshalErr
		}
		var existing Operation
		queryErr := scanOperation(repos.Queries().QueryRow(ctx, `SELECT id::text,task_id::text,COALESCE(job_id::text,''),generation,operation_type,identity,status,request,result,COALESCE(remote_id,'')
			FROM github_operations WHERE id=$1::uuid FOR UPDATE`, operationID), &existing)
		if errors.Is(queryErr, pgx.ErrNoRows) {
			_, insertErr := repos.Queries().Exec(ctx, `INSERT INTO github_operations(id,task_id,generation,operation_type,identity,expected_head_sha,expected_base_sha,request,result,status,created_at)
				VALUES($1::uuid,$2::uuid,$3,'merge','A',$4,$5,$6::jsonb,$7::jsonb,'IN_FLIGHT',$8)`, operationID, taskID, generation,
				current.Snapshot.HeadSHA, current.Snapshot.BaseSHA, string(requestJSON), inFlightResult(s.clock.Now()), s.clock.Now().UTC())
			if insertErr != nil {
				return insertErr
			}
			accepted = Operation{ID: operationID, TaskID: taskID, Generation: generation, Type: ActionMerge, Identity: "A", Status: "IN_FLIGHT", Request: requestJSON}
			invoke = true
			return nil
		}
		if queryErr != nil {
			return queryErr
		}
		if existing.TaskID != taskID || existing.Generation != generation || existing.JobID != "" || existing.Type != ActionMerge || existing.Identity != "A" || !jsonEqualRaw(existing.Request, requestJSON) {
			return ErrConflict
		}
		switch existing.Status {
		case "CONFIRMED":
			accepted = existing
			return nil
		case "UNKNOWN":
			accepted = existing
			if queriedNotApplied {
				tag, updateErr := repos.Queries().Exec(ctx, `UPDATE github_operations SET status='IN_FLIGHT',result=$2::jsonb,remote_id=NULL WHERE id=$1::uuid AND status='UNKNOWN'`, operationID, inFlightResult(s.clock.Now()))
				if updateErr != nil {
					return updateErr
				}
				if tag.RowsAffected() != 1 {
					return ErrInFlight
				}
				accepted.Status = "IN_FLIGHT"
				invoke = true
			}
			return nil
		default:
			return ErrInFlight
		}
	})
	if err != nil {
		return Operation{}, err
	}
	if accepted.Status == "CONFIRMED" {
		return accepted, nil
	}
	if !invoke {
		observation, reconcileErr := s.Reconcile(ctx, operationID)
		if reconcileErr != nil {
			return Operation{}, reconcileErr
		}
		if observation.Status == "CONFIRMED" {
			return observation, nil
		}
		if observation.Status == "IN_FLIGHT" {
			return Operation{}, ErrInFlight
		}
		return s.merge(ctx, taskID, operationID, true)
	}
	remoteOperation := RemoteOperation{ID: operationID, Action: ActionMerge, ApplicationRole: "A", Repository: repository, PRNumber: prNumber,
		TargetBranch: remotePR.BaseRef, ExpectedHeadSHA: head, ExpectedBaseSHA: base,
		Snapshot: contracts.Snapshot{HeadSHA: head, BaseSHA: base, IntegrationSHA: integration}}
	return s.perform(ctx, operationID, remoteOperation)
}

func (s *Service) prepare(ctx context.Context, lease leases.Lease, request Request) (immutableRequest, error) {
	if !validUUID(request.OperationID) || request.OperationID != strings.ToLower(request.OperationID) || request.Action == "" || request.Force || request.EditWorkflow {
		return immutableRequest{}, ErrInvalid
	}
	var repository string
	var prNumber *int64
	var head, base, integration *string
	err := s.pool.QueryRow(ctx, `SELECT t.repo_full_name,p.pr_number,p.head_sha,p.base_sha,p.integration_sha
		FROM tasks t LEFT JOIN prs p ON p.task_id=t.id WHERE t.id=$1::uuid`, lease.TaskID).Scan(&repository, &prNumber, &head, &base, &integration)
	if errors.Is(err, pgx.ErrNoRows) {
		return immutableRequest{}, storage.ErrNotFound
	}
	if err != nil {
		return immutableRequest{}, err
	}
	if _, enrolled := s.config.Repositories[repository]; !enrolled {
		return immutableRequest{}, ErrForbidden
	}
	snapshot := contracts.Snapshot{HeadSHA: optionalString(head), BaseSHA: optionalString(base), IntegrationSHA: optionalString(integration)}
	var remote *RemotePullRequest
	if prNumber != nil {
		remoteCtx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
		read, readErr := s.transport.PullRequest(remoteCtx, repository, *prNumber)
		cancel()
		if readErr != nil {
			return immutableRequest{}, fmt.Errorf("read assigned remote pull request: %w", readErr)
		}
		remote = &read
		if read.Number != *prNumber || read.HeadSHA != snapshot.HeadSHA || read.BaseSHA != snapshot.BaseSHA || !read.Open {
			return immutableRequest{}, ErrStale
		}
	}
	branchCtx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
	branch, err := s.branches.Resolve(branchCtx, BranchInput{TaskID: lease.TaskID, Repository: repository, PRNumber: prNumber, Snapshot: snapshot, Remote: remote})
	cancel()
	if err != nil {
		return immutableRequest{}, err
	}
	if branch == "" || strings.TrimSpace(branch) != branch {
		return immutableRequest{}, ErrForbidden
	}
	if request.Action == ActionPush && prNumber == nil ||
		(request.Action == ActionPush || request.Action == ActionPublish) && !validSHA(request.NewHeadSHA) {
		return immutableRequest{}, ErrInvalid
	}
	if request.Action == ActionPublish || request.Action == ActionPush {
		filesCtx, filesCancel := context.WithTimeout(ctx, s.config.RPCTimeout)
		files, filesErr := s.transport.ChangedFiles(filesCtx, repository, snapshot.HeadSHA, request.NewHeadSHA)
		filesCancel()
		if filesErr != nil {
			return immutableRequest{}, fmt.Errorf("inspect proposed commit files: %w", filesErr)
		}
		for _, path := range files {
			clean := strings.TrimLeft(strings.ReplaceAll(path, "\\", "/"), "/")
			if clean == ".github/workflows" || strings.HasPrefix(clean, ".github/workflows/") {
				return immutableRequest{}, ErrForbidden
			}
		}
	}
	if request.Action == ActionReview && request.Review == nil {
		return immutableRequest{}, ErrInvalid
	}
	if request.Action == ActionResolve && !validUUID(request.FindingID) {
		return immutableRequest{}, ErrInvalid
	}
	if request.Action == ActionReply && (!validUUID(request.ParentOperationID) || !validUUID(request.FindingID)) {
		return immutableRequest{}, ErrInvalid
	}
	if request.Action != ActionPush && request.Action != ActionReview && request.Action != ActionResolve && request.Action != ActionReply &&
		request.Action != ActionCreatePR && request.Action != ActionPublish && request.Action != ActionMerge {
		return immutableRequest{}, ErrForbidden
	}
	return immutableRequest{Action: request.Action, Snapshot: snapshot, Branch: branch, Title: request.Title, Body: request.Body,
		NewHeadSHA: request.NewHeadSHA, FindingID: request.FindingID, ParentOperationID: request.ParentOperationID,
		AddressedFindings: append([]string(nil), request.AddressedFindings...), ReplyIntents: append([]ReplyIntent(nil), request.ReplyIntents...), Review: request.Review}, nil
}

func (s *Service) lockAdmission(ctx context.Context, repos *storage.Repositories, lease leases.Lease, request Request) (runBinding, storage.LockedTask, error) {
	locked, err := repos.LockTask(ctx, lease.TaskID)
	if err != nil {
		return runBinding{}, storage.LockedTask{}, err
	}
	if err := leases.ValidateLocked(ctx, repos, lease, s.clock); err != nil {
		if errors.Is(err, leases.ErrStale) || errors.Is(err, leases.ErrInvalid) {
			return runBinding{}, storage.LockedTask{}, fmt.Errorf("validate mutation lease: %w", ErrStale)
		}
		return runBinding{}, storage.LockedTask{}, err
	}
	var binding runBinding
	var payload []byte
	err = repos.Queries().QueryRow(ctx, `SELECT j.task_id::text,j.id::text,r.id::text,r.lease_token::text,r.generation,r.agent_type,j.operation_type,j.payload,j.status
		FROM jobs j JOIN agent_runs r ON r.job_id=j.id AND r.task_id=j.task_id
		WHERE j.id=$1::uuid AND j.task_id=$2::uuid AND r.id=$3::uuid FOR UPDATE OF j,r`, lease.JobID, lease.TaskID, lease.RunID).
		Scan(&binding.taskID, &binding.jobID, &binding.runID, &binding.token, &binding.generation, &binding.role, &binding.jobOperation, &payload, &binding.jobStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return runBinding{}, storage.LockedTask{}, ErrStale
	}
	if err != nil {
		return runBinding{}, storage.LockedTask{}, err
	}
	binding.job, err = contracts.DecodeJob(payload)
	if err != nil {
		return runBinding{}, storage.LockedTask{}, fmt.Errorf("decode durable mutation job: %w", ErrStale)
	}
	if binding.taskID != lease.TaskID || binding.jobID != lease.JobID || binding.runID != lease.RunID || binding.token != lease.Token ||
		binding.generation != lease.Generation || binding.job.OperationID != request.OperationID || binding.job.Operation != binding.jobOperation ||
		binding.job.TaskID != binding.taskID || binding.job.JobID != binding.jobID || binding.job.Generation != binding.generation ||
		!permitsRole(binding.role, binding.jobOperation, request.Action) {
		return runBinding{}, storage.LockedTask{}, ErrForbidden
	}
	return binding, locked, nil
}

func (s *Service) verifyPrepared(ctx context.Context, repos *storage.Repositories, binding runBinding, task storage.Task, prepared *immutableRequest, request Request) error {
	if binding.jobStatus != "LEASED" || task.ID != binding.taskID || task.State == "PAUSED" || task.State == "ESCALATED" ||
		task.State == "MERGED" || task.State == "CLOSED" || task.Generation != binding.generation || !sameSnapshot(task.Snapshot, prepared.Snapshot) ||
		!sameSnapshot(binding.job.Snapshot, task.Snapshot) || request.OperationID != binding.job.OperationID {
		return ErrStale
	}
	if request.Branch != "" && request.Branch != prepared.Branch {
		return ErrForbidden
	}
	prepared.RunID, prepared.LeaseToken = binding.runID, binding.token
	prepared.taskID, prepared.jobID, prepared.role, prepared.generation = binding.taskID, binding.jobID, binding.role, binding.generation
	if prepared.Action == ActionReview {
		review := prepared.Review
		if review == nil || review.Validate() != nil || review.TaskID != binding.taskID || review.JobID != binding.jobID || review.RunID != binding.runID ||
			review.Generation != binding.generation || review.LeaseToken != binding.token || review.OperationID != binding.job.OperationID ||
			review.CorrelationID != binding.job.CorrelationID || !sameSnapshot(review.Snapshot, task.Snapshot) {
			return ErrForbidden
		}
	}
	if prepared.Action == ActionPush {
		if !validSHA(prepared.NewHeadSHA) || !sameUniqueUUIDs(prepared.AddressedFindings, replyFindingIDs(prepared.ReplyIntents)) ||
			!validReplyIntents(prepared.ReplyIntents) {
			return ErrInvalid
		}
		for _, findingID := range prepared.AddressedFindings {
			var exists bool
			if task.PRID == nil {
				return ErrStale
			}
			if err := repos.Queries().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM findings f WHERE f.id=$1::uuid AND f.disposition='OPEN'
				AND f.review_id IN (SELECT id FROM review_cycles WHERE pr_id=$2))`, findingID, *task.PRID).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return ErrStale
			}
		}
	}
	if prepared.Action == ActionResolve {
		if task.PRID == nil {
			return ErrStale
		}
		var verified bool
		if err := repos.Queries().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM findings f JOIN review_cycles rc ON rc.id=f.review_id
			WHERE f.id=$1::uuid AND rc.pr_id=$2 AND f.disposition='ADDRESSED' AND f.resolved_head_sha=$3)`,
			prepared.FindingID, *task.PRID, task.Snapshot.HeadSHA).Scan(&verified); err != nil {
			return err
		}
		if !verified {
			return ErrForbidden
		}
	}
	if prepared.Action == ActionReply {
		if task.PRID == nil {
			return ErrStale
		}
		var parentTask, parentType, parentStatus, parentHead string
		var parentGeneration int64
		var parentRequest []byte
		if err := repos.Queries().QueryRow(ctx, `SELECT task_id::text,operation_type,status,generation,result->>'head_sha',request
			FROM github_operations WHERE id=$1::uuid FOR UPDATE`, prepared.ParentOperationID).
			Scan(&parentTask, &parentType, &parentStatus, &parentGeneration, &parentHead, &parentRequest); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrStale
			}
			return err
		}
		if parentTask != task.ID || parentType != "push" || parentStatus != "CONFIRMED" || parentGeneration+1 != task.Generation || parentHead != task.Snapshot.HeadSHA {
			return ErrStale
		}
		var parent struct {
			ReplyIntents []ReplyIntent `json:"reply_intents"`
		}
		if json.Unmarshal(parentRequest, &parent) != nil {
			return ErrStale
		}
		found := false
		for _, intent := range parent.ReplyIntents {
			if intent.FindingID == prepared.FindingID {
				prepared.Body, found = intent.Body, true
				break
			}
		}
		if !found || strings.TrimSpace(prepared.Body) == "" {
			return ErrForbidden
		}
	}
	return nil
}

func buildRemoteOperation(task storage.Task, prepared immutableRequest) RemoteOperation {
	remote := RemoteOperation{ID: prepared.OperationID, Action: prepared.Action, ApplicationRole: prepared.role, Repository: task.RepoFullName, Branch: prepared.Branch,
		Title: prepared.Title, Body: prepared.Body, HeadSHA: prepared.NewHeadSHA, FindingID: prepared.FindingID,
		ExpectedHeadSHA: prepared.Snapshot.HeadSHA, ExpectedBaseSHA: prepared.Snapshot.BaseSHA, Snapshot: prepared.Snapshot,
		Review: reviewContent(prepared.Review), AddressedFindingID: append([]string(nil), prepared.AddressedFindings...),
		ReplyIntents: append([]ReplyIntent(nil), prepared.ReplyIntents...)}
	if task.PRID != nil {
		remote.PRNumber = *task.PRID
	}
	return remote
}

func reviewContent(review *contracts.Review) *RemoteReview {
	if review == nil {
		return nil
	}
	return &RemoteReview{Verdict: review.Verdict, Summary: review.Summary, RequestedChange: review.RequestedChange,
		Findings: append([]contracts.Finding(nil), review.Findings...)}
}

func (s *Service) perform(ctx context.Context, operationID string, remote RemoteOperation) (Operation, error) {
	bounded, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
	receipt, callErr := s.transport.Execute(bounded, remote)
	cancel()
	if callErr == nil && !remoteOutcomeMatches(remote.Action, remote.HeadSHA, receipt) {
		callErr = ErrUnknown
	}
	status, remoteID := "CONFIRMED", receipt.RemoteID
	if callErr != nil {
		status, remoteID = "UNKNOWN", ""
	}
	result, err := json.Marshal(receipt)
	if err != nil {
		return Operation{}, err
	}
	operation, persistErr := s.updateDisposition(ctx, operationID, "IN_FLIGHT", status, remoteID, result)
	if persistErr != nil {
		return Operation{}, fmt.Errorf("persist remote operation outcome: %w", persistErr)
	}
	if operation.Status == "CONFIRMED" {
		return operation, nil
	}
	if callErr != nil {
		return operation, fmt.Errorf("remote GitHub operation outcome unresolved: %w: %v", ErrUnknown, callErr)
	}
	return operation, nil
}

func (s *Service) updateDisposition(ctx context.Context, operationID, expectedStatus, status, remoteID string, result []byte) (Operation, error) {
	var updated Operation
	err := storage.WithUnitOfWork(ctx, s.pool, s.clock, func(ctx context.Context, repos *storage.Repositories) error {
		tag, err := repos.Queries().Exec(ctx, `UPDATE github_operations SET status=$3,remote_id=NULLIF($4,''),result=$5::jsonb
			WHERE id=$1::uuid AND status=$2`, operationID, expectedStatus, status, remoteID, string(result))
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return scanOperation(repos.Queries().QueryRow(ctx, `SELECT id::text,task_id::text,COALESCE(job_id::text,''),generation,operation_type,identity,status,request,result,COALESCE(remote_id,'')
				FROM github_operations WHERE id=$1::uuid`, operationID), &updated)
		}
		return scanOperation(repos.Queries().QueryRow(ctx, `SELECT id::text,task_id::text,COALESCE(job_id::text,''),generation,operation_type,identity,status,request,result,COALESCE(remote_id,'')
			FROM github_operations WHERE id=$1::uuid`, operationID), &updated)
	})
	return updated, err
}

func inFlightResult(now time.Time) string {
	return `{"in_flight_at":"` + now.UTC().Format(time.RFC3339Nano) + `"}`
}

func remoteExpectedHead(intent Operation) string {
	var request struct {
		NewHeadSHA string `json:"new_head_sha"`
	}
	if json.Unmarshal(intent.Request, &request) == nil && request.NewHeadSHA != "" {
		return request.NewHeadSHA
	}
	var merge struct {
		ExpectedHeadSHA string `json:"expected_head_sha"`
	}
	if err := json.Unmarshal(intent.Request, &merge); err != nil {
		return ""
	}
	return merge.ExpectedHeadSHA
}

func remoteOutcomeMatches(action Action, expectedHead string, receipt RemoteReceipt) bool {
	switch action {
	case ActionPush, ActionPublish:
		return expectedHead != "" && receipt.HeadSHA == expectedHead
	case ActionMerge:
		return receipt.Merged
	default:
		return true
	}
}

type rowScanner interface{ Scan(...any) error }

func scanOperation(row rowScanner, operation *Operation) error {
	var operationType string
	if err := row.Scan(&operation.ID, &operation.TaskID, &operation.JobID, &operation.Generation, &operationType,
		&operation.Identity, &operation.Status, &operation.Request, &operation.Result, &operation.RemoteID); err != nil {
		return err
	}
	operation.Type = Action(operationType)
	return nil
}

func (s *Service) readMergeEvidence(ctx context.Context, repository string, prNumber int64, snapshot contracts.Snapshot) (RemotePullRequest, []ci.Observation, error) {
	remoteCtx, cancel := context.WithTimeout(ctx, s.config.RPCTimeout)
	remote, err := s.transport.PullRequest(remoteCtx, repository, prNumber)
	cancel()
	if err != nil {
		return RemotePullRequest{}, nil, fmt.Errorf("read merge pull request: %w", err)
	}
	if remote.Number != prNumber || remote.HeadSHA != snapshot.HeadSHA || remote.BaseSHA != snapshot.BaseSHA {
		return RemotePullRequest{}, nil, ErrStale
	}
	checkCtx, checkCancel := context.WithTimeout(ctx, s.config.RPCTimeout)
	observations, err := s.transport.Checks(checkCtx, repository, prNumber, snapshot)
	checkCancel()
	if err != nil {
		return RemotePullRequest{}, nil, fmt.Errorf("read aggregate CI observations: %w", err)
	}
	return remote, observations, nil
}

func jsonEqual(existing []byte, expected immutableRequest) bool {
	encoded, err := json.Marshal(expected)
	return err == nil && jsonEqualRaw(existing, encoded)
}

func jsonEqualRaw(a, b []byte) bool {
	var left, right any
	if json.Unmarshal(a, &left) != nil || json.Unmarshal(b, &right) != nil {
		return false
	}
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func validReplyIntents(intents []ReplyIntent) bool {
	seen := make(map[string]struct{}, len(intents))
	for _, intent := range intents {
		if !validUUID(intent.FindingID) || strings.TrimSpace(intent.Body) == "" || len(intent.Body) > 65536 {
			return false
		}
		if _, exists := seen[intent.FindingID]; exists {
			return false
		}
		seen[intent.FindingID] = struct{}{}
	}
	return true
}

func sameUniqueUUIDs(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, id := range left {
		if !validUUID(id) {
			return false
		}
		if _, ok := seen[id]; ok {
			return false
		}
		seen[id] = struct{}{}
	}
	for _, id := range right {
		if _, ok := seen[id]; !ok {
			return false
		}
	}
	return true
}

func replyFindingIDs(intents []ReplyIntent) []string {
	ids := make([]string, len(intents))
	for i := range intents {
		ids[i] = intents[i].FindingID
	}
	return ids
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
	return value != "00000000-0000-0000-0000-000000000000"
}

func validSHA(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func optionalString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func nullableSHA(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func sameSnapshot(left, right contracts.Snapshot) bool {
	return left.HeadSHA == right.HeadSHA && left.BaseSHA == right.BaseSHA && left.IntegrationSHA == right.IntegrationSHA
}
