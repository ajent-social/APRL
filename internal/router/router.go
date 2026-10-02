// Package router turns durable GitHub inbox receipts into fenced task work.
package router

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/ajent-social/APRL/internal/clock"
	"github.com/ajent-social/APRL/internal/contracts"
	"github.com/ajent-social/APRL/internal/lifecycle"
	"github.com/ajent-social/APRL/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// DispositionProcessed marks a durably routed inbox receipt.
	DispositionProcessed = "PROCESSED"
	// DispositionIgnored marks a valid receipt with no authorized work.
	DispositionIgnored = "IGNORED"
	// DispositionRejected marks a malformed or policy-invalid receipt.
	DispositionRejected = "REJECTED"
	// OperationAuthor is the stable author-job operation name.
	OperationAuthor = "author"
	// OperationCIReconcile is the stable CI reconciliation operation name.
	OperationCIReconcile = "ci_reconcile"
)

// ErrPushFence indicates a recorded push does not match the task's current authority tuple.
var ErrPushFence = errors.New("recorded push does not match current task fence")

// RepositoryPolicy explicitly provisions enrollment identity and the task
// account mapping for one installed repository. Organization budgets must
// already exist; the router never creates or changes them.
type RepositoryPolicy struct {
	OrgID                      string             // OrgID selects a pre-provisioned organization budget account.
	PolicyVersion              string             // PolicyVersion identifies the enrollment policy applied to new tasks.
	IssueEnrollmentLabel       string             // IssueEnrollmentLabel opts labeled issues into APRL authoring.
	AuthorizedIssueLabelerIDs  map[int64]struct{} // AuthorizedIssueLabelerIDs contains permitted issue-enrollment actors.
	AuthorizedHumanPRAuthorIDs map[int64]struct{} // AuthorizedHumanPRAuthorIDs contains permitted human PR enrollment actors.
	TaskBudgetLimitMicroUSD    int64              // TaskBudgetLimitMicroUSD is the task ceiling in integer micro-USD.
}

// Config trusts event producers by identity and enrollment by repo policy.
// Empty trust sets fail closed for the corresponding event classes.
type Config struct {
	Repositories          map[string]RepositoryPolicy // Repositories contains explicitly enrolled repository policies.
	TrustedAPRLAppIDs     map[int64]struct{}          // TrustedAPRLAppIDs contains allowed APRL GitHub App IDs.
	TrustedAPRLActorIDs   map[int64]struct{}          // TrustedAPRLActorIDs contains allowed APRL sender IDs.
	TrustedCIAppIDs       map[int64]struct{}          // TrustedCIAppIDs contains allowed CI App IDs.
	TrustedCIActorIDs     map[int64]struct{}          // TrustedCIActorIDs contains allowed CI actor IDs.
	TrustedCISenderLogins map[string]struct{}         // TrustedCISenderLogins contains allowed status senders.
	MaxTaskBudgetMicroUSD int64                       // MaxTaskBudgetMicroUSD is the global task ceiling in integer micro-USD.
}

// Router applies trusted webhook observations to task state in a PostgreSQL unit of work.
type Router struct {
	pool  *pgxpool.Pool
	clock clock.Clock
	cfg   Config
}

var issueBranch = regexp.MustCompile(`^feature/aprl-([0-9]+)$`)

// New constructs a router whose policy is explicit and fail-closed.
func New(pool *pgxpool.Pool, c clock.Clock, cfg Config) (*Router, error) {
	if pool == nil || c == nil {
		return nil, errors.New("router requires PostgreSQL pool and clock")
	}
	if cfg.MaxTaskBudgetMicroUSD <= 0 {
		cfg.MaxTaskBudgetMicroUSD = 5_000_000
	}
	cfg = cloneConfig(cfg)
	for name, policy := range cfg.Repositories {
		if strings.TrimSpace(name) == "" || strings.TrimSpace(policy.OrgID) == "" || strings.TrimSpace(policy.PolicyVersion) == "" {
			return nil, fmt.Errorf("repository %q has incomplete routing policy", name)
		}
		if policy.TaskBudgetLimitMicroUSD <= 0 || policy.TaskBudgetLimitMicroUSD > cfg.MaxTaskBudgetMicroUSD {
			return nil, fmt.Errorf("repository %q task budget is outside configured ceiling", name)
		}
		if policy.IssueEnrollmentLabel != "" && len(policy.AuthorizedIssueLabelerIDs) == 0 {
			return nil, fmt.Errorf("repository %q issue enrollment requires authorized labeler identities", name)
		}
	}
	return &Router{pool: pool, clock: c, cfg: cfg}, nil
}

func cloneConfig(cfg Config) Config {
	repositories := cfg.Repositories
	cfg.Repositories = make(map[string]RepositoryPolicy, len(repositories))
	for name, policy := range repositories {
		policy.AuthorizedIssueLabelerIDs = cloneIDSet(policy.AuthorizedIssueLabelerIDs)
		policy.AuthorizedHumanPRAuthorIDs = cloneIDSet(policy.AuthorizedHumanPRAuthorIDs)
		cfg.Repositories[name] = policy
	}
	cfg.TrustedAPRLAppIDs = cloneIDSet(cfg.TrustedAPRLAppIDs)
	cfg.TrustedAPRLActorIDs = cloneIDSet(cfg.TrustedAPRLActorIDs)
	cfg.TrustedCIAppIDs = cloneIDSet(cfg.TrustedCIAppIDs)
	cfg.TrustedCIActorIDs = cloneIDSet(cfg.TrustedCIActorIDs)
	cfg.TrustedCISenderLogins = cloneStringSet(cfg.TrustedCISenderLogins)
	return cfg
}

func cloneIDSet(src map[int64]struct{}) map[int64]struct{} {
	if src == nil {
		return nil
	}
	dst := make(map[int64]struct{}, len(src))
	for id := range src {
		dst[id] = struct{}{}
	}
	return dst
}

func cloneStringSet(src map[string]struct{}) map[string]struct{} {
	if src == nil {
		return nil
	}
	dst := make(map[string]struct{}, len(src))
	for value := range src {
		dst[value] = struct{}{}
	}
	return dst
}

// RouteDelivery reads one raw durable webhook receipt and atomically applies
// any task transition, logical job, outbox intent, and final disposition.
func (r *Router) RouteDelivery(ctx context.Context, deliveryID string) error {
	if strings.TrimSpace(deliveryID) == "" {
		return errors.New("delivery ID is required")
	}
	return storage.WithUnitOfWork(ctx, r.pool, r.clock, func(ctx context.Context, repos *storage.Repositories) error {
		var eventType, disposition string
		var payload []byte
		err := repos.Queries().QueryRow(ctx, `SELECT event_type,payload,COALESCE(disposition,'INBOX') FROM webhook_deliveries WHERE delivery_id=$1 FOR UPDATE`, deliveryID).Scan(&eventType, &payload, &disposition)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("route delivery %q: %w", deliveryID, storage.ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("read inbox delivery %q: %w", deliveryID, err)
		}
		if disposition != "INBOX" {
			return nil
		}
		result, err := r.routeRaw(ctx, repos, eventType, payload)
		if err != nil {
			return err
		}
		return repos.SetDeliveryDisposition(ctx, deliveryID, result)
	})
}

// RouteTaskEvent consumes a normalized task-correlated event after routing has
// established its task/job identity. It intentionally grants no lease. Stale
// completion observations are durably ignored and cannot create active work.
func (r *Router) RouteTaskEvent(ctx context.Context, deliveryID string, event contracts.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}
	return storage.WithUnitOfWork(ctx, r.pool, r.clock, func(ctx context.Context, repos *storage.Repositories) error {
		var disposition string
		var payload []byte
		if err := repos.Queries().QueryRow(ctx, `SELECT COALESCE(disposition,'INBOX'),payload FROM webhook_deliveries WHERE delivery_id=$1 FOR UPDATE`, deliveryID).Scan(&disposition, &payload); errors.Is(err, pgx.ErrNoRows) {
			return storage.ErrNotFound
		} else if err != nil {
			return err
		}
		if disposition != "INBOX" {
			return nil
		}
		var raw githubPayload
		if err := json.Unmarshal(payload, &raw); err != nil {
			return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionRejected)
		}
		if raw.Repository.FullName == "" || !strings.Contains(raw.Repository.FullName, "/") {
			return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionRejected)
		}
		if _, ok := r.cfg.Repositories[raw.Repository.FullName]; !ok {
			return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionRejected)
		}
		var jobPayload []byte
		var jobTaskID string
		err := repos.Queries().QueryRow(ctx, `SELECT task_id::text,payload FROM jobs WHERE id=$1::uuid`, event.JobID).Scan(&jobTaskID, &jobPayload)
		if errors.Is(err, pgx.ErrNoRows) {
			return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionRejected)
		}
		if err != nil {
			return err
		}
		job, err := contracts.DecodeJob(jobPayload)
		if err != nil {
			return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionRejected)
		}
		if jobTaskID != event.TaskID || job.TaskID != event.TaskID || job.JobID != event.JobID || job.Generation != event.Generation || job.OperationID != event.OperationID || job.CorrelationID != event.CorrelationID {
			return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionRejected)
		}
		locked, err := lockRoutedTask(ctx, repos, event.TaskID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionRejected)
			}
			return err
		}
		current := locked.Record()
		if raw.Repository.FullName != current.RepoFullName {
			return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionRejected)
		}
		switch event.EventType {
		case "completion", "worker.completed":
			if event.Generation != current.Generation {
				return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionIgnored)
			}
			if !sameRouterSnapshot(job.Snapshot, event.Snapshot) {
				return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionRejected)
			}
			if !lifecycle.CompletionCurrent(lifecycle.State(current.State), current.Generation, current.Snapshot, event) {
				return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionIgnored)
			}
			// Completion transitions are owned by the authenticated result boundary.
			return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionProcessed)
		case "snapshot_observed":
			if event.Generation != current.Generation {
				return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionIgnored)
			}
			if !sameRouterSnapshot(job.Snapshot, current.Snapshot) {
				return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionIgnored)
			}
			if current.PRID == nil || event.Snapshot.Validate(true) != nil {
				return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionRejected)
			}
			result, routeErr := r.applySnapshot(ctx, repos, locked, event.Snapshot, true)
			if routeErr != nil {
				return routeErr
			}
			return repos.SetDeliveryDisposition(ctx, deliveryID, result)
		default:
			return repos.SetDeliveryDisposition(ctx, deliveryID, DispositionRejected)
		}
	})
}

type actor struct {
	ID    int64  `json:"id"`
	Login string `json:"login"`
	Type  string `json:"type"`
}
type app struct {
	ID    int64 `json:"id"`
	AppID int64 `json:"app_id"`
}
type repository struct {
	FullName string `json:"full_name"`
}
type issue struct {
	ID     int64 `json:"id"`
	Number int   `json:"number"`
	User   actor `json:"user"`
}
type prRef struct {
	SHA string `json:"sha"`
	Ref string `json:"ref"`
}
type pullRequest struct {
	Number int   `json:"number"`
	Merged bool  `json:"merged"`
	Draft  bool  `json:"draft"`
	User   actor `json:"user"`
	Head   prRef `json:"head"`
	Base   prRef `json:"base"`
}
type githubPayload struct {
	Action       string      `json:"action"`
	Ref          string      `json:"ref"`
	Before       string      `json:"before"`
	After        string      `json:"after"`
	SHA          string      `json:"sha"`
	Repository   repository  `json:"repository"`
	Sender       actor       `json:"sender"`
	Installation app         `json:"installation"`
	Issue        issue       `json:"issue"`
	PullRequest  pullRequest `json:"pull_request"`
	Label        struct {
		Name string `json:"name"`
	} `json:"label"`
	CheckRun struct {
		HeadSHA string `json:"head_sha"`
		App     app    `json:"app"`
	} `json:"check_run"`
	CheckSuite struct {
		HeadSHA string `json:"head_sha"`
		App     app    `json:"app"`
	} `json:"check_suite"`
	WorkflowRun struct {
		HeadSHA string `json:"head_sha"`
		Actor   actor  `json:"actor"`
	} `json:"workflow_run"`
}

func (r *Router) routeRaw(ctx context.Context, repos *storage.Repositories, eventType string, raw []byte) (string, error) {
	var payload githubPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return DispositionRejected, nil
	}
	policy, configured := r.cfg.Repositories[payload.Repository.FullName]
	if !configured {
		return DispositionIgnored, nil
	}
	switch eventType {
	case "issues":
		return r.routeIssue(ctx, repos, policy, payload)
	case "pull_request":
		return r.routePullRequest(ctx, repos, policy, payload)
	case "push":
		return r.routePush(ctx, repos, payload)
	case "check_run", "check_suite", "workflow_run", "status":
		return r.routeCIObservation(ctx, repos, eventType, payload)
	default:
		return DispositionIgnored, nil
	}
}

func (r *Router) lockOrgBudget(ctx context.Context, repos *storage.Repositories, orgID string) error {
	var found string
	err := repos.Queries().QueryRow(ctx, `SELECT org_id FROM org_budgets WHERE org_id=$1 FOR UPDATE`, orgID).Scan(&found)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrNotFound
	}
	if err != nil {
		return err
	}
	return nil
}

func (r *Router) routeIssue(ctx context.Context, repos *storage.Repositories, policy RepositoryPolicy, p githubPayload) (string, error) {
	if p.Action != "labeled" || policy.IssueEnrollmentLabel == "" || p.Label.Name != policy.IssueEnrollmentLabel || p.Issue.Number <= 0 || p.Issue.User.ID <= 0 || !containsID(policy.AuthorizedIssueLabelerIDs, p.Sender.ID) {
		return DispositionIgnored, nil
	}
	if err := r.lockOrgBudget(ctx, repos, policy.OrgID); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return DispositionRejected, nil
		}
		return "", err
	}
	key := fmt.Sprintf("issue:%d", p.Issue.Number)
	now := r.clock.Now()
	var taskID string
	err := repos.Queries().QueryRow(ctx, `SELECT id::text FROM tasks WHERE repo_full_name=$1 AND source_key=$2`, p.Repository.FullName, key).Scan(&taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		err = repos.Queries().QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,state,budget_limit_micro_usd,policy_version,created_at,updated_at) VALUES($1,$2,$3,$4,'AUTHORING',$5,$6,$7,$7) ON CONFLICT(repo_full_name,source_key) DO NOTHING RETURNING id::text`, policy.OrgID, p.Repository.FullName, key, strconv.FormatInt(p.Issue.User.ID, 10), policy.TaskBudgetLimitMicroUSD, policy.PolicyVersion, now).Scan(&taskID)
		if errors.Is(err, pgx.ErrNoRows) {
			err = repos.Queries().QueryRow(ctx, `SELECT id::text FROM tasks WHERE repo_full_name=$1 AND source_key=$2`, p.Repository.FullName, key).Scan(&taskID)
		}
	}
	if err != nil {
		return "", fmt.Errorf("enroll issue task: %w", err)
	}
	locked, err := lockRoutedTask(ctx, repos, taskID)
	if err != nil {
		return "", err
	}
	if lifecycle.State(locked.Record().State) == lifecycle.Authoring {
		if _, _, err = r.insertJob(ctx, repos, locked, OperationAuthor); err != nil {
			return "", err
		}
	}
	return DispositionProcessed, nil
}

func (r *Router) routePullRequest(ctx context.Context, repos *storage.Repositories, policy RepositoryPolicy, p githubPayload) (string, error) {
	if p.PullRequest.Number <= 0 {
		return DispositionRejected, nil
	}
	var taskID string
	err := repos.Queries().QueryRow(ctx, `SELECT task_id::text FROM prs WHERE repo_full_name=$1 AND pr_number=$2`, p.Repository.FullName, p.PullRequest.Number).Scan(&taskID)
	missingPR := errors.Is(err, pgx.ErrNoRows)
	if missingPR && p.Action == "opened" {
		// Absence is the expected enrollment path; any later query error is kept.
		err = nil
	}
	if missingPR && p.Action == "opened" {
		// An A-created PR is associated by the RFC branch convention. The task
		// must already exist, and its owner is never inferred from a bot sender.
		if match := issueBranch.FindStringSubmatch(p.PullRequest.Head.Ref); match != nil {
			err = repos.Queries().QueryRow(ctx, `SELECT id::text FROM tasks WHERE repo_full_name=$1 AND source_key=$2`, p.Repository.FullName, "issue:"+match[1]).Scan(&taskID)
			if errors.Is(err, pgx.ErrNoRows) {
				taskID = ""
				err = nil
			}
		}
		if err != nil {
			return "", err
		}
		if taskID == "" {
			if !policyAllowsHumanPR(policy, p.PullRequest.User, p.Sender) {
				return DispositionIgnored, nil
			}
			if e := r.lockOrgBudget(ctx, repos, policy.OrgID); e != nil {
				if errors.Is(e, storage.ErrNotFound) {
					return DispositionRejected, nil
				}
				return "", e
			}
			key := fmt.Sprintf("github-pr:%d", p.PullRequest.Number)
			now := r.clock.Now()
			err = repos.Queries().QueryRow(ctx, `INSERT INTO tasks(org_id,repo_full_name,source_key,owner_id,state,budget_limit_micro_usd,policy_version,created_at,updated_at) VALUES($1,$2,$3,$4,'WAITING_CI',$5,$6,$7,$7) ON CONFLICT(repo_full_name,source_key) DO NOTHING RETURNING id::text`, policy.OrgID, p.Repository.FullName, key, strconv.FormatInt(p.PullRequest.User.ID, 10), policy.TaskBudgetLimitMicroUSD, policy.PolicyVersion, now).Scan(&taskID)
			if errors.Is(err, pgx.ErrNoRows) {
				return DispositionIgnored, nil
			}
			if err != nil {
				return "", fmt.Errorf("enroll human PR task: %w", err)
			}
			if err = r.insertPR(ctx, repos, taskID, p); err != nil {
				return "", err
			}
			locked, e := lockRoutedTask(ctx, repos, taskID)
			if e != nil {
				return "", e
			}
			if _, _, e = r.insertCIJob(ctx, repos, locked); e != nil {
				return "", e
			}
			return DispositionProcessed, nil
		}
		// Attach a convention-linked PR to its pre-existing issue task.
		locked, e := lockRoutedTask(ctx, repos, taskID)
		if e != nil {
			return "", e
		}
		currentTask := locked.Record()
		if !policyAllowsHumanPR(policy, p.PullRequest.User, p.Sender) {
			trustedAPRLEvent := containsID(r.cfg.TrustedAPRLActorIDs, p.Sender.ID) || containsID(r.cfg.TrustedAPRLAppIDs, p.Installation.AppID)
			if !trustedAPRLEvent {
				return DispositionIgnored, nil
			}
			trusted, trustErr := r.correlatedPRCreation(ctx, repos, currentTask.ID, currentTask.Generation, p.PullRequest.Head.Ref, p.PullRequest.Head.SHA, p.PullRequest.Number)
			if trustErr != nil {
				return "", trustErr
			}
			if !trusted {
				return DispositionIgnored, nil
			}
		}
		if lifecycle.State(currentTask.State) == lifecycle.Merged || lifecycle.State(currentTask.State) == lifecycle.Closed {
			return DispositionIgnored, nil
		}
		observed := contracts.Snapshot{HeadSHA: p.PullRequest.Head.SHA, BaseSHA: p.PullRequest.Base.SHA}
		transition, e := lifecycle.ObserveSnapshot(lifecycle.State(currentTask.State), currentTask.Generation, currentTask.Snapshot, observed, true)
		if e != nil {
			return DispositionRejected, nil
		}
		if err = r.insertPR(ctx, repos, taskID, p); err != nil {
			return "", err
		}
		locked, e = repos.LockTask(ctx, taskID)
		if e != nil {
			return "", e
		}
		currentTask = locked.Record()
		updated, e := repos.UpdateTaskState(ctx, locked, currentTask.Generation, transition.Generation, string(transition.State))
		if e != nil {
			return "", e
		}
		_ = updated
		locked, e = repos.LockTask(ctx, taskID)
		if e != nil {
			return "", e
		}
		if transition.State == lifecycle.Paused || transition.State == lifecycle.Escalated {
			return DispositionProcessed, nil
		}
		if _, _, e = r.insertCIJob(ctx, repos, locked); e != nil {
			return "", e
		}
		return DispositionProcessed, nil
	}
	if err != nil {
		return "", err
	}
	locked, err := lockRoutedTask(ctx, repos, taskID)
	if err != nil {
		return "", err
	}
	current := locked.Record()
	if p.Action == "closed" {
		if lifecycle.State(current.State) == lifecycle.Merged || lifecycle.State(current.State) == lifecycle.Closed {
			return DispositionProcessed, nil
		}
		next := lifecycle.Closed
		if p.PullRequest.Merged {
			next = lifecycle.Merged
		}
		if current.Generation == math.MaxInt64 {
			return DispositionRejected, nil
		}
		updated, err := repos.UpdateTaskState(ctx, locked, current.Generation, current.Generation+1, string(next))
		if err != nil {
			return "", err
		}
		if current.PRID == nil {
			return "", storage.ErrNotFound
		}
		if _, err = repos.Queries().Exec(ctx, `UPDATE prs SET approved_head_sha=NULL,approved_base_sha=NULL,human_approval_id=NULL,ci_status='PENDING' WHERE id=$1`, *current.PRID); err != nil {
			return "", err
		}
		if err = r.cancelJobs(ctx, repos, updated.Record().ID); err != nil {
			return "", err
		}
		return DispositionProcessed, nil
	}
	if p.Action != "synchronize" && p.Action != "edited" && p.Action != "reopened" {
		return DispositionIgnored, nil
	}
	observed := contracts.Snapshot{HeadSHA: p.PullRequest.Head.SHA, BaseSHA: p.PullRequest.Base.SHA}
	if err := observed.Validate(true); err != nil {
		return DispositionRejected, nil
	}
	// A matching recorded push operation is the sole authority for APRL-written
	// head changes. Base-only movement is an expected repository observation.
	baseOnly := observed.HeadSHA == current.Snapshot.HeadSHA && observed.BaseSHA != current.Snapshot.BaseSHA && policyAllowsHumanPR(policy, p.PullRequest.User, p.Sender)
	var corr pushCorrelation
	if observed.HeadSHA != current.Snapshot.HeadSHA {
		corr, err = r.correlatedPush(ctx, repos, current.ID, current.Generation, current.Snapshot.HeadSHA, current.Snapshot.BaseSHA, p.PullRequest.Head.Ref, observed.HeadSHA)
		if err != nil {
			return "", err
		}
		if corr.found && !corr.currentMatch {
			return DispositionIgnored, nil
		}
	}
	disposition, err := r.applySnapshot(ctx, repos, locked, observed, baseOnly || corr.found)
	if err == nil && corr.found && !corr.alreadyApplied {
		err = r.markPushHandoff(ctx, repos, corr.id, current.Generation+1, observed.HeadSHA)
	}
	return disposition, err
}

func (r *Router) routePush(ctx context.Context, repos *storage.Repositories, p githubPayload) (string, error) {
	if p.After == "" || p.Before == "" {
		return DispositionIgnored, nil
	}
	var taskID string
	err := repos.Queries().QueryRow(ctx, `SELECT t.id::text FROM tasks t JOIN prs pr ON pr.task_id=t.id WHERE pr.repo_full_name=$1 AND (pr.head_sha=$2 OR pr.head_sha=$3) ORDER BY (pr.head_sha=$2) DESC LIMIT 1`, p.Repository.FullName, p.Before, p.After).Scan(&taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return DispositionIgnored, nil
	}
	if err != nil {
		return "", err
	}
	locked, err := lockRoutedTask(ctx, repos, taskID)
	if err != nil {
		return "", err
	}
	current := locked.Record()
	observed := current.Snapshot
	observed.HeadSHA = p.After
	if err := observed.Validate(true); err != nil {
		return DispositionRejected, nil
	}
	if observed.HeadSHA == current.Snapshot.HeadSHA {
		return DispositionProcessed, nil
	}
	corr, err := r.correlatedPush(ctx, repos, current.ID, current.Generation, p.Before, current.Snapshot.BaseSHA, strings.TrimPrefix(p.Ref, "refs/heads/"), p.After)
	if err != nil {
		return "", err
	}
	if corr.found && !corr.currentMatch {
		return DispositionIgnored, nil
	}
	disposition, err := r.applySnapshot(ctx, repos, locked, observed, corr.found)
	if err == nil && corr.found && !corr.alreadyApplied {
		err = r.markPushHandoff(ctx, repos, corr.id, current.Generation+1, p.After)
	}
	return disposition, err
}

func (r *Router) routeCIObservation(ctx context.Context, repos *storage.Repositories, eventType string, p githubPayload) (string, error) {
	sha := ""
	trusted := false
	switch eventType {
	case "check_run":
		sha = p.CheckRun.HeadSHA
		trusted = containsID(r.cfg.TrustedCIAppIDs, p.CheckRun.App.ID)
	case "check_suite":
		sha = p.CheckSuite.HeadSHA
		trusted = containsID(r.cfg.TrustedCIAppIDs, p.CheckSuite.App.ID)
	case "workflow_run":
		sha = p.WorkflowRun.HeadSHA
		trusted = containsID(r.cfg.TrustedCIActorIDs, p.WorkflowRun.Actor.ID)
	case "status":
		sha = p.SHA
		trusted = containsID(r.cfg.TrustedCIActorIDs, p.Sender.ID)
		if _, loginTrusted := r.cfg.TrustedCISenderLogins[p.Sender.Login]; loginTrusted {
			trusted = true
		}
	}
	if !trusted || sha == "" {
		return DispositionIgnored, nil
	}
	var taskID string
	err := repos.Queries().QueryRow(ctx, `SELECT t.id::text FROM tasks t JOIN prs pr ON pr.task_id=t.id WHERE pr.repo_full_name=$1 AND (pr.head_sha=$2 OR pr.integration_sha=$2)`, p.Repository.FullName, sha).Scan(&taskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return DispositionIgnored, nil
	}
	if err != nil {
		return "", err
	}
	locked, err := lockRoutedTask(ctx, repos, taskID)
	if err != nil {
		return "", err
	}
	current := locked.Record()
	if current.State == string(lifecycle.Paused) || current.State == string(lifecycle.Escalated) || current.State == string(lifecycle.Closed) || current.State == string(lifecycle.Merged) {
		return DispositionProcessed, nil
	}
	if sha != current.Snapshot.HeadSHA && sha != current.Snapshot.IntegrationSHA {
		return DispositionIgnored, nil
	}
	if _, _, err = r.insertCIJob(ctx, repos, locked); err != nil {
		return "", err
	}
	return DispositionProcessed, nil
}

func (r *Router) applySnapshot(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, observed contracts.Snapshot, authorized bool) (string, error) {
	current := locked.Record()
	transition, err := lifecycle.ObserveSnapshot(lifecycle.State(current.State), current.Generation, current.Snapshot, observed, authorized)
	if err != nil {
		return DispositionRejected, nil
	}
	if !transition.Changed {
		return DispositionProcessed, nil
	}
	updated, err := repos.UpdateTaskState(ctx, locked, current.Generation, transition.Generation, string(transition.State))
	if err != nil {
		return "", err
	}
	command, err := repos.Queries().Exec(ctx, `UPDATE prs SET head_sha=$2,base_sha=$3,integration_sha=$4,approved_head_sha=NULL,approved_base_sha=NULL,human_approval_id=NULL,ci_status='PENDING' WHERE task_id=$1::uuid`, current.ID, observed.HeadSHA, observed.BaseSHA, nullableRouterSHA(observed.IntegrationSHA))
	if err != nil {
		return "", err
	}
	if command.RowsAffected() != 1 {
		return "", storage.ErrNotFound
	}
	locked, err = repos.LockTask(ctx, current.ID)
	if err != nil {
		return "", err
	}
	if transition.State == lifecycle.Paused || transition.State == lifecycle.Escalated || transition.State == lifecycle.Closed || transition.State == lifecycle.Merged {
		if err = r.cancelJobs(ctx, repos, current.ID); err != nil {
			return "", err
		}
		_ = updated
		return DispositionProcessed, nil
	}
	if _, _, err = r.insertCIJob(ctx, repos, locked); err != nil {
		return "", err
	}
	return DispositionProcessed, nil
}

// ApplyConfirmedPush is the transaction-bound push handoff seam for the
// authenticated result path. The caller must already hold the task lock after
// locking its organization budget. It validates the immutable push request and
// either records its one generation/snapshot handoff or recognizes that marker.
// It does not create successor finding replies; the result controller owns that
// marker independently through reply_jobs_created.
func (r *Router) ApplyConfirmedPush(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, operationID, branch, newHeadSHA string) (storage.LockedTask, bool, error) {
	task := locked.Record()
	if err := repos.RequireTaskGeneration(ctx, locked, task.Generation); err != nil {
		return storage.LockedTask{}, false, err
	}
	var opTaskID, jobTaskID string
	var opGeneration, jobGeneration int64
	var expectedHead, expectedBase, requestedBranch, requestedHead, status, handoffGeneration, resultHead *string
	err := repos.Queries().QueryRow(ctx, `SELECT op.task_id::text,j.task_id::text,op.generation,j.generation,op.expected_head_sha,op.expected_base_sha,op.request->>'branch',op.request->>'new_head_sha',op.status,op.result->>'handoff_generation',op.result->>'head_sha' FROM github_operations op JOIN jobs j ON j.id=op.job_id AND j.task_id=op.task_id WHERE op.id=$1::uuid AND j.payload->>'operation_id'=op.id::text FOR UPDATE OF op`, operationID).Scan(&opTaskID, &jobTaskID, &opGeneration, &jobGeneration, &expectedHead, &expectedBase, &requestedBranch, &requestedHead, &status, &handoffGeneration, &resultHead)
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.LockedTask{}, false, storage.ErrNotFound
	}
	if err != nil {
		return storage.LockedTask{}, false, err
	}
	if opTaskID != task.ID || jobTaskID != task.ID || jobGeneration != opGeneration || status == nil || (*status != "INTENDED" && *status != "IN_FLIGHT" && *status != "UNKNOWN" && *status != "CONFIRMED") || requestedBranch == nil || *requestedBranch != branch || requestedHead == nil || *requestedHead != newHeadSHA {
		return storage.LockedTask{}, false, ErrPushFence
	}
	if task.Generation > 0 && opGeneration == task.Generation-1 && handoffGeneration != nil && resultHead != nil && *handoffGeneration == strconv.FormatInt(task.Generation, 10) && *resultHead == newHeadSHA && task.Snapshot.HeadSHA == newHeadSHA {
		return locked, false, nil
	}
	if opGeneration != task.Generation || expectedHead == nil || expectedBase == nil || *expectedHead != task.Snapshot.HeadSHA || *expectedBase != task.Snapshot.BaseSHA {
		return storage.LockedTask{}, false, ErrPushFence
	}
	observed := contracts.Snapshot{HeadSHA: newHeadSHA, BaseSHA: task.Snapshot.BaseSHA}
	if err := observed.Validate(true); err != nil {
		return storage.LockedTask{}, false, ErrPushFence
	}
	if _, err := r.applySnapshot(ctx, repos, locked, observed, true); err != nil {
		return storage.LockedTask{}, false, err
	}
	updated, err := repos.LockTask(ctx, task.ID)
	if err != nil {
		return storage.LockedTask{}, false, err
	}
	if updated.Record().Generation != task.Generation+1 || updated.Record().Snapshot.HeadSHA != newHeadSHA {
		return storage.LockedTask{}, false, ErrPushFence
	}
	if err := r.markPushHandoff(ctx, repos, operationID, updated.Record().Generation, newHeadSHA); err != nil {
		return storage.LockedTask{}, false, err
	}
	return updated, true, nil
}

func (r *Router) insertPR(ctx context.Context, repos *storage.Repositories, taskID string, p githubPayload) error {
	s := contracts.Snapshot{HeadSHA: p.PullRequest.Head.SHA, BaseSHA: p.PullRequest.Base.SHA}
	if err := s.Validate(true); err != nil {
		return err
	}
	_, err := repos.Queries().Exec(ctx, `INSERT INTO prs(task_id,repo_full_name,pr_number,head_sha,base_ref,base_sha,is_draft) VALUES($1::uuid,$2,$3,$4,$5,$6,$7)`, taskID, p.Repository.FullName, p.PullRequest.Number, s.HeadSHA, p.PullRequest.Base.Ref, s.BaseSHA, p.PullRequest.Draft)
	return err
}

func (r *Router) insertJob(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, operation string) (storage.JobRecord, bool, error) {
	return r.insertLogicalJob(ctx, repos, locked, operation)
}
func (r *Router) insertCIJob(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask) (storage.JobRecord, bool, error) {
	return r.insertLogicalJob(ctx, repos, locked, OperationCIReconcile)
}

func (r *Router) insertLogicalJob(ctx context.Context, repos *storage.Repositories, locked storage.LockedTask, operation string) (storage.JobRecord, bool, error) {
	task := locked.Record()
	logicalKey := fmt.Sprintf("task:%s:generation:%d:%s", task.ID, task.Generation, operation)
	jobID := stableRouterUUID("job:" + logicalKey)
	opID := stableRouterUUID("operation:" + logicalKey)
	correlationID := stableRouterUUID("correlation:" + logicalKey)
	job := contracts.Job{Version: 1, TaskID: task.ID, JobID: jobID, Generation: task.Generation, Snapshot: task.Snapshot, Attempt: 1, OperationID: opID, CorrelationID: correlationID, Operation: operation}
	encoded, err := json.Marshal(job)
	if err != nil {
		return storage.JobRecord{}, false, err
	}
	return repos.InsertJobAndOutbox(ctx, locked, storage.JobInput{ID: jobID, LogicalKey: logicalKey, OperationType: operation, PRID: task.PRID, Payload: encoded})
}

func (r *Router) cancelJobs(ctx context.Context, repos *storage.Repositories, taskID string) error {
	rows, err := repos.Queries().Query(ctx, `UPDATE jobs SET status='CANCELLED' WHERE task_id=$1::uuid AND status IN ('PENDING','LEASED') RETURNING id::text`, taskID)
	if err != nil {
		return err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	for _, id := range ids {
		if _, err := repos.Queries().Exec(ctx, `INSERT INTO outbox(task_id,job_id,kind,payload) VALUES($1::uuid,$2::uuid,'CANCEL',jsonb_build_object('job_id',$2::uuid))`, taskID, id); err != nil {
			return err
		}
	}
	return nil
}

type pushCorrelation struct {
	id             string
	generation     int64
	alreadyApplied bool
	found          bool
	currentMatch   bool
}

func (r *Router) correlatedPush(ctx context.Context, repos *storage.Repositories, taskID string, generation int64, oldHead, base, branch, newHead string) (pushCorrelation, error) {
	var result pushCorrelation
	var expectedHead, expectedBase, handoffGeneration, confirmedHead *string
	err := repos.Queries().QueryRow(ctx, `SELECT op.id::text,op.generation,op.expected_head_sha,op.expected_base_sha,op.result->>'handoff_generation',op.result->>'head_sha' FROM github_operations op JOIN jobs j ON j.id=op.job_id AND j.task_id=op.task_id WHERE op.task_id=$1::uuid AND op.generation<=$2 AND j.generation=op.generation AND j.payload->>'operation_id'=op.id::text AND op.operation_type='push' AND op.status IN ('INTENDED','IN_FLIGHT','UNKNOWN','CONFIRMED') AND op.request->>'branch'=$3 AND op.request->>'new_head_sha'=$4 ORDER BY op.generation DESC LIMIT 1 FOR UPDATE OF op`, taskID, generation, branch, newHead).Scan(&result.id, &result.generation, &expectedHead, &expectedBase, &handoffGeneration, &confirmedHead)
	if errors.Is(err, pgx.ErrNoRows) {
		return pushCorrelation{}, nil
	}
	if err != nil {
		return pushCorrelation{}, fmt.Errorf("correlate recorded APRL push: %w", err)
	}
	result.found = true
	result.currentMatch = result.generation == generation && expectedHead != nil && expectedBase != nil && *expectedHead == oldHead && *expectedBase == base
	if result.generation+1 == generation && handoffGeneration != nil && confirmedHead != nil && *handoffGeneration == strconv.FormatInt(generation, 10) && *confirmedHead == newHead {
		result.currentMatch = true
		result.alreadyApplied = true
	}
	return result, nil
}

func (r *Router) markPushHandoff(ctx context.Context, repos *storage.Repositories, operationID string, generation int64, headSHA string) error {
	command, err := repos.Queries().Exec(ctx, `UPDATE github_operations SET status='CONFIRMED',result=COALESCE(result,'{}'::jsonb)||jsonb_build_object('head_sha',$2::text,'handoff_generation',$3::bigint) WHERE id=$1::uuid AND operation_type='push' AND status IN ('INTENDED','IN_FLIGHT','UNKNOWN','CONFIRMED')`, operationID, headSHA, generation)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return storage.ErrNotFound
	}
	return nil
}

func (r *Router) correlatedPRCreation(ctx context.Context, repos *storage.Repositories, taskID string, generation int64, branch, headSHA string, prNumber int) (bool, error) {
	var ok bool
	err := repos.Queries().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM github_operations op JOIN jobs j ON j.id=op.job_id AND j.task_id=op.task_id WHERE op.task_id=$1::uuid AND op.generation=$2 AND j.generation=$2 AND j.operation_type='author' AND j.payload->>'operation_id'=op.id::text AND op.operation_type IN ('create_pr','create_pull_request') AND op.status IN ('IN_FLIGHT','UNKNOWN','CONFIRMED') AND op.request->>'branch'=$3 AND (op.result->>'head_sha'=$4 OR op.result->>'pr_number'=$5 OR op.result->>'number'=$5))`, taskID, generation, branch, headSHA, strconv.Itoa(prNumber)).Scan(&ok)
	return ok, err
}

func lockRoutedTask(ctx context.Context, repos *storage.Repositories, taskID string) (storage.LockedTask, error) {
	var orgID string
	if err := repos.Queries().QueryRow(ctx, `SELECT org_id FROM tasks WHERE id=$1::uuid`, taskID).Scan(&orgID); errors.Is(err, pgx.ErrNoRows) {
		return storage.LockedTask{}, storage.ErrNotFound
	} else if err != nil {
		return storage.LockedTask{}, err
	}
	_, locked, err := repos.LockOrgBudgetAndTask(ctx, orgID, taskID)
	return locked, err
}

func sameRouterSnapshot(left, right contracts.Snapshot) bool {
	return left.HeadSHA == right.HeadSHA && left.BaseSHA == right.BaseSHA && left.IntegrationSHA == right.IntegrationSHA
}

func policyAllowsHumanPR(policy RepositoryPolicy, user, sender actor) bool {
	return user.ID > 0 && user.Type == "User" && sender.Type == "User" && sender.ID == user.ID && containsID(policy.AuthorizedHumanPRAuthorIDs, user.ID)
}
func containsID(set map[int64]struct{}, id int64) bool { _, ok := set[id]; return id > 0 && ok }
func nullableRouterSHA(sha string) any {
	if sha == "" {
		return nil
	}
	return sha
}
func stableRouterUUID(key string) string {
	sum := sha256.Sum256([]byte("aprl.router.v1:" + key))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]), hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]), hex.EncodeToString(b[10:16]))
}
