// Package policy provides fail-closed repository and lifecycle admission rules.
package policy

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/contracts"
)

var (
	// ErrEmptyRequiredChecks reports an autonomous policy with no CI checks.
	ErrEmptyRequiredChecks = errors.New("required CI check policy is empty")
	// ErrInvalidPolicy reports a malformed required-check or producer policy.
	ErrInvalidPolicy = errors.New("invalid repository policy")
)

// ProducerKind identifies how a GitHub CI producer is authenticated.
type ProducerKind string

const (
	// ProducerApp identifies a GitHub App by numeric App ID.
	ProducerApp ProducerKind = "app"
	// ProducerActor identifies a GitHub actor by numeric account ID.
	ProducerActor ProducerKind = "actor"
	// ProducerStatusSender identifies a commit-status sender by exact login.
	ProducerStatusSender ProducerKind = "status_sender"
)

// Producer is a trusted GitHub check producer identity.
type Producer struct {
	Kind ProducerKind // Kind selects the authenticated GitHub producer identity class.
	ID   string       // ID is the numeric GitHub ID or exact commit-status sender login.
}

// RequiredCheck binds one required GitHub check name to its trusted producer.
type RequiredCheck struct {
	Name     string   // Name is the exact GitHub check name.
	Producer Producer // Producer is the only identity allowed to satisfy this check.
}

// CIConfig freezes the trusted producers, required checks, and bounded retry timing.
type CIConfig struct {
	TrustedProducers []Producer      // TrustedProducers is the operator-approved producer set.
	RequiredChecks   []RequiredCheck // RequiredChecks is the nonempty set required for autonomous progression.
	WaitTimeout      time.Duration   // WaitTimeout is the persisted per-snapshot CI deadline duration.
	PollInterval     time.Duration   // PollInterval bounds the deferred CI retry delay.
}

// Validate checks that every required check has a configured trusted producer.
func (c CIConfig) Validate() error {
	if len(c.RequiredChecks) == 0 {
		return ErrEmptyRequiredChecks
	}
	return c.ValidateTiming()
}

// ValidateTiming verifies a bounded CI wait and polling interval independently of check presence.
func (c CIConfig) ValidateTiming() error {
	if c.WaitTimeout <= 0 || c.PollInterval <= 0 || c.PollInterval >= c.WaitTimeout {
		return fmt.Errorf("CI timeout and poll interval must be positive and bounded: %w", ErrInvalidPolicy)
	}
	trusted := make(map[Producer]struct{}, len(c.TrustedProducers))
	for _, producer := range c.TrustedProducers {
		if !validProducer(producer) {
			return ErrInvalidPolicy
		}
		trusted[producer] = struct{}{}
	}
	seen := make(map[string]struct{}, len(c.RequiredChecks))
	for _, check := range c.RequiredChecks {
		if strings.TrimSpace(check.Name) == "" || !validProducer(check.Producer) {
			return ErrInvalidPolicy
		}
		key := check.Name + "\x00" + string(check.Producer.Kind) + "\x00" + check.Producer.ID
		if _, exists := seen[key]; exists {
			return ErrInvalidPolicy
		}
		seen[key] = struct{}{}
		if _, ok := trusted[check.Producer]; !ok {
			return ErrInvalidPolicy
		}
	}
	return nil
}

func validProducer(producer Producer) bool {
	if strings.TrimSpace(producer.ID) == "" {
		return false
	}
	switch producer.Kind {
	case ProducerApp, ProducerActor:
		id, err := strconv.ParseInt(producer.ID, 10, 64)
		return err == nil && id > 0
	case ProducerStatusSender:
		return producer.ID == strings.TrimSpace(producer.ID)
	default:
		return false
	}
}

// ReviewAdmission contains the gates required before creating a B review job.
type ReviewAdmission struct {
	TaskState       string             // TaskState must be WAITING_CI.
	CurrentSnapshot contracts.Snapshot // CurrentSnapshot is the durable task snapshot.
	TestedSnapshot  contracts.Snapshot // TestedSnapshot is the exact CI integration tested.
	RequiredChecks  int                // RequiredChecks is the configured nonempty check count.
	ChecksPassed    bool               // ChecksPassed is the trusted aggregate verdict.
}

// CanAdmitReview allows B only for a current, fully tested, nonempty CI policy.
func CanAdmitReview(input ReviewAdmission) bool {
	return input.TaskState == "WAITING_CI" && input.RequiredChecks > 0 && input.ChecksPassed && sameCompleteSnapshot(input.CurrentSnapshot, input.TestedSnapshot)
}

// MergeAdmission contains the full snapshot and lifecycle inputs to merge policy.
type MergeAdmission struct {
	TaskState               string             // TaskState must be READY_TO_MERGE.
	CurrentSnapshot         contracts.Snapshot // CurrentSnapshot is the durable task snapshot.
	ReviewedSnapshot        contracts.Snapshot // ReviewedSnapshot is the snapshot approved by B.
	CISnapshot              contracts.Snapshot // CISnapshot is the integration commit with passing checks.
	RequiredChecks          int                // RequiredChecks must be nonzero.
	ChecksPassed            bool               // ChecksPassed is the trusted aggregate verdict.
	ReviewApproved          bool               // ReviewApproved is B's explicit approval.
	ReviewHasBlockers       bool               // ReviewHasBlockers fences any blocker finding.
	AutonomousMergeEnabled  bool               // AutonomousMergeEnabled is the operator/repository rollout gate.
	TargetBranch            string             // TargetBranch is the configured PR base branch.
	AllowedTargetBranches   []string           // AllowedTargetBranches is the allowlist/pattern set.
	ProtectedTargetBranches []string           // ProtectedTargetBranches requires current human approval.
	HumanApprovalCurrent    bool               // HumanApprovalCurrent confirms approval of the same snapshot.
	Draft                   bool               // Draft fences pull requests still marked draft.
	Conflicted              bool               // Conflicted fences pull requests with merge conflicts.
}

// CanAutonomouslyMerge denies empty policies and any stale or incomplete gate.
func CanAutonomouslyMerge(input MergeAdmission) bool {
	if input.TaskState != "READY_TO_MERGE" || !input.AutonomousMergeEnabled || input.RequiredChecks <= 0 || !input.ChecksPassed || !input.ReviewApproved || input.ReviewHasBlockers || input.Draft || input.Conflicted || strings.TrimSpace(input.TargetBranch) == "" {
		return false
	}
	if !sameCompleteSnapshot(input.CurrentSnapshot, input.ReviewedSnapshot) || !sameCompleteSnapshot(input.CurrentSnapshot, input.CISnapshot) {
		return false
	}
	if !matchesBranch(input.TargetBranch, input.AllowedTargetBranches) {
		return false
	}
	for _, protected := range input.ProtectedTargetBranches {
		if branchMatches(input.TargetBranch, protected) && !input.HumanApprovalCurrent {
			return false
		}
	}
	return true
}

func sameCompleteSnapshot(a, b contracts.Snapshot) bool {
	return a.Validate(true) == nil && b.Validate(true) == nil && a.HeadSHA == b.HeadSHA && a.BaseSHA == b.BaseSHA && a.IntegrationSHA != "" && a.IntegrationSHA == b.IntegrationSHA
}

func matchesBranch(branch string, patterns []string) bool {
	for _, pattern := range patterns {
		if branchMatches(branch, pattern) {
			return true
		}
	}
	return false
}

func branchMatches(branch, pattern string) bool {
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(branch, strings.TrimSuffix(pattern, "*"))
	}
	return branch == pattern
}
