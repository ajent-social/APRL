// Package broker admits durable GitHub operations for the separately owned
// author, reviewer, and fixer application identities.
package broker

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ajent-social/APRL/internal/policy"
)

var (
	// ErrInvalid reports a malformed operation request or broker configuration.
	ErrInvalid = errors.New("invalid broker request")
	// ErrForbidden reports an operation outside the durable job role's capabilities.
	ErrForbidden = errors.New("broker operation is not authorized")
	// ErrStale reports a fenced lease, task, or snapshot.
	ErrStale = errors.New("broker admission is stale")
	// ErrConflict reports operation ID reuse with different immutable meaning.
	ErrConflict = errors.New("broker operation conflicts with durable intent")
	// ErrUnknown reports an operation with an unresolved remote outcome.
	ErrUnknown = errors.New("broker operation outcome is unknown")
	// ErrInFlight reports a durable operation whose remote call is still in progress.
	ErrInFlight = errors.New("broker operation is in flight")
)

// Action is a supported worker or host mutation capability.
type Action string

const (
	// ActionCreatePR creates a pull request from an already published host branch.
	ActionCreatePR Action = "create_pr"
	// ActionPublish publishes author output to its host-assigned branch.
	ActionPublish Action = "publish"
	// ActionPush pushes fixer output to the assigned source branch.
	ActionPush Action = "push"
	// ActionReview posts a validated reviewer decision.
	ActionReview Action = "review"
	// ActionResolve resolves a finding verified at the current corrected head.
	ActionResolve Action = "resolve_finding"
	// ActionReply posts a reply backed by a confirmed push intent.
	ActionReply Action = "reply"
	// ActionMerge submits a host-gated merge with an expected head SHA.
	ActionMerge Action = "merge"
)

// RepositoryPolicy holds host-owned target and merge gates for one repository.
type RepositoryPolicy struct {
	// AllowedTargetBranches lists target branches eligible for autonomous merge.
	AllowedTargetBranches []string
	// ProtectedTargetBranches lists targets requiring current human approval.
	ProtectedTargetBranches []string
	// AutonomousMergeEnabled is the repository-level merge rollout gate.
	AutonomousMergeEnabled bool
	// CI is the trusted aggregate-check policy for this repository.
	CI policy.CIConfig
}

// Config is host-owned broker policy. Worker requests cannot modify it.
type Config struct {
	// Repositories maps exact repository full names to host-owned policy.
	Repositories map[string]RepositoryPolicy
	// RPCTimeout bounds each individual remote read or write.
	RPCTimeout time.Duration
}

// Validate checks the configured repository gates and RPC bound.
func (c Config) Validate() error {
	if len(c.Repositories) == 0 || c.RPCTimeout <= 0 || c.RPCTimeout > 2*time.Minute {
		return ErrInvalid
	}
	for repository, rule := range c.Repositories {
		if strings.TrimSpace(repository) == "" || strings.TrimSpace(repository) != repository || len(rule.AllowedTargetBranches) == 0 {
			return ErrInvalid
		}
		for _, branch := range append(append([]string(nil), rule.AllowedTargetBranches...), rule.ProtectedTargetBranches...) {
			if strings.TrimSpace(branch) == "" || strings.TrimSpace(branch) != branch || strings.Contains(branch, "..") || strings.ContainsAny(branch, " ~^:?*[\\") {
				return ErrInvalid
			}
		}
		if rule.AutonomousMergeEnabled {
			if err := rule.CI.ValidateTiming(); err != nil {
				return fmt.Errorf("validate CI timing for %s: %w", repository, err)
			}
			if err := rule.CI.Validate(); err != nil {
				return fmt.Errorf("validate CI policy for %s: %w", repository, err)
			}
		}
	}
	return nil
}

// permitsRole reports the narrow capability matrix for worker operations.
// Merge is intentionally absent: only the trusted host Merge method may admit it.
func permitsRole(role, jobOperation string, action Action) bool {
	switch role {
	case "A":
		return jobOperation == "author" && (action == ActionCreatePR || action == ActionPublish)
	case "B":
		return jobOperation == "review" && (action == ActionReview || action == ActionResolve)
	case "C":
		if jobOperation == "fix" {
			return action == ActionPush
		}
		return jobOperation == "reply" && action == ActionReply
	default:
		return false
	}
}

// branchAllowed reports whether a target branch matches a literal or trailing-star prefix.
func branchAllowed(branch string, patterns []string) bool {
	for _, pattern := range patterns {
		if strings.HasSuffix(pattern, "*") {
			if strings.HasPrefix(branch, strings.TrimSuffix(pattern, "*")) {
				return true
			}
		} else if branch == pattern {
			return true
		}
	}
	return false
}
