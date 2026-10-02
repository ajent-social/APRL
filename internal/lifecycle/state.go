// Package lifecycle contains pure APRL task-state and snapshot transition rules.
package lifecycle

import (
	"errors"

	"github.com/ajent-social/APRL/internal/contracts"
)

// State is a persisted task lifecycle state.
type State string

const (
	// Authoring marks an enrolled task awaiting its initial author job.
	Authoring State = "AUTHORING"
	// WaitingCI marks a snapshot waiting for CI reconciliation.
	WaitingCI State = "WAITING_CI"
	// InReview marks a task awaiting review.
	InReview State = "IN_REVIEW"
	// ChangesRequested marks a task with requested review changes.
	ChangesRequested State = "CHANGES_REQUESTED"
	// Fixing marks a task with an active remediation job.
	Fixing State = "FIXING"
	// ReadyToMerge marks a task eligible for a later guarded merge policy.
	ReadyToMerge State = "READY_TO_MERGE"
	// Paused marks work stopped for an unexpected repository change.
	Paused State = "PAUSED"
	// Escalated marks work handed to a human decision path.
	Escalated State = "ESCALATED"
	// Merged marks a terminal merged pull request.
	Merged State = "MERGED"
	// Closed marks a terminal closed pull request.
	Closed State = "CLOSED"
)

// ErrGenerationExhausted indicates that a snapshot cannot advance past BIGINT.
var ErrGenerationExhausted = errors.New("task generation is exhausted")

// SnapshotTransition is the lifecycle result of observing a repository snapshot.
type SnapshotTransition struct {
	State      State // State is the resulting task state.
	Generation int64 // Generation is the resulting fencing generation.
	Changed    bool  // Changed reports whether the snapshot advanced.
}

// ObserveSnapshot applies a GitHub snapshot observation. A changed snapshot
// fences the previous generation. Authorized APRL writes return to CI; an
// uncorrelated writer pauses active work. Pause/escalation and terminal states
// are retained.
func ObserveSnapshot(state State, generation int64, current, observed contracts.Snapshot, authorizedAPRL bool) (SnapshotTransition, error) {
	if generation < 0 {
		return SnapshotTransition{}, errors.New("negative generation")
	}
	if err := current.Validate(false); err != nil {
		return SnapshotTransition{}, err
	}
	if err := observed.Validate(false); err != nil {
		return SnapshotTransition{}, err
	}
	if terminal(state) {
		return SnapshotTransition{State: state, Generation: generation}, nil
	}
	if sameSnapshot(current, observed) {
		return SnapshotTransition{State: state, Generation: generation}, nil
	}
	if generation == int64(^uint64(0)>>1) {
		return SnapshotTransition{}, ErrGenerationExhausted
	}
	next := state
	switch state {
	case Paused, Escalated:
	case Authoring, WaitingCI, InReview, ChangesRequested, Fixing, ReadyToMerge:
		if authorizedAPRL {
			next = WaitingCI
		} else {
			next = Paused
		}
	default:
		return SnapshotTransition{}, errors.New("unknown lifecycle state")
	}
	return SnapshotTransition{State: next, Generation: generation + 1, Changed: true}, nil
}

// CompletionCurrent reports whether an event completion is still eligible for
// active lifecycle work. Result persistence remains owned by the result lane.
func CompletionCurrent(state State, generation int64, snapshot contracts.Snapshot, event contracts.Event) bool {
	return !terminal(state) && state != Paused && state != Escalated &&
		generation == event.Generation && sameSnapshot(snapshot, event.Snapshot)
}

func terminal(state State) bool { return state == Merged || state == Closed }

func sameSnapshot(left, right contracts.Snapshot) bool {
	return left.HeadSHA == right.HeadSHA && left.BaseSHA == right.BaseSHA && left.IntegrationSHA == right.IntegrationSHA
}
