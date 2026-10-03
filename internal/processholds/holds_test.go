package processholds

import (
	"strings"
	"testing"
	"time"
)

func TestConfigRequiresExplicitScopeCapacityAndBoundedVerifier(t *testing.T) {
	base := Config{ResourceScope: "host-pool-a", MaxActive: 2, VerifierTimeout: time.Second}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, config := range map[string]Config{
		"empty scope":        {MaxActive: 2, VerifierTimeout: time.Second},
		"zero capacity":      {ResourceScope: "host-pool-a", VerifierTimeout: time.Second},
		"unbounded verifier": {ResourceScope: "host-pool-a", MaxActive: 2},
		"too-long verifier":  {ResourceScope: "host-pool-a", MaxActive: 2, VerifierTimeout: 31 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			if err := config.Validate(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestReapEvidenceRequiresExactProcessGroupIdentity(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	process := &ProcessIdentity{PID: 42, PGID: 40, StartIdentity: "boot-7/process-start-42"}
	h := Hold{RunID: "10000000-0000-4000-8000-000000000001", TaskID: "10000000-0000-4000-8000-000000000002",
		JobID: "10000000-0000-4000-8000-000000000003", ResourceScope: "host-pool-a", LeaseTokenSHA: strings.Repeat("a", 64),
		Workspace: "/private/workspaces/one", SupervisorID: "supervisor-a", State: StateUnknown, Process: process,
		UnknownReason: ReasonSupervisorLost, Revision: 3, CreatedAt: now.Add(-time.Minute), UpdatedAt: now}
	proof := ReapEvidence{Kind: ProofGroupDrained, RunID: h.RunID, TaskID: h.TaskID, JobID: h.JobID, ResourceScope: h.ResourceScope,
		LeaseTokenSHA: h.LeaseTokenSHA, Workspace: h.Workspace, SupervisorID: h.SupervisorID, Process: copyProcess(process), VerifiedAt: now, VerifierID: "verifier-run-1"}
	if err := validateEvidence(h, proof, now); err != nil {
		t.Fatalf("exact drain proof rejected: %v", err)
	}
	stale := proof
	stale.VerifiedAt = h.UpdatedAt.Add(-time.Nanosecond)
	if err := validateEvidence(h, stale, now); err == nil {
		t.Fatal("proof captured before the latest hold observation accepted")
	}
	wrong := proof
	wrong.Process = copyProcess(proof.Process)
	wrong.Process.StartIdentity = "reused-pid"
	if err := validateEvidence(h, wrong, now); err == nil {
		t.Fatal("PID/PGID proof with mismatched start identity accepted")
	}
	wrong = proof
	wrong.Process = &ProcessIdentity{PID: process.PID, PGID: process.PGID}
	if err := validateEvidence(h, wrong, now); err == nil {
		t.Fatal("PID/PGID-only proof accepted")
	}
}

func TestNeverStartedProofCannotReleaseARecordedProcess(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	h := Hold{RunID: "10000000-0000-4000-8000-000000000001", TaskID: "10000000-0000-4000-8000-000000000002",
		JobID: "10000000-0000-4000-8000-000000000003", ResourceScope: "host-pool-a", LeaseTokenSHA: strings.Repeat("a", 64),
		Workspace: "/private/workspaces/one", SupervisorID: "supervisor-a", State: StateStarted,
		Process: &ProcessIdentity{PID: 42, PGID: 40, StartIdentity: "boot-7/process-start-42"}, Revision: 2,
		CreatedAt: now.Add(-time.Minute), UpdatedAt: now}
	proof := ReapEvidence{Kind: ProofNeverStarted, RunID: h.RunID, TaskID: h.TaskID, JobID: h.JobID, ResourceScope: h.ResourceScope,
		LeaseTokenSHA: h.LeaseTokenSHA, Workspace: h.Workspace, SupervisorID: h.SupervisorID, VerifiedAt: now, VerifierID: "verifier-run-1"}
	if err := validateEvidence(h, proof, now); err == nil {
		t.Fatal("never-started proof released a hold with process identity")
	}
}
