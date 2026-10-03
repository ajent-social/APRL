package plantasks

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// DeliveryV1ProtocolVersion identifies the caller-facing wire contract.
	DeliveryV1ProtocolVersion = "code-delivery/v1"
	DeliveryV1MaxJSONBytes    = 1 << 20

	deliveryV1MaxIDLength          = 128
	deliveryV1MaxTextLength        = 4096
	deliveryV1MaxAcceptance        = 64
	deliveryV1MaxChildren          = 256
	deliveryV1MaxDependencies      = 256
	deliveryV1MaxFindings          = 256
	deliveryV1MaxReasonLength      = 4096
	deliveryV1MaxConcurrent        = 6
	deliveryV1MaxPullRequestDigits = 12
)

var (
	// ErrDeliveryV1Malformed reports invalid JSON structure or encoding.
	ErrDeliveryV1Malformed = errors.New("malformed code-delivery/v1 document")
	ErrDeliveryV1Oversize  = errors.New("code-delivery/v1 document exceeds size limit")
	ErrDeliveryV1Invalid   = errors.New("invalid code-delivery/v1 value")

	deliveryV1ObjectIDPattern = regexp.MustCompile(`^(?:[0-9a-f]{40}|[0-9a-f]{64})$`)
	deliveryV1DigestPattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	deliveryV1BranchPattern   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,254}$`)
)

// DeliveryV1Envelope is the caller-facing resource envelope. It is a wire DTO,
// not APRL's internal execution budget.
type DeliveryV1Envelope struct {
	MaxAttempts   int       `json:"max_attempts"`
	MaxConcurrent int       `json:"max_concurrent"`
	MaxCostCents  int64     `json:"max_cost_cents"`
	ExpiresAt     time.Time `json:"expires_at"`
}

// DeliveryV1Spec is the immutable caller-approved execution scope.
type DeliveryV1Spec struct {
	Repository      string             `json:"repository"`
	TargetBranch    string             `json:"target_branch"`
	SourceCommit    string             `json:"source_commit"`
	Acceptance      []string           `json:"acceptance"`
	PolicyRevision  string             `json:"policy_revision"`
	ProfileRevision string             `json:"profile_revision"`
	ExecutionMode   string             `json:"execution_mode"`
	Envelope        DeliveryV1Envelope `json:"envelope"`
}

// DeliveryV1Request carries the frozen v1 caller and scope binding.
type DeliveryV1Request struct {
	Version      string         `json:"version"`
	DelegationID string         `json:"delegation_id"`
	CallerID     string         `json:"caller_id"`
	ProjectID    string         `json:"project_id"`
	JobID        string         `json:"job_id"`
	TaskID       string         `json:"task_id"`
	PlanRevision int            `json:"plan_revision"`
	PlanDigest   string         `json:"plan_digest"`
	Spec         DeliveryV1Spec `json:"spec"`
}

// DeliveryV1ChildTask projects one canonical executable lifecycle task.
type DeliveryV1ChildTask struct {
	ID         string   `json:"id"`
	Kind       string   `json:"kind"`
	State      string   `json:"state"`
	DependsOn  []string `json:"depends_on"`
	FindingIDs []string `json:"finding_ids"`
	PRURL      string   `json:"pr_url"`
	HeadCommit string   `json:"head_commit"`
}

// DeliveryV1Accounting is deliberately separate from internal budget state.
type DeliveryV1Accounting struct {
	SpentCents    int64  `json:"spent_cents"`
	ReservedCents int64  `json:"reserved_cents"`
	UnknownCents  int64  `json:"unknown_cents"`
	SettlementID  string `json:"settlement_id"`
}

// DeliveryV1LandedReceipt records the frozen trusted landing evidence shape.
type DeliveryV1LandedReceipt struct {
	Repository     string    `json:"repository"`
	TargetBranch   string    `json:"target_branch"`
	PRURL          string    `json:"pr_url"`
	ReviewedHead   string    `json:"reviewed_head"`
	ReviewedBase   string    `json:"reviewed_base"`
	PolicyRevision string    `json:"policy_revision"`
	LandedCommit   string    `json:"landed_commit"`
	SourceDigest   string    `json:"source_digest"`
	Reviewer       string    `json:"reviewer"`
	Author         string    `json:"author"`
	Verifier       string    `json:"verifier"`
	VerifiedAt     time.Time `json:"verified_at"`
}

// DeliveryV1Observation is a request-bound view of authoritative lifecycle
// state. Landed is optional on the wire and omitted only when nil.
type DeliveryV1Observation struct {
	Version       string                   `json:"version"`
	DelegationID  string                   `json:"delegation_id"`
	RequestDigest string                   `json:"request_digest"`
	LifecycleID   string                   `json:"lifecycle_id"`
	Sequence      uint64                   `json:"sequence"`
	State         string                   `json:"state"`
	Children      []DeliveryV1ChildTask    `json:"children"`
	Accounting    DeliveryV1Accounting     `json:"accounting"`
	Landed        *DeliveryV1LandedReceipt `json:"landed,omitempty"`
	Reason        string                   `json:"reason"`
}

// DecodeDeliveryV1Request strictly parses a bounded request before validating
// its semantic values. JSON object keys are exact-case and may not repeat.
func DecodeDeliveryV1Request(data []byte) (DeliveryV1Request, error) {
	var request DeliveryV1Request
	if err := validateDeliveryV1Document(data, deliveryV1RequestShape()); err != nil {
		return request, err
	}
	if err := json.Unmarshal(data, &request); err != nil {
		return request, malformedDeliveryV1("decode request: %v", err)
	}
	if err := request.Validate(); err != nil {
		return request, err
	}
	return request, nil
}

// EncodeDeliveryV1Request returns the canonical compact JSON bytes whose
// SHA-256 is the request idempotency digest.
func EncodeDeliveryV1Request(request DeliveryV1Request) ([]byte, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	request.Spec.Envelope.ExpiresAt = request.Spec.Envelope.ExpiresAt.UTC()
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, invalidDeliveryV1("marshal request: %v", err)
	}
	if len(encoded) > DeliveryV1MaxJSONBytes {
		return nil, ErrDeliveryV1Oversize
	}
	return encoded, nil
}

// Digest returns the lower-case SHA-256 digest of the canonical v1 request.
func (request DeliveryV1Request) Digest() (string, error) {
	encoded, err := EncodeDeliveryV1Request(request)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

// DecodeDeliveryV1Observation parses and validates an observation against the
// exact request it describes.
func DecodeDeliveryV1Observation(data []byte, request DeliveryV1Request) (DeliveryV1Observation, error) {
	var observation DeliveryV1Observation
	if err := validateDeliveryV1Document(data, deliveryV1ObservationShape()); err != nil {
		return observation, err
	}
	if err := json.Unmarshal(data, &observation); err != nil {
		return observation, malformedDeliveryV1("decode observation: %v", err)
	}
	if err := observation.Validate(request); err != nil {
		return observation, err
	}
	return observation, nil
}

// EncodeDeliveryV1Observation returns compact ordered JSON only after binding
// the observation to its request.
func EncodeDeliveryV1Observation(observation DeliveryV1Observation, request DeliveryV1Request) ([]byte, error) {
	if err := observation.Validate(request); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		return nil, invalidDeliveryV1("marshal observation: %v", err)
	}
	if len(encoded) > DeliveryV1MaxJSONBytes {
		return nil, ErrDeliveryV1Oversize
	}
	return encoded, nil
}

// Validate checks caller-side scope and envelope semantics. Authorization,
// current expiry, admission, billing, and execution are outside this codec.
func (spec DeliveryV1Spec) Validate() error {
	if !deliveryV1CanonicalRepository(spec.Repository) {
		return invalidDeliveryV1("repository must be a canonical HTTPS repository URL")
	}
	if !deliveryV1ValidBranch(spec.TargetBranch) {
		return invalidDeliveryV1("target_branch is invalid")
	}
	if !deliveryV1ValidObjectID(spec.SourceCommit) {
		return invalidDeliveryV1("source_commit must be a full lowercase Git object ID")
	}
	if len(spec.Acceptance) == 0 || len(spec.Acceptance) > deliveryV1MaxAcceptance {
		return invalidDeliveryV1("acceptance must contain 1..%d items", deliveryV1MaxAcceptance)
	}
	for _, item := range spec.Acceptance {
		if !deliveryV1ValidText(item, deliveryV1MaxTextLength) {
			return invalidDeliveryV1("acceptance items must be nonempty bounded text without controls")
		}
	}
	if !deliveryV1ValidText(spec.PolicyRevision, deliveryV1MaxIDLength) || !deliveryV1ValidText(spec.ProfileRevision, deliveryV1MaxIDLength) {
		return invalidDeliveryV1("policy_revision and profile_revision are required bounded values")
	}
	if spec.ExecutionMode != "subscription_only" {
		return invalidDeliveryV1("execution_mode must be subscription_only")
	}
	envelope := spec.Envelope
	if envelope.MaxAttempts < 1 || envelope.MaxConcurrent < 1 || envelope.MaxConcurrent > deliveryV1MaxConcurrent {
		return invalidDeliveryV1("envelope attempts and concurrency are out of range")
	}
	if envelope.MaxCostCents != 0 {
		return invalidDeliveryV1("v1 requires max_cost_cents to be zero")
	}
	if envelope.ExpiresAt.IsZero() || envelope.ExpiresAt.Location() != time.UTC {
		return invalidDeliveryV1("expires_at must be a nonzero UTC time")
	}
	return nil
}

// Validate checks the immutable request values without performing any host
// authorization or comparing expiry to the current wall clock.
func (request DeliveryV1Request) Validate() error {
	if request.Version != DeliveryV1ProtocolVersion {
		return invalidDeliveryV1("unsupported protocol version")
	}
	for _, field := range []struct{ name, value string }{
		{"delegation_id", request.DelegationID},
		{"caller_id", request.CallerID},
		{"project_id", request.ProjectID},
		{"job_id", request.JobID},
		{"task_id", request.TaskID},
	} {
		if !deliveryV1ValidID(field.value) {
			return invalidDeliveryV1("%s is required and must be a bounded identifier", field.name)
		}
	}
	if request.PlanRevision < 1 || !deliveryV1ValidDigest(request.PlanDigest) {
		return invalidDeliveryV1("plan_revision and lowercase SHA-256 plan_digest are required")
	}
	return request.Spec.Validate()
}

// Validate checks observation consistency and child graph semantics against
// the frozen request. It does not authenticate the producer or prove a landing.
func (observation DeliveryV1Observation) Validate(request DeliveryV1Request) error {
	if err := request.Validate(); err != nil {
		return invalidDeliveryV1("invalid request binding: %v", err)
	}
	digest, err := request.Digest()
	if err != nil {
		return err
	}
	if observation.Version != DeliveryV1ProtocolVersion || observation.DelegationID != request.DelegationID || observation.RequestDigest != digest {
		return invalidDeliveryV1("observation protocol, delegation, or request digest mismatch")
	}
	if !deliveryV1ValidID(observation.LifecycleID) {
		return invalidDeliveryV1("lifecycle_id is required and must be bounded")
	}
	if observation.Sequence == 0 {
		return invalidDeliveryV1("sequence must be positive")
	}
	switch observation.State {
	case "admitted", "running", "changes_requested", "landed", "failed", "paused", "canceled", "unknown":
	default:
		return invalidDeliveryV1("unsupported observation state")
	}
	if observation.Reason != "" && !deliveryV1ValidText(observation.Reason, deliveryV1MaxReasonLength) {
		return invalidDeliveryV1("reason exceeds bounds or contains control characters")
	}
	if err := deliveryV1ValidateAccounting(observation.Accounting); err != nil {
		return err
	}
	if observation.Accounting.SettlementID != "" && !deliveryV1ValidID(observation.Accounting.SettlementID) {
		return invalidDeliveryV1("settlement_id is invalid")
	}
	if len(observation.Children) > deliveryV1MaxChildren {
		return invalidDeliveryV1("too many child tasks")
	}
	if err := deliveryV1ValidateChildren(observation.Children, request.Spec.Repository); err != nil {
		return err
	}
	if observation.State == "landed" {
		if observation.Landed == nil {
			return invalidDeliveryV1("landed observation requires a receipt")
		}
		if err := deliveryV1ValidateLanded(*observation.Landed, request); err != nil {
			return err
		}
	} else if observation.Landed != nil {
		return invalidDeliveryV1("only landed observations may carry a landed receipt")
	}
	return nil
}

func deliveryV1ValidateAccounting(accounting DeliveryV1Accounting) error {
	if accounting.SpentCents < 0 || accounting.ReservedCents < 0 || accounting.UnknownCents < 0 {
		return invalidDeliveryV1("accounting values must be nonnegative")
	}
	maxInt64 := int64(1<<63 - 1)
	if accounting.SpentCents > maxInt64-accounting.ReservedCents {
		return invalidDeliveryV1("accounting sum overflows int64")
	}
	sum := accounting.SpentCents + accounting.ReservedCents
	if sum > maxInt64-accounting.UnknownCents {
		return invalidDeliveryV1("accounting sum overflows int64")
	}
	if sum+accounting.UnknownCents != 0 {
		return invalidDeliveryV1("subscription-only v1 accounting must remain zero-cost")
	}
	return nil
}

func deliveryV1ValidateChildren(children []DeliveryV1ChildTask, repository string) error {
	byID := make(map[string]DeliveryV1ChildTask, len(children))
	for _, child := range children {
		if !deliveryV1ValidID(child.ID) {
			return invalidDeliveryV1("child task ID is required and bounded")
		}
		if _, exists := byID[child.ID]; exists {
			return invalidDeliveryV1("duplicate child task ID %q", child.ID)
		}
		switch child.Kind {
		case "author", "review", "fix", "re_review", "delivery":
		default:
			return invalidDeliveryV1("child %q has unsupported kind", child.ID)
		}
		switch child.State {
		case "pending", "ready", "running", "completed", "blocked", "canceled":
		default:
			return invalidDeliveryV1("child %q has unsupported state", child.ID)
		}
		if len(child.DependsOn) > deliveryV1MaxDependencies || len(child.FindingIDs) > deliveryV1MaxFindings {
			return invalidDeliveryV1("child %q has too many dependencies or findings", child.ID)
		}
		if child.PRURL != "" && (!deliveryV1ValidText(child.PRURL, deliveryV1MaxTextLength) || !deliveryV1CanonicalPullURL(child.PRURL)) {
			return invalidDeliveryV1("child %q has invalid PR URL", child.ID)
		}
		if child.HeadCommit != "" && !deliveryV1ValidObjectID(child.HeadCommit) {
			return invalidDeliveryV1("child %q has invalid head commit", child.ID)
		}
		needsPR := child.Kind == "review" || child.Kind == "fix" || child.Kind == "re_review" || child.Kind == "delivery"
		needsHead := child.Kind == "review" || child.Kind == "re_review"
		if needsHead && (child.PRURL != "" || child.HeadCommit != "") && (child.PRURL == "" || child.HeadCommit == "") {
			return invalidDeliveryV1("child %q has a partial PR/head binding", child.ID)
		}
		requireBinding := child.State != "pending"
		if child.Kind == "review" || child.Kind == "re_review" {
			requireBinding = child.State == "ready" || child.State == "running" || child.State == "completed"
		}
		if (child.Kind == "author" || child.Kind == "fix") && child.State == "completed" {
			requireBinding = true
			needsPR, needsHead = true, true
		}
		if requireBinding && (needsPR || needsHead) {
			if needsPR && child.PRURL == "" || needsHead && child.HeadCommit == "" {
				return invalidDeliveryV1("child %q requires complete PR/head handoff", child.ID)
			}
		}
		if child.PRURL != "" && !deliveryV1PullBelongsToRepository(child.PRURL, repository) {
			return invalidDeliveryV1("child %q PR URL does not belong to request repository", child.ID)
		}
		if child.Kind == "fix" && len(child.FindingIDs) == 0 {
			return invalidDeliveryV1("fix child %q must bind at least one finding", child.ID)
		}
		byID[child.ID] = child
	}

	for _, child := range children {
		seen := make(map[string]struct{}, len(child.DependsOn))
		for _, dependency := range child.DependsOn {
			if dependency == child.ID {
				return invalidDeliveryV1("child %q depends on itself", child.ID)
			}
			if _, exists := seen[dependency]; exists {
				return invalidDeliveryV1("child %q repeats dependency %q", child.ID, dependency)
			}
			seen[dependency] = struct{}{}
			if _, exists := byID[dependency]; !exists {
				return invalidDeliveryV1("child %q depends on unknown task %q", child.ID, dependency)
			}
		}
		for _, findingID := range child.FindingIDs {
			if !deliveryV1ValidID(findingID) {
				return invalidDeliveryV1("child %q has invalid finding ID", child.ID)
			}
		}
	}
	marks := make(map[string]uint8, len(children))
	var visit func(string) bool
	visit = func(taskID string) bool {
		if marks[taskID] == 1 {
			return false
		}
		if marks[taskID] == 2 {
			return true
		}
		marks[taskID] = 1
		for _, dependency := range byID[taskID].DependsOn {
			if !visit(dependency) {
				return false
			}
		}
		marks[taskID] = 2
		return true
	}
	for _, child := range children {
		if !visit(child.ID) {
			return invalidDeliveryV1("child task dependencies contain a cycle")
		}
	}
	return nil
}

func deliveryV1ValidateLanded(landed DeliveryV1LandedReceipt, request DeliveryV1Request) error {
	if landed.Repository != request.Spec.Repository || landed.TargetBranch != request.Spec.TargetBranch || landed.PolicyRevision != request.Spec.PolicyRevision {
		return invalidDeliveryV1("landed receipt repository, target, or policy does not match request")
	}
	if !deliveryV1CanonicalPullURL(landed.PRURL) || !deliveryV1ValidObjectID(landed.ReviewedHead) || !deliveryV1ValidObjectID(landed.ReviewedBase) || !deliveryV1ValidObjectID(landed.LandedCommit) || !deliveryV1ValidDigest(landed.SourceDigest) {
		return invalidDeliveryV1("landed receipt has invalid PR, commit, or source digest binding")
	}
	if !deliveryV1ValidText(landed.Reviewer, deliveryV1MaxIDLength) || !deliveryV1ValidText(landed.Author, deliveryV1MaxIDLength) || !deliveryV1ValidText(landed.Verifier, deliveryV1MaxIDLength) {
		return invalidDeliveryV1("landed receipt reviewer, author, and verifier are required")
	}
	if landed.Reviewer == landed.Author {
		return invalidDeliveryV1("landed receipt reviewer must be independent of author")
	}
	if landed.VerifiedAt.IsZero() || landed.VerifiedAt.Location() != time.UTC {
		return invalidDeliveryV1("landed receipt verified_at must be nonzero UTC")
	}
	if !deliveryV1PullBelongsToRepository(landed.PRURL, landed.Repository) {
		return invalidDeliveryV1("landed receipt PR URL does not belong to repository")
	}
	return nil
}

func validateDeliveryV1Document(data []byte, shape deliveryV1JSONShape) error {
	if len(data) > DeliveryV1MaxJSONBytes {
		return ErrDeliveryV1Oversize
	}
	if !utf8.Valid(data) {
		return malformedDeliveryV1("document is not valid UTF-8")
	}
	// encoding/json replaces unpaired UTF-16 escapes with U+FFFD. Reject them
	// before tokenization so distinct malformed byte strings cannot normalize
	// silently into the same protocol value. Literal U+FFFD remains valid UTF-8.
	if !deliveryV1ValidUnicodeEscapes(data) {
		return malformedDeliveryV1("document contains an invalid Unicode escape")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := walkDeliveryV1JSON(decoder, shape, "$"); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return malformedDeliveryV1("document contains a trailing JSON value")
		}
		return malformedDeliveryV1("invalid trailing data: %v", err)
	}
	return nil
}

func deliveryV1ValidUnicodeEscapes(data []byte) bool {
	inString := false
	for index := 0; index < len(data); {
		current := data[index]
		if !inString {
			if current == '"' {
				inString = true
			}
			index++
			continue
		}
		switch current {
		case '"':
			inString = false
			index++
		case '\\':
			if index+1 >= len(data) {
				return false
			}
			if data[index+1] != 'u' {
				index += 2
				continue
			}
			codeUnit, ok := deliveryV1HexCodeUnit(data, index+2)
			if !ok {
				return false
			}
			switch {
			case codeUnit >= 0xd800 && codeUnit <= 0xdbff:
				if index+12 > len(data) || data[index+6] != '\\' || data[index+7] != 'u' {
					return false
				}
				low, ok := deliveryV1HexCodeUnit(data, index+8)
				if !ok || low < 0xdc00 || low > 0xdfff {
					return false
				}
				index += 12
			case codeUnit >= 0xdc00 && codeUnit <= 0xdfff:
				return false
			default:
				index += 6
			}
		default:
			index++
		}
	}
	return !inString
}

func deliveryV1HexCodeUnit(data []byte, start int) (uint16, bool) {
	if start+4 > len(data) {
		return 0, false
	}
	var value uint16
	for _, digit := range data[start : start+4] {
		value <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			value |= uint16(digit - '0')
		case digit >= 'a' && digit <= 'f':
			value |= uint16(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			value |= uint16(digit-'A') + 10
		default:
			return 0, false
		}
	}
	return value, true
}

type deliveryV1JSONKind uint8

const (
	deliveryV1JSONString deliveryV1JSONKind = iota
	deliveryV1JSONTimestamp
	deliveryV1JSONInt
	deliveryV1JSONUint
	deliveryV1JSONArray
	deliveryV1JSONObject
)

type deliveryV1JSONShape struct {
	kind     deliveryV1JSONKind
	object   deliveryV1ObjectKind
	element  *deliveryV1JSONShape
	maxItems int
	nullable bool
}

type deliveryV1ObjectKind uint8

const (
	deliveryV1RequestObject deliveryV1ObjectKind = iota
	deliveryV1SpecObject
	deliveryV1EnvelopeObject
	deliveryV1ObservationObject
	deliveryV1ChildObject
	deliveryV1AccountingObject
	deliveryV1LandedObject
)

func deliveryV1StringShape() deliveryV1JSONShape {
	return deliveryV1JSONShape{kind: deliveryV1JSONString}
}

func deliveryV1TimestampShape() deliveryV1JSONShape {
	return deliveryV1JSONShape{kind: deliveryV1JSONTimestamp}
}

func deliveryV1IntShape() deliveryV1JSONShape {
	return deliveryV1JSONShape{kind: deliveryV1JSONInt}
}

func deliveryV1UintShape() deliveryV1JSONShape {
	return deliveryV1JSONShape{kind: deliveryV1JSONUint}
}

func deliveryV1ArrayShape(nullable bool, maxItems int) deliveryV1JSONShape {
	return deliveryV1JSONShape{
		kind: deliveryV1JSONArray, element: &deliveryV1JSONShape{kind: deliveryV1JSONString},
		maxItems: maxItems, nullable: nullable,
	}
}

func deliveryV1ObjectShape(object deliveryV1ObjectKind, nullable bool) deliveryV1JSONShape {
	return deliveryV1JSONShape{kind: deliveryV1JSONObject, object: object, nullable: nullable}
}

func deliveryV1RequestShape() deliveryV1JSONShape {
	return deliveryV1ObjectShape(deliveryV1RequestObject, false)
}

func deliveryV1ObservationShape() deliveryV1JSONShape {
	return deliveryV1ObjectShape(deliveryV1ObservationObject, false)
}

func deliveryV1Fields(object deliveryV1ObjectKind) (map[string]deliveryV1JSONShape, string, bool) {
	switch object {
	case deliveryV1RequestObject:
		return map[string]deliveryV1JSONShape{
			"version": deliveryV1StringShape(), "delegation_id": deliveryV1StringShape(),
			"caller_id": deliveryV1StringShape(), "project_id": deliveryV1StringShape(),
			"job_id": deliveryV1StringShape(), "task_id": deliveryV1StringShape(),
			"plan_revision": deliveryV1IntShape(), "plan_digest": deliveryV1StringShape(),
			"spec": deliveryV1ObjectShape(deliveryV1SpecObject, false),
		}, "", false
	case deliveryV1SpecObject:
		return map[string]deliveryV1JSONShape{
			"repository": deliveryV1StringShape(), "target_branch": deliveryV1StringShape(),
			"source_commit": deliveryV1StringShape(), "acceptance": deliveryV1ArrayShape(true, deliveryV1MaxAcceptance),
			"policy_revision": deliveryV1StringShape(), "profile_revision": deliveryV1StringShape(),
			"execution_mode": deliveryV1StringShape(), "envelope": deliveryV1ObjectShape(deliveryV1EnvelopeObject, false),
		}, "", false
	case deliveryV1EnvelopeObject:
		return map[string]deliveryV1JSONShape{
			"max_attempts": deliveryV1IntShape(), "max_concurrent": deliveryV1IntShape(),
			"max_cost_cents": deliveryV1IntShape(), "expires_at": deliveryV1TimestampShape(),
		}, "", false
	case deliveryV1ObservationObject:
		return map[string]deliveryV1JSONShape{
			"version": deliveryV1StringShape(), "delegation_id": deliveryV1StringShape(),
			"request_digest": deliveryV1StringShape(), "lifecycle_id": deliveryV1StringShape(),
			"sequence": deliveryV1UintShape(), "state": deliveryV1StringShape(),
			"children":   {kind: deliveryV1JSONArray, element: ptrDeliveryV1Shape(deliveryV1ObjectShape(deliveryV1ChildObject, false)), maxItems: deliveryV1MaxChildren, nullable: true},
			"accounting": deliveryV1ObjectShape(deliveryV1AccountingObject, false),
			"landed":     deliveryV1ObjectShape(deliveryV1LandedObject, true),
			"reason":     deliveryV1StringShape(),
		}, "landed", true
	case deliveryV1ChildObject:
		return map[string]deliveryV1JSONShape{
			"id": deliveryV1StringShape(), "kind": deliveryV1StringShape(), "state": deliveryV1StringShape(),
			"depends_on": deliveryV1ArrayShape(true, deliveryV1MaxDependencies), "finding_ids": deliveryV1ArrayShape(true, deliveryV1MaxFindings),
			"pr_url": deliveryV1StringShape(), "head_commit": deliveryV1StringShape(),
		}, "", false
	case deliveryV1AccountingObject:
		return map[string]deliveryV1JSONShape{
			"spent_cents": deliveryV1IntShape(), "reserved_cents": deliveryV1IntShape(),
			"unknown_cents": deliveryV1IntShape(), "settlement_id": deliveryV1StringShape(),
		}, "", false
	case deliveryV1LandedObject:
		return map[string]deliveryV1JSONShape{
			"repository": deliveryV1StringShape(), "target_branch": deliveryV1StringShape(),
			"pr_url": deliveryV1StringShape(), "reviewed_head": deliveryV1StringShape(),
			"reviewed_base": deliveryV1StringShape(), "policy_revision": deliveryV1StringShape(),
			"landed_commit": deliveryV1StringShape(), "source_digest": deliveryV1StringShape(),
			"reviewer": deliveryV1StringShape(), "author": deliveryV1StringShape(),
			"verifier": deliveryV1StringShape(), "verified_at": deliveryV1TimestampShape(),
		}, "", false
	default:
		return nil, "", false
	}
}

func ptrDeliveryV1Shape(shape deliveryV1JSONShape) *deliveryV1JSONShape {
	return &shape
}

func walkDeliveryV1JSON(decoder *json.Decoder, shape deliveryV1JSONShape, path string) error {
	token, err := decoder.Token()
	if err != nil {
		return malformedDeliveryV1("%s: invalid JSON value: %v", path, err)
	}
	if token == nil && shape.nullable {
		return nil
	}
	switch shape.kind {
	case deliveryV1JSONString:
		if _, ok := token.(string); !ok {
			return malformedDeliveryV1("%s must be a string", path)
		}
	case deliveryV1JSONTimestamp:
		value, ok := token.(string)
		if !ok || !strings.HasSuffix(value, "Z") {
			return malformedDeliveryV1("%s must be a UTC RFC3339Nano string ending in Z", path)
		}
		if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
			return malformedDeliveryV1("%s has invalid timestamp syntax: %v", path, err)
		}
	case deliveryV1JSONInt, deliveryV1JSONUint:
		if _, ok := token.(json.Number); !ok {
			return malformedDeliveryV1("%s must be an integer", path)
		}
	case deliveryV1JSONArray:
		delim, ok := token.(json.Delim)
		if !ok || delim != '[' {
			return malformedDeliveryV1("%s must be an array or null", path)
		}
		for index := 0; decoder.More(); index++ {
			if shape.maxItems > 0 && index >= shape.maxItems {
				return invalidDeliveryV1("%s has more than %d entries", path, shape.maxItems)
			}
			if err := walkDeliveryV1JSON(decoder, *shape.element, fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
		if err := deliveryV1ExpectDelimiter(decoder, ']'); err != nil {
			return malformedDeliveryV1("%s: %v", path, err)
		}
	case deliveryV1JSONObject:
		delim, ok := token.(json.Delim)
		if !ok || delim != '{' {
			return malformedDeliveryV1("%s must be an object", path)
		}
		fields, optionalField, objectOptional := deliveryV1Fields(shape.object)
		if fields == nil {
			return malformedDeliveryV1("%s has an unsupported object shape", path)
		}
		seen := make(map[string]struct{}, len(fields))
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return malformedDeliveryV1("%s: read object member: %v", path, err)
			}
			key, ok := keyToken.(string)
			if !ok {
				return malformedDeliveryV1("%s has a non-string object key", path)
			}
			fieldShape, exists := fields[key]
			if !exists {
				return malformedDeliveryV1("%s has unknown or non-exact member %q", path, key)
			}
			if _, duplicate := seen[key]; duplicate {
				return malformedDeliveryV1("%s repeats member %q", path, key)
			}
			seen[key] = struct{}{}
			if err := walkDeliveryV1JSON(decoder, fieldShape, path+"."+key); err != nil {
				return err
			}
		}
		if err := deliveryV1ExpectDelimiter(decoder, '}'); err != nil {
			return malformedDeliveryV1("%s: %v", path, err)
		}
		for field := range fields {
			if objectOptional && field == optionalField {
				continue
			}
			if _, exists := seen[field]; !exists {
				return malformedDeliveryV1("%s is missing member %q", path, field)
			}
		}
	default:
		return malformedDeliveryV1("%s has unsupported value shape", path)
	}
	return nil
}

func deliveryV1ExpectDelimiter(decoder *json.Decoder, expected json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != expected {
		return fmt.Errorf("expected delimiter %q", expected)
	}
	return nil
}

func deliveryV1ValidID(value string) bool {
	return deliveryV1ValidText(value, deliveryV1MaxIDLength) && value != "." && value != ".." && !strings.ContainsAny(value, " /\\")
}

func deliveryV1ValidText(value string, maxLength int) bool {
	if value == "" || len(value) > maxLength || !utf8.ValidString(value) || value != strings.TrimSpace(value) {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func deliveryV1ValidObjectID(value string) bool {
	return deliveryV1ObjectIDPattern.MatchString(value)
}

func deliveryV1ValidDigest(value string) bool {
	return deliveryV1DigestPattern.MatchString(value)
}

func deliveryV1ValidBranch(value string) bool {
	if !deliveryV1BranchPattern.MatchString(value) || strings.Contains(value, "..") || strings.Contains(value, "//") || strings.Contains(value, "@{") || strings.HasSuffix(value, ".") || strings.HasSuffix(value, "/") || strings.Contains(value, "/.") || strings.Contains(value, "./") {
		return false
	}
	return !strings.ContainsAny(value, " ~^:?*[\\")
}

func deliveryV1CanonicalRepository(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Opaque != "" || parsed.RawPath != "" || parsed.Path == "" || strings.HasSuffix(parsed.Path, "/") {
		return false
	}
	if parsed.Host != strings.ToLower(parsed.Host) || parsed.EscapedPath() != parsed.Path || strings.Contains(parsed.Path, "//") {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(parsed.Path, "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || strings.HasSuffix(parts[1], ".git") {
		return false
	}
	return deliveryV1ValidURLSegment(parts[0]) && deliveryV1ValidURLSegment(parts[1])
}

func deliveryV1CanonicalPullURL(raw string) bool {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" || parsed.Host != strings.ToLower(parsed.Host) {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(parsed.Path, "/"), "/")
	return len(parts) == 4 && deliveryV1ValidURLSegment(parts[0]) && deliveryV1ValidURLSegment(parts[1]) && parts[2] == "pull" && deliveryV1PositiveDecimal(parts[3])
}

func deliveryV1PullBelongsToRepository(pullURL, repository string) bool {
	pull, _ := url.Parse(pullURL)
	repo, _ := url.Parse(repository)
	return pull.Host == repo.Host && strings.HasPrefix(pull.Path, repo.Path+"/pull/")
}

func deliveryV1ValidURLSegment(segment string) bool {
	if segment == "" || segment == "." || segment == ".." {
		return false
	}
	for _, character := range segment {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || strings.ContainsRune("-_", character) {
			continue
		}
		return false
	}
	return true
}

func deliveryV1PositiveDecimal(value string) bool {
	if value == "" || len(value) > deliveryV1MaxPullRequestDigits || value[0] == '0' {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func malformedDeliveryV1(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrDeliveryV1Malformed, fmt.Sprintf(format, args...))
}

func invalidDeliveryV1(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrDeliveryV1Invalid, fmt.Sprintf(format, args...))
}
