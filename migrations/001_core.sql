-- APRL core schema, migration version 001_core. Applied atomically by storage.Migrate.
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE org_budgets (
    org_id VARCHAR(100) PRIMARY KEY,
    rolling_limit_micro_usd BIGINT NOT NULL DEFAULT 100000000
        CHECK (rolling_limit_micro_usd >= 0),
    emergency_mode BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    org_id VARCHAR(100) NOT NULL,
    repo_full_name VARCHAR(255) NOT NULL CHECK (btrim(repo_full_name) <> ''),
    source_key TEXT NOT NULL CHECK (btrim(source_key) <> ''),
    owner_id VARCHAR(100) NOT NULL CHECK (btrim(owner_id) <> ''),
    state VARCHAR(30) NOT NULL DEFAULT 'AUTHORING'
        CHECK (state IN ('AUTHORING', 'WAITING_CI', 'IN_REVIEW',
            'CHANGES_REQUESTED', 'FIXING', 'READY_TO_MERGE',
            'PAUSED', 'ESCALATED', 'MERGED', 'CLOSED')),
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    cycle_count INT NOT NULL DEFAULT 0 CHECK (cycle_count >= 0),
    max_review_cycles INT NOT NULL DEFAULT 3 CHECK (max_review_cycles > 0),
    budget_limit_micro_usd BIGINT NOT NULL DEFAULT 5000000
        CHECK (budget_limit_micro_usd >= 0),
    spent_micro_usd BIGINT NOT NULL DEFAULT 0 CHECK (spent_micro_usd >= 0),
    reserved_micro_usd BIGINT NOT NULL DEFAULT 0 CHECK (reserved_micro_usd >= 0),
    policy_version TEXT NOT NULL CHECK (btrim(policy_version) <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (repo_full_name, source_key)
);

COMMENT ON COLUMN tasks.budget_limit_micro_usd IS 'Task ceiling in integer micro-USD (1 USD = 1000000).';
COMMENT ON COLUMN tasks.spent_micro_usd IS 'Settled task spend in integer micro-USD (1 USD = 1000000).';
COMMENT ON COLUMN tasks.reserved_micro_usd IS 'Outstanding task reservations in integer micro-USD (1 USD = 1000000).';
COMMENT ON COLUMN org_budgets.rolling_limit_micro_usd IS 'Rolling organization ceiling in integer micro-USD (1 USD = 1000000).';

CREATE TABLE prs (
    id BIGSERIAL PRIMARY KEY,
    task_id UUID NOT NULL UNIQUE REFERENCES tasks(id),
    repo_full_name VARCHAR(255) NOT NULL CHECK (btrim(repo_full_name) <> ''),
    pr_number INT NOT NULL CHECK (pr_number > 0),
    head_sha VARCHAR(40) NOT NULL CHECK (head_sha ~ '^[0-9a-f]{40}$'),
    base_ref TEXT NOT NULL CHECK (btrim(base_ref) <> ''),
    base_sha VARCHAR(40) NOT NULL CHECK (base_sha ~ '^[0-9a-f]{40}$'),
    integration_sha VARCHAR(40) CHECK (integration_sha IS NULL OR integration_sha ~ '^[0-9a-f]{40}$'),
    approved_head_sha VARCHAR(40) CHECK (approved_head_sha IS NULL OR approved_head_sha ~ '^[0-9a-f]{40}$'),
    approved_base_sha VARCHAR(40) CHECK (approved_base_sha IS NULL OR approved_base_sha ~ '^[0-9a-f]{40}$'),
    human_approval_id TEXT,
    ci_status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
        CHECK (ci_status IN ('PENDING', 'SUCCESS', 'FAILURE', 'ERROR', 'CANCELLED')),
    ci_deadline_at TIMESTAMPTZ,
    is_draft BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (repo_full_name, pr_number),
    CHECK ((approved_head_sha IS NULL) = (approved_base_sha IS NULL))
);

CREATE TABLE webhook_deliveries (
    delivery_id VARCHAR(100) PRIMARY KEY CHECK (btrim(delivery_id) <> ''),
    event_type TEXT NOT NULL CHECK (btrim(event_type) <> ''),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    received_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMPTZ,
    disposition TEXT,
    routing_attempts INT NOT NULL DEFAULT 0 CHECK (routing_attempts >= 0),
    CHECK (processed_at IS NULL OR processed_at >= received_at)
);

CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES tasks(id),
    pr_id BIGINT REFERENCES prs(id),
    source_delivery_id VARCHAR(100) REFERENCES webhook_deliveries(delivery_id),
    logical_key TEXT NOT NULL UNIQUE CHECK (btrim(logical_key) <> ''),
    operation_type TEXT NOT NULL CHECK (btrim(operation_type) <> ''),
    generation BIGINT NOT NULL CHECK (generation >= 0),
    expected_head_sha VARCHAR(40),
    expected_base_sha VARCHAR(40),
    remediation_attempt INT CHECK (remediation_attempt IS NULL OR remediation_attempt > 0),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'LEASED', 'COMPLETED', 'FAILED', 'CANCELLED', 'REJECTED')),
    lease_token UUID UNIQUE,
    lease_expires_at TIMESTAMPTZ,
    dispatch_attempts INT NOT NULL DEFAULT 0 CHECK (dispatch_attempts >= 0),
    UNIQUE (id, task_id),
    CHECK ((expected_head_sha IS NULL) = (expected_base_sha IS NULL)),
    CHECK (expected_head_sha IS NULL OR expected_head_sha ~ '^[0-9a-f]{40}$'),
    CHECK (expected_base_sha IS NULL OR expected_base_sha ~ '^[0-9a-f]{40}$'),
    CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
    CHECK (status <> 'LEASED' OR lease_token IS NOT NULL)
);

CREATE INDEX jobs_task_generation_status_idx ON jobs (task_id, generation, status);
CREATE INDEX jobs_source_delivery_idx ON jobs (source_delivery_id) WHERE source_delivery_id IS NOT NULL;

CREATE TABLE outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES tasks(id),
    job_id UUID REFERENCES jobs(id),
    kind TEXT NOT NULL CHECK (kind IN ('DISPATCH', 'CANCEL', 'LABEL_SYNC', 'NOTIFY')),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    attempt_count INT NOT NULL DEFAULT 0 CHECK (attempt_count >= 0)
);

CREATE INDEX outbox_due_idx ON outbox (next_attempt_at, created_at) WHERE published_at IS NULL;

CREATE TABLE agent_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL,
    job_id UUID NOT NULL,
    attempt_number INT NOT NULL CHECK (attempt_number > 0),
    agent_type VARCHAR(20) NOT NULL CHECK (btrim(agent_type) <> ''),
    codex_prompt_hash VARCHAR(64) NOT NULL CHECK (codex_prompt_hash ~ '^[0-9a-f]{64}$'),
    generation BIGINT NOT NULL CHECK (generation >= 0),
    lease_token UUID NOT NULL CHECK (lease_token <> '00000000-0000-0000-0000-000000000000'),
    expected_head_sha VARCHAR(40),
    expected_base_sha VARCHAR(40),
    expected_integration_sha VARCHAR(40),
    supervisor_identity TEXT NOT NULL CHECK (btrim(supervisor_identity) <> ''),
    supervisor_credential_id TEXT NOT NULL CHECK (btrim(supervisor_credential_id) <> ''),
    raw_output TEXT,
    execution_status VARCHAR(30) NOT NULL
        CHECK (execution_status IN ('RUNNING', 'SUCCESS', 'FAILED', 'TERMINATED')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    process_id BIGINT,
    process_group_id BIGINT,
    workspace_id UUID UNIQUE,
    process_started_at TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    execution_deadline_at TIMESTAMPTZ,
    UNIQUE (job_id, attempt_number),
    UNIQUE (id, task_id, job_id),
    UNIQUE (id, task_id),
    FOREIGN KEY (job_id, task_id) REFERENCES jobs(id, task_id),
    CHECK ((expected_head_sha IS NULL) = (expected_base_sha IS NULL)),
    CHECK (expected_head_sha IS NULL OR expected_head_sha ~ '^[0-9a-f]{40}$'),
    CHECK (expected_base_sha IS NULL OR expected_base_sha ~ '^[0-9a-f]{40}$'),
    CHECK (expected_integration_sha IS NULL OR expected_integration_sha ~ '^[0-9a-f]{40}$'),
    CHECK (expected_integration_sha IS NULL OR expected_head_sha IS NOT NULL),
    CHECK ((execution_status = 'RUNNING') = (finished_at IS NULL)),
    CHECK (finished_at IS NULL OR finished_at >= started_at),
    CHECK ((process_id IS NULL AND process_group_id IS NULL AND workspace_id IS NULL
            AND process_started_at IS NULL AND last_heartbeat_at IS NULL AND execution_deadline_at IS NULL)
           OR (process_id IS NOT NULL AND process_id > 0 AND process_group_id IS NOT NULL AND process_group_id > 0
               AND workspace_id IS NOT NULL AND process_started_at IS NOT NULL AND last_heartbeat_at IS NOT NULL
               AND execution_deadline_at IS NOT NULL AND process_started_at >= started_at
               AND last_heartbeat_at >= process_started_at AND execution_deadline_at > process_started_at))
);

CREATE FUNCTION reject_agent_run_admission_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.task_id, NEW.job_id, NEW.attempt_number, NEW.agent_type, NEW.codex_prompt_hash,
           NEW.generation, NEW.lease_token,
           NEW.expected_head_sha, NEW.expected_base_sha, NEW.expected_integration_sha,
           NEW.supervisor_identity, NEW.supervisor_credential_id)
       IS DISTINCT FROM
       ROW(OLD.task_id, OLD.job_id, OLD.attempt_number, OLD.agent_type, OLD.codex_prompt_hash,
           OLD.generation, OLD.lease_token,
           OLD.expected_head_sha, OLD.expected_base_sha, OLD.expected_integration_sha,
           OLD.supervisor_identity, OLD.supervisor_credential_id) THEN
        RAISE EXCEPTION 'agent run admission tuple is immutable'
            USING ERRCODE = '23514', CONSTRAINT = 'agent_runs_admission_immutable';
    END IF;
    IF OLD.process_id IS NOT NULL AND
       ROW(NEW.process_id, NEW.process_group_id, NEW.workspace_id, NEW.process_started_at, NEW.execution_deadline_at)
       IS DISTINCT FROM
       ROW(OLD.process_id, OLD.process_group_id, OLD.workspace_id, OLD.process_started_at, OLD.execution_deadline_at) THEN
        RAISE EXCEPTION 'run process ownership is immutable'
            USING ERRCODE = '23514', CONSTRAINT = 'agent_runs_process_immutable';
    END IF;
    IF OLD.process_id IS NULL AND NEW.process_id IS NOT NULL AND
       (OLD.execution_status <> 'RUNNING' OR NEW.execution_status <> 'RUNNING') THEN
        RAISE EXCEPTION 'only a running admission can record process ownership'
            USING ERRCODE = '23514', CONSTRAINT = 'agent_runs_process_immutable';
    END IF;
    IF NEW.last_heartbeat_at < OLD.last_heartbeat_at THEN
        RAISE EXCEPTION 'run heartbeat cannot move backward'
            USING ERRCODE = '23514', CONSTRAINT = 'agent_runs_heartbeat_monotonic';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER agent_runs_admission_immutable
BEFORE UPDATE ON agent_runs
FOR EACH ROW EXECUTE FUNCTION reject_agent_run_admission_change();

CREATE TABLE budget_reservations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES tasks(id),
    org_id VARCHAR(100) NOT NULL REFERENCES org_budgets(org_id),
    run_id UUID NOT NULL UNIQUE,
    envelope_micro_usd BIGINT NOT NULL CHECK (envelope_micro_usd > 0),
    remaining_micro_usd BIGINT NOT NULL CHECK (remaining_micro_usd >= 0),
    pricing_version TEXT NOT NULL CHECK (btrim(pricing_version) <> ''),
    admission_envelope JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(admission_envelope) = 'object'),
    status TEXT NOT NULL CHECK (status IN ('RESERVED', 'SETTLING', 'SETTLED', 'UNKNOWN')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    settled_at TIMESTAMPTZ,
    FOREIGN KEY (run_id, task_id) REFERENCES agent_runs(id, task_id),
    CHECK (remaining_micro_usd <= envelope_micro_usd),
    CHECK ((status = 'SETTLED') = (settled_at IS NOT NULL)),
    CHECK (settled_at IS NULL OR settled_at >= created_at)
);

COMMENT ON COLUMN budget_reservations.envelope_micro_usd IS 'Reserved amount in integer micro-USD (1 USD = 1000000).';
COMMENT ON COLUMN budget_reservations.remaining_micro_usd IS 'Unsettled reservation in integer micro-USD (1 USD = 1000000).';
CREATE INDEX budget_reservations_org_status_idx ON budget_reservations (org_id, status, created_at);

CREATE TABLE cost_entries (
    request_id TEXT PRIMARY KEY CHECK (btrim(request_id) <> ''),
    reservation_id UUID NOT NULL REFERENCES budget_reservations(id),
    model TEXT NOT NULL CHECK (btrim(model) <> ''),
    usage JSONB NOT NULL CHECK (jsonb_typeof(usage) = 'object'),
    cost_micro_usd BIGINT NOT NULL CHECK (cost_micro_usd >= 0),
    charged_at TIMESTAMPTZ NOT NULL
);

COMMENT ON COLUMN cost_entries.cost_micro_usd IS 'Charged amount in integer micro-USD (1 USD = 1000000).';
CREATE INDEX cost_entries_reservation_idx ON cost_entries (reservation_id, charged_at);

-- Admission ceilings are enforced under organization/task locks. Actual settled
-- charges may exceed those ceilings and must remain faithfully recorded.
CREATE FUNCTION enforce_budget_reservation_identity() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'budget reservations retain billing coverage' USING ERRCODE = '23514';
    END IF;
    IF ROW(NEW.id, NEW.task_id, NEW.org_id, NEW.run_id, NEW.envelope_micro_usd,
           NEW.pricing_version, NEW.admission_envelope, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.id, OLD.task_id, OLD.org_id, OLD.run_id, OLD.envelope_micro_usd,
           OLD.pricing_version, OLD.admission_envelope, OLD.created_at) THEN
        RAISE EXCEPTION 'budget reservation identity is immutable' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER budget_reservations_identity_immutable
    BEFORE UPDATE OR DELETE ON budget_reservations
    FOR EACH ROW EXECUTE FUNCTION enforce_budget_reservation_identity();

CREATE FUNCTION enforce_cost_entry_append_only() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'cost entries are append-only' USING ERRCODE = '23514';
END;
$$;
CREATE TRIGGER cost_entries_append_only
    BEFORE UPDATE OR DELETE ON cost_entries
    FOR EACH ROW EXECUTE FUNCTION enforce_cost_entry_append_only();

CREATE TABLE review_cycles (
    id BIGSERIAL PRIMARY KEY,
    pr_id BIGINT NOT NULL REFERENCES prs(id),
    generation BIGINT NOT NULL CHECK (generation >= 0),
    head_sha VARCHAR(40) NOT NULL CHECK (head_sha ~ '^[0-9a-f]{40}$'),
    base_sha VARCHAR(40) NOT NULL CHECK (base_sha ~ '^[0-9a-f]{40}$'),
    verdict VARCHAR(30) NOT NULL CHECK (verdict IN ('approve', 'request_changes')),
    normalized_version TEXT NOT NULL CHECK (btrim(normalized_version) <> ''),
    blocker_fingerprints JSONB NOT NULL CHECK (jsonb_typeof(blocker_fingerprints) = 'array'),
    overlap_score NUMERIC(3, 2) CHECK (overlap_score IS NULL OR overlap_score BETWEEN 0 AND 1),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (pr_id, generation)
);

CREATE TABLE findings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    review_id BIGINT NOT NULL REFERENCES review_cycles(id),
    severity TEXT NOT NULL CHECK (severity IN ('blocker', 'warning', 'nit')),
    body TEXT NOT NULL CHECK (btrim(body) <> ''),
    anchor JSONB,
    github_thread_id TEXT,
    disposition TEXT NOT NULL DEFAULT 'OPEN'
        CHECK (disposition IN ('OPEN', 'ADDRESSED', 'RESOLVED', 'DISMISSED')),
    resolved_head_sha VARCHAR(40) CHECK (resolved_head_sha IS NULL OR resolved_head_sha ~ '^[0-9a-f]{40}$'),
    CHECK (
        anchor IS NULL OR CASE WHEN jsonb_typeof(anchor) = 'object' THEN
            anchor ?& ARRAY['file', 'line', 'side']
            AND anchor - ARRAY['file', 'line', 'side']::TEXT[] = '{}'::jsonb
            AND btrim(COALESCE(anchor->>'file', '')) <> ''
            AND COALESCE(anchor->>'line', '') ~ '^[1-9][0-9]*$'
            AND anchor->>'side' IN ('LEFT', 'RIGHT')
        ELSE FALSE END
    )
);

CREATE INDEX findings_review_disposition_idx ON findings (review_id, disposition);

CREATE TABLE github_operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID,
    task_id UUID NOT NULL REFERENCES tasks(id),
    generation BIGINT NOT NULL CHECK (generation >= 0),
    operation_type TEXT NOT NULL CHECK (btrim(operation_type) <> ''),
    identity TEXT NOT NULL CHECK (btrim(identity) <> ''),
    expected_head_sha VARCHAR(40),
    expected_base_sha VARCHAR(40),
    request JSONB NOT NULL CHECK (jsonb_typeof(request) = 'object'),
    status TEXT NOT NULL CHECK (status IN ('INTENDED', 'IN_FLIGHT', 'CONFIRMED', 'FAILED', 'UNKNOWN')),
    remote_id TEXT,
    result JSONB CHECK (result IS NULL OR jsonb_typeof(result) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id, task_id) REFERENCES jobs(id, task_id),
    CHECK ((expected_head_sha IS NULL) = (expected_base_sha IS NULL)),
    CHECK (expected_head_sha IS NULL OR expected_head_sha ~ '^[0-9a-f]{40}$'),
    CHECK (expected_base_sha IS NULL OR expected_base_sha ~ '^[0-9a-f]{40}$')
);

CREATE INDEX github_operations_task_created_idx ON github_operations (task_id, created_at);
CREATE INDEX github_operations_unknown_idx ON github_operations (created_at) WHERE status = 'UNKNOWN';

CREATE FUNCTION enforce_github_operation_intent() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'GitHub operation intents are append-only'
            USING ERRCODE = '23514', CONSTRAINT = 'github_operations_intent_immutable';
    END IF;
    IF ROW(NEW.id, NEW.job_id, NEW.task_id, NEW.generation, NEW.operation_type,
           NEW.identity, NEW.expected_head_sha, NEW.expected_base_sha, NEW.request, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.id, OLD.job_id, OLD.task_id, OLD.generation, OLD.operation_type,
           OLD.identity, OLD.expected_head_sha, OLD.expected_base_sha, OLD.request, OLD.created_at) THEN
        RAISE EXCEPTION 'GitHub operation admission intent is immutable'
            USING ERRCODE = '23514', CONSTRAINT = 'github_operations_intent_immutable';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER github_operations_intent_immutable
    BEFORE UPDATE OR DELETE ON github_operations
    FOR EACH ROW EXECUTE FUNCTION enforce_github_operation_intent();

CREATE TABLE escalations (
    id BIGSERIAL PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(id),
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    details TEXT,
    resolved_by TEXT,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK ((resolved_by IS NULL) = (resolved_at IS NULL)),
    CHECK (resolved_at IS NULL OR resolved_at >= created_at)
);

CREATE TABLE control_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_id UUID NOT NULL REFERENCES tasks(id),
    actor_id TEXT NOT NULL CHECK (btrim(actor_id) <> ''),
    action TEXT NOT NULL CHECK (btrim(action) <> ''),
    reason TEXT NOT NULL CHECK (btrim(reason) <> ''),
    previous_generation BIGINT NOT NULL CHECK (previous_generation >= 0),
    new_generation BIGINT NOT NULL CHECK (new_generation >= previous_generation),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (new_generation - previous_generation <= 1)
);

CREATE INDEX control_actions_task_created_idx ON control_actions (task_id, created_at);

-- Durable result receipts keep the original admission tuple for replay checks
-- even after a successful operation increments the task generation.
CREATE TABLE worker_result_receipts (
    operation_id UUID NOT NULL UNIQUE CHECK (operation_id <> '00000000-0000-0000-0000-000000000000'),
    task_id UUID NOT NULL,
    job_id UUID NOT NULL,
    run_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation >= 0),
    lease_token UUID NOT NULL CHECK (lease_token <> '00000000-0000-0000-0000-000000000000'),
    expected_head_sha VARCHAR(40),
    expected_base_sha VARCHAR(40),
    expected_integration_sha VARCHAR(40),
    correlation_id UUID NOT NULL CHECK (correlation_id <> '00000000-0000-0000-0000-000000000000'),
    supervisor_identity TEXT NOT NULL CHECK (btrim(supervisor_identity) <> ''),
    supervisor_credential_id TEXT NOT NULL CHECK (btrim(supervisor_credential_id) <> ''),
    result_status TEXT NOT NULL CHECK (result_status IN ('succeeded', 'failed', 'cancelled')),
    result_payload JSONB NOT NULL CHECK (jsonb_typeof(result_payload) = 'object'),
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (task_id, job_id, operation_id),
    FOREIGN KEY (run_id, task_id, job_id) REFERENCES agent_runs(id, task_id, job_id),
    CHECK ((expected_head_sha IS NULL) = (expected_base_sha IS NULL)),
    CHECK (expected_head_sha IS NULL OR expected_head_sha ~ '^[0-9a-f]{40}$'),
    CHECK (expected_base_sha IS NULL OR expected_base_sha ~ '^[0-9a-f]{40}$'),
    CHECK (expected_integration_sha IS NULL OR expected_integration_sha ~ '^[0-9a-f]{40}$'),
    CHECK (expected_integration_sha IS NULL OR expected_head_sha IS NOT NULL)
);

CREATE FUNCTION reject_result_receipt_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'worker result receipts are immutable'
        USING ERRCODE = '23514', CONSTRAINT = 'worker_result_receipts_immutable';
END;
$$;

CREATE TRIGGER worker_result_receipts_immutable
BEFORE UPDATE OR DELETE ON worker_result_receipts
FOR EACH ROW EXECUTE FUNCTION reject_result_receipt_mutation();

CREATE INDEX worker_result_receipts_run_idx ON worker_result_receipts (run_id, accepted_at);

INSERT INTO schema_migrations (version) VALUES ('001_core')
ON CONFLICT (version) DO NOTHING;
