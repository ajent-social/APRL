-- Durable caller/delegation identity and the transactionally admitted lifecycle.
CREATE TABLE plan_delegations (
    caller_id TEXT NOT NULL CHECK (length(caller_id) BETWEEN 1 AND 128),
    delegation_id TEXT NOT NULL CHECK (length(delegation_id) BETWEEN 1 AND 128),
    request_bytes BYTEA NOT NULL CHECK (octet_length(request_bytes) BETWEEN 1 AND 1048576),
    request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    request_scope JSONB NOT NULL CHECK (jsonb_typeof(request_scope) = 'object'),
    project_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    plan_revision BIGINT NOT NULL CHECK (plan_revision > 0),
    plan_digest TEXT NOT NULL,
    repository TEXT NOT NULL,
    target_branch TEXT NOT NULL,
    source_commit TEXT NOT NULL,
    acceptance JSONB NOT NULL CHECK (jsonb_typeof(acceptance) = 'array'),
    policy_revision TEXT NOT NULL,
    profile_revision TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    budget_envelope JSONB NOT NULL CHECK (jsonb_typeof(budget_envelope) = 'object'),
    max_concurrent INTEGER NOT NULL CHECK (max_concurrent BETWEEN 1 AND 6),
    max_attempts BIGINT NOT NULL CHECK (max_attempts > 0),
    expires_at_exact TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('intent', 'denied', 'admitted', 'cancelled')),
    authorization JSONB CHECK (authorization IS NULL OR jsonb_typeof(authorization) = 'object'),
    lifecycle_id UUID UNIQUE REFERENCES plan_lifecycles(id),
    lifecycle_revision BIGINT CHECK (lifecycle_revision IS NULL OR lifecycle_revision >= 0),
    observation_sequence BIGINT NOT NULL CHECK (observation_sequence > 0),
    cancelled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (caller_id, delegation_id),
    CONSTRAINT plan_delegations_lifecycle_status CHECK (
        (status = 'admitted' AND lifecycle_id IS NOT NULL AND lifecycle_revision IS NOT NULL)
        OR (status IN ('intent', 'denied', 'cancelled') AND lifecycle_id IS NULL AND lifecycle_revision IS NULL)
    ),
    CONSTRAINT plan_delegations_cancelled_status CHECK (status <> 'cancelled' OR cancelled=TRUE)
);

CREATE INDEX plan_delegations_lifecycle_id_idx ON plan_delegations (lifecycle_id)
    WHERE lifecycle_id IS NOT NULL;
