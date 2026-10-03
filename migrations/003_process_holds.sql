-- A process hold accounts for host resources independently of mutable task/job
-- status. Unknown executions remain charged until trusted host reconciliation.
CREATE TABLE process_holds (
    run_id UUID PRIMARY KEY,
    task_id UUID NOT NULL REFERENCES tasks(id),
    job_id UUID NOT NULL,
    generation BIGINT NOT NULL CHECK (generation >= 0),
    resource_scope TEXT NOT NULL CHECK (btrim(resource_scope) <> '' AND length(resource_scope) <= 128),
    lease_token_sha256 VARCHAR(64) NOT NULL CHECK (lease_token_sha256 ~ '^[0-9a-f]{64}$'),
    workspace TEXT NOT NULL CHECK (btrim(workspace) <> '' AND length(workspace) <= 1024),
    supervisor_identity TEXT NOT NULL CHECK (btrim(supervisor_identity) <> '' AND length(supervisor_identity) <= 256),
    state VARCHAR(10) NOT NULL CHECK (state IN ('RESERVED','STARTED','UNKNOWN','REAPED')),
    process_id BIGINT,
    process_group_id BIGINT,
    process_start_identity TEXT,
    unknown_reason VARCHAR(32),
    reap_evidence JSONB CHECK (reap_evidence IS NULL OR jsonb_typeof(reap_evidence) = 'object'),
    revision BIGINT NOT NULL CHECK (revision > 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL CHECK (updated_at >= created_at),
    FOREIGN KEY (job_id,task_id) REFERENCES jobs(id,task_id),
    FOREIGN KEY (run_id,task_id,job_id) REFERENCES agent_runs(id,task_id,job_id),
    CHECK ((process_id IS NULL AND process_group_id IS NULL AND process_start_identity IS NULL)
        OR (process_id IS NOT NULL AND process_id > 0 AND process_group_id IS NOT NULL AND process_group_id > 0
            AND process_start_identity IS NOT NULL AND btrim(process_start_identity) <> '' AND length(process_start_identity) <= 256)),
    CHECK (
        (state = 'RESERVED' AND process_id IS NULL AND unknown_reason IS NULL AND reap_evidence IS NULL)
        OR (state = 'STARTED' AND process_id IS NOT NULL AND unknown_reason IS NULL AND reap_evidence IS NULL)
        OR (state = 'UNKNOWN' AND unknown_reason IS NOT NULL AND unknown_reason IN ('START_AMBIGUOUS','SUPERVISOR_LOST','RECOVERY_REQUIRED') AND reap_evidence IS NULL)
        OR (state = 'REAPED' AND unknown_reason IS NULL AND reap_evidence IS NOT NULL AND jsonb_typeof(reap_evidence) = 'object'
            AND reap_evidence ?& ARRAY['kind','run_id','task_id','job_id','generation','resource_scope','lease_token_sha256','workspace','supervisor_id','verified_at','verifier_id']
            AND reap_evidence->>'kind' IS NOT NULL
            AND octet_length(reap_evidence::text) <= 4096
            AND reap_evidence->>'kind' IN ('NEVER_STARTED','GROUP_DRAINED')
            AND reap_evidence->>'run_id' = run_id::text
            AND reap_evidence->>'task_id' = task_id::text
            AND reap_evidence->>'job_id' = job_id::text
            AND reap_evidence->>'generation' = generation::text
            AND reap_evidence->>'resource_scope' = resource_scope
            AND reap_evidence->>'lease_token_sha256' = lease_token_sha256
            AND reap_evidence->>'workspace' = workspace
            AND reap_evidence->>'supervisor_id' = supervisor_identity)
    ),
    CHECK (state <> 'REAPED' OR
        ((reap_evidence->>'kind' = 'NEVER_STARTED' AND process_id IS NULL AND reap_evidence->'process' IS NULL) OR
         (reap_evidence->>'kind' = 'GROUP_DRAINED' AND process_id IS NOT NULL
          AND reap_evidence ? 'process' AND jsonb_typeof(reap_evidence->'process') = 'object'
          AND reap_evidence->'process' ?& ARRAY['pid','pgid','start_identity']
          AND reap_evidence->'process'->>'pid' = process_id::text
          AND reap_evidence->'process'->>'pgid' = process_group_id::text
          AND reap_evidence->'process'->>'start_identity' = process_start_identity))
    )
);

CREATE UNIQUE INDEX process_holds_one_unresolved_task
    ON process_holds(task_id) WHERE state <> 'REAPED';
CREATE INDEX process_holds_unresolved_scope_idx
    ON process_holds(resource_scope, created_at, run_id) WHERE state <> 'REAPED';

-- Holds are append-only resource-accounting identities. Updates may advance
-- only through the finite lifecycle and may never retarget a process proof.
CREATE FUNCTION process_holds_guard_mutation() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'process holds cannot be deleted';
    END IF;
    IF NEW.run_id IS DISTINCT FROM OLD.run_id
       OR NEW.task_id IS DISTINCT FROM OLD.task_id
       OR NEW.job_id IS DISTINCT FROM OLD.job_id
       OR NEW.generation IS DISTINCT FROM OLD.generation
       OR NEW.resource_scope IS DISTINCT FROM OLD.resource_scope
       OR NEW.lease_token_sha256 IS DISTINCT FROM OLD.lease_token_sha256
       OR NEW.workspace IS DISTINCT FROM OLD.workspace
       OR NEW.supervisor_identity IS DISTINCT FROM OLD.supervisor_identity
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
       OR NEW.revision <> OLD.revision + 1
       OR NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'process hold identity or revision cannot be rebound';
    END IF;
    IF OLD.process_id IS NOT NULL AND
       (NEW.process_id IS DISTINCT FROM OLD.process_id
        OR NEW.process_group_id IS DISTINCT FROM OLD.process_group_id
        OR NEW.process_start_identity IS DISTINCT FROM OLD.process_start_identity) THEN
        RAISE EXCEPTION 'process identity cannot be rebound';
    END IF;
    IF OLD.state = 'REAPED' THEN
        RAISE EXCEPTION 'reaped process holds are immutable';
    ELSIF OLD.state = 'RESERVED' AND NEW.state NOT IN ('STARTED','UNKNOWN','REAPED') THEN
        RAISE EXCEPTION 'invalid reserved process hold transition';
    ELSIF OLD.state = 'STARTED' AND NEW.state NOT IN ('UNKNOWN','REAPED') THEN
        RAISE EXCEPTION 'invalid started process hold transition';
    ELSIF OLD.state = 'UNKNOWN' AND NEW.state NOT IN ('UNKNOWN','REAPED') THEN
        RAISE EXCEPTION 'invalid unknown process hold transition';
    END IF;
    IF OLD.state = 'UNKNOWN' AND NEW.state = 'UNKNOWN'
       AND (NEW.unknown_reason IS DISTINCT FROM OLD.unknown_reason OR OLD.process_id IS NOT NULL OR NEW.process_id IS NULL) THEN
        RAISE EXCEPTION 'unknown process hold may only fill a missing process identity';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER process_holds_guard_mutation
    BEFORE UPDATE OR DELETE ON process_holds
    FOR EACH ROW EXECUTE FUNCTION process_holds_guard_mutation();
