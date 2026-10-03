-- First-class plan lifecycles are separate from existing PR task/run identities.
CREATE TABLE plan_lifecycles (
    id UUID PRIMARY KEY,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    state JSONB NOT NULL CHECK (jsonb_typeof(state) = 'object')
);

CREATE TABLE plan_task_receipts (
    id UUID PRIMARY KEY,
    lifecycle_id UUID NOT NULL REFERENCES plan_lifecycles(id),
    task_id UUID NOT NULL,
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object')
);

CREATE FUNCTION reject_plan_receipt_change() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'plan task receipts are append-only';
END;
$$;

CREATE TRIGGER plan_task_receipts_immutable
BEFORE UPDATE OR DELETE ON plan_task_receipts
FOR EACH ROW EXECUTE FUNCTION reject_plan_receipt_change();
