CREATE TABLE webhook_events (
    id uuid PRIMARY KEY,
    source_svix_id text NOT NULL UNIQUE,
    source_svix_timestamp text NOT NULL,
    received_at timestamptz NOT NULL,
    resend_event_id text,
    resend_event_type text,
    raw_body bytea NOT NULL,
    headers jsonb NOT NULL,
    matched_destination_count integer NOT NULL CHECK (matched_destination_count >= 0)
);

CREATE INDEX webhook_events_received_at_idx ON webhook_events (received_at DESC);
CREATE INDEX webhook_events_resend_event_id_idx ON webhook_events (resend_event_id);

CREATE TABLE delivery_jobs (
    id uuid PRIMARY KEY,
    event_id uuid NOT NULL REFERENCES webhook_events(id) ON DELETE CASCADE,
    destination_name text NOT NULL,
    destination_url text NOT NULL,
    status text NOT NULL CHECK (status IN ('queued', 'delivering', 'retrying', 'succeeded', 'failed')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_attempt_at timestamptz NOT NULL,
    deadline_at timestamptz NOT NULL,
    locked_at timestamptz,
    locked_by text,
    last_attempt_at timestamptz,
    last_status_code integer,
    last_error text,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX delivery_jobs_due_idx
    ON delivery_jobs (next_attempt_at ASC)
    WHERE status IN ('queued', 'retrying');

CREATE INDEX delivery_jobs_status_idx ON delivery_jobs (status);
CREATE INDEX delivery_jobs_event_id_idx ON delivery_jobs (event_id);
CREATE INDEX delivery_jobs_locked_at_idx ON delivery_jobs (locked_at) WHERE status = 'delivering';

CREATE TABLE delivery_attempts (
    id uuid PRIMARY KEY,
    job_id uuid NOT NULL REFERENCES delivery_jobs(id) ON DELETE CASCADE,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    attempted_at timestamptz NOT NULL,
    status_code integer,
    error text,
    duration_ms integer NOT NULL CHECK (duration_ms >= 0),
    will_retry boolean NOT NULL,
    next_attempt_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (job_id, attempt_number)
);

CREATE INDEX delivery_attempts_job_id_idx ON delivery_attempts (job_id, attempt_number ASC);
