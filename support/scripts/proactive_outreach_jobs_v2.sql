-- Proactive Outreach Jobs Table and RPC Functions (v2)
-- This table tracks scheduled proactive outreach jobs for each user
-- Updated: Removed deprecated user_email_demands reference, added 2-day interval support

-- =============================================================================
-- Table: proactive_outreach_jobs
-- =============================================================================
CREATE TABLE IF NOT EXISTS proactive_outreach_jobs (
    user_id uuid PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    status text NOT NULL CHECK (status IN ('queued', 'running', 'done', 'failed', 'skipped')),

    -- Last successful outreach stats
    last_outreach_at timestamptz,
    last_outreach_signal_id uuid,  -- References opportunity or signal used
    last_outreach_connection_request_id uuid REFERENCES connection_requests(id) ON DELETE SET NULL,

    -- Next scheduled run
    run_after timestamptz NOT NULL,

    -- Retry tracking
    attempts int NOT NULL DEFAULT 0,
    last_error text,
    last_skip_reason text,  -- Why job was skipped (no_suggestions, no_match, user_opted_out, etc.)

    -- Worker claim (for multi-instance safety)
    claimed_by text,
    claimed_at timestamptz,

    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Index for finding due jobs
CREATE INDEX IF NOT EXISTS proactive_outreach_jobs_due_idx
    ON proactive_outreach_jobs (status, run_after)
    WHERE status IN ('queued', 'running');

-- =============================================================================
-- RPC: schedule_proactive_outreach_job_v1
-- Schedule or reschedule a proactive outreach job for a user
-- =============================================================================
CREATE OR REPLACE FUNCTION schedule_proactive_outreach_job_v1(
    p_user_id uuid,
    p_run_after timestamptz DEFAULT NULL,
    p_interval_days int DEFAULT 2  -- Default to 2 days for new jobs
)
RETURNS proactive_outreach_jobs
LANGUAGE plpgsql
AS $$
DECLARE
    out_row proactive_outreach_jobs;
    default_run_after timestamptz;
BEGIN
    -- Default: next 6 PM UTC
    default_run_after := COALESCE(
        p_run_after,
        date_trunc('day', now() AT TIME ZONE 'UTC') + interval '18 hours'
    );
    -- If already past 6 PM UTC today, schedule for interval_days from now
    IF default_run_after <= now() THEN
        default_run_after := date_trunc('day', now() AT TIME ZONE 'UTC')
                            + (p_interval_days || ' days')::interval
                            + interval '18 hours';
    END IF;

    INSERT INTO proactive_outreach_jobs (user_id, status, run_after, updated_at)
    VALUES (p_user_id, 'queued', default_run_after, now())
    ON CONFLICT (user_id) DO UPDATE
    SET
        -- Don't interrupt a running job
        status = CASE
            WHEN proactive_outreach_jobs.status = 'running' THEN 'running'
            ELSE 'queued'
        END,
        run_after = CASE
            WHEN proactive_outreach_jobs.status = 'running' THEN proactive_outreach_jobs.run_after
            ELSE EXCLUDED.run_after
        END,
        updated_at = now()
    RETURNING * INTO out_row;

    RETURN out_row;
END;
$$;

-- =============================================================================
-- RPC: claim_proactive_outreach_jobs_v1
-- Claim up to max_jobs that are due for processing
-- Uses FOR UPDATE SKIP LOCKED for multi-instance safety
-- =============================================================================
CREATE OR REPLACE FUNCTION claim_proactive_outreach_jobs_v1(
    p_worker_id text,
    p_max_jobs int DEFAULT 5,
    p_stale_after interval DEFAULT '30 minutes'
)
RETURNS SETOF proactive_outreach_jobs
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH candidates AS (
        SELECT user_id
        FROM proactive_outreach_jobs
        WHERE (
            -- Job is queued and due
            (status = 'queued' AND run_after <= now())
            -- Or job is stale (claimed but not completed)
            OR (status = 'running' AND claimed_at <= now() - p_stale_after)
        )
        ORDER BY run_after ASC
        FOR UPDATE SKIP LOCKED
        LIMIT p_max_jobs
    )
    UPDATE proactive_outreach_jobs j
    SET
        status = 'running',
        claimed_by = p_worker_id,
        claimed_at = now(),
        updated_at = now()
    FROM candidates c
    WHERE j.user_id = c.user_id
    RETURNING j.*;
END;
$$;

-- =============================================================================
-- RPC: complete_proactive_outreach_job_v1
-- Mark a job as complete (outreach was sent) and schedule next run
-- =============================================================================
CREATE OR REPLACE FUNCTION complete_proactive_outreach_job_v1(
    p_user_id uuid,
    p_worker_id text,
    p_demand_id uuid DEFAULT NULL,  -- Now stores signal_id (kept param name for compatibility)
    p_connection_request_id uuid DEFAULT NULL,
    p_interval_days int DEFAULT 2   -- Default to 2 days between runs
)
RETURNS proactive_outreach_jobs
LANGUAGE plpgsql
AS $$
DECLARE
    out_row proactive_outreach_jobs;
    next_run timestamptz;
BEGIN
    -- Schedule next run for N days from now at 6 PM UTC
    next_run := date_trunc('day', now() AT TIME ZONE 'UTC')
                + (p_interval_days || ' days')::interval
                + interval '18 hours';

    UPDATE proactive_outreach_jobs j
    SET
        status = 'done',
        last_outreach_at = now(),
        last_outreach_signal_id = p_demand_id,
        last_outreach_connection_request_id = p_connection_request_id,
        run_after = next_run,
        attempts = 0,
        last_error = NULL,
        last_skip_reason = NULL,
        claimed_by = NULL,
        claimed_at = NULL,
        updated_at = now()
    WHERE j.user_id = p_user_id
      AND j.claimed_by = p_worker_id
    RETURNING * INTO out_row;

    RETURN out_row;
END;
$$;

-- =============================================================================
-- RPC: skip_proactive_outreach_job_v1
-- Mark a job as skipped (no outreach sent) and schedule next run
-- =============================================================================
CREATE OR REPLACE FUNCTION skip_proactive_outreach_job_v1(
    p_user_id uuid,
    p_worker_id text,
    p_skip_reason text,
    p_interval_days int DEFAULT 2   -- Default to 2 days between runs
)
RETURNS proactive_outreach_jobs
LANGUAGE plpgsql
AS $$
DECLARE
    out_row proactive_outreach_jobs;
    next_run timestamptz;
BEGIN
    -- Schedule next run for N days from now at 6 PM UTC
    next_run := date_trunc('day', now() AT TIME ZONE 'UTC')
                + (p_interval_days || ' days')::interval
                + interval '18 hours';

    UPDATE proactive_outreach_jobs j
    SET
        status = 'skipped',
        last_skip_reason = p_skip_reason,
        run_after = next_run,
        attempts = 0,
        last_error = NULL,
        claimed_by = NULL,
        claimed_at = NULL,
        updated_at = now()
    WHERE j.user_id = p_user_id
      AND j.claimed_by = p_worker_id
    RETURNING * INTO out_row;

    RETURN out_row;
END;
$$;

-- =============================================================================
-- RPC: fail_proactive_outreach_job_v1
-- Mark a job as failed with exponential backoff
-- =============================================================================
CREATE OR REPLACE FUNCTION fail_proactive_outreach_job_v1(
    p_user_id uuid,
    p_worker_id text,
    p_error text,
    p_backoff_seconds int DEFAULT 1800,  -- 30 minutes default
    p_max_attempts int DEFAULT 5
)
RETURNS proactive_outreach_jobs
LANGUAGE plpgsql
AS $$
DECLARE
    out_row proactive_outreach_jobs;
BEGIN
    UPDATE proactive_outreach_jobs j
    SET
        attempts = j.attempts + 1,
        last_error = left(COALESCE(p_error, ''), 2000),
        status = CASE
            WHEN (j.attempts + 1) >= p_max_attempts THEN 'failed'
            ELSE 'queued'
        END,
        run_after = greatest(j.run_after, now() + (p_backoff_seconds || ' seconds')::interval),
        claimed_by = NULL,
        claimed_at = NULL,
        updated_at = now()
    WHERE j.user_id = p_user_id
      AND j.claimed_by = p_worker_id
    RETURNING * INTO out_row;

    RETURN out_row;
END;
$$;

-- =============================================================================
-- RPC: release_proactive_outreach_job_v1
-- Release a claimed job back to queued status
-- =============================================================================
CREATE OR REPLACE FUNCTION release_proactive_outreach_job_v1(
    p_user_id uuid,
    p_worker_id text
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE proactive_outreach_jobs j
    SET
        status = 'queued',
        claimed_by = NULL,
        claimed_at = NULL,
        updated_at = now()
    WHERE j.user_id = p_user_id
      AND j.claimed_by = p_worker_id;
END;
$$;
