-- Update proactive outreach job scheduling to support configurable interval
-- Run this after proactive_outreach_jobs.sql
-- Default interval changed from 1 day to 2 days

-- =============================================================================
-- RPC: complete_proactive_outreach_job_v1
-- Mark a job as complete (outreach was sent) and schedule next run
-- Updated to accept configurable interval
-- =============================================================================
create or replace function complete_proactive_outreach_job_v1(
    p_user_id uuid,
    p_worker_id text,
    p_demand_id uuid default null,
    p_connection_request_id uuid default null,
    p_interval_days int default 2  -- Default to 2 days between runs
)
returns proactive_outreach_jobs
language plpgsql
as $$
declare
    out_row proactive_outreach_jobs;
    next_run timestamptz;
begin
    -- Schedule next run for N days from now at 6 PM UTC
    next_run := date_trunc('day', now() at time zone 'UTC')
                + (p_interval_days || ' days')::interval
                + interval '18 hours';

    update proactive_outreach_jobs j
    set
        status = 'done',
        last_outreach_at = now(),
        last_outreach_demand_id = p_demand_id,
        last_outreach_connection_request_id = p_connection_request_id,
        run_after = next_run,
        attempts = 0,
        last_error = null,
        last_skip_reason = null,
        claimed_by = null,
        claimed_at = null,
        updated_at = now()
    where j.user_id = p_user_id
      and j.claimed_by = p_worker_id
    returning * into out_row;

    return out_row;
end;
$$;

-- =============================================================================
-- RPC: skip_proactive_outreach_job_v1
-- Mark a job as skipped (no outreach sent) and schedule next run
-- Updated to accept configurable interval
-- =============================================================================
create or replace function skip_proactive_outreach_job_v1(
    p_user_id uuid,
    p_worker_id text,
    p_skip_reason text,
    p_interval_days int default 2  -- Default to 2 days between runs
)
returns proactive_outreach_jobs
language plpgsql
as $$
declare
    out_row proactive_outreach_jobs;
    next_run timestamptz;
begin
    -- Schedule next run for N days from now at 6 PM UTC
    next_run := date_trunc('day', now() at time zone 'UTC')
                + (p_interval_days || ' days')::interval
                + interval '18 hours';

    update proactive_outreach_jobs j
    set
        status = 'skipped',
        last_skip_reason = p_skip_reason,
        run_after = next_run,
        attempts = 0,
        last_error = null,
        claimed_by = null,
        claimed_at = null,
        updated_at = now()
    where j.user_id = p_user_id
      and j.claimed_by = p_worker_id
    returning * into out_row;

    return out_row;
end;
$$;

-- =============================================================================
-- RPC: schedule_proactive_outreach_job_v1
-- Schedule or reschedule a proactive outreach job for a user
-- Updated to accept configurable interval
-- =============================================================================
create or replace function schedule_proactive_outreach_job_v1(
    p_user_id uuid,
    p_run_after timestamptz default null,
    p_interval_days int default 2  -- Default to 2 days for new jobs
)
returns proactive_outreach_jobs
language plpgsql
as $$
declare
    out_row proactive_outreach_jobs;
    default_run_after timestamptz;
begin
    -- Default: next 6 PM UTC
    default_run_after := coalesce(
        p_run_after,
        date_trunc('day', now() at time zone 'UTC') + interval '18 hours'
    );
    -- If already past 6 PM UTC today, schedule for interval_days from now
    if default_run_after <= now() then
        default_run_after := date_trunc('day', now() at time zone 'UTC')
                            + (p_interval_days || ' days')::interval
                            + interval '18 hours';
    end if;

    insert into proactive_outreach_jobs (user_id, status, run_after, updated_at)
    values (p_user_id, 'queued', default_run_after, now())
    on conflict (user_id) do update
    set
        -- Don't interrupt a running job
        status = case
            when proactive_outreach_jobs.status = 'running' then 'running'
            else 'queued'
        end,
        run_after = case
            when proactive_outreach_jobs.status = 'running' then proactive_outreach_jobs.run_after
            else excluded.run_after
        end,
        updated_at = now()
    returning * into out_row;

    return out_row;
end;
$$;
