-- LLM Token Usage Tracking
-- Tracks all LLM API calls for cost analysis and monitoring
-- Run this migration in Supabase SQL Editor

-- =============================================================================
-- TABLE: llm_usage_log
-- =============================================================================

CREATE TABLE IF NOT EXISTS llm_usage_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Context identification
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,  -- NULL for system/background jobs without user context
    chat_guid TEXT,                                         -- Group chat context (optional)
    job_type TEXT,                                          -- Background job identifier (optional)

    -- API call identification
    trace_label TEXT NOT NULL,                              -- Operation identifier (e.g., "interaction_agent", "classify_intent")
    deployment TEXT NOT NULL,                               -- Model deployment name (e.g., "gpt-4o-mini", "gpt-5-mini")
    api_type TEXT NOT NULL DEFAULT 'chat'                   -- 'chat' or 'embedding'
        CHECK (api_type IN ('chat', 'embedding')),

    -- Token usage
    prompt_tokens INT NOT NULL DEFAULT 0,
    completion_tokens INT NOT NULL DEFAULT 0,
    total_tokens INT NOT NULL DEFAULT 0,

    -- Cost tracking (in USD cents for precision, stored as numeric for decimal accuracy)
    cost_cents NUMERIC(12, 4) NOT NULL DEFAULT 0,           -- e.g., 0.0150 = $0.00015

    -- Performance tracking
    duration_ms INT,                                        -- API call duration in milliseconds
    success BOOLEAN NOT NULL DEFAULT TRUE,
    error_message TEXT,

    -- Request metadata (for debugging and analysis)
    request_metadata JSONB DEFAULT '{}'::jsonb,             -- Optional: message_count, text_length, etc.

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- INDEXES
-- =============================================================================

-- Per-user usage lookup (most common query pattern)
CREATE INDEX IF NOT EXISTS llm_usage_log_user_id_idx
    ON llm_usage_log (user_id, created_at DESC)
    WHERE user_id IS NOT NULL;

-- By operation type (for identifying expensive operations)
CREATE INDEX IF NOT EXISTS llm_usage_log_trace_label_idx
    ON llm_usage_log (trace_label, created_at DESC);

-- By model deployment (for cost analysis per model)
CREATE INDEX IF NOT EXISTS llm_usage_log_deployment_idx
    ON llm_usage_log (deployment, created_at DESC);

-- Time-based queries (daily/weekly reports)
CREATE INDEX IF NOT EXISTS llm_usage_log_created_at_idx
    ON llm_usage_log (created_at DESC);

-- Group chat usage tracking
CREATE INDEX IF NOT EXISTS llm_usage_log_chat_guid_idx
    ON llm_usage_log (chat_guid, created_at DESC)
    WHERE chat_guid IS NOT NULL;

-- Background job tracking
CREATE INDEX IF NOT EXISTS llm_usage_log_job_type_idx
    ON llm_usage_log (job_type, created_at DESC)
    WHERE job_type IS NOT NULL;

-- =============================================================================
-- VIEWS: Aggregate reporting
-- =============================================================================

-- Daily summary by deployment and operation
CREATE OR REPLACE VIEW llm_usage_daily_summary AS
SELECT
    DATE_TRUNC('day', created_at) AS day,
    deployment,
    trace_label,
    COUNT(*) AS call_count,
    SUM(prompt_tokens) AS total_prompt_tokens,
    SUM(completion_tokens) AS total_completion_tokens,
    SUM(total_tokens) AS total_tokens,
    SUM(cost_cents) AS total_cost_cents,
    AVG(duration_ms)::INT AS avg_duration_ms,
    COUNT(*) FILTER (WHERE NOT success) AS error_count
FROM llm_usage_log
GROUP BY 1, 2, 3;

-- Per-user daily summary
CREATE OR REPLACE VIEW llm_usage_user_summary AS
SELECT
    user_id,
    DATE_TRUNC('day', created_at) AS day,
    COUNT(*) AS call_count,
    SUM(total_tokens) AS total_tokens,
    SUM(cost_cents) AS total_cost_cents
FROM llm_usage_log
WHERE user_id IS NOT NULL
GROUP BY 1, 2;

-- =============================================================================
-- RPC FUNCTIONS
-- =============================================================================

-- Get user usage summary for the past N days
CREATE OR REPLACE FUNCTION get_user_llm_usage_summary_v1(
    p_user_id UUID,
    p_days INT DEFAULT 30
)
RETURNS TABLE (
    total_calls BIGINT,
    total_tokens BIGINT,
    total_cost_cents NUMERIC,
    avg_tokens_per_call NUMERIC,
    top_operations JSONB
) AS $$
BEGIN
    RETURN QUERY
    WITH user_stats AS (
        SELECT
            COUNT(*) AS calls,
            SUM(total_tokens) AS tokens,
            SUM(cost_cents) AS cost,
            AVG(total_tokens)::NUMERIC(10,2) AS avg_tokens
        FROM llm_usage_log
        WHERE user_id = p_user_id
          AND created_at > NOW() - (p_days || ' days')::INTERVAL
    ),
    top_ops AS (
        SELECT jsonb_agg(
            jsonb_build_object(
                'operation', trace_label,
                'calls', cnt,
                'tokens', tokens,
                'cost_cents', cost
            )
            ORDER BY cost DESC
        ) AS ops
        FROM (
            SELECT
                trace_label,
                COUNT(*) AS cnt,
                SUM(total_tokens) AS tokens,
                SUM(cost_cents) AS cost
            FROM llm_usage_log
            WHERE user_id = p_user_id
              AND created_at > NOW() - (p_days || ' days')::INTERVAL
            GROUP BY trace_label
            ORDER BY SUM(cost_cents) DESC
            LIMIT 10
        ) sub
    )
    SELECT
        COALESCE(user_stats.calls, 0),
        COALESCE(user_stats.tokens, 0),
        COALESCE(user_stats.cost, 0),
        COALESCE(user_stats.avg_tokens, 0),
        COALESCE(top_ops.ops, '[]'::jsonb)
    FROM user_stats, top_ops;
END;
$$ LANGUAGE plpgsql;

-- Get daily usage stats for monitoring dashboard
CREATE OR REPLACE FUNCTION get_daily_llm_usage_stats_v1(
    p_days INT DEFAULT 7
)
RETURNS TABLE (
    day DATE,
    total_calls BIGINT,
    total_tokens BIGINT,
    total_cost_usd NUMERIC,
    unique_users BIGINT,
    error_rate NUMERIC,
    top_deployments JSONB
) AS $$
BEGIN
    RETURN QUERY
    WITH daily AS (
        SELECT
            DATE_TRUNC('day', created_at)::DATE AS d,
            COUNT(*) AS calls,
            SUM(total_tokens) AS tokens,
            SUM(cost_cents) / 100.0 AS cost_usd,
            COUNT(DISTINCT user_id) AS users,
            (COUNT(*) FILTER (WHERE NOT success)::NUMERIC / NULLIF(COUNT(*), 0) * 100)::NUMERIC(5,2) AS err_rate
        FROM llm_usage_log
        WHERE created_at > NOW() - (p_days || ' days')::INTERVAL
        GROUP BY DATE_TRUNC('day', created_at)::DATE
    ),
    deployments_per_day AS (
        SELECT
            DATE_TRUNC('day', created_at)::DATE AS d,
            jsonb_agg(
                jsonb_build_object(
                    'deployment', deployment,
                    'calls', cnt,
                    'cost_usd', cost
                )
                ORDER BY cost DESC
            ) AS deps
        FROM (
            SELECT
                DATE_TRUNC('day', created_at)::DATE AS d,
                deployment,
                COUNT(*) AS cnt,
                (SUM(cost_cents) / 100.0)::NUMERIC(10,4) AS cost
            FROM llm_usage_log
            WHERE created_at > NOW() - (p_days || ' days')::INTERVAL
            GROUP BY DATE_TRUNC('day', created_at)::DATE, deployment
        ) sub
        GROUP BY d
    )
    SELECT
        daily.d,
        daily.calls,
        daily.tokens,
        daily.cost_usd,
        daily.users,
        daily.err_rate,
        COALESCE(deployments_per_day.deps, '[]'::jsonb)
    FROM daily
    LEFT JOIN deployments_per_day ON daily.d = deployments_per_day.d
    ORDER BY daily.d DESC;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON TABLE llm_usage_log IS 'Tracks all LLM API calls for cost analysis and monitoring';
COMMENT ON COLUMN llm_usage_log.trace_label IS 'Operation identifier from code (e.g., interaction_agent, classify_intent, profile_synthesis)';
COMMENT ON COLUMN llm_usage_log.cost_cents IS 'Calculated cost in USD cents based on model pricing';
COMMENT ON COLUMN llm_usage_log.request_metadata IS 'Optional debug info like message_count, text_length, etc.';
