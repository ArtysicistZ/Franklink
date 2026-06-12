-- User Networking Opportunities Table (v2)
-- Stores ranked networking opportunities extracted from user email activity (via Zep)
--
-- This table stores the output of rank_purposes_for_proactive() which includes:
-- 1. Purpose suggestions from _get_connection_purpose_suggestions() (extracted from Zep)
-- 2. LLM-assigned ranking, match_type, and max_matches from ranking step
--
-- Historical record: New opportunities are appended, old ones are NOT expired.
-- Each extraction creates a new batch_id to group opportunities from the same run.

-- =============================================================================
-- Table: user_networking_opportunities
-- =============================================================================
CREATE TABLE IF NOT EXISTS user_networking_opportunities (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    -- Batch tracking: group opportunities from the same extraction run
    batch_id uuid NOT NULL DEFAULT gen_random_uuid(),

    -- Opportunity details (from _get_connection_purpose_suggestions)
    purpose text NOT NULL,              -- e.g., "finding a study partner for CIS 520 final"
    group_name text,                    -- Short name for iMessage group (e.g., "CS 520 Study Group")
    rationale text,                     -- Why this connection would help
    evidence text,                      -- Which email triggered this suggestion
    activity_type text DEFAULT 'general'
        CHECK (activity_type IN ('academic', 'event', 'project', 'research', 'social', 'hobby', 'activity', 'practice', 'career', 'collaboration', 'mentorship', 'networking', 'interview', 'meeting', 'workshop', 'competition', 'general')),
    event_date date,                    -- YYYY-MM-DD or null if ongoing
    urgency text DEFAULT 'medium'
        CHECK (urgency IN ('high', 'medium', 'low')),

    -- Ranking results (from rank_purposes_for_proactive)
    rank int NOT NULL,                  -- Position in batch (1 = best)
    match_type text DEFAULT 'single'
        CHECK (match_type IN ('single', 'multi')),
    max_matches int DEFAULT 1,          -- 1 for single, 2-5 for multi

    -- Source tracking
    source text DEFAULT 'proactive'     -- 'proactive' or 'user_requested'
        CHECK (source IN ('proactive', 'user_requested')),

    -- Status tracking
    status text DEFAULT 'active'
        CHECK (status IN ('active', 'used', 'skipped', 'expired')),
    used_at timestamptz,                -- When this opportunity was used for outreach
    connection_request_id uuid,         -- Links to connection_request if used

    -- Timestamps
    extracted_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Index for finding opportunities by user
CREATE INDEX IF NOT EXISTS user_networking_opportunities_user_idx
    ON user_networking_opportunities (user_id);

-- Index for finding active opportunities by user
CREATE INDEX IF NOT EXISTS user_networking_opportunities_user_active_idx
    ON user_networking_opportunities (user_id, status)
    WHERE status = 'active';

-- Index for batch lookups
CREATE INDEX IF NOT EXISTS user_networking_opportunities_batch_idx
    ON user_networking_opportunities (batch_id);

-- Index for finding recent opportunities by user
CREATE INDEX IF NOT EXISTS user_networking_opportunities_user_recent_idx
    ON user_networking_opportunities (user_id, extracted_at DESC);

-- =============================================================================
-- RPC: insert_networking_opportunities_batch_v1
-- Insert a batch of ranked opportunities for a user
-- =============================================================================
CREATE OR REPLACE FUNCTION insert_networking_opportunities_batch_v1(
    p_user_id uuid,
    p_source text,
    p_opportunities jsonb  -- Array of opportunity objects
)
RETURNS uuid  -- Returns batch_id
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id uuid := gen_random_uuid();
    opp_record jsonb;
BEGIN
    FOR opp_record IN SELECT * FROM jsonb_array_elements(p_opportunities)
    LOOP
        INSERT INTO user_networking_opportunities (
            user_id,
            batch_id,
            purpose,
            group_name,
            rationale,
            evidence,
            activity_type,
            event_date,
            urgency,
            rank,
            match_type,
            max_matches,
            source,
            status,
            extracted_at,
            created_at
        )
        VALUES (
            p_user_id,
            v_batch_id,
            opp_record->>'purpose',
            opp_record->>'group_name',
            opp_record->>'rationale',
            opp_record->>'evidence',
            COALESCE(opp_record->>'activity_type', 'general'),
            CASE
                WHEN opp_record->>'event_date' IS NOT NULL
                THEN (opp_record->>'event_date')::date
                ELSE NULL
            END,
            COALESCE(opp_record->>'urgency', 'medium'),
            COALESCE((opp_record->>'rank')::int, 1),
            COALESCE(opp_record->>'match_type', 'single'),
            COALESCE((opp_record->>'max_matches')::int, 1),
            COALESCE(p_source, 'proactive'),
            'active',
            now(),
            now()
        );
    END LOOP;

    RETURN v_batch_id;
END;
$$;

-- =============================================================================
-- RPC: get_recent_networking_opportunities_v1
-- Get recent opportunities for a user, optionally filtered by status
-- =============================================================================
CREATE OR REPLACE FUNCTION get_recent_networking_opportunities_v1(
    p_user_id uuid,
    p_days int DEFAULT 7,
    p_status text DEFAULT NULL
)
RETURNS TABLE (
    id uuid,
    batch_id uuid,
    purpose text,
    group_name text,
    rationale text,
    evidence text,
    activity_type text,
    event_date date,
    urgency text,
    rank int,
    match_type text,
    max_matches int,
    source text,
    status text,
    extracted_at timestamptz
)
LANGUAGE sql
STABLE
AS $$
    SELECT
        id, batch_id, purpose, group_name, rationale, evidence,
        activity_type, event_date, urgency, rank, match_type,
        max_matches, source, status, extracted_at
    FROM user_networking_opportunities
    WHERE user_id = p_user_id
      AND extracted_at > now() - (p_days || ' days')::interval
      AND (p_status IS NULL OR status = p_status)
    ORDER BY extracted_at DESC, rank ASC;
$$;

-- =============================================================================
-- RPC: get_active_opportunities_purposes_v1
-- Get purpose texts from active opportunities for deduplication
-- =============================================================================
CREATE OR REPLACE FUNCTION get_active_opportunities_purposes_v1(
    p_user_id uuid,
    p_days int DEFAULT 7
)
RETURNS TABLE (purpose text)
LANGUAGE sql
STABLE
AS $$
    SELECT DISTINCT purpose
    FROM user_networking_opportunities
    WHERE user_id = p_user_id
      AND extracted_at > now() - (p_days || ' days')::interval
      AND status IN ('active', 'used')
    ORDER BY purpose;
$$;

-- =============================================================================
-- RPC: mark_opportunity_used_v1
-- Mark an opportunity as used and link to connection request
-- =============================================================================
CREATE OR REPLACE FUNCTION mark_opportunity_used_v1(
    p_opportunity_id uuid,
    p_connection_request_id uuid
)
RETURNS user_networking_opportunities
LANGUAGE plpgsql
AS $$
DECLARE
    out_row user_networking_opportunities;
BEGIN
    UPDATE user_networking_opportunities
    SET
        status = 'used',
        used_at = now(),
        connection_request_id = p_connection_request_id
    WHERE id = p_opportunity_id
    RETURNING * INTO out_row;

    RETURN out_row;
END;
$$;

-- =============================================================================
-- RPC: mark_opportunity_skipped_v1
-- Mark an opportunity as skipped (no match found or duplicate)
-- =============================================================================
CREATE OR REPLACE FUNCTION mark_opportunity_skipped_v1(
    p_opportunity_id uuid
)
RETURNS user_networking_opportunities
LANGUAGE plpgsql
AS $$
DECLARE
    out_row user_networking_opportunities;
BEGIN
    UPDATE user_networking_opportunities
    SET status = 'skipped'
    WHERE id = p_opportunity_id
    RETURNING * INTO out_row;

    RETURN out_row;
END;
$$;

-- =============================================================================
-- Note: Old user_email_signals table is deprecated
-- This new table replaces it with proper structure matching the actual code
-- =============================================================================
