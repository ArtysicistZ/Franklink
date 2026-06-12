-- RPC: match_users_complementary
-- Purpose: Find users with complementary skills (supply-demand matching)
--
-- Unlike embedding similarity, this uses set intersection on structured arrays:
--   - Their offering_skills overlap with my seeking_skills (they can help me)
--   - Their seeking_skills overlap with my offering_skills (I can help them)
--
-- This directly solves the founder<->marketer problem where embedding
-- cosine similarity would filter out dissimilar-but-complementary matches.

CREATE OR REPLACE FUNCTION match_users_complementary(
    p_seeking_skills TEXT[],
    p_offering_skills TEXT[],
    p_exclude_user_id UUID,
    p_seeking_relationship_types TEXT[] DEFAULT '{}',
    p_match_count INT DEFAULT 20
)
RETURNS TABLE (
    id UUID,
    name TEXT,
    phone_number TEXT,
    university TEXT,
    major TEXT,
    year TEXT,
    location TEXT,
    career_interests TEXT[],
    all_demand TEXT,
    all_value TEXT,
    context_summary TEXT,
    needs JSONB,
    linkedin_data JSONB,
    seeking_skills TEXT[],
    offering_skills TEXT[],
    supply_match_count INT,
    demand_match_count INT,
    relationship_match_count INT,
    complementary_score INT,
    similarity FLOAT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    WITH scored AS (
        SELECT
            u.id,
            u.name,
            u.phone_number,
            u.university,
            u.major,
            u.year::TEXT,
            u.location,
            u.career_interests,
            u.all_demand,
            u.all_value,
            u.context_summary,
            u.needs,
            u.linkedin_data,
            u.seeking_skills,
            u.offering_skills,
            -- Compute each intersection count exactly once
            COALESCE(array_length(
                ARRAY(SELECT unnest(u.offering_skills) INTERSECT SELECT unnest(p_seeking_skills)),
                1
            ), 0)::INT AS _supply,
            COALESCE(array_length(
                ARRAY(SELECT unnest(u.seeking_skills) INTERSECT SELECT unnest(p_offering_skills)),
                1
            ), 0)::INT AS _demand,
            COALESCE(array_length(
                ARRAY(SELECT unnest(u.offering_relationship_types) INTERSECT SELECT unnest(p_seeking_relationship_types)),
                1
            ), 0)::INT AS _rel
        FROM users u
        WHERE u.id != p_exclude_user_id
          AND u.is_onboarded = true
          AND u.phone_number IS NOT NULL
          AND u.offering_skills != '{}'
          AND (
              (u.offering_skills && p_seeking_skills)
              OR (u.seeking_skills && p_offering_skills)
          )
    )
    SELECT
        scored.id,
        scored.name,
        scored.phone_number,
        scored.university,
        scored.major,
        scored.year,
        scored.location,
        scored.career_interests,
        scored.all_demand,
        scored.all_value,
        scored.context_summary,
        scored.needs,
        scored.linkedin_data,
        scored.seeking_skills,
        scored.offering_skills,
        scored._supply AS supply_match_count,
        scored._demand AS demand_match_count,
        scored._rel AS relationship_match_count,
        (scored._supply + scored._demand + scored._rel)::INT AS complementary_score,
        -- Normalize to 0-1 range; use COALESCE to prevent NULL from array_length on empty arrays
        LEAST(1.0,
            (scored._supply + scored._demand + scored._rel)::FLOAT
            / GREATEST(1.0,
                COALESCE(array_length(p_seeking_skills, 1), 0)::FLOAT
                + COALESCE(array_length(p_offering_skills, 1), 0)::FLOAT
            )
        )::FLOAT AS similarity
    FROM scored
    ORDER BY (scored._supply + scored._demand + scored._rel) DESC, scored._supply DESC
    LIMIT p_match_count;
END;
$$;

-- Grant execute permissions
GRANT EXECUTE ON FUNCTION match_users_complementary(TEXT[], TEXT[], UUID, TEXT[], INT) TO authenticated;
GRANT EXECUTE ON FUNCTION match_users_complementary(TEXT[], TEXT[], UUID, TEXT[], INT) TO service_role;
