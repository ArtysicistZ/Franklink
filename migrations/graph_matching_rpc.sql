-- RPC: Graph-based matching queries
-- Purpose: Find users connected through shared context in the knowledge graph
--
-- These queries complement the structured complementary matching (Phase 1)
-- by discovering connections through shared organizations, domains, and projects
-- that array intersection alone cannot find.
--
-- The graph is populated from Zep facts via the materialization job.

-- =============================================================================
-- match_users_graph_combined: Unified graph matching query
-- =============================================================================
-- Combines multiple graph matching strategies:
--   1. Shared context: users who share an organization or project
--   2. Domain bridges: users interested in the same domain
--   3. Skill graph paths: users connected through skill-based edges
--   4. Event matches: users participating in the same event
--   5. Course matches: users enrolled in the same course
--   6. Club matches: users in the same student organization
--   7. Location matches: users based in the same location
--   8. Role matches: users seeking the same career role
--
-- Returns user profiles with graph_score and match context.

CREATE OR REPLACE FUNCTION match_users_graph_combined(
    p_user_id UUID,
    p_exclude_user_ids UUID[] DEFAULT '{}',
    p_match_count INT DEFAULT 15
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
    graph_score INT,
    shared_context TEXT[],
    match_strategies TEXT[]
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    v_person_node_id UUID;
BEGIN
    -- Look up the person node for the initiator
    SELECT gn.id INTO v_person_node_id
    FROM graph_nodes gn
    WHERE gn.node_type = 'person'
      AND gn.properties->>'user_id' = p_user_id::TEXT;

    -- If no person node exists, return empty
    IF v_person_node_id IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    WITH
    -- Strategy 1: Shared context (same org or project)
    shared_context_matches AS (
        SELECT
            (gn_other.properties->>'user_id')::UUID AS match_user_id,
            gn_shared.name AS shared_name,
            'shared_context' AS strategy
        FROM graph_edges e1
        JOIN graph_nodes gn_shared ON e1.target_node_id = gn_shared.id
        JOIN graph_edges e2 ON e2.target_node_id = gn_shared.id
        JOIN graph_nodes gn_other ON e2.source_node_id = gn_other.id
        WHERE e1.source_node_id = v_person_node_id
          AND e1.edge_type IN ('attends', 'works_on')
          AND e2.edge_type IN ('attends', 'works_on')
          AND gn_other.node_type = 'person'
          AND gn_other.id != v_person_node_id
          AND (gn_other.properties->>'user_id')::UUID != ALL(p_exclude_user_ids)
    ),

    -- Strategy 2: Domain bridges (same interest domain)
    domain_bridge_matches AS (
        SELECT
            (gn_other.properties->>'user_id')::UUID AS match_user_id,
            gn_domain.name AS shared_name,
            'domain_bridge' AS strategy
        FROM graph_edges e1
        JOIN graph_nodes gn_domain ON e1.target_node_id = gn_domain.id
        JOIN graph_edges e2 ON e2.target_node_id = gn_domain.id
        JOIN graph_nodes gn_other ON e2.source_node_id = gn_other.id
        WHERE e1.source_node_id = v_person_node_id
          AND e1.edge_type = 'interested_in'
          AND e2.edge_type = 'interested_in'
          AND gn_domain.node_type = 'domain'
          AND gn_other.node_type = 'person'
          AND gn_other.id != v_person_node_id
          AND (gn_other.properties->>'user_id')::UUID != ALL(p_exclude_user_ids)
    ),

    -- Strategy 3: Skill graph (they offer what I need, via graph edges)
    skill_graph_matches AS (
        SELECT
            (gn_other.properties->>'user_id')::UUID AS match_user_id,
            gn_skill.name AS shared_name,
            'skill_graph' AS strategy
        FROM graph_edges e1
        JOIN graph_nodes gn_skill ON e1.target_node_id = gn_skill.id
        JOIN graph_edges e2 ON e2.target_node_id = gn_skill.id
        JOIN graph_nodes gn_other ON e2.source_node_id = gn_other.id
        WHERE e1.source_node_id = v_person_node_id
          AND e1.edge_type = 'needs'
          AND e2.edge_type = 'offers'
          AND gn_skill.node_type = 'skill'
          AND gn_other.node_type = 'person'
          AND gn_other.id != v_person_node_id
          AND (gn_other.properties->>'user_id')::UUID != ALL(p_exclude_user_ids)
    ),

    -- Strategy 4: Event matches (users participating in same event)
    event_matches AS (
        SELECT
            (gn_other.properties->>'user_id')::UUID AS match_user_id,
            gn_event.name AS shared_name,
            'event_match' AS strategy
        FROM graph_edges e1
        JOIN graph_nodes gn_event ON e1.target_node_id = gn_event.id
        JOIN graph_edges e2 ON e2.target_node_id = gn_event.id
        JOIN graph_nodes gn_other ON e2.source_node_id = gn_other.id
        WHERE e1.source_node_id = v_person_node_id
          AND e1.edge_type = 'participating_in'
          AND e2.edge_type = 'participating_in'
          AND gn_event.node_type = 'event'
          AND gn_other.node_type = 'person'
          AND gn_other.id != v_person_node_id
          AND (gn_other.properties->>'user_id')::UUID != ALL(p_exclude_user_ids)
    ),

    -- Strategy 5: Course matches (users enrolled in same course)
    course_matches AS (
        SELECT
            (gn_other.properties->>'user_id')::UUID AS match_user_id,
            gn_course.name AS shared_name,
            'course_match' AS strategy
        FROM graph_edges e1
        JOIN graph_nodes gn_course ON e1.target_node_id = gn_course.id
        JOIN graph_edges e2 ON e2.target_node_id = gn_course.id
        JOIN graph_nodes gn_other ON e2.source_node_id = gn_other.id
        WHERE e1.source_node_id = v_person_node_id
          AND e1.edge_type = 'enrolled_in'
          AND e2.edge_type = 'enrolled_in'
          AND gn_course.node_type = 'course'
          AND gn_other.node_type = 'person'
          AND gn_other.id != v_person_node_id
          AND (gn_other.properties->>'user_id')::UUID != ALL(p_exclude_user_ids)
    ),

    -- Strategy 6: Club matches (users in same club via member_of or leads)
    club_matches AS (
        SELECT
            (gn_other.properties->>'user_id')::UUID AS match_user_id,
            gn_club.name AS shared_name,
            'club_match' AS strategy
        FROM graph_edges e1
        JOIN graph_nodes gn_club ON e1.target_node_id = gn_club.id
        JOIN graph_edges e2 ON e2.target_node_id = gn_club.id
        JOIN graph_nodes gn_other ON e2.source_node_id = gn_other.id
        WHERE e1.source_node_id = v_person_node_id
          AND e1.edge_type IN ('member_of', 'leads')
          AND e2.edge_type IN ('member_of', 'leads')
          AND gn_club.node_type = 'organization'
          AND gn_other.node_type = 'person'
          AND gn_other.id != v_person_node_id
          AND (gn_other.properties->>'user_id')::UUID != ALL(p_exclude_user_ids)
    ),

    -- Strategy 7: Location matches (users based in same location)
    location_matches AS (
        SELECT
            (gn_other.properties->>'user_id')::UUID AS match_user_id,
            gn_location.name AS shared_name,
            'location_match' AS strategy
        FROM graph_edges e1
        JOIN graph_nodes gn_location ON e1.target_node_id = gn_location.id
        JOIN graph_edges e2 ON e2.target_node_id = gn_location.id
        JOIN graph_nodes gn_other ON e2.source_node_id = gn_other.id
        WHERE e1.source_node_id = v_person_node_id
          AND e1.edge_type = 'located_in'
          AND e2.edge_type = 'located_in'
          AND gn_location.node_type = 'location'
          AND gn_other.node_type = 'person'
          AND gn_other.id != v_person_node_id
          AND (gn_other.properties->>'user_id')::UUID != ALL(p_exclude_user_ids)
    ),

    -- Strategy 8: Role matches (users seeking same career role)
    role_matches AS (
        SELECT
            (gn_other.properties->>'user_id')::UUID AS match_user_id,
            gn_role.name AS shared_name,
            'role_match' AS strategy
        FROM graph_edges e1
        JOIN graph_nodes gn_role ON e1.target_node_id = gn_role.id
        JOIN graph_edges e2 ON e2.target_node_id = gn_role.id
        JOIN graph_nodes gn_other ON e2.source_node_id = gn_other.id
        WHERE e1.source_node_id = v_person_node_id
          AND e1.edge_type = 'seeking_role'
          AND e2.edge_type = 'seeking_role'
          AND gn_role.node_type = 'role'
          AND gn_other.node_type = 'person'
          AND gn_other.id != v_person_node_id
          AND (gn_other.properties->>'user_id')::UUID != ALL(p_exclude_user_ids)
    ),

    -- Combine all strategies
    all_matches AS (
        SELECT match_user_id, shared_name, strategy FROM shared_context_matches
        UNION ALL
        SELECT match_user_id, shared_name, strategy FROM domain_bridge_matches
        UNION ALL
        SELECT match_user_id, shared_name, strategy FROM skill_graph_matches
        UNION ALL
        SELECT match_user_id, shared_name, strategy FROM event_matches
        UNION ALL
        SELECT match_user_id, shared_name, strategy FROM course_matches
        UNION ALL
        SELECT match_user_id, shared_name, strategy FROM club_matches
        UNION ALL
        SELECT match_user_id, shared_name, strategy FROM location_matches
        UNION ALL
        SELECT match_user_id, shared_name, strategy FROM role_matches
    ),

    -- Aggregate per user: count matches, collect context, collect strategies
    scored AS (
        SELECT
            am.match_user_id,
            COUNT(DISTINCT am.shared_name)::INT AS _graph_score,
            ARRAY_AGG(DISTINCT am.shared_name) AS _shared_context,
            ARRAY_AGG(DISTINCT am.strategy) AS _match_strategies
        FROM all_matches am
        GROUP BY am.match_user_id
    )

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
        s._graph_score AS graph_score,
        s._shared_context AS shared_context,
        s._match_strategies AS match_strategies
    FROM scored s
    JOIN users u ON u.id = s.match_user_id
    WHERE u.is_onboarded = true
      AND u.phone_number IS NOT NULL
    ORDER BY s._graph_score DESC
    LIMIT p_match_count;
END;
$$;

-- =============================================================================
-- Grant execute permissions
-- =============================================================================

GRANT EXECUTE ON FUNCTION match_users_graph_combined(UUID, UUID[], INT) TO authenticated;
GRANT EXECUTE ON FUNCTION match_users_graph_combined(UUID, UUID[], INT) TO service_role;
