-- Migration: Cross-user knowledge graph tables
-- Purpose: Store a relational graph (nodes + edges) for multi-hop matching
--
-- The graph is populated from Zep facts via LLM classification.
-- Shared nodes (skills, orgs, domains, projects) enable cross-user queries
-- like "find users who attend the same org AND have skills I need."
--
-- Node types: person, skill, organization, domain, project
-- Edge types: needs, offers, attends, interested_in, works_on, seeking_role

-- =============================================================================
-- graph_nodes: Vertices in the knowledge graph
-- =============================================================================

CREATE TABLE IF NOT EXISTS graph_nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    node_type TEXT NOT NULL,       -- 'person', 'skill', 'organization', 'domain', 'project'
    name TEXT NOT NULL,            -- normalized lowercase-hyphenated
    properties JSONB DEFAULT '{}', -- type-specific data (user_id for person, description, etc.)
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(node_type, name)        -- prevents duplicate "marketing" skill nodes
);

-- =============================================================================
-- graph_edges: Directed relationships between nodes
-- =============================================================================

CREATE TABLE IF NOT EXISTS graph_edges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_node_id UUID NOT NULL REFERENCES graph_nodes(id) ON DELETE CASCADE,
    target_node_id UUID NOT NULL REFERENCES graph_nodes(id) ON DELETE CASCADE,
    edge_type TEXT NOT NULL,       -- 'needs', 'offers', 'attends', 'interested_in', 'works_on', 'seeking_role'
    properties JSONB DEFAULT '{}', -- edge-specific data (urgency, experience_level, context)
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(source_node_id, target_node_id, edge_type)
);

-- =============================================================================
-- Indexes for efficient graph traversal
-- =============================================================================

-- Node lookups by type and name
CREATE INDEX IF NOT EXISTS idx_graph_nodes_type
    ON graph_nodes(node_type);

CREATE INDEX IF NOT EXISTS idx_graph_nodes_type_name
    ON graph_nodes(node_type, name);

-- Fast person node lookup by user_id (stored in properties)
CREATE INDEX IF NOT EXISTS idx_graph_nodes_person_user_id
    ON graph_nodes((properties->>'user_id'))
    WHERE node_type = 'person';

-- Edge traversal in both directions
CREATE INDEX IF NOT EXISTS idx_graph_edges_source
    ON graph_edges(source_node_id);

CREATE INDEX IF NOT EXISTS idx_graph_edges_target
    ON graph_edges(target_node_id);

-- Edge type filtering
CREATE INDEX IF NOT EXISTS idx_graph_edges_type
    ON graph_edges(edge_type);

-- Composite index for common traversal pattern: outgoing edges by type
CREATE INDEX IF NOT EXISTS idx_graph_edges_source_type
    ON graph_edges(source_node_id, edge_type);

-- =============================================================================
-- RLS policies
-- =============================================================================

ALTER TABLE graph_nodes ENABLE ROW LEVEL SECURITY;
ALTER TABLE graph_edges ENABLE ROW LEVEL SECURITY;

-- Service role has full access (used by backend)
CREATE POLICY graph_nodes_service_all ON graph_nodes
    FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY graph_edges_service_all ON graph_edges
    FOR ALL TO service_role USING (true) WITH CHECK (true);

-- Authenticated users can read (for client-side queries if needed)
CREATE POLICY graph_nodes_auth_select ON graph_nodes
    FOR SELECT TO authenticated USING (true);

CREATE POLICY graph_edges_auth_select ON graph_edges
    FOR SELECT TO authenticated USING (true);

-- =============================================================================
-- Grant permissions
-- =============================================================================

GRANT SELECT, INSERT, UPDATE, DELETE ON graph_nodes TO service_role;
GRANT SELECT ON graph_nodes TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON graph_edges TO service_role;
GRANT SELECT ON graph_edges TO authenticated;
