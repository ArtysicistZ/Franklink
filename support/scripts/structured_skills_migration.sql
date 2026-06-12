-- Migration: Add structured skill/need columns for complementary matching
-- Purpose: Enable supply-demand matching beyond embedding similarity
--
-- These arrays allow direct set-intersection matching:
--   "Find users whose offering_skills overlap with my seeking_skills"
-- This catches complementary matches (founder <-> marketer) that
-- cosine similarity over embeddings would miss.

-- =============================================================================
-- Add structured skill columns to users table
-- =============================================================================

ALTER TABLE users ADD COLUMN IF NOT EXISTS seeking_skills TEXT[] DEFAULT '{}';
ALTER TABLE users ADD COLUMN IF NOT EXISTS offering_skills TEXT[] DEFAULT '{}';
ALTER TABLE users ADD COLUMN IF NOT EXISTS seeking_relationship_types TEXT[] DEFAULT '{}';
ALTER TABLE users ADD COLUMN IF NOT EXISTS offering_relationship_types TEXT[] DEFAULT '{}';

-- GIN indexes for array containment/overlap queries (&&, @>, <@)
CREATE INDEX IF NOT EXISTS idx_users_seeking_skills
    ON users USING GIN (seeking_skills)
    WHERE seeking_skills IS NOT NULL AND seeking_skills != '{}';

CREATE INDEX IF NOT EXISTS idx_users_offering_skills
    ON users USING GIN (offering_skills)
    WHERE offering_skills IS NOT NULL AND offering_skills != '{}';

CREATE INDEX IF NOT EXISTS idx_users_seeking_relationship_types
    ON users USING GIN (seeking_relationship_types)
    WHERE seeking_relationship_types IS NOT NULL AND seeking_relationship_types != '{}';

CREATE INDEX IF NOT EXISTS idx_users_offering_relationship_types
    ON users USING GIN (offering_relationship_types)
    WHERE offering_relationship_types IS NOT NULL AND offering_relationship_types != '{}';
