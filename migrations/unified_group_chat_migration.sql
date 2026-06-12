-- Migration: Unify group chat storage model
-- Purpose: Ensure ALL group chats (2-person and multi-person) use the same storage pattern
--
-- BEFORE: 2-person chats stored with user_a_id/user_b_id in group_chats table
--         Multi-person chats stored only in group_chat_participants table
-- AFTER:  ALL chats have a group_chats record (identity) + group_chat_participants records (membership)
--
-- RUN ORDER: Run this BEFORE deploying code changes
-- PREREQUISITES: group_chats and group_chat_participants tables must exist

-- =============================================================================
-- Step 1: Make user_a_id and user_b_id nullable (they're deprecated)
-- =============================================================================
ALTER TABLE group_chats ALTER COLUMN user_a_id DROP NOT NULL;
ALTER TABLE group_chats ALTER COLUMN user_b_id DROP NOT NULL;
ALTER TABLE group_chats ALTER COLUMN user_a_mode DROP NOT NULL;
ALTER TABLE group_chats ALTER COLUMN user_b_mode DROP NOT NULL;

-- =============================================================================
-- Step 2: Add member_count column to group_chats
-- =============================================================================
ALTER TABLE group_chats ADD COLUMN IF NOT EXISTS member_count INTEGER DEFAULT 2;

-- =============================================================================
-- Step 3: Backfill existing 2-person chats into group_chat_participants
-- This ensures legacy chats have participant records
-- =============================================================================

-- Insert user_a as participant (if not already exists)
INSERT INTO group_chat_participants (chat_guid, user_id, role, mode, connection_request_id, joined_at, created_at)
SELECT
    gc.chat_guid,
    gc.user_a_id,
    'member',
    COALESCE(gc.user_a_mode, 'active'),
    gc.connection_request_id,
    gc.created_at,
    gc.created_at
FROM group_chats gc
WHERE gc.user_a_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM group_chat_participants gcp
      WHERE gcp.chat_guid = gc.chat_guid AND gcp.user_id = gc.user_a_id
  );

-- Insert user_b as participant (if not already exists)
INSERT INTO group_chat_participants (chat_guid, user_id, role, mode, connection_request_id, joined_at, created_at)
SELECT
    gc.chat_guid,
    gc.user_b_id,
    'member',
    COALESCE(gc.user_b_mode, 'active'),
    gc.connection_request_id,
    gc.created_at,
    gc.created_at
FROM group_chats gc
WHERE gc.user_b_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM group_chat_participants gcp
      WHERE gcp.chat_guid = gc.chat_guid AND gcp.user_id = gc.user_b_id
  );

-- =============================================================================
-- Step 4: Create group_chats records for multi-person chats that only exist in participants table
-- This ensures all chats have an identity record
-- =============================================================================
INSERT INTO group_chats (chat_guid, member_count, created_at)
SELECT DISTINCT
    gcp.chat_guid,
    (SELECT COUNT(*) FROM group_chat_participants p WHERE p.chat_guid = gcp.chat_guid),
    MIN(gcp.created_at)
FROM group_chat_participants gcp
WHERE NOT EXISTS (
    SELECT 1 FROM group_chats gc WHERE gc.chat_guid = gcp.chat_guid
)
GROUP BY gcp.chat_guid;

-- =============================================================================
-- Step 5: Update member_count for all existing group_chats records
-- =============================================================================
UPDATE group_chats gc
SET member_count = (
    SELECT COUNT(*) FROM group_chat_participants gcp WHERE gcp.chat_guid = gc.chat_guid
)
WHERE EXISTS (
    SELECT 1 FROM group_chat_participants gcp WHERE gcp.chat_guid = gc.chat_guid
);

-- For chats without any participants yet (shouldn't happen, but safety), default to 2
UPDATE group_chats
SET member_count = 2
WHERE member_count IS NULL OR member_count = 0;

-- =============================================================================
-- Step 6: Create index for member_count queries (optional optimization)
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_group_chats_member_count ON group_chats(member_count);

-- =============================================================================
-- Verification queries (run these manually to verify migration success)
-- =============================================================================
-- Check group_chats count: SELECT COUNT(*) FROM group_chats;
-- Check participants count: SELECT COUNT(*) FROM group_chat_participants;
-- Verify all chats have participants:
--   SELECT gc.chat_guid, gc.member_count, COUNT(gcp.id) as actual_count
--   FROM group_chats gc
--   LEFT JOIN group_chat_participants gcp ON gcp.chat_guid = gc.chat_guid
--   GROUP BY gc.chat_guid, gc.member_count
--   HAVING gc.member_count != COUNT(gcp.id);
-- (Should return 0 rows if migration is correct)
