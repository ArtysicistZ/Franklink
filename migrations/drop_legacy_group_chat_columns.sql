-- Migration: Drop deprecated legacy columns from group_chats table
-- Purpose: Clean up legacy user_a_id/user_b_id columns after unified storage migration
--
-- PREREQUISITES: Run unified_group_chat_migration.sql FIRST
-- VERIFICATION: Ensure all chats have been migrated to group_chat_participants table
--
-- RUN ORDER: Run this AFTER verifying the unified migration is complete and code is deployed

-- =============================================================================
-- Pre-migration verification queries (run these manually BEFORE dropping columns)
-- =============================================================================
-- Check that all group_chats have participant records:
-- SELECT COUNT(*) as chats_without_participants
-- FROM group_chats gc
-- WHERE NOT EXISTS (
--     SELECT 1 FROM group_chat_participants gcp WHERE gcp.chat_guid = gc.chat_guid
-- );
-- (Should return 0)

-- Check member_count matches actual participant count:
-- SELECT gc.chat_guid, gc.member_count, COUNT(gcp.id) as actual_count
-- FROM group_chats gc
-- LEFT JOIN group_chat_participants gcp ON gcp.chat_guid = gc.chat_guid
-- GROUP BY gc.chat_guid, gc.member_count
-- HAVING gc.member_count != COUNT(gcp.id);
-- (Should return 0 rows)

-- =============================================================================
-- Step 1: Drop deprecated columns
-- =============================================================================
ALTER TABLE group_chats DROP COLUMN IF EXISTS user_a_id;
ALTER TABLE group_chats DROP COLUMN IF EXISTS user_b_id;
ALTER TABLE group_chats DROP COLUMN IF EXISTS user_a_mode;
ALTER TABLE group_chats DROP COLUMN IF EXISTS user_b_mode;

-- =============================================================================
-- Post-migration verification
-- =============================================================================
-- Verify columns are dropped:
-- SELECT column_name FROM information_schema.columns
-- WHERE table_name = 'group_chats'
-- ORDER BY ordinal_position;
-- (Should NOT include user_a_id, user_b_id, user_a_mode, user_b_mode)
