-- Migration: Add display_name column to group_chats table
-- Purpose: Store the group chat display name (e.g., "Alex & Sam" or "Algo Trading Study Group")
--
-- This column stores the same name that's set in iMessage via Photon,
-- allowing queries to retrieve group chats with their human-readable names.

ALTER TABLE group_chats ADD COLUMN IF NOT EXISTS display_name TEXT;

-- Optional: Create index for display_name queries
CREATE INDEX IF NOT EXISTS idx_group_chats_display_name ON group_chats(display_name);
