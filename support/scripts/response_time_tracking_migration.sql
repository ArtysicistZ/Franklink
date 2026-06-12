-- Migration: Add group_created_at column to connection_requests
-- Purpose: Track when group chats are created after connection acceptance
--
-- RUN ORDER: Run this after connection_requests table exists

-- =============================================================================
-- Add group_created_at column to connection_requests table
-- =============================================================================

-- group_created_at: When the group chat was successfully created
-- (Set when status changes to GROUP_CREATED)
ALTER TABLE connection_requests
ADD COLUMN IF NOT EXISTS group_created_at TIMESTAMPTZ;

-- Index for metrics queries on group creation times
CREATE INDEX IF NOT EXISTS connection_requests_group_created_idx
    ON connection_requests(group_created_at)
    WHERE group_created_at IS NOT NULL;
