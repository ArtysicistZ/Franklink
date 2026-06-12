-- Migration: Remove user_email_intent_events table
-- This table is no longer used after migrating to Zep-based signal extraction.
--
-- Previous flow (REMOVED):
--   1. email_highlights → process_email_intent_events_from_highlights() → user_email_intent_events
--   2. user_email_intent_events → extract_top_signals() → signals
--
-- New flow (Zep-based):
--   1. Zep search_graph() + get_user_context() → extract_signals_from_zep() → signals
--
-- The intent_events intermediate table is no longer populated or queried.
-- Email highlights are now synced directly to Zep's knowledge graph.

-- Drop the table
DROP TABLE IF EXISTS user_email_intent_events CASCADE;

-- Also drop any related functions if they exist
DROP FUNCTION IF EXISTS upsert_user_email_intent_events(uuid, jsonb);
DROP FUNCTION IF EXISTS upsert_user_email_intent_events_v1(uuid, jsonb);
