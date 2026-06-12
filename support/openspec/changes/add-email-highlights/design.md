## Context
We already ingest and store raw emails in `user_emails` after the Gmail connection flow, but we do not persist the connected address or derive a curated set of high-signal emails for agent use.

## Goals / Non-Goals
- Goals: persist connected Gmail address into `users.email`, derive key email highlights from stored raw emails, and store them in a new table with idempotent writes.
- Non-Goals: LLM-based summarization, outbound email sending, or altering Gmail permissions.

## Decisions
- Decision: Store the connected Gmail address in `users.email` when email connection is confirmed.
- Decision: Process `user_emails` with deterministic keyword matching and ad suppression; always keep outbound emails.
- Decision: Persist results to `user_email_highlights` with a unique `(user_id, message_id)` constraint and an `is_from_me` flag.

## Risks / Trade-offs
- If the Gmail address is unavailable from Composio metadata, outbound detection will be skipped until available.
- Keyword matching may miss nuanced signals; keep rules simple and add tuning hooks for later.

## Migration Plan
- Add the new table via SQL migration or Supabase update.
- Backfill highlights by running the helper for existing users (optional, offline).

## Open Questions
- Confirm the table name `user_email_highlights`.
- No additional scoring fields will be stored for highlights.
