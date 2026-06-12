## Context
We already store raw emails and filtered highlights, and onboarding can call an LLM to extract immediate insights. Those insights are not persisted and are not structured as event-level records.

## Goals / Non-Goals
- Goals: derive event-level email intent insights from highlights, persist them in a dedicated table, and make the process idempotent and safe to rerun.
- Non-Goals: sending emails, modifying Gmail permissions, or building a UI for the insights.

## Decisions
- Decision: Create a new table `user_email_intent_events` with a unique key per user and event.
- Decision: Use LLM output in a strict JSON schema with only `event_key`, `intent_summary`, and `status`.
- Decision: Feed only title, sender, is_from_me, and content into the LLM prompt.
- Decision: Merge new events into existing rows when the event key matches, so the total rows are fewer than highlights.
- Decision: Run the processor after new highlights are inserted, pass the LLM output to the onboarding response agent, and provide a backfill script for existing highlights.

## Data Model (proposed)
Table: `user_email_intent_events`
- id (uuid, primary key)
- user_id (uuid, fk)
- event_key (text)  # deterministic key for an intent group
- status (text)     # e.g. active, completed, stalled
- intent_summary (text)
- first_seen_at (timestamptz)
- last_seen_at (timestamptz)
- created_at, updated_at (timestamptz)

## LLM Output Schema (proposed)
```
{
  "events": [
    {
      "event_key": "job_search_internship",
      "status": "active",
      "intent_summary": "user is interviewing for summer internships"
    }
  ]
}
```

## Idempotency and Deduplication
- Unique constraint on `(user_id, event_key)`.
- On insert conflict, update `intent_summary`, `status`, and `last_seen_at`.

## Migration Plan
1) Create the new table and indexes.
2) Add DB client methods for upsert and query.
3) Add processor that consumes highlights and writes events.
4) Integrate into onboarding after highlight insert.
5) Backfill existing highlights.

## Open Questions
- Final table name (`user_email_intent_events` vs `user_email_intents`).
- Retention policy for completed or stale events.
