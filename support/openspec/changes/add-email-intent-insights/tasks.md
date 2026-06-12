## 1. Implementation
- [x] 1.1 Add a new table `user_email_intent_events` with a unique constraint on (user_id, event_key).
- [x] 1.2 Add DB client methods to upsert and list intent events.
- [x] 1.3 Implement an LLM processor that groups highlights into event-level intents and validates JSON output (event_key, intent_summary, status).
- [x] 1.4 Merge new events into existing rows by event_key (update summary, status, last_seen_at).
- [x] 1.5 Integrate the processor after highlights are stored in onboarding and pass LLM output to the response agent.
- [x] 1.6 Add a backfill script that processes existing highlights.
- [ ] 1.7 Add tests for JSON validation, idempotent upserts, and onboarding integration.
