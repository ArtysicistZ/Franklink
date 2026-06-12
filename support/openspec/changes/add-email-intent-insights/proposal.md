## Why
Email highlights are stored as individual messages, but they are not summarized into a small set of user intents. We need a structured, event-level view of what a user is working on or cares about so other agents can use it directly and the data is smaller than raw highlights.

## What Changes
- Add a new table to store event-level email intent insights derived from highlights.
- Add LLM processing that groups multiple highlights into a single intent event.
- Make the processing idempotent and safe to run repeatedly.
- Limit LLM output to `event_key`, `intent_summary`, and `status`, and only pass title, sender, is_from_me, and content as input.
- Integrate the processing after highlights are stored, feed the LLM output to the onboarding response agent, and add a backfill script.

## Impact
- Affected specs: new capability `email-intent-insights`.
- Affected code: email highlight pipeline, onboarding email connect flow, new DB client methods, backfill scripts.
