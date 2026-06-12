## Context
Franklink currently routes every message through a graph-first intent classifier. This makes the system fragile when users reply with meta questions, clarification requests, or off-topic responses, and profile updates are applied immediately. OpenPoke demonstrates a more resilient interaction pattern where a front-of-house agent decides when to answer directly and when to delegate to tools.

## Goals / Non-Goals
- Goals: introduce a post-onboarding interaction loop with action selection, unify confirmation handling for updates and match proposals, preserve pending flows without breaking on meta replies, and integrate read-only email context signals.
- Non-Goals: rewrite all graphs end-to-end, send emails via Composio, or change the user-facing tone.

## Decisions
- Add an interaction engine that outputs structured actions (JSON) per message, capped at 2–3 actions.
- Enforce onboarding as a lifecycle gate: pre-onboarded users always route to onboarding; post-complete users never re-enter onboarding.
- Persist `pending_confirmation` in `users.personal_facts` with `type`, `draft`, `prompt`, `expires_at`, and `attempts`.
- Add a universal confirmation reply classifier with outcomes: confirm, decline, modify, unrelated, meta_question.
- Route initiator/target confirmations through the universal classifier; unrelated/meta replies trigger `repair_explain` and re-ask without clearing on first miss.
- Integrate Composio via read-only Gmail tools (no send/draft), with DM-only OAuth initiation and groupchat refusal.
- Expose graph runner as an internal tool for the interaction engine to execute networking/recommendation/general actions as needed.

## Risks / Trade-offs
- The interaction engine could respond directly and skip state updates; mitigate with prompt rules and a fallback to graph execution when intent is unclear.
- Confirmation loops could annoy users; mitigate by clearing after 2 unrelated replies.
- Composio outages or missing credentials; mitigate with clear fallback messaging and feature gating.

## Migration Plan
1) Add interaction engine runtime and action schema.
2) Implement `pending_confirmation` helpers + classifier and update update/networking flows.
3) Add Composio read-only integration and email signals storage.
4) Wire interaction engine into DM and groupchat paths behind a feature flag.
5) Validate key flows and monitor logs.

## Open Questions
None.
