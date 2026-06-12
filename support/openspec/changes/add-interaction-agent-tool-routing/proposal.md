## Why
The current graph-first router is brittle: users can derail flows with meta questions or off-topic replies, and updates are applied immediately without confirmation. We need an OpenPoke-inspired interaction layer that dynamically chooses actions post-onboarding, preserves confirmations across turns, and integrates read-only email context for smarter matchmaking.

## What Changes
- Add a post-onboarding interaction loop that outputs structured actions (respond, repair_explain, draft_profile_update, propose_match, connect_email, fetch_email_context) with a 2–3 step cap.
- Introduce a universal `pending_confirmation` state machine for profile updates and match proposals, including a classifier for confirm/decline/modify/unrelated/meta.
- Change profile updates to draft + confirm before applying demand/value changes.
- Harden match proposal confirmation so meta/unrelated replies never break the flow.
- Add read-only Composio Gmail context (connect, fetch threads, store summarized signals) and ensure group chats never initiate inbox connect.
- Add a shared interaction engine for DM and groupchat paths, with stricter privacy rules in groups.

## Impact
- Affected specs: interaction-routing, confirmation, email-context
- Affected code: app/orchestrator.py, app/groupchat/runtime/router.py, app/graphs/update/graph.py, app/graphs/networking/*, app/interaction_engine/*, app/integrations/composio_client.py, prompts, config flags, logging
