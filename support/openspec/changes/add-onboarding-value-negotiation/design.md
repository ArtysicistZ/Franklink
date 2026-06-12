## Context
Value evaluation needs more variety and a visible progress mechanic. Introducing an intro fee that drops as value becomes clear creates a simple gamified loop without changing core onboarding logic.

## Goals / Non-Goals
- Goals: have the LLM propose an intro fee each turn, persist it across sessions, keep it non-increasing within the loop, add humor/roast, and ban em dash output.
- Non-Goals: implement Stripe checkout or change subscription tiers in this change.

## Decisions
- Store `intro_fee_cents` in `personal_facts["frank_value_eval"]`, `users.intro_fee_cents`, and mirror it in onboarding state for easy access.
- The LLM returns `intro_fee_cents` and an optional `price_note`; the system clamps the fee to configured bounds (default 0-9900) and never allows increases.
- Each ask turn applies a computed per-turn ceiling so the fee always reaches 0 by the final allowed ask; accept forces fee to 0 immediately.
- The fee drops below $10 on the first ask turn and keeps decimal cents in the displayed amount.
- Fee values are continuous within bounds (not fixed steps).
- Initial fee defaults to 9900 when no prior fee exists; floor is 0.
- Sanitize em dash and en dash characters in response_text before sending.

## Risks / Trade-offs
- LLM may ignore price constraints; clamping and repair passes mitigate.
- Added humor can reduce clarity; keep roasts brief and respectful.

## Migration Plan
1) Add `intro_fee_cents` column to `users`.
2) Update prompts and output parsing to include intro fee fields.
3) Persist fee in personal_facts, db, and surface it in state.
4) Add response sanitization for em dash and en dash characters.

## Open Questions
None (decisions confirmed).
