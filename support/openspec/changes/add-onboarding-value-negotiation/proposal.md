## Why
Value evaluation feels templated and less viral than it should. Adding light humor/roast and a gamified intro fee negotiation makes the loop more engaging while keeping the high-signal bar.

## What Changes
- Add intro fee negotiation to value-eval; the LLM proposes a fee each turn, bounded and non-increasing.
- Persist intro fee state across sessions in `personal_facts["frank_value_eval"]` and `users.intro_fee_cents` to keep progress.
- Update value-eval prompts for more humor/roast, varied phrasing, and a hard ban on em dash output.
- Surface the negotiated fee in state so it can be used by future payment handoff.

## Impact
- Affected specs: onboarding
- Affected code: `app/graphs/onboarding/prompts/value_stage_frank.py`, `app/graphs/onboarding/utils/value_proof.py`, `app/graphs/onboarding/nodes/collect_value_proof.py`, `app/graphs/state.py`, `app/graphs/onboarding/utils/payment_handler.py`, `app/database/client/users.py`, `app/database/models.py`
