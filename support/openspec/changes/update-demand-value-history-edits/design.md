## Context
Value history is append-only today. Users can retract or correct earlier value claims, but the system keeps outdated entries and derived fields.

## Goals / Non-Goals
- Goals:
  - Support value_history edits (append, replace, remove, clear) driven by user updates.
  - Preserve created_at for retained entries when editing history.
  - Apply the behavior across all flows that call apply_demand_value_updates.
  - Refresh all_value and embeddings from the edited history.
  - Record the applied edit plan in user profile metadata for traceability.
- Non-Goals:
  - Editing demand_history (remains append-only).
  - Manual or admin-driven history edits.
  - Backfilling or rewriting historical data outside user-triggered updates.

## Decisions
- Decision: Introduce an LLM-driven value edit plan that compares value_update text with the current value_history and outputs edit actions (append, replace, remove, clear).
- Decision: Apply the edit plan with a deterministic helper that validates indices, preserves created_at for retained entries, and falls back to append on invalid edits.
- Decision: A "clear" action removes all value_history entries and resets all_value/value_embedding accordingly.
- Decision: Store the edit plan plus timestamp in user_profile.metadata (keyed under value_history_edits).

## Alternatives considered
- Full history rewrite from LLM output (simpler shape, but loses created_at and increases risk of unintended changes).
- Rule-based diffing (too brittle for natural language edits and retractions).

## Risks / Trade-offs
- Over-editing or excessive deletions from LLM output.
  - Mitigation: conservative prompt, validation of indices, and fallback to append.

## Migration Plan
No data migration. Changes apply only to new value updates.

## Open Questions
None.
