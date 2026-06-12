# Onboarding Value + Need Expansion Plan (No Code Changes Yet)

This plan covers the three requested features:
1) Summarize accepted user value into `users.value_history` (append-only JSON).
2) Add a new needs-intake loop (ask/accept only, min 2 turns, max 4).
3) Summarize accepted user needs into `users.demand_history` (append-only JSON), then mark onboarding complete.

This document is based on the current onboarding flow and storage logic in the codebase.

## Current Flow (Baseline)
- Per-message graph state is ephemeral; persistent data lives in Supabase `users` and `personal_facts`.
- `collect_value_proof` stores value-eval state in `personal_facts["frank_value_eval"]`.
- On value accept, `collect_value_proof` currently sets `is_onboarded=True` and `onboarding_stage="complete"`.
- `onboarding_stage` is persisted inside `personal_facts["onboarding_stage"]`.

## New Stages and Storage Keys (Proposed)
### Onboarding stages
- Add a new stage: `needs_eval`.
- Flow becomes: `name -> school -> career_interest -> value_eval -> needs_eval -> complete`.

### New persistent keys (personal_facts)
- `frank_need_eval`: JSON state for the needs loop (parallel to `frank_value_eval`).
- Optional: store `value_summary` and `need_summary` inside `personal_facts` for redundancy (in addition to `value_history` / `demand_history`).

## Feature 1: Value Summary on Accept
### Trigger
When value evaluation returns `decision == "accept"` in `collect_value_proof`.

### Inputs to the summary LLM
Must include all value-related context:
- `personal_facts["frank_value_eval"]` (includes `user_value`, `asked_questions`, `turn_history`, `last_result`).
- The full `turn_history` list (from that same block).
- Current profile fields (`name`, `university`, `career_interests`).
  - No Zep usage for now.

### Output
- A short, clear, text-only summary of the user's value (1-2 sentences, user's wording lightly trimmed).
- Store this as a new entry in `users.value_history` (JSON array of `{text, created_at}`).
- Keep `personal_facts["frank_value_eval"]["user_value"]` as structured data.

### Storage changes
Implementation will require:
- New Supabase columns: `value_history` and `demand_history` (jsonb arrays) on `users`.
- Add `value_history` and `demand_history` to `User` model and `UserProfileState`.
- Extend `DatabaseClient.update_user_profile` allowed fields to include `value_history` and `demand_history`.
- Ensure graph state hydration reads these new columns.

## Feature 2: Needs Intake Loop (Ask/Accept Only)
### Summary
After value is accepted and summarized, onboarding transitions to `needs_eval` and starts a new LLM loop to gather needs.

### Behavior requirements
- Only two decisions: `ask` or `accept` (no reject).
- Min 2 turns, max 4 turns for the loop.
  - Interpretation: total assistant questions in the loop.
  - Implementation mapping:
    - `MAX_FOLLOWUPS_AFTER_INITIAL = 3` (initial + 3 followups = 4 turns max).
    - `MIN_FOLLOWUPS_BEFORE_ACCEPT = 1` (must ask at least one followup after the initial question).
- If max turns are reached, force `accept` with best available summary.

### Storage
Persist a new block in `personal_facts["frank_need_eval"]`, similar to `frank_value_eval`:
```
{
  "status": "pending"|"accepted",
  "mode": "gathering"|"closing",
  "asked_questions": [...],
  "turn_history": [...],
  "user_need": {...},
  "last_result": {...},
  "updated_at": "ISO timestamp",
  "followups_used": 0,
  "followups_remaining": 3
}
```

### LLM prompt requirements
Create a new prompt and evaluator that:
- Keeps Frank's tone consistent.
- Asks about:
  - Who they want to meet
  - What skills or outcomes they want from networking
  - Constraints (role, industry, seniority, geography, timing)
- Returns JSON only, with:
  - `decision`: "ask" or "accept"
  - `response_text`
  - `user_need`: dict (structured)
  - `question_type`: small enum (targets, skills, constraints, timeline)
  - `confidence`: 0-1
- Response text format: 2-3 lines, confirmatory tone.

## Graph and Routing Changes
### Add a new node
- New node `collect_need_proof` analogous to `collect_value_proof`.
- Add a new prompt module for needs evaluation.

### Route logic updates
- Add `needs_eval` to `OnboardingStage`.
- Route `needs_eval` to `collect_need_proof`.
- In `check_status`, set `waiting_for="user_input"` for `needs_eval`.
- In `handle_general_response`, add a re-ask branch for `needs_eval`.
- In main router, treat `needs_eval` as onboarding (not fully onboarded).

## Transition from Value Accept to Needs Loop
On value accept:
- Call value summary LLM and append to `value_history`.
- Set `onboarding_stage="needs_eval"` (not complete).
- Keep `is_onboarded=False`.
- Combine acceptance + first needs question in a single response message (2-3 lines).
  - Example structure: line 1 accept, line 2 short framing, line 3 single needs question.
- This requires generating the first needs question immediately (either via a small helper or by calling the needs-eval LLM in "initial ask" mode).

Note: the current value-eval prompt enforces no question marks on accept. The combined acceptance + needs question should be built in `collect_value_proof` after the value decision is `accept`, rather than relying on the value-eval LLM response verbatim.

## Feature 3: Needs Summary + Complete Onboarding
### Trigger
When needs loop returns `decision == "accept"` in `collect_need_proof`.

### Inputs to the summary LLM
- `personal_facts["frank_need_eval"]` (includes `user_need`, `turn_history`, `asked_questions`).
- The full `turn_history` list.
- Current profile fields and any previously known context.
  - No Zep usage for now.

### Output and persistence
- Summary text goes to `users.demand_history` as a new entry (1-2 sentences, user's wording lightly trimmed).
- Optionally store summary inside `personal_facts`.
- Set:
  - `is_onboarded=True`
  - `onboarding_stage="complete"`
  - `waiting_for=None`

### Additional behavior
- Consider calling `generate_career_interest_embedding` after final accept (same as current complete flow).
- Leave `mark_complete` node behavior as-is; `collect_need_proof` can call the embedding directly when it completes onboarding.

## Error Handling and Safety
- If summary LLM fails: store a safe fallback summary (e.g., from `user_value` or last user message) and continue.
- If needs loop LLM returns invalid JSON: default to `ask` with a safe prompt; never reject.
- If max turns reached: force accept and summarize best available data.

## Database Changes Checklist
- Add `value_history` JSONB column to `users` (default empty array).
- Add `demand_history` JSONB column to `users` (default empty array).
- Update allowed update fields and model fields accordingly.
- Backfill defaults as NULL (no changes to existing users).

## Validation / Testing Plan
- Manual flow for:
  - Early accept with minimal data (ensure summary fallback works).
  - Needs loop hits max turns (forced accept).
  - Off-topic messages during needs (general response + re-ask).
- Logging:
  - Log `followups_used` / `remaining` for needs loop.
  - Log summary output length and storage success.

## Open Questions to Confirm Before Implementation
- None. Summary style confirmed: 1-2 sentences for entries in `value_history` and `demand_history`.
