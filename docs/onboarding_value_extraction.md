# Onboarding System (Value Extraction + Persistence)

This doc explains the current onboarding flow with a focus on the value extraction loop and how memory persists across turns when the per-message graph state is discarded.

## High-level Flow
- Entry: `InteractionAgent.process_message` builds a fresh `GraphState` per inbound message (no checkpoints) and hydrates `state.user_profile` from Supabase.
- Router: `app/agents/interaction/router.py` routes to onboarding when `onboarding_stage` is `name|school|career_interest|value_eval` or the user is not onboarded.
- Onboarding graph: `check_status -> extract_input -> (handle_general OR route_stage) -> collect_* -> save_zep -> END`.

## What Persists vs What Resets
### Ephemeral (per message only)
- `GraphState` in memory, including `temp_data` and `conversation_context`.
- `temp_data.onboarding_extraction`: holds LLM extraction results for the *current* message only.

### Persistent
- Supabase `users` table fields (loaded into `state.user_profile` each message).
- `personal_facts` JSON on the user record:
  - `onboarding_stage` (the canonical stage used for routing).
  - `frank_value_eval` (value extraction / evaluation state).
  - flags like `asked_for_name` and `frank_introduced`.
- `users.intro_fee_cents` (latest intro fee used during value-eval).
- Zep memory (if enabled): conversation messages + summary, stored under session ID `{phone_number}_chat`.

The key point: the graph state *is* thrown away after each message, but the onboarding stage and value evaluation state are persisted in the user record (and optionally Zep) and rehydrated on the next message.

## Onboarding Field Extraction (Name / School / Career Interests)
Code: `app/agents/execution/onboarding/nodes/extract_input.py`, `app/agents/execution/onboarding/utils/llm_extractor.py`

1. `extract_onboarding_input` runs first after `check_status`.
2. It calls `extract_onboarding_fields` (LLM) on the latest user message:
   - Outputs JSON: `{name, school, career_interests, confidence, needs_general}`.
   - Normalizes interests and school names.
3. Results are stored in `state.temp_data.onboarding_extraction` with:
   - `source_message`: the exact user message used.
   - `applied_fields`: which fields were written.
4. `apply_extracted_fields` writes to `state.user_profile`, updates `waiting_for`, and *persists* to Supabase via `update_user_profile`.
   - `update_user_profile` stores `onboarding_stage` inside `personal_facts["onboarding_stage"]`.

## Mixed or Off-topic Messages (needs_general)
Code: `app/agents/execution/onboarding/nodes/handle_general_response.py`

If the extractor sets `needs_general=1`:
- The onboarding graph routes into the general graph for an actual answer.
- After the answer, it appends a re-ask for the current missing onboarding field (based on `waiting_for` and stage).
- Onboarding field persistence still happens in `extract_input` / `apply_extracted_fields`.

## Stage Nodes (Name -> School -> Career Interest)
Code: `app/agents/execution/onboarding/nodes/collect_*`

- `collect_name`:
  - Uses `personal_facts["asked_for_name"]` to avoid re-asking.
  - Persists name and advances to `school`.
- `collect_school`:
  - Avoids reusing the same message if it was already consumed for other fields.
  - Persists university and advances to `career_interest`.
- `collect_career_interest`:
  - Requires LLM-extracted interests; if none, it re-prompts.
  - Persists interests and moves to `value_eval`.
  - Seeds the value-eval loop with the initial gate prompt and optional "scoff."

## Value Extraction / Evaluation Loop
Code: `app/agents/execution/onboarding/nodes/collect_value_proof.py`, `app/agents/execution/onboarding/utils/value_proof.py`

### Entry behavior
- The loop state is stored in `personal_facts["frank_value_eval"]`.
- If the user message was just used to fill profile fields (name/school/interests), the value loop does **not** consume it as a value answer:
  - `source_message` and `applied_fields` are used to detect this.
  - The system replies with the first gate prompt instead.

### LLM evaluation
`evaluate_user_value` returns:
- `decision`: `ask | accept | reject`
- `response_text`: Frank response
- `signals`: scores for clarity/credibility/judgment
- `question_type`, `mode`
- `user_value`: extracted "what the user can do" (the value extraction payload)
- `intro_fee_cents`: current intro fee in cents (0-9900)
- `price_note`: optional note about why the fee moved
If `decision == "ask"` and `intro_fee_cents > 0`, the response includes the current fee and how it can drop to $0. Each ask applies a per-turn ceiling so the fee reaches $0 by the final allowed ask; accept forces the fee to $0. The first ask drops the fee below $10 and the displayed fee keeps decimal cents.

### How prior value extraction is stored and reused
`frank_value_eval` in `personal_facts` persists:
- `asked_questions`: all prior value questions asked (prevents repeats).
- `turn_history`: last ~24 turns (short transcript for continuity).
- `user_value`: structured extraction of the user's value.
- `intro_fee_cents`: current intro fee (non-increasing across the value loop).
- `last_result`: last LLM decision + signals + metadata.
- `boundary_asked / breadth_asked / credibility_asked`: tracking flags.
- `status`, `mode`, timestamps.

On each new message, `collect_value_proof` loads this dict from the user record and passes it into `evaluate_user_value`. The LLM then:
- Incorporates the prior `user_value` and merges new value signals via `_merge_user_value`.
- Avoids repeating old questions.
- Decides whether the info is sufficient (`accept`), insufficient (`ask`), or a rejection (`reject`).

### Follow-up rules
- Hard cap: `max_followups_after_initial = 5`
- Minimum followups before accept: `min_followups_before_accept = 3`
- If followups are exhausted, the LLM is forced to decide (accept/reject).

### Decisions and persistence
After each evaluation turn:
- `personal_facts["frank_value_eval"]` is updated and saved to Supabase.
- `onboarding_stage` is persisted in `personal_facts["onboarding_stage"]`.
- `users.intro_fee_cents` is updated with the current intro fee.
- If accepted: stage moves to `needs_eval` (not onboarded yet), value summary appended to `users.value_history` (1-2 sentences).
- If rejected: stage `rejected`.
- If asking: stage `value_eval`, `waiting_for="user_input"`.

## Needs Intake Loop (Post-Accept)
Code: `app/agents/execution/onboarding/nodes/collect_need_proof.py`, `app/agents/execution/onboarding/utils/need_proof.py`

### Entry behavior
- When value is accepted, the system immediately asks the first needs question in the same 2-3 line message.
- Needs loop state is stored in `personal_facts["frank_need_eval"]`.

### LLM evaluation
`evaluate_user_need` returns:
- `decision`: `ask | accept`
- `response_text`: Frank response (2-3 lines)
- `question_type`, `user_need`, `confidence`

### Turn limits
- Min: 2 total question turns (must ask at least one followup after the initial question).
- Max: 4 total question turns (forced accept after cap).

### Completion
- On accept, the system appends a 1-2 sentence needs summary to `users.demand_history`.
- Then it sets `is_onboarded=True` and `onboarding_stage="complete"`.

## Zep Memory (External Context)
Code: `app/agents/execution/onboarding/nodes/save_zep.py`, `app/agents/execution/onboarding/utils/value_proof.py`

- Every onboarding turn is stored to Zep (if enabled) after a response is prepared.
- `evaluate_user_value` pulls recent Zep messages and summary to provide extra context in value evaluation prompts.
- This is the second persistence layer beyond Supabase, and it survives across sessions.

## Key Data Structures (Simplified)
Example `personal_facts` layout:
```json
{
  "onboarding_stage": "value_eval",
  "asked_for_name": false,
  "frank_introduced": true,
  "frank_value_eval": {
    "status": "pending",
    "mode": "evaluating",
    "asked_questions": ["first gate question", "..."],
    "turn_history": [
      {"role": "frank", "content": "..."},
      {"role": "user", "content": "..."}
    ],
    "user_value": {"lanes": ["...", "..."], "claimed_value": "..."},
    "intro_fee_cents": 9900,
    "last_result": {"decision": "ask", "signals": {"clarity": 3, "credibility": 2, "judgment": 1}},
    "updated_at": "2025-01-01T00:00:00Z"
  }
}
```

## Quick Answers to the "State Disappears" Concern
- Yes, the per-message `GraphState` disappears after each message.
- The *actual onboarding memory* is persisted in Supabase:
  - `personal_facts["onboarding_stage"]`
  - `personal_facts["frank_value_eval"]` (value extraction state)
  - `personal_facts["frank_need_eval"]` (needs extraction state)
  - core profile fields (name, university, career_interests, is_onboarded)
  - `users.value_history` (append-only value summaries, 1-2 sentences each)
  - `users.demand_history` (append-only needs summaries, 1-2 sentences each)
- Zep is an optional second memory layer used during value evaluation and general chat.
