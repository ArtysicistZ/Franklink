# Onboarding LLM Acceleration Plan

This doc captures how the current graph system routes onboarding and the plan to (1) ensure onboarding LLM calls use `gpt-4o-mini` for speed and (2) add a smart extractor that can pull name/school/career_interest from free-form user replies and persist them. Docs live under `support/docs/`.

## Current Graph Shape (Frank)
- `app/agents/interaction/router.py` loads user (DB), infers `onboarding_stage`, and routes: onboarding when stage in `{name, school, career_interest}`, networking for email confirmation, recommendation/general via intent LLM.
- Onboarding (`app/agents/execution/onboarding/graph.py`): linear nodes `check_status -> collect_* -> mark_complete`. Each `collect_*` node simply prompts, sets `waiting_for`, and updates DB via `profile_updater`.
- Recommendation (`app/agents/execution/recommendation/graph_enhanced.py`): memory -> needs_analysis (LLM) -> search -> selection (LLM) -> formatting (LLM).
- Networking (`app/agents/execution/networking/graph.py`): matcher -> LLM selection -> draft/confirm flow.
- General (`app/agents/execution/general/graph.py`): loads memory -> LLM generate -> save to Zep.

## Model Usage Audit (response-generation/intent calls)
- Default Azure deployment is `gpt-5-mini` (from `.env`: `AZURE_OPENAI_DEPLOYMENT_NAME` and `REASONING` both gpt-5-mini).
- Uses gpt-5-mini today (no explicit model override): general graph chat response; networking graph (match selection, present match, draft email, revisions/confirmations); query entity extraction in `user_matcher`; router intent classification.
- Uses gpt-4o-mini today (explicit override): recommendation graph needs_analysis, resource selection, response formatting.
- Action: switch intent classification, general responses, networking responses, and matcher entity extraction to `model="gpt-4o-mini"` so no gpt-5-mini is used for generation.

## Gaps in Onboarding
- No LLM is used; user must reply step-by-step even if they provide multiple fields at once.
- Router depends on `waiting_for` and `onboarding_stage`; we must keep these accurate when auto-filling.
- Azure client defaults to `settings.azure_openai_deployment_name` (currently gpt-5-mini). For any onboarding LLM we add, force `model="gpt-4o-mini"` for latency.

## Implementation Plan
1) **LLM extraction helper (gpt-4o-mini)**
   - Add `app/agents/execution/onboarding/utils/llm_extractor.py` with `async extract_onboarding_fields(message: str, history: list | None, profile: dict) -> ExtractedFields`.
   - Use `AzureOpenAIClient.generate_response` with `model="gpt-4o-mini"`, low temperature, and a strict JSON schema: `{ "name": str|null, "school": str|null, "career_interests": [str], "confidence": 0-1 }` (no LLM "reason").
   - Prompt rules: extract only from user text; prefer exact quotes; avoid hallucination; normalize school names; split interests on commas/slashes; ignore bot text.
   - Optional: accept recent conversation/history so it can recover if the user answers across turns.

2) **Auto-apply extraction to onboarding state (latest message wins)**
   - Add a new node (e.g., `extract_onboarding_input`) after `check_status` that runs the helper on `current_message["content"]` and stores results in `state["temp_data"]["onboarding_extraction"]`.
   - Apply extracted fields regardless of confidence (latest user message overrides prior data). Keep confidence for logging only.
   - Update `user_profile` in-memory, set `onboarding_stage`/`waiting_for` accordingly, and call `update_user_profile` once with all fields + `onboarding_stage` + `is_onboarded` when complete.
   - Support multi-field messages: if name + school + interests are present, jump straight to `mark_complete` and clear `waiting_for`.

3) **Tighten collect_* nodes to leverage extraction**
   - `collect_name`: before prompting, check `temp_data.onboarding_extraction` and set fields immediately (name, plus any school/interests present) even if confidence is low; advance to the next missing field. Only prompt when nothing was extracted.
   - `collect_school`: use `temp_data` extraction; set university and advance.
   - `collect_career_interest`: use extraction; normalize to list; set `is_onboarded=True`, `waiting_for=None`, then thank the user.
   - Keep `personal_facts["asked_for_name"]` logic intact so we don't re-ask unnecessarily.

4) **Routing and persistence safety**
   - Ensure `waiting_for` mirrors the next missing field so `router.should_onboard` and `route_by_pending_action` remain correct.
   - Always apply the latest user-provided values (even low confidence) to state and persist; log when overwriting populated fields.
   - When auto-complete finishes, store `personal_facts["onboarding_stage"]="complete"` and `is_onboarded=True` for future sessions.

5) **Telemetry and validation**
   - Add structured logs for extraction decisions (fields accepted/overwritten with confidence) to debug mis-parses.
   - Add a lightweight unit-style test or harness for the extractor prompt with sample inputs (e.g., "Sam at MIT into AI") to guard JSON format and stage advancement.

6) **Model switches to gpt-4o-mini**
   - `router.classify_intent_node`: pass `model="gpt-4o-mini"`.
   - `general` graph generate node: pass `model="gpt-4o-mini"`.
   - `networking` graph LLM calls (match selection, present_match, draft_email, revise_email, confirmation prompts) and `user_matcher` entity extraction: set `model="gpt-4o-mini"`.
   - Keep recommendation graph on `gpt-4o-mini` as already implemented.

## Notes / Open Questions
- Overwrite rules are now "latest user message wins"; confidence is for observability, not gating.
- If the user explicitly corrects a field, we overwrite even if it was previously set.
- Prompt length: no need to constrain by max tokens for these calls, but keep prompts concise for latency.
