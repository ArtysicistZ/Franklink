# Handling Unrelated Messages During Onboarding (Plan Only)

Goal: If a user replies with content unrelated to the current onboarding question, detect it (intent classification), respond via the general graph, and then re-ask the required onboarding field. Integrate this with the existing onboarding extractor to avoid extra LLM calls. No code has been changed yet; this is the implementation plan.

## Current onboarding flow (recap)
- `check_status` sets `waiting_for` based on `user_profile.onboarding_stage`.
- `extract_input` runs LLM extraction and may advance stages.
- Routing via `route_onboarding_stage` calls one of: `collect_name`, `collect_school`, `collect_career_interest`, `mark_complete`.
- Each `collect_*` node currently assumes the incoming message is relevant; there’s no intent filter inside onboarding.

## Desired behavior
- When onboarding is in progress (stage in {name, school, career_interest}), run a fast relevance check on the incoming message:
  - If the extractor returns non-empty fields (name/school/interests), update them and advance onboarding. If the message also has general asks, set `needs_general=1`.
  - If the extractor finds no onboarding fields but the user has general asks, set `needs_general=1` and do not advance onboarding; reply via general and re-ask the needed field.

## Proposed design (integrated with current extractor for speed)
1) **Fold general-detection into the existing onboarding extractor**
   - Update `extract_onboarding_fields` to also return `needs_general` ∈ {0,1}:
     - `needs_general = 1` if the message contains general asks (non-onboarding content/questions) even if onboarding fields are present.
   - Store `needs_general` in `temp_data["onboarding_extraction"]` alongside extracted fields and `source_message`.
   - This avoids a separate intent LLM call; we reuse the extractor invocation already in the onboarding flow.

2) **Routing branch for general handling**
   - After `extract_input`, add a conditional edge:
     - If `needs_general == 0` → continue to `route_onboarding_stage` (normal flow).
     - If `needs_general == 1` → branch to `handle_general_response`.

3) **Handle off-topic**
   - `handle_onboarding_offtopic` should:
     - Call the general graph (`create_general_conversation_graph`) to produce a reply to the off-topic message. Capture its `response_text`.
     - Append a short follow-up that re-asks the needed field, based on current `onboarding_stage`:
       - stage name → “btw, what’s your name?”
       - stage school → “btw, which school are you at?”
       - stage career_interest → “btw, what careers are you interested in? (comma separated)”
     - Do not change `onboarding_stage`; keep `waiting_for` pointing to the needed field.
     - Do not persist any onboarding fields in this path.

4) **State and temp_data handling**
   - Preserve `temp_data` from the general graph call only as needed (response text); avoid overwriting onboarding state.
   - Ensure `waiting_for` remains set to the current stage’s required input so the next user message is routed back into onboarding.

5) **Failure handling**
   - If the extractor fails to return `related`, default to `related=1` (treat as relevant) to avoid blocking onboarding.
   - If the general graph call fails, fall back to a simple reply plus the re-ask prompt.

6) **Testing scenarios**
   - Onboarding at name stage; user sends “tell me a joke” → classify off-topic → general reply + “btw, what’s your name?”; waiting_for stays `user_input`.
   - Onboarding at school stage; user sends “how are you” → off-topic branch → general reply + “btw, which school are you at?”; waiting_for stays `school`.
   - Onboarding at career_interest stage; user sends “I’m at UPenn” → still relevant to school, but if classifier marks off-topic, re-ask interests; confirm that no interests are saved unless extractor returns them.

## Integration points (no code yet)
- New node(s): `handle_onboarding_offtopic`; reuse `extract_input` for relevance detection (no extra classifier node).
- Graph wiring: after `extract_input`, add conditional edges based on `related`; normal path continues to `route_onboarding_stage`.
- Reuse `create_general_conversation_graph` for off-topic response generation; then append the re-ask prompt.

## Notes
- Keep all persistence (Supabase writes) confined to the normal onboarding nodes; off-topic handling must not call `update_user_profile`.
- Ensure logging captures: extracted fields, `related` flag, chosen branch, and the re-ask prompt used.
- When updating the extractor prompt/schema, require the `related` flag and keep `_normalize_interests` filtering; only persist interests when non-empty and `related=1`.
