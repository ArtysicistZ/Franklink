## Why
We need a structured way to capture and persist a concise value and need summary during onboarding, and to collect networking needs in a controlled loop after value acceptance.

## What Changes
- Add value summary generation on value acceptance and store it in `users.all_value` (text)
- Introduce a new `needs_eval` onboarding stage with an ask/accept loop (min 2 turns, max 4)
- Summarize accepted needs into `users.all_demand` (text) and then mark onboarding complete
- Persist needs-loop state in `personal_facts["frank_need_eval"]`
- Combine value acceptance + first needs question in one 2-3 line response

## Impact
- Affected specs: onboarding
- Affected code: onboarding graph nodes, onboarding utils/prompts, state hydration, user profile update, DB schema
