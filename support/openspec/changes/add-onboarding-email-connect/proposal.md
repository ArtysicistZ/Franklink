## Why
Users want Franklink to learn from their work/school inbox context for better matching, so we should require Gmail connection during onboarding while intent is high.

## What Changes
- Add an `email_connect` onboarding stage after `career_interest` and before `needs_eval`.
- Prompt the user to connect a work/school Gmail via Composio, include the auth link in the initial prompt, and store connection status in `personal_facts`.
- Advance to needs evaluation only after the user confirms they have connected their inbox.
- Use LLM-based classification for email-connect responses (no rule-based skip handling).
- Update onboarding routing/state handling to recognize the new stage.

## Impact
- Affected specs: onboarding
- Affected code: `app/agents/execution/onboarding/*`, `app/agents/interaction/router.py`, `app/agents/interaction/agent.py`, `app/integrations/composio_client.py`, onboarding test scripts
