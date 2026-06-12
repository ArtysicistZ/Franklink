## Context
Onboarding currently collects name → school → career interests → needs → value. We want to insert a mandatory Gmail connect step after career interests to capture inbox context via Composio without disrupting the onboarding gate.

## Goals / Non-Goals
- Goals:
  - Insert a new `email_connect` stage after `career_interest` and before `needs_eval`.
  - Use Composio to generate a Gmail auth link and store connection status.
  - Require inbox connection before onboarding can advance to needs evaluation.
- Non-Goals:
  - Sending email or drafting messages.
  - Using inbox context during onboarding itself (just collect the connection).

## Decisions
- Stage name: `email_connect` with waiting_for `email_connect`.
- Store connection metadata in `personal_facts["email_connect"]` (status, updated_at, last_prompt_at).
- Send the Composio auth link in the first `email_connect` prompt and require a "done" confirmation before advancing.
- Use an LLM-only classifier for connect stage replies (connect, connected/done, unclear). No rule-based skip handling.

## Risks / Trade-offs
- Users may resist the mandatory connect step; UX friction risk.

## Migration Plan
- Add the new stage and node.
- Update routing/derivation to recognize `email_connect`.
- Add/adjust tests to verify sequence.

## Open Questions
- None.
