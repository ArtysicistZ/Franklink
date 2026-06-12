## 1. Implementation
- [x] 1.1 Add `email_connect` to onboarding stage validation/derivation and waiting_for handling.
- [x] 1.2 Add onboarding node + prompt for email connect; route after `career_interest` and before `needs_eval`.
- [x] 1.3 Integrate ComposioClient to generate auth link and persist connection metadata in `personal_facts`.
- [x] 1.4 Add LLM-based response classification for the email-connect stage (connect vs connected vs unclear).
- [x] 1.5 Advance to `needs_eval` only after LLM confirms the user connected.
- [x] 1.6 Update onboarding transcripts/tests to cover the email-connect stage.
- [ ] 1.5 Update onboarding transcripts/tests to cover the email-connect stage.

## 2. Validation
- [x] 2.1 Run onboarding e2e and verify email connect prompt appears after career interests.
- [x] 2.2 Verify Composio auth link is returned on consent and onboarding does not proceed until the user confirms connection.
