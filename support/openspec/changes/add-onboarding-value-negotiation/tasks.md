## 1. Decisions
- [x] 1.1 Confirm intro fee bounds and whether steps are fixed or continuous.
- [x] 1.2 Confirm storage location (personal_facts only vs new `users.intro_fee_cents` column).

## 2. Implementation
- [x] 2.1 Add `intro_fee_cents` column to `users` and update models/hydration.
- [x] 2.2 Update value-eval prompts to add humor/roast, varied phrasing, and no em dash output.
- [x] 2.3 Extend value-eval LLM output schema to include `intro_fee_cents` and optional `price_note`.
- [x] 2.4 Parse fee output, clamp to bounds, and enforce non-increasing behavior.
- [x] 2.5 Persist fee in `personal_facts["frank_value_eval"]` and `users.intro_fee_cents`.
- [x] 2.6 Surface fee in onboarding state for future payment handoff.
- [x] 2.7 Update docs/tests for the negotiated fee messaging.

## 3. Validation
- [ ] 3.1 Manual: initial value prompt mentions $99 and how to earn $0.
- [ ] 3.2 Manual: fee decreases or stays the same across followups; never increases.
- [ ] 3.3 Manual: response_text contains no em dash or en dash characters.
- [ ] 3.4 Manual: fee persists across sessions (personal_facts reload).
