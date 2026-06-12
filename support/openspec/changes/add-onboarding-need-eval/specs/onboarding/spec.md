## ADDED Requirements
### Requirement: Value Summary Persistence
The system SHALL summarize accepted user value into a 2-3 line text summary and store it in `users.all_value` after the value-eval loop accepts the user.

#### Scenario: Value accepted
- **WHEN** the value-eval loop returns `decision=accept`
- **THEN** the system stores a 2-3 line summary in `users.all_value`

### Requirement: Needs Evaluation Loop
The system SHALL run a needs-eval loop after value acceptance, using an ask/accept-only LLM with 2-4 total question turns, and persist loop state in `personal_facts["frank_need_eval"]`.

#### Scenario: Needs loop starts after value accept
- **WHEN** value-eval accepts a user
- **THEN** the onboarding stage becomes `needs_eval` and a needs question is asked in the same 2-3 line acceptance response

#### Scenario: Needs loop bounds enforced
- **WHEN** the needs loop reaches its maximum number of turns
- **THEN** the system forces `decision=accept` and proceeds with the best available needs summary

### Requirement: Needs Summary and Completion
The system SHALL summarize accepted user needs into a 2-3 line text summary and store it in `users.all_demand`, then mark onboarding complete.

#### Scenario: Needs accepted
- **WHEN** the needs-eval loop returns `decision=accept`
- **THEN** the system stores a 2-3 line summary in `users.all_demand` and sets onboarding to complete
