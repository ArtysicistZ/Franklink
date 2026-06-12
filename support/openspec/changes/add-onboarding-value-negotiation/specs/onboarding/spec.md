## ADDED Requirements
### Requirement: Intro Fee Negotiation
The system SHALL use the value-eval LLM output to set `intro_fee_cents` each turn and persist it in both `personal_facts["frank_value_eval"]["intro_fee_cents"]` and `users.intro_fee_cents`. The fee MUST be an integer cents value within configured bounds (default 0-9900), MUST NOT increase within a value-eval session, and MAY take any value within the bounds (not fixed steps). On every ask while `intro_fee_cents > 0`, the fee MUST decrease and drop below $10 by the first ask turn. For each ask turn while `intro_fee_cents > 0`, the system SHALL enforce a per-turn ceiling that guarantees the fee can reach 0 by the final allowed ask. When decision is "accept", the system SHALL set `intro_fee_cents = 0`. When decision is "ask" and `intro_fee_cents` is greater than 0, the response_text SHALL mention the current fee and how it can drop with more value.

#### Scenario: Initial fee set
- **WHEN** the value-eval loop starts with no persisted intro_fee_cents
- **THEN** the system initializes intro_fee_cents to 9900 and includes the fee and a path to $0 in response_text

#### Scenario: Fee cannot increase
- **WHEN** the LLM proposes a fee higher than the persisted intro_fee_cents
- **THEN** the system keeps the lower fee and uses it in response_text

#### Scenario: Fee reaches zero by end
- **WHEN** value-eval reaches an accept decision or the final allowed ask
- **THEN** the system sets intro_fee_cents to 0

#### Scenario: Fee drops below $10 quickly
- **WHEN** the first value-eval ask turn is generated after the initial gate
- **THEN** the system sets intro_fee_cents below 1000

#### Scenario: Fee persists across turns
- **WHEN** a later value-eval turn runs for the same user
- **THEN** the system uses the persisted intro_fee_cents as the baseline for that turn

#### Scenario: Fee stored in user record
- **WHEN** intro_fee_cents is updated during value-eval
- **THEN** the system writes the value to `users.intro_fee_cents`

### Requirement: Value Stage Humor and Variety
The system SHALL instruct the value-eval LLM to include light humor or roasting and vary phrasing and cadence across turns while remaining respectful and high-signal.

#### Scenario: Ask response uses humor
- **WHEN** decision is "ask" during value-eval
- **THEN** response_text includes a brief humorous or roasting line and a specific value question

### Requirement: No Em Dash Output
The system SHALL ensure value-eval response_text contains no em dash or en dash characters by sanitizing the LLM output before sending.

#### Scenario: Sanitization
- **WHEN** the LLM output includes an em dash or en dash character
- **THEN** the system replaces those characters before sending response_text
