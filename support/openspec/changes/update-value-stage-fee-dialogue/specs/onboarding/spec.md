## ADDED Requirements
### Requirement: Zero-Fee Mention Tracking
The system SHALL track whether "$0" has been mentioned during the value-eval stage for a user (persisted in state such as `personal_facts`).

#### Scenario: First mention allowed
- **WHEN** the value-eval stage begins and "$0" has not been mentioned
- **THEN** the response may include "$0" as part of the fee explanation

#### Scenario: Subsequent mentions avoided
- **WHEN** "$0" has already been mentioned in a prior value-eval response
- **THEN** future value-eval responses MUST NOT include "$0" and should instead use a non-dollar phrase such as "fee can be waived"

### Requirement: Prompt Length Flexibility
The value-eval system prompt SHALL NOT impose explicit character or line-length limits on response_text.

#### Scenario: Prompt does not restrict length
- **WHEN** the system constructs the value-eval prompt
- **THEN** it contains no constraints like "stay under X characters" or "2-4 lines"

### Requirement: Integrated Response Composition
The system SHALL generate a single value-eval response_text that integrates humor, fee mention, and the follow-up question without stitching or injecting fee lines after the fact.

#### Scenario: Fee is missing
- **WHEN** the LLM response_text omits the required fee mention
- **THEN** the system re-prompts/repairs the response rather than concatenating a fee line

## MODIFIED Requirements
### Requirement: Intro Fee Negotiation
The system SHALL use the value-eval LLM output to set `intro_fee_cents` each turn and persist it. The fee MUST be an integer cents value within configured bounds, MUST NOT increase within a value-eval session, and MUST reach 0 on accept. On every ask while `intro_fee_cents > 0`, the response_text MUST mention the current fee amount. The response_text MUST include a "$0" mention only on the first value-eval turn that references the fee; subsequent turns MUST avoid "$0".

#### Scenario: Fee always mentioned on ask
- **WHEN** decision is "ask" during value-eval
- **THEN** response_text includes the current fee amount

#### Scenario: "$0" only once
- **WHEN** "$0" has already been mentioned in value-eval
- **THEN** later asks omit "$0" while still mentioning the current fee

#### Scenario: Accept response without repeated "$0"
- **WHEN** decision is "accept" and "$0" has already been mentioned
- **THEN** response_text states the fee is waived without repeating "$0"

### Requirement: Value Stage Humor and Variety
The system SHALL instruct the value-eval LLM to include medium-aggressive humor: roast vague answers, respect specific ones, and vary phrasing/cadence across turns. The fee mention MUST be woven into the same response as the humor and question.

#### Scenario: Medium aggressiveness
- **WHEN** a user gives a vague response
- **THEN** the response includes a brief roast about the content, not the person, and a specific follow-up

#### Scenario: Respect specificity
- **WHEN** a user provides concrete evidence
- **THEN** the response acknowledges it without roasting and moves to the next value lane
