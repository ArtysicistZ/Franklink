## ADDED Requirements
### Requirement: Store Email Intent Events
The system SHALL persist event-level email intent insights derived from email highlights.

#### Scenario: Event stored
- **WHEN** a highlight processor produces an event for a user
- **THEN** a row is written to `user_email_intent_events` with `user_id`, `event_key`, `intent_summary`, and `status`

### Requirement: Event Idempotency
The system SHALL be idempotent when writing intent events for the same user and event key.

#### Scenario: Reprocessing
- **WHEN** the same event is produced again for the same user
- **THEN** the system updates the existing row instead of inserting a duplicate

### Requirement: LLM Output Fields
The system SHALL require LLM output to include only `event_key`, `intent_summary`, and `status` for each intent event.

#### Scenario: Output validation
- **WHEN** the LLM response is parsed
- **THEN** each event must include only `event_key`, `intent_summary`, and `status` before it is stored

### Requirement: Highlight Aggregation
The system SHALL aggregate multiple highlights into fewer intent events.

#### Scenario: Many highlights map to one event
- **WHEN** several highlights describe the same user intent
- **THEN** only one intent event row is stored for that intent

### Requirement: LLM Input Selection
The system SHALL provide only title, sender, is_from_me, and content to the LLM when generating intent events.

#### Scenario: Prompt input
- **WHEN** the processor builds the LLM prompt from highlights
- **THEN** the prompt includes only title, sender, is_from_me, and content fields

### Requirement: Onboarding Integration
The system SHALL run the intent event processor after new email highlights are stored.

#### Scenario: Post-highlight processing
- **WHEN** new email highlights are written during onboarding
- **THEN** intent events are generated and persisted

### Requirement: Onboarding Response Use
The system SHALL pass intent event output to the onboarding response agent.

#### Scenario: Response enrichment
- **WHEN** intent events are generated during onboarding
- **THEN** the output is provided to the response agent for user-facing messaging
