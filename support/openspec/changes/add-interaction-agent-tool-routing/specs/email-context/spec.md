## ADDED Requirements
### Requirement: Read-Only Email Context
The system SHALL integrate read-only email context signals via Composio.

#### Scenario: OAuth connection
- **WHEN** a user requests inbox connection in DMs
- **THEN** the system returns an OAuth link and does not attempt to send emails

#### Scenario: Group chat refusal
- **WHEN** a user requests inbox connection in a group chat
- **THEN** the system refuses and asks the user to DM for connection

### Requirement: Email Signals Storage
The system SHALL store summarized email signals for downstream use.

#### Scenario: Signals stored
- **WHEN** recent email threads are fetched
- **THEN** the system stores a summary in memory (Zep or user_profile.personal_facts)
