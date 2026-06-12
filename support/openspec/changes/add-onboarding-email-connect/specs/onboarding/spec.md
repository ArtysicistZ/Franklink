## ADDED Requirements
### Requirement: Email Connect Stage
The system SHALL insert an `email_connect` onboarding stage after `career_interest` and before `needs_eval`, prompting the user to connect a work/school Gmail inbox.

#### Scenario: Stage routing after career interests
- **WHEN** the user completes the `career_interest` stage
- **THEN** the system sets onboarding_stage to `email_connect`, includes the Gmail auth link, and asks the user to reply when done

#### Scenario: User confirms connection
- **WHEN** the user replies that the inbox is connected or they are done
- **THEN** the system records a `connected` status and advances to `needs_eval`

#### Scenario: User declines to connect
- **WHEN** the user declines the email connect step
- **THEN** the system re-prompts for connection and does not advance to `needs_eval`

### Requirement: Composio Auth Link
The system SHALL use Composio to generate the Gmail auth link during the `email_connect` stage and include it in the initial prompt.

#### Scenario: Stage entry sends link
- **WHEN** the system enters the `email_connect` stage
- **THEN** the system initiates a Composio Gmail connection and returns the auth link in the response, and waits for confirmation before advancing

### Requirement: LLM-Based Email Connect Classification
The system SHALL use LLM-based classification to interpret user replies during the `email_connect` stage.

#### Scenario: Unclear reply
- **WHEN** the user response is ambiguous about connecting the inbox
- **THEN** the system uses the LLM classification to re-prompt for connection without advancing stages
