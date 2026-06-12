## ADDED Requirements
### Requirement: Post-Onboarding Interaction Loop
The system SHALL use a post-onboarding interaction loop that outputs structured actions instead of fixed graph routing.

#### Scenario: Action decision output
- **WHEN** a fully onboarded user sends a message
- **THEN** the system emits 1–3 structured actions (e.g., respond, repair_explain, draft_profile_update, propose_match)

### Requirement: Onboarding Lifecycle Gate
The system SHALL gate onboarding as a lifecycle rule.

#### Scenario: Pre-onboarded user
- **WHEN** the user is not onboarded or onboarding_stage is not complete
- **THEN** the system routes to onboarding and does not use the post-onboarding loop

#### Scenario: Onboarded user
- **WHEN** onboarding_stage is complete
- **THEN** the system does not re-enter onboarding

### Requirement: Graph Execution Tooling
The system SHALL expose internal graph execution for interaction actions that require graph handling.

#### Scenario: Graph action execution
- **WHEN** an action requires networking, recommendation, or general handling
- **THEN** the system invokes the corresponding graph and returns its response

### Requirement: Pending Flow Repair
The system SHALL handle meta or unrelated replies without breaking flow.

#### Scenario: Meta reply during pending flow
- **WHEN** a pending confirmation exists and the user asks a meta question
- **THEN** the system sends a repair_explain response and keeps the pending confirmation active

### Requirement: Group Chat Agentic Handling
The system SHALL support agentic actions in group chats with stricter privacy rules.

#### Scenario: Group chat invocation
- **WHEN** a user invokes Frank in a group chat
- **THEN** the interaction engine runs with channel=group and enforces group constraints
