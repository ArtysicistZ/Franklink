## ADDED Requirements

### Requirement: Separate Groupchat Networking Agent
The system SHALL implement group chat expansion using a dedicated groupchat networking agent that is separate from DM networking.
The system SHALL keep the DM networking workflow unchanged.

#### Scenario: Group chat expansion uses groupchat agent
- **WHEN** a user says "@frank can you find a marketing person for this group" inside a group chat
- **THEN** the system routes the request to the groupchat networking agent (not DM networking)

#### Scenario: DM networking unchanged
- **WHEN** a user requests standard networking in DM
- **THEN** the DM networking flow runs without group chat expansion logic

### Requirement: Group Chat Expansion Invocation
The system SHALL only trigger group chat expansion when:
- the message is inside a group chat,
- Frank is explicitly invoked, and
- the message explicitly requests finding/inviting a new person for the existing group chat.
The system SHALL NOT allow group chat expansion requests to be initiated via DM.

#### Scenario: Group chat request routes to networking expansion
- **WHEN** a user says "@frank can you find a marketing person for this group" inside a group chat
- **THEN** the system routes the request to the groupchat networking agent in expansion mode

#### Scenario: DM request is rejected
- **WHEN** a user asks in DM to add a person to an existing group chat
- **THEN** the system responds that this request must be made inside the group chat

### Requirement: Single-Invite Only
For group chat expansion, the system SHALL invite exactly one candidate at a time and SHALL NOT use multi-match flows.

#### Scenario: Single invite
- **WHEN** a user asks for a new participant in a group chat
- **THEN** the system uses single-match flow (find_match) and invites one candidate

### Requirement: Demand Resolution
If the user provides a clear demand (role/skill/goal), the system SHALL use that demand.
If the user does not provide a clear demand, the system SHALL derive the demand from group chat context (summary + recent messages + participant profiles).

#### Scenario: Clear demand is used
- **WHEN** a user asks for "an engineer" in the group chat
- **THEN** the system uses "engineer" as the demand for matching

#### Scenario: Unclear demand is inferred
- **WHEN** a user asks to "add someone to this group" without specifying a role
- **THEN** the system derives a demand from the group chat context and uses that for matching

### Requirement: Consent-Based Join Flow
The system SHALL invite a candidate via the existing networking handshake flow and SHALL only add the candidate to the existing group chat after the candidate accepts.
The system SHALL mark the request status as GROUP_CREATED after acceptance.

#### Scenario: Target accepts and is added
- **WHEN** the invited candidate accepts the invitation
- **THEN** the system adds the candidate to the existing group chat and records the join with status GROUP_CREATED

#### Scenario: Target declines
- **WHEN** the invited candidate declines the invitation
- **THEN** the system does not add the candidate to the group chat

### Requirement: Participant Storage Transition
If a 2-person chat (stored only in group_chats.user_a_id/user_b_id) adds a third participant, the system SHALL store all participants in group_chat_participants and use that table for subsequent membership operations.

#### Scenario: Third participant added to 2-person chat
- **WHEN** a third participant is accepted into a 2-person chat
- **THEN** the system backfills group_chat_participants for the original two users and adds the new participant

### Requirement: Candidate Filtering
The system SHALL exclude current group chat participants and already-invited users from candidate selection.

#### Scenario: Existing participant is excluded
- **WHEN** a candidate is already in the group chat
- **THEN** the system does not select that candidate

### Requirement: LLM-Driven Routing
All routing and tool triggers for group chat expansion SHALL be decided by the LLM, with the only hardcoded exception being detection of explicit Frank invocation.

#### Scenario: LLM decides routing
- **WHEN** a group chat message explicitly invokes Frank and requests a new participant
- **THEN** the LLM determines the appropriate task and tool usage
