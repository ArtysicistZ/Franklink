## ADDED Requirements

### Requirement: Calendar connection via Composio
The system SHALL use Composio managed auth to connect a user Google Calendar account, without implementing Google OAuth directly.

#### Scenario: User needs to connect calendar
- **WHEN** a scheduling request is received and no active Composio calendar connection exists for the requester
- **THEN** the system returns a Composio connect link and marks the request as waiting for calendar connection

#### Scenario: User already connected
- **WHEN** an active Composio calendar connection exists for the requester
- **THEN** the system proceeds to event creation without prompting for connection

### Requirement: Create calendar events for group chats
The system SHALL create a calendar event in the requester's calendar via Composio using the parsed time window, purpose/title, and attendee emails, and SHALL return structured confirmation data for the group chat response.

#### Scenario: Event created successfully
- **WHEN** the requester is connected and the event payload is valid
- **THEN** the system creates the event and returns the event id, start/end times, and attendee list

### Requirement: Event persistence and idempotency
The system SHALL persist scheduled event metadata (chat_guid, organizer_user_id, event_id, start/end, attendees) and avoid creating duplicate events for the same scheduling request.

#### Scenario: Duplicate scheduling request
- **WHEN** a scheduling request with the same chat_guid, organizer, and time window is received
- **THEN** the system reuses the existing event metadata instead of creating a new event

### Requirement: Safe fallback on Composio errors
The system SHALL return a safe fallback response when Composio is unavailable or tool execution fails, without breaking the group chat pipeline.

#### Scenario: Composio unavailable
- **WHEN** the Composio client is not available or returns an error
- **THEN** the system returns an under_development or unavailable status and does not attempt event creation
