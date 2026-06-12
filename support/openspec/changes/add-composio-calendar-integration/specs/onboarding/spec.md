## ADDED Requirements

### Requirement: Calendar authorization in the email-connect prompt
The system SHALL include Google Calendar authorization via Composio in the SAME onboarding prompt that requests email access, and SHALL persist the calendar connection status for routing decisions.

#### Scenario: Combined prompt shows both links
- **WHEN** the onboarding flow reaches the email-connect prompt
- **THEN** the system provides both the email connect link and the calendar connect link in the same message

#### Scenario: User already connected
- **WHEN** the user already has an active Composio calendar connection
- **THEN** the system omits the calendar link and still includes the email connect link (if needed)
