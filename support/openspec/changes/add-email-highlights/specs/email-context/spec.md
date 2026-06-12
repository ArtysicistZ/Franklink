## ADDED Requirements
### Requirement: Persist Connected Gmail Address
The system SHALL store the connected Gmail address in `users.email` when the user confirms email connection.

#### Scenario: Connected account email persisted
- **WHEN** the email connect flow is confirmed and the Gmail address is available
- **THEN** the system stores the address in `users.email`

### Requirement: Generate Email Highlights
The system SHALL generate email highlights from stored `user_emails` and store them in a dedicated table.

#### Scenario: Processed highlights stored
- **WHEN** the highlight helper is executed
- **THEN** key emails are written to `user_email_highlights` with `user_id` and `message_id`

### Requirement: Outbound Emails Always Kept
The system SHALL retain emails sent by the user based on the connected Gmail address.

#### Scenario: Outbound detection
- **WHEN** an email sender matches the connected Gmail address
- **THEN** the email is included in highlights regardless of keyword match

### Requirement: Inbound Keyword Filtering with Ad Suppression
The system SHALL retain inbound emails only when keyword matching indicates relevance and promotional messages are excluded.

#### Scenario: Relevant inbound email retained
- **WHEN** an inbound email contains relevant keywords tied to the user¡¯s active context
- **AND** the email is not promotional
- **THEN** the email is included in highlights

### Requirement: Idempotent Highlight Writes
The system SHALL avoid duplicate highlight rows for the same user and message.

#### Scenario: Reprocessing
- **WHEN** the highlight helper runs multiple times
- **THEN** no duplicate rows are written for the same `user_id` + `message_id`
### Requirement: Highlight Direction Flag
The system SHALL store whether a highlight originated from the user in `user_email_highlights.is_from_me`.

#### Scenario: Highlight stores direction flag
- **WHEN** a highlight is written for an outbound email
- **THEN** `is_from_me` is set to true
