## 1. Discovery
- [ ] 1.1 Confirm Composio Google Calendar toolkit slug(s), required scopes, and tool names via Composio docs/dashboard.
- [ ] 1.2 Decide event payload shape (title, description, attendees, timezone) and map to Composio tool arguments.

## 2. Config + Client
- [ ] 2.1 Add calendar-specific Composio settings (provider/toolkit slug, auth_config_id, optional callback URL override).
- [ ] 2.2 Extend ComposioClient (or add CalendarComposioClient) to:
  - initiate calendar connect links
  - resolve connected account id for calendar
  - execute calendar tools with Composio SDK

## 3. Data Model
- [ ] 3.1 Add a persistence model for scheduled events (chat_guid, organizer_user_id, event_id, start/end, attendees, status).
- [ ] 3.2 Add DB client methods for create/read/update event records.

## 4. Group Chat Scheduling Flow
- [ ] 4.1 Update schedule_meeting tool to:
  - parse/validate time and duration
  - verify organizer calendar connection
  - generate connect link when missing
  - create calendar event via Composio when connected
  - return structured data for InteractionAgent synthesis
- [ ] 4.2 Update group chat maintenance task instructions and tool output handling to include calendar success + connect-needed paths.
- [ ] 4.3 Add attendee resolution from group chat participants (use user emails; prompt for missing).

## 5. Onboarding: Calendar Authorization (Same Prompt)
- [ ] 5.1 Extend the existing email-connect prompt/window to also request calendar access (no new onboarding step).
- [ ] 5.2 Add a second Composio connect link for calendar alongside the email link in the same message.
- [ ] 5.3 Persist calendar connection status in user `personal_facts` (or a dedicated field) and ensure routing respects completion.
- [ ] 5.4 Update onboarding response classification to handle combined email+calendar connect outcomes (email-only, calendar-only, both, unclear).

## 6. Observability + Safety
- [ ] 6.1 Add logging and error codes for connect failures and tool execution errors.
- [ ] 6.2 Add feature flag (calendar_enabled) with safe fallback to current placeholder messaging.

## 7. Tests
- [ ] 7.1 Unit tests for Composio calendar tool wrapper (mock SDK).
- [ ] 7.2 Unit tests for schedule_meeting success/fallback paths.
- [ ] 7.3 Integration test for connect-link generation (if environment allows).
- [ ] 7.4 Onboarding tests for combined email+calendar connect prompt.

## 8. Validation
- [ ] 8.1 Manual: request scheduling in a test group chat with connected organizer; verify event created and message sent.
- [ ] 8.2 Manual: request scheduling without connection; verify connect link and waiting state.
- [ ] 8.3 Manual: onboarding flow shows combined email+calendar connect prompt and records both statuses.
