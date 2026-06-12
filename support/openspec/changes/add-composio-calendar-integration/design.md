## Context
- Group chat meeting scheduling currently returns a placeholder response.
- Composio is already used for Gmail read-only access; the SDK and auth flow exist in app/integrations/composio_client.py.
- We want calendar scheduling without implementing Google OAuth directly; Composio provides managed auth + tool execution.

## Goals / Non-Goals
- Goals:
  - Allow a user to connect their Google Calendar via Composio.
  - Include calendar authorization in the SAME onboarding prompt as email connect (no extra step).
  - Create calendar events from group chat scheduling requests.
  - Store event metadata for idempotency and later updates/cancellations.
  - Provide clear fallback when Composio is unavailable or user is not connected.
- Non-Goals:
  - Full calendar management UI.
  - Automatic rescheduling or multi-calendar selection.
  - Supporting non-Google calendar providers in this change.

## Decisions
- Use Composio managed auth (connect link) rather than Google OAuth directly.
- Extend ComposioClient to support calendar tool execution and connected account lookup for calendar.
- Add calendar connect to the existing email-connect prompt; do not add a separate onboarding step.
- Persist event metadata in a new table keyed by chat_guid + organizer_user_id + event time.
- Default organizer is the requesting user; attendees are group chat participants with known emails.
- Keep schedule_meeting as the single entry point; InteractionAgent handles user-facing copy.

## Risks / Trade-offs
- Missing participant emails -> requires clarification flow.
- Composio tool name/argument drift -> mitigate via centralized wrapper + tests.
- Duplicate event creation if idempotency keys are not enforced -> mitigate via event table and request hashing.
- Timezone ambiguity -> require explicit timezone or use requester profile default.

## Migration Plan
1. Add config fields and ComposioClient extensions behind a feature flag.
2. Add DB table and client methods.
3. Update schedule_meeting tool to use Composio path when enabled.
4. Validate in a test group chat, then enable for production.

## Open Questions
- Which Composio Google Calendar tool slug(s) should we standardize on for create/update/delete?
- Do we require the organizer to confirm before sending invites to other participants?
- What is the desired default event duration when user does not specify it?
- Should we support creating video conference links via Composio (if available)?
