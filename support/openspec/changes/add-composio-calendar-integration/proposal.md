## Why
Group chat meeting scheduling currently stops at a placeholder Google Calendar client. Composio provides a managed auth + tool execution layer for Google Calendar, so we can ship real scheduling without running our own Google OAuth flow.

## What Changes
- Add Composio Google Calendar connection support (connect link + connected account resolution).
- Extend onboarding to include calendar authorization alongside email access in the SAME prompt/window (Composio-managed), not a separate step.
- Implement calendar event creation for group chat scheduling using Composio tools.
- Persist event metadata for idempotency and later updates/cancellations.
- Add feature flags, error handling, and observability around calendar actions.

## Impact
- Affected specs: calendar-integration, onboarding
- Affected code: app/integrations/composio_client.py, app/agents/tools/groupchat_maintenance.py, app/agents/tasks/groupchat_maintenance.py, app/agents/tools/onboarding/*, app/agents/interaction/*, app/config.py, app/database/*, docs
