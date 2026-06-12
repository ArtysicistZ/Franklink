## Why
We need to preserve the connected Gmail address in the user profile and generate a curated set of key emails so agents can rely on higher-signal, non-promotional context.

## What Changes
- Add a Composio fetch helper to resolve the connected Gmail address and store it in `users.email` after email connect.
- Add a helper to process raw `user_emails` into key email highlights using keyword matching and ad suppression.
- Add a new `user_email_highlights` table (or equivalent) to store processed outputs with idempotent writes.

## Impact
- Affected specs: email-context
- Affected code: `app/integrations/composio_client.py`, `app/agents/tools/onboarding/executor.py`, new helper module, `app/database/client/*` for new table
- Data: new table for processed email highlights
