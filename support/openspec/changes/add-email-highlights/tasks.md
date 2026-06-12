## 1. Implementation
- [x] 1.1 Add Composio helper to resolve connected Gmail address and return it to callers.
- [x] 1.2 Persist connected Gmail address into `users.email` when email connect is confirmed.
- [x] 1.3 Implement processed email helper to filter `user_emails` into highlights (outbound + keyword-matched inbound, ad suppression).
- [x] 1.4 Add database client methods and schema for `user_email_highlights` with idempotent writes and an `is_from_me` flag.
- [x] 1.5 Add tests for keyword matching, ad suppression, and outbound detection.
- [x] 1.6 Add a backfill script to process existing `user_emails` into highlights and populate `users.email` when missing.
- [x] 1.7 Trigger highlight processing on newly stored emails during onboarding email connect.
