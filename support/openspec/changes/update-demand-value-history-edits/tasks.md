## 1. Implementation
- [x] 1.1 Add a value-history edit interpreter that uses value_update plus current value_history and returns edit actions (append, replace, remove, clear).
- [x] 1.2 Add a demand_value_history helper to apply edit actions, validate indices, and preserve created_at for retained entries.
- [x] 1.3 Update apply_demand_value_updates to fetch current history, apply value edits, record edit-plan metadata, and keep demand append-only.
- [x] 1.4 Update user DB updates to persist edited value_history, metadata, and clear value_embedding when all_value is empty.

## 2. Validation
- [x] 2.1 Add tests or scripted checks for replace/remove/clear behavior and metadata recording.
- [x] 2.2 Run `python -m py_compile` on updated modules.
