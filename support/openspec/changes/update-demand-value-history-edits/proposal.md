## Why
Value updates currently only append to history, which keeps stale or contradictory value entries when a user retracts or corrects what they can offer.

## What Changes
- Interpret value updates against existing value_history to decide append/replace/remove edits.
- Persist the edited value_history and refresh derived fields and embeddings from the edited history.
- Record applied value-history edit plans in user metadata.
- Keep demand updates append-only.

## Impact
- Affected specs: user-profile
- Affected code: app/utils/demand_value_interpreter.py, app/utils/demand_value_history.py, app/utils/demand_value_updates.py, app/database/client/users.py, graph nodes that call apply_demand_value_updates
