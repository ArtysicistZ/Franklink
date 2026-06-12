## ADDED Requirements
### Requirement: Value History Edits
The system SHALL interpret value updates against existing value_history and apply edits that append, replace, remove, or clear value entries. Demand updates SHALL remain append-only.

#### Scenario: Value entry removed
- **WHEN** a user retracts a previously stated value capability
- **THEN** the system removes the targeted value entry instead of appending a contradiction

#### Scenario: Value history cleared
- **WHEN** a user indicates they no longer offer any prior value
- **THEN** value_history is cleared

#### Scenario: Demand append-only
- **WHEN** a user updates demand
- **THEN** the new demand is appended to demand_history without editing prior entries

### Requirement: Value Derived Fields After Edits
The system SHALL recompute all_value from the edited value_history and refresh value_embedding accordingly. If value_history is empty, all_value and value_embedding MUST be cleared.

#### Scenario: Embedding refreshed on edit
- **WHEN** value_history changes due to edit actions
- **THEN** value_embedding is recomputed using the updated all_value text

#### Scenario: Empty history clears embedding
- **WHEN** value_history becomes empty after an edit
- **THEN** all_value is empty and value_embedding is cleared

### Requirement: Value Edit Metadata
The system SHALL store the applied value-history edit plan in user profile metadata.

#### Scenario: Metadata stored
- **WHEN** value-history edits are applied
- **THEN** metadata includes the edit actions and timestamp
