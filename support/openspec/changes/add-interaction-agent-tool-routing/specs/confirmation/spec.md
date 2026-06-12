## ADDED Requirements
### Requirement: Pending Confirmation State
The system SHALL persist a `pending_confirmation` object for profile updates and match proposals.

#### Scenario: Draft stored
- **WHEN** the system drafts a profile update or match proposal
- **THEN** it stores `pending_confirmation` with type, draft, prompt, attempts, and expires_at

### Requirement: Universal Confirmation Reply Classification
The system SHALL classify replies to pending confirmations into confirm, decline, modify, unrelated, or meta_question.

#### Scenario: Confirmation reply classification
- **WHEN** a user replies while a pending_confirmation exists
- **THEN** the system assigns one of the five labels and routes accordingly

### Requirement: Profile Update Confirmation Gate
The system SHALL not apply demand/value updates until the user confirms.

#### Scenario: Drafted update awaiting confirmation
- **WHEN** the user provides a demand/value update
- **THEN** the system asks for confirmation and does not change stored values

#### Scenario: Update confirmed
- **WHEN** the user confirms a drafted update
- **THEN** the system applies the changes to demand/value history and acknowledges the update

### Requirement: Match Proposal Confirmation Gate
The system SHALL not advance a match proposal without explicit confirmation.

#### Scenario: Meta reply to match proposal
- **WHEN** the user replies with a meta question to a match proposal
- **THEN** the system sends a repair_explain response and re-asks without dropping the pending proposal
