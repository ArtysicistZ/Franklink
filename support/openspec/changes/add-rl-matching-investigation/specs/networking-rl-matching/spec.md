## ADDED Requirements

### Requirement: Match Decision Dataset Capture
The system SHALL record match-decision datasets that include the initiator profile, candidate pool snapshot, selected candidate, and outcome signals (accept/decline/response).

#### Scenario: Successful match logging
- **WHEN** a match is selected and a connection request is created
- **THEN** the candidate pool, selection, and immediate outcome are logged as a training example

### Requirement: Privacy-First Training Data
The system SHALL redact or transform PII before any data is exported for model training or labeling.

#### Scenario: PII scrub before export
- **WHEN** a training dataset is generated
- **THEN** raw emails, phone numbers, and message text are removed or anonymized

### Requirement: Preference Labeling Pipeline
The system SHALL support preference labeling via an AI judge model, with periodic human audits to calibrate label quality.

#### Scenario: AI-judge labeling
- **WHEN** a batch of candidate-pair examples is prepared
- **THEN** the judge model produces a preferred candidate and rationale for each pair

### Requirement: Configurable Optimization Method
The system SHALL support at least one RLHF method (PPO) and one direct preference method (DPO/IPO/KTO/ORPO) for policy training.

#### Scenario: Switching optimization method
- **WHEN** a training run is configured with a supported method
- **THEN** the training pipeline uses the specified optimization algorithm without code changes

### Requirement: Offline Evaluation Metrics
The system SHALL compute offline ranking metrics (e.g., NDCG/MRR) and acceptance-rate estimates for each policy candidate.

#### Scenario: Offline evaluation run
- **WHEN** a policy checkpoint is evaluated
- **THEN** the system outputs metrics and a comparison against the baseline matcher

### Requirement: Safe Rollout And Fallback
The system SHALL support shadow scoring and A/B tests with automatic fallback to the existing matcher when guardrails fail.

#### Scenario: Shadow-mode deployment
- **WHEN** a new policy is enabled in shadow mode
- **THEN** it scores matches but does not affect user-facing decisions
