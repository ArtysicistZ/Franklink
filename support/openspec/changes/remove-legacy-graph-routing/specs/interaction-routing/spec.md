## MODIFIED Requirements
### Requirement: Post-Onboarding Interaction Loop
The system SHALL use the interaction agent as the default post-onboarding routing path, without falling back to legacy graph routing.

#### Scenario: Onboarded user message
- **WHEN** a fully onboarded user sends a message
- **THEN** the interaction agent selects actions and invokes execution agents directly

## ADDED Requirements
### Requirement: Legacy Graph Routing Removal
The system SHALL remove legacy graph-first routing paths and their code artifacts.

#### Scenario: Runtime routing
- **WHEN** a message is processed through the orchestrator
- **THEN** legacy graph runner code is not invoked

### Requirement: Feature Flag Removal
The system SHALL not depend on a `use_multi_agent` flag to enable interaction/execution routing.

#### Scenario: Configuration defaults
- **WHEN** configuration is loaded
- **THEN** interaction/execution routing is the only available path and no feature flag is required
