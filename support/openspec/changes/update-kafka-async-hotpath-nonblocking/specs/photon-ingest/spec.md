## ADDED Requirements

### Requirement: Non-blocking async hot path
The system SHALL avoid event-loop-blocking synchronous I/O in inbound async hot-path handlers for Redis idempotency/cache operations and database query execution.

#### Scenario: Long dependency wait does not stall concurrent message handling
- **WHEN** one inbound message experiences a long external wait
- **THEN** other inbound messages continue to progress through the async pipeline without waiting for the first message to complete

### Requirement: Async Redis on inbound hot path
The system SHALL provide an async Redis client path for hot-path idempotency and cache operations, with explicit startup/shutdown lifecycle and feature-flagged activation.

#### Scenario: Async idempotency gate in Kafka consumer path
- **WHEN** Kafka consumer receives an event
- **THEN** idempotency is checked through the async Redis path when `REDIS_ASYNC_ENABLED` is true

#### Scenario: Safe rollback to sync Redis path
- **WHEN** `REDIS_ASYNC_ENABLED` is false
- **THEN** the system uses the existing sync Redis path without changing business behavior

### Requirement: Async wrapper for sync DB execute operations
The system SHALL offload synchronous Supabase `.execute()` calls from async handlers to a non-event-loop execution context.

#### Scenario: Async database method executes query without blocking event loop
- **WHEN** an async DB method performs a query
- **THEN** sync execution is run through the DB offload adapter when `DB_IO_OFFLOAD_ENABLED` is true

### Requirement: Staged rollout controls for async I/O migration
The system SHALL support independent rollout and rollback of async Redis and DB offload changes via runtime configuration.

#### Scenario: Isolated rollout of Redis async path
- **WHEN** only `REDIS_ASYNC_ENABLED` is enabled
- **THEN** Redis hot-path operations use async I/O while DB execution behavior remains unchanged

#### Scenario: Isolated rollback of DB offload
- **WHEN** `DB_IO_OFFLOAD_ENABLED` is disabled after deployment
- **THEN** DB methods return to prior execution mode without requiring Kafka topology changes

### Requirement: Concurrency SLO validation for async correctness
The system SHALL validate that concurrency improvements preserve latency objectives using baseline and concurrent load measurements.

#### Scenario: p95 latency target under concurrent load
- **WHEN** the system is tested with 50 concurrent inbound users
- **THEN** p95 end-to-end latency is less than or equal to 1.3 times the single-user p95 baseline
