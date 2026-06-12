## ADDED Requirements

### Requirement: Key-scoped concurrency within a Kafka partition
The worker consumer SHALL allow concurrent processing of records in the same partition when their `partition_key` values differ, while preserving sequential processing for records with the same `partition_key`.

#### Scenario: Different users in one partition process concurrently
- **WHEN** two records from the same topic-partition have different `partition_key` values
- **THEN** the consumer is allowed to process both records concurrently

#### Scenario: Same conversation remains ordered
- **WHEN** two records in the same partition have the same `partition_key`
- **THEN** the consumer processes them sequentially in original offset order

### Requirement: Contiguous offset commit tracker
The consumer SHALL commit offsets per partition only through the highest contiguous sequence of successfully handled offsets.

#### Scenario: Higher offset finishes first
- **WHEN** offset `N+1` finishes before offset `N`
- **THEN** the committed offset does not advance beyond `N-1` until `N` is handled

### Requirement: Commit eligibility gating
A record offset SHALL become commit-eligible only when handling is complete, where handling includes successful processing or successful routing to retry/DLQ.

#### Scenario: Retry route is commit-eligible
- **WHEN** processing fails and the event is successfully published to a retry topic
- **THEN** the original offset is eligible for contiguous commit advancement

#### Scenario: Unhandled failure is not commit-eligible
- **WHEN** processing fails and retry/DLQ routing fails
- **THEN** the original offset remains uncommitted for redelivery

### Requirement: Rebalance and shutdown commit safety
On consumer stop/restart/rebalance, the system SHALL not commit beyond the highest contiguous handled offset per partition.

#### Scenario: Shutdown with in-flight gap
- **WHEN** a partition has completed higher offsets but a lower offset is still incomplete during shutdown
- **THEN** final commit advancement stops before the incomplete offset

### Requirement: Generation-fenced task finalization
The consumer SHALL fence task finalization by partition assignment generation so stale tasks from revoked assignments cannot modify current partition commit state.

#### Scenario: Stale task completion after reassignment
- **WHEN** a task from an older partition assignment generation completes after reassignment
- **THEN** its completion result is ignored for commit-tracker mutation and commit advancement

### Requirement: Commit-gap backpressure
The consumer SHALL pause and resume partition consumption based on commit-gap thresholds to keep tracker state bounded during long-running low-offset work.

#### Scenario: Gap threshold exceeded
- **WHEN** uncommitted gap depth for a partition exceeds configured threshold
- **THEN** the consumer pauses fetching from that partition until gap depth falls below resume watermark

### Requirement: Idempotency completion safety
The inbound processing pipeline SHALL only set terminal duplicate-suppression idempotency state after a record reaches handled status.

#### Scenario: Crash before handled completion
- **WHEN** processing crashes after start but before handled completion
- **THEN** terminal idempotency state is not present and redelivery remains possible
