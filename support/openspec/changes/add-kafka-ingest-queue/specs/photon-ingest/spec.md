## ADDED Requirements

### Requirement: Single active Photon listener
The system SHALL ensure that at most one active Photon Socket.IO listener is receiving inbound events in production at any time.

#### Scenario: Worker scale-out does not create duplicate listeners
- **WHEN** the worker service scales to multiple ECS tasks
- **THEN** only the designated photon-ingest service receives Photon events

### Requirement: Kafka-backed ingest
The photon-ingest service SHALL normalize inbound Photon events and publish them to a Kafka topic for downstream processing.

#### Scenario: Inbound message becomes a Kafka event
- **WHEN** Photon delivers a new inbound message
- **THEN** the message is published to `photon.inbound.v1` with the normalized schema

### Requirement: Ordered partitioning by conversation
Inbound events SHALL be keyed so that messages for the same conversation are delivered in order to a single partition.

#### Scenario: Ordering for a group chat
- **WHEN** two messages arrive for the same `chat_guid`
- **THEN** both are published with the same Kafka key and processed in order

### Requirement: Consumer group processing
Worker tasks SHALL consume inbound events as a single Kafka consumer group and process each event at least once.

#### Scenario: Horizontal scaling
- **WHEN** 10 worker tasks are running in the same consumer group
- **THEN** each Kafka event is processed by exactly one worker task (at-least-once)

### Requirement: Idempotent processing
Worker-side processing SHALL be idempotent using a stable `event_id` so duplicate deliveries do not create duplicate side effects.

#### Scenario: Duplicate delivery
- **WHEN** the same `event_id` is delivered more than once
- **THEN** only the first delivery triggers side effects and subsequent deliveries are ignored or treated as no-ops

### Requirement: Retry and DLQ handling
Transient failures SHALL be retried with backoff and a capped attempt count, while permanent failures SHALL be sent to a DLQ with diagnostic metadata.

#### Scenario: Transient failure
- **WHEN** processing fails due to a transient error
- **THEN** the event is published to the next retry topic with an incremented attempt count

#### Scenario: Permanent failure
- **WHEN** processing fails due to a non-retryable error or exceeds max attempts
- **THEN** the event is published to the DLQ with the failure reason

### Requirement: Latency SLO
The system SHALL achieve p95 end-to-end latency under 50 concurrent inbound messages within 1.3x the p95 latency of a single inbound message.

#### Scenario: Concurrency spike
- **WHEN** 50 users send inbound messages concurrently
- **THEN** p95 latency stays within 30% of the single-user p95 baseline

### Requirement: Observability
The system SHALL emit metrics and logs for ingestion, processing latency, idempotency hits, retries, DLQ volume, and Kafka lag.

#### Scenario: Debugging a retry storm
- **WHEN** retry rates spike
- **THEN** dashboards and logs identify the affected topic, error class, and event_ids

### Requirement: Backpressure controls
The worker service SHALL apply backpressure (pause consumption or limit in-flight processing) when downstream latency or lag exceeds defined thresholds.

#### Scenario: Downstream slowdown
- **WHEN** processing time exceeds the configured threshold
- **THEN** consumers pause or reduce in-flight work to prevent overload
