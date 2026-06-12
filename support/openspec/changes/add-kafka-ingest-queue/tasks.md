## 1. Planning and Specification
- [ ] 1.1 Confirm Kafka provider, region, and VPC networking (MSK vs Confluent Cloud).
- [ ] 1.2 Finalize topic names, partition count, replication, and retention.
- [ ] 1.3 Finalize message schema fields and schema versioning approach.
- [ ] 1.4 Confirm idempotency TTL and Redis key strategy for inbound events.
- [ ] 1.5 Define the exact p95 latency measurement window and baseline capture.
- [ ] 1.6 Confirm single-cluster, multi-service deployment plan and worker separation.

## 2. Infrastructure and Deployment
- [ ] 2.1 Provision Kafka cluster and topics (prod + staging).
- [ ] 2.2 Add IAM/SASL credentials and secrets distribution for producer/consumer.
- [ ] 2.3 Add ECS service definitions for photon-ingest and frank-worker (with separate task roles).
- [ ] 2.4 Add autoscaling policies based on Kafka lag and CPU/memory.
- [ ] 2.5 Add background-workers ECS service (single service with multiple worker containers).

## 3. photon-ingest Service (Producer)
- [ ] 3.1 Implement Kafka producer with idempotence enabled.
- [ ] 3.2 Normalize Photon payloads into schema v1 and compute event_id.
- [ ] 3.3 Implement bounded publish retry with metrics and alerting.
- [ ] 3.4 Add health checks for Socket.IO connection and Kafka publish status.

## 4. frank-worker Service (Consumer)
- [ ] 4.1 Implement Kafka consumer group integration (configurable enable/disable).
- [ ] 4.2 Add idempotency check before orchestrator processing.
- [ ] 4.3 Implement retry topic routing and DLQ publishing for failures.
- [ ] 4.4 Ensure offset commit occurs only after successful processing.
- [ ] 4.5 Add backpressure controls (pause/resume, max in-flight).

## 5. Observability and SLOs
- [ ] 5.1 Emit metrics for lag, processing time, retries, DLQ volume, idempotency hits.
- [ ] 5.2 Add structured logs with event_id, trace_id, attempt, and chat_guid.
- [ ] 5.3 Build dashboards and alerts for listener health and p95 latency.

## 6. Testing and Validation
- [ ] 6.1 Unit tests for event normalization, idempotency, and retry routing.
- [ ] 6.2 Load test 50 concurrent messages; validate p95 <= 1.3x baseline.
- [ ] 6.3 Chaos test Kafka outage and verify retry/DLQ behavior.
- [ ] 6.4 Add docker-compose for local testing (Kafka + Zookeeper, Option B) with real Photon inbound/outbound.

## 7. Cutover Plan
- [ ] 7.1 Deploy photon-ingest and kafka consumer in shadow mode.
- [ ] 7.2 Disable in-process listener in workers and enable Kafka consumer.
- [ ] 7.3 Run canary with 1-2 workers, then scale to 5 (20 async per worker).
- [ ] 7.4 Document rollback steps to revert to direct listener if needed.
