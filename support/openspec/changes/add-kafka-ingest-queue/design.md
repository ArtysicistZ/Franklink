## Context
Today each ECS task runs FastAPI plus the Photon Socket.IO listener. When we scale to 10 tasks, we also run 10 listeners. Photon delivers the same inbound events to each listener, which triggers duplicate processing and idempotency warnings. The listener currently does in-process dedupe (12s TTL) and Redis idempotency (300s TTL) before forwarding to the orchestrator.

We need a single inbound listener, with Kafka fanout to 10 worker tasks, while preserving idempotency and keeping p95 latency within 30% of the single-user baseline even when 50 users send messages concurrently.

## Goals / Non-Goals
- Goals:
  - Single active Photon listener in production while keeping worker scale-out to 10+ ECS tasks.
  - Kafka-based decoupling between ingest and processing with at-least-once delivery.
  - Preserve ordering per conversation (chat or DM) where possible.
  - Keep idempotency guarantees for all side effects.
  - Robust retry and DLQ handling with clear observability.
  - Meet the p95 latency requirement under 50-concurrent inbound messages.
- Non-Goals:
  - Rewriting downstream business logic (orchestrator, agents, group chat runtime).
  - End-to-end exactly-once side effects across all dependencies.
  - Replacing existing idempotency checks inside group chat or database layers.

## Decisions
- Decision: Split runtime into two ECS services.
  - photon-ingest: a minimal service that connects to Photon and publishes normalized inbound events to Kafka.
  - frank-worker: the existing API logic running 10+ ECS tasks that consume from Kafka and run the orchestrator pipeline.
- Decision: Kafka provides durability and fanout. Consumers use a single consumer group so each message is processed once per group (at-least-once semantics).
- Decision: Idempotency is enforced in the worker using Redis (and existing downstream dedupe). The listener may do a best-effort dedupe to reduce load, but it is not the source of truth.
- Decision: Message keying preserves ordering per conversation: use chat_guid when present, else from_number, else message_id.
- Decision: Retry via retry topics with exponential backoff and a DLQ for permanent failures.
- Decision: Track latency from ingest timestamp to worker completion and enforce the p95 SLO with capacity and backpressure controls.
- Decision: Use AWS MSK in VPC `vpc-093674fd2e6eb1765` with private subnets
  `subnet-0a0879a770fef81aa`, `subnet-0455c68b9b8346f2f`, `subnet-0f91f902f1c0a653d`.
- Decision: Defer hot-standby + leader election; start with a single listener and ECS auto-restart, then evaluate HA after cutover.
- Decision: Start with 5 worker tasks and a per-worker async concurrency limit of 20.
- Decision: Use a single ECS cluster per environment with three services: photon-ingest, frank-worker, and a consolidated background-workers service.
- Decision: Consolidate background workers (groupchat summary, groupchat followup, daily email, proactive outreach) into a single ECS service (one task definition with multiple containers) to reduce service sprawl.

## Architecture Overview
1) Photon Socket.IO -> photon-ingest service (single active listener)
2) photon-ingest normalizes inbound events and publishes to Kafka topic `photon.inbound.v1`
3) frank-worker ECS tasks consume from Kafka as one consumer group
4) Worker runs current `MainOrchestrator.handle_message(...)`
5) On success, worker commits offset; on failure, worker publishes to retry or DLQ

## Deployment Model (ECS)
- Single ECS cluster per environment (staging, prod).
- Multiple ECS services within the cluster:
  - `photon-ingest` (1 task)
  - `frank-worker` (start 5 tasks, 20 async each)
  - `background-workers` (single service housing multiple worker containers)
- Background workers included in `background-workers`:
  - groupchat summary worker
  - groupchat followup worker
  - daily email worker
  - proactive outreach worker
- One codebase and one container image; behavior controlled by env flags/entrypoint per container.

## Kafka Topology
- Topics:
  - `photon.inbound.v1` (primary inbound)
  - `photon.inbound.retry.30s`, `photon.inbound.retry.2m`, `photon.inbound.retry.10m` (backoff tiers)
  - `photon.inbound.dlq.v1` (dead-letter queue)
- Partitions:
  - Start with 24 or 32 partitions to ensure headroom for 10 workers and future scale.
  - Replication factor 3 for durability (or provider equivalent).
- Retention:
  - Primary and retry topics: at least 3 days for replay/debug.
  - DLQ: 14-30 days for investigation.

## MSK Placement (Networking)
- VPC: `vpc-093674fd2e6eb1765`
- Private subnets (one per AZ):
  - `subnet-0a0879a770fef81aa`
  - `subnet-0455c68b9b8346f2f`
  - `subnet-0f91f902f1c0a653d`
- Subnet route tables MUST NOT include `0.0.0.0/0 -> igw-...` (no public internet).

## Local Docker Testing (Option B: Kafka + Zookeeper)
Goal: preserve real Photon inbound and real outbound behavior in local tests.

- Use Kafka + Zookeeper containers locally.
- Run two app containers from the same image:
  - `photon-ingest` with `PHOTON_INGEST_MODE=listener`
  - `frank-worker` with `PHOTON_CONSUMER_MODE=consumer`
- Use real Photon credentials and server URL in `.env`:
  - `PHOTON_SERVER_URL`, `PHOTON_DEFAULT_NUMBER`, `PHOTON_API_KEY`
- Inbound path: real Photon Socket.IO -> local `photon-ingest` container -> Kafka.
- Outbound path: `frank-worker` -> `PhotonClient` -> real Photon server.
- Local testing requires outbound connectivity from Docker to Photon; no synthetic webhook shortcuts.

## Message Schema (JSON v1)
Required fields:
- event_id: stable ID (Photon guid when available; else hash)
- source: "photon"
- received_at: ISO-8601 timestamp at ingest
- chat_guid: optional
- from_number: optional
- to_number: optional
- content: optional
- media_url: optional
- message_id: Photon guid if present
- is_group: boolean
- raw_payload: optional (truncated) for debugging
- attempt: integer (starts at 0)
- trace_id: for correlation

Versioning:
- Include `schema_version: 1` to allow future evolution.

## Idempotency Strategy
- Compute `event_id` at ingest:
  - Prefer Photon `guid` when present.
  - Fallback: SHA-256 hash of (from_number, chat_guid, content, media_url).
- Worker performs Redis idempotency check before running the orchestrator:
  - Key: `photon_msg:{event_id}` (env-prefixed as today).
  - TTL: align to current 300s for fast dedupe; consider extending to 24h if Kafka retries or replays are expected.
- Idempotency is enforced for all messages (DM and group chat). Duplicates are treated as no-ops, and downstream side effects remain idempotent using existing keys.

## Retry and Failure Handling
- Classify errors:
  - Transient (network, rate limits, timeouts): publish to next retry topic with incremented attempt.
  - Permanent (validation errors, missing required fields): publish directly to DLQ.
- Backoff tiers:
  - 30s -> 2m -> 10m (configurable), max attempts 6.
- DLQ payload includes failure reason and stack fingerprint for triage.

## Concurrency and Latency SLO
- p95 latency target: p95(50 concurrent) <= 1.3x p95(single user) measured from `received_at` to worker completion.
- Capacity tactics:
  - Partition count >= 2-3x worker count to enable parallelism.
  - Worker uses bounded internal concurrency (async tasks) and commits offsets only after all tasks for a batch finish.
  - Per-partition ordering is preserved by limiting in-flight work per partition to 1; global in-flight is capped at 20.
  - Avoid heavyweight work in photon-ingest; it only normalizes and publishes.
  - Apply consumer pause/resume when downstream latency or backlog spikes.
  - Autoscale workers based on Kafka lag and p95 processing time.

## Observability
- Metrics:
  - Kafka lag per topic/partition
  - p50/p95 processing time, queue time, and end-to-end latency
  - Idempotency hit rate
  - Retry counts by tier and DLQ volume
  - Listener connection status and publish errors
- Logs:
  - Include `event_id`, `chat_guid`, `trace_id`, `attempt`, and `consumer_group`
- Tracing:
  - Propagate `trace_id` from ingest to worker to downstream calls.

## Security and Config
- Kafka access via TLS + SASL (SCRAM or IAM) and least-privilege credentials.
- Secrets in AWS Secrets Manager or SSM Parameter Store.
- Config flags:
  - `PHOTON_INGEST_MODE=listener|off`
  - `PHOTON_CONSUMER_MODE=consumer|off`
  - `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_USERNAME`, `KAFKA_PASSWORD`

## Migration Plan
1) Provision Kafka cluster and topics.
2) Build photon-ingest service (listener + Kafka producer), deploy as a single-task ECS service.
3) Add Kafka consumer path in workers while keeping the old listener path behind config flags.
4) Disable the listener inside worker tasks; enable Kafka consumer.
5) Run canary with 1-2 workers, compare processing latency and idempotency metrics.
6) Scale workers to 10 and validate p95 latency target.
7) Remove or permanently disable in-process listener in workers after validation.

## Risks / Trade-offs
- Single listener is a potential SPOF. Mitigate with ECS health checks, auto-restart, and optional leader election for a hot-standby listener.
- At-least-once delivery means duplicates; must keep idempotency robust in workers and downstream.
- Kafka adds operational complexity and cost; monitor lag and retention carefully.

## Open Questions
- Where is the prior Kafka architecture document? (Not found in repo search.)
- Final partition count and retention policy based on expected message volume.
- Desired idempotency TTL for inbound processing (300s vs 24h).
