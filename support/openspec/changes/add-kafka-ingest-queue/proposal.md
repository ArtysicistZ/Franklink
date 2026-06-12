## Why
Today each ECS task runs its own Photon Socket.IO listener. When we scale to 10 tasks, we effectively run 10 listeners, which causes duplicate inbound deliveries and idempotency warnings. We need a single inbound listener, while still distributing work across 10 worker tasks and preserving idempotency and latency.

## What Changes
- Introduce a Kafka-backed ingest pipeline: one Photon listener service publishes normalized inbound events to Kafka.
- Split runtime into an ingest service (listener only) and a worker service (10+ ECS tasks) that consume from Kafka.
- Define message schema, partitioning/ordering, idempotency strategy, and retry/DLQ handling.
- Add observability and latency SLO measurement for the new pipeline.

## Impact
- Affected specs: photon-ingest (new)
- Affected code: app/integrations/photon_listener.py, app/main.py, app/orchestrator.py, app/utils/redis_client.py, new Kafka integration modules, infrastructure/ECS/Kafka provisioning, env/config/docs
