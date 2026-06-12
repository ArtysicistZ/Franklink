## Why
The current Kafka-based inbound pipeline uses `asyncio`, but parts of the hot path still call synchronous Redis and synchronous Supabase `.execute()` operations from inside `async def` handlers. When those calls block (network jitter, Redis stalls, DB latency spikes), the event loop can pause and reduce effective concurrency, causing multi-user latency regressions.

This change creates a focused remediation plan to make the inbound processing path non-blocking under normal operation, while preserving existing business behavior and idempotency guarantees.

## What Changes
- Add an async Redis access layer for idempotency and chat GUID cache operations used in inbound/worker hot paths.
- Add a database execution adapter that offloads sync Supabase `.execute()` calls from async handlers using `asyncio.to_thread`.
- Migrate key hot-path call sites in listener/consumer/orchestrator to await non-blocking I/O paths.
- Add feature flags for staged rollout and rollback: `REDIS_ASYNC_ENABLED`, `DB_IO_OFFLOAD_ENABLED`.
- Add validation and observability requirements to verify two-user overlap and p95 latency behavior.

## Impact
- Affected specs: `photon-ingest`
- Affected code:
  - `app/main.py`
  - `app/orchestrator.py`
  - `app/integrations/photon_listener.py`
  - `app/utils/redis_client.py` (and new async Redis module)
  - `app/database/client/*.py` (execution wrapper integration)
  - related tests and deployment/env config
