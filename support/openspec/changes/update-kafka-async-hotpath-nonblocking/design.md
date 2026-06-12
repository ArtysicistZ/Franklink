## Context
The Kafka architecture split (single ingest listener + worker consumer group) is in place, but async correctness is not complete. The inbound processing path still includes synchronous I/O executed inside `async def` code paths:

- Redis sync calls from async handlers (`check_idempotency`, cache access).
- Supabase sync `.execute()` calls inside async database methods.

Under slow network or dependency latency, these sync calls can block the event loop and reduce effective concurrency. This causes situations where one user's long request degrades another user's response latency.

## Goals
- Ensure event-loop progress is not blocked by Redis or DB I/O in inbound hot path.
- Preserve existing business logic, message semantics, retries, and idempotency behavior.
- Enable staged rollout with rapid rollback.
- Improve confidence in multi-user overlap and p95 concurrency SLO.

## Non-Goals
- Rewriting all database layers to fully async drivers.
- Changing Kafka topology, topic strategy, or retry semantics in this change.
- Refactoring unrelated background worker paths.

## Hot Path Definition
This change targets the path below:

1. Photon event arrives (listener mode) or Kafka event is consumed (consumer mode).
2. Idempotency gate executes.
3. Orchestrator runs user processing.
4. DB reads/writes and outbound message operations occur.
5. Offset commit happens on success.

Any sync I/O in steps 2-4 can block concurrent progress.

## Design Decisions

### 1) Async Redis for Hot Path
Use `redis.asyncio` for hot-path idempotency/cache APIs while preserving the legacy sync client for compatibility.

Decision details:
- New async Redis module exposes methods mirroring existing call semantics for:
  - idempotency check
  - chat GUID cache get/set
  - simple get/set cache where needed
- Module includes connection pool lifecycle (`init`, `close`) to prevent leaked connections on ECS shutdown.
- Feature-gated execution via `REDIS_ASYNC_ENABLED`.
- If async Redis fails, behavior follows explicit policy:
  - for idempotency: fail-open only where current behavior already does so
  - for cache read: fallback to miss

Why:
- Removes blocking socket operations from event loop.
- Minimal behavior risk due to method parity and flag gating.

### 2) DB Offload Adapter for Sync Supabase Calls
Keep current Supabase client but run sync `.execute()` calls outside the event loop using `asyncio.to_thread`.

Decision details:
- Introduce one shared helper used by async DB methods.
- Helper wraps query execution and preserves exception propagation.
- Apply first to inbound-hot-path methods used per message.
- Controlled by `DB_IO_OFFLOAD_ENABLED` for staged rollout.

Why:
- No immediate full DB driver migration required.
- Reduces event-loop blocking without broad schema/client changes.

### 3) Migration Strategy by Risk
Adopt phased migration order:

1. Add async Redis module and wire flags (no behavior change with flags off).
2. Migrate idempotency/caching calls in hot-path files.
3. Add DB offload helper and migrate hot-path methods first.
4. Validate concurrency and p95 goals.

Why:
- Limits blast radius.
- Makes regressions attributable to one subsystem at a time.

## Detailed Implementation Map

### A. Redis
- New module (example target): `app/utils/redis_async_client.py`
- Update call sites:
  - `app/main.py` (`_handle_kafka_event`)
  - `app/integrations/photon_listener.py` (listener mode)
  - `app/orchestrator.py` (outbound dedupe and similar guards)
- Keep existing `app/utils/redis_client.py` for compatibility in non-hot paths during migration.

### B. DB
- Add helper (example target): `app/database/client/async_exec.py`
- Update async methods under `app/database/client/` used by inbound flow to call helper for sync query execution.
- Ensure any retry wrapper remains compatible with helper-thrown exceptions.

### C. Lifecycle
- Startup: initialize async Redis client when flag enabled.
- Shutdown: close async Redis resources before process exits.
- Preserve existing startup/shutdown flow for Kafka consumer/producer and Photon listener.

## Failure Modes and Handling
- Async Redis init failure:
  - flag-enabled path should fail fast during startup in environments requiring Redis.
- Async Redis runtime failure:
  - use existing fail-open/fail-safe semantics already established per call type.
- DB offload saturation:
  - watch thread-pool pressure; if degraded, roll back via `DB_IO_OFFLOAD_ENABLED`.

## Observability Plan
- Add/keep structured logs with:
  - `event_id`, `trace_id`, `chat_guid`, `attempt`
  - `queue_ms`, `processing_ms`, `e2e_ms`
  - failure class and retry route (retry topic vs DLQ)
- Add dedicated markers for:
  - async Redis path active/inactive
  - DB offload path active/inactive
- Track:
  - event-loop health proxy metrics (processing jitter, lag spikes)
  - consumer lag and inflight counts

## Verification Plan

### Functional
- Message processing unchanged with flags off.
- Message processing unchanged with each flag independently enabled.
- Idempotency duplicate suppression unchanged.
- Retry and DLQ routing unchanged.

### Concurrency
- Two-user overlap test:
  - User A triggers long external call.
  - User B message still proceeds without waiting for A completion.
- 50-concurrent load test:
  - verify stable error rate
  - verify lag recovers

### SLO
- Validate `p95_50_concurrent <= 1.3 * p95_single_user`.

## Rollout Plan
1. Deploy code with both flags disabled.
2. Enable `REDIS_ASYNC_ENABLED` in testing; monitor.
3. Enable `DB_IO_OFFLOAD_ENABLED` in testing; monitor.
4. Enable both and run full load test.
5. Promote to production with same staged order.

## Rollback Plan
- If Redis-related errors rise, disable `REDIS_ASYNC_ENABLED`.
- If DB offload introduces instability, disable `DB_IO_OFFLOAD_ENABLED`.
- Keep Kafka topology and service split unchanged during rollback.

## Open Questions
- Whether to migrate all Redis usage to async now vs only hot path in this change.
- Whether to size a dedicated executor for DB offload vs default thread pool.
- Whether to set strict policy to ban sync `.execute()` in all async DB methods after this change.
