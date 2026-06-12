## 1. Scope Lock and Baseline
- [ ] 1.1 Identify and document the exact async hot path for inbound processing (listener/consumer -> orchestrator -> outbound response).
- [ ] 1.2 Record baseline metrics in testing ECS:
  - [ ] p95 single-user latency
  - [ ] p95 latency at 50 concurrent users
  - [ ] error rate
  - [ ] Kafka lag trend

## 2. Async Redis Layer
- [ ] 2.1 Add `redis.asyncio` client module with connection pooling and explicit startup/shutdown lifecycle support.
- [ ] 2.2 Implement async equivalents for hot-path methods:
  - [ ] idempotency check/set
  - [ ] chat GUID cache get/set
  - [ ] simple cache get/set used by inbound flow
- [ ] 2.3 Keep current sync Redis module for non-hot-path compatibility during migration.
- [ ] 2.4 Add feature flag `REDIS_ASYNC_ENABLED` and wire fallback behavior to existing sync path.

## 3. Database Offload Adapter
- [ ] 3.1 Create one shared helper for async contexts that executes sync DB operations via `asyncio.to_thread`.
- [ ] 3.2 Ensure helper preserves exceptions and existing retry behavior.
- [ ] 3.3 Update inbound-hot-path DB methods first (users/conversations and methods transitively called by message handling).
- [ ] 3.4 Add feature flag `DB_IO_OFFLOAD_ENABLED` to control staged rollout.

## 4. Hot-Path Call Site Migration
- [ ] 4.1 Update `app/main.py:_handle_kafka_event` idempotency call to await async Redis path when enabled.
- [ ] 4.2 Update `app/integrations/photon_listener.py` listener-mode idempotency/cache calls to async Redis path when enabled.
- [ ] 4.3 Update `app/orchestrator.py` outbound dedupe checks to async Redis path when enabled.
- [ ] 4.4 Confirm no blocking Redis/DB calls remain in the inbound async hot path.

## 5. Observability and Regression Guards
- [ ] 5.1 Emit structured timing logs for queue wait, processing time, and end-to-end time on both success and failure.
- [ ] 5.2 Add a guard test or CI check that fails on new sync Redis usage in async hot-path files.
- [ ] 5.3 Add a guard test or CI check that fails on raw `.execute()` inside async DB methods unless wrapped by the offload helper.

## 6. Verification
- [ ] 6.1 Functional verification:
  - [ ] single user end-to-end message flow works
  - [ ] retry/DLQ behavior unchanged
  - [ ] idempotency behavior unchanged
- [ ] 6.2 Concurrency verification:
  - [ ] two-user overlap test (A blocked on long external wait, B still progresses)
  - [ ] 50-user load test
- [ ] 6.3 SLO verification:
  - [ ] `p95_50_concurrent <= 1.3 * p95_single_user`

## 7. Rollout
- [ ] 7.1 Deploy with both flags off (no behavior change), confirm stability.
- [ ] 7.2 Enable `REDIS_ASYNC_ENABLED` only, monitor 24h.
- [ ] 7.3 Enable `DB_IO_OFFLOAD_ENABLED` only (if needed in isolation), monitor.
- [ ] 7.4 Enable both flags and run full load validation.
- [ ] 7.5 Document rollback matrix and operational runbook updates.
