## 1. Implementation
- [ ] 1.1 Add `PartitionTracker` abstraction in `app/integrations/kafka_pipeline.py` for per-partition commit state.
- [ ] 1.2 Add key-lock manager per partition keyed by `event.partition_key` (with safe fallback key).
- [ ] 1.3 Replace partition-wide lock path in `_handle_record` with key-lock execution path.
- [ ] 1.4 Implement contiguous commit advancement function and replace direct per-record commit calls.
- [ ] 1.5 Ensure decode-failure, success, retry-routed, and DLQ-routed records are marked commit-eligible (`handled=True`).
- [ ] 1.6 Ensure unhandled failures remain uncommitted.
- [ ] 1.7 Add stop/restart/rebalance drain flow that advances final contiguous commits only.
- [ ] 1.8 Add generation fencing for task finalization:
  - [ ] increment partition generation on assignment/revocation.
  - [ ] carry generation into spawned tasks.
  - [ ] ignore stale-generation finalize events.
- [ ] 1.9 Add per-partition commit call serialization (`commit_lock`) and only update `last_committed` after commit ack.
- [ ] 1.10 Add partition pause/resume backpressure driven by commit-gap threshold.
- [ ] 1.11 Add idempotency/commit coordination contract:
  - [ ] non-terminal processing marker (lease).
  - [ ] terminal completed marker only on handled path.

## 2. Configuration and Controls
- [ ] 2.1 Add feature flag `KAFKA_KEYED_CONCURRENCY_ENABLED`.
- [ ] 2.2 Add optional lock cleanup settings:
  - [ ] `KAFKA_KEY_LOCK_IDLE_TTL_SECONDS`
  - [ ] `KAFKA_KEY_LOCKS_MAX_PER_PARTITION`
- [ ] 2.3 Add gap/backpressure and fencing settings:
  - [ ] `KAFKA_MAX_COMMIT_GAP_PER_PARTITION`
  - [ ] `KAFKA_COMMIT_GAP_RESUME_WATERMARK`
  - [ ] `KAFKA_REBALANCE_DRAIN_TIMEOUT_SECONDS`

## 3. Observability
- [ ] 3.1 Add metrics for in-flight records, commit-gaps, lock cardinality, and commit-advance counts.
- [ ] 3.2 Add structured logs for record lifecycle and commit advancement.
- [ ] 3.3 Add metrics/logs for:
  - [ ] stale task drops due to generation mismatch
  - [ ] partition pause/resume due to gap
  - [ ] idempotency state transitions (processing -> completed)

## 4. Test Coverage
- [ ] 4.1 Unit test: no commit past gap when higher offsets complete first.
- [ ] 4.2 Unit test: same key preserves order.
- [ ] 4.3 Unit test: different keys in same partition run concurrently.
- [ ] 4.4 Unit test: unhandled failure does not mark offset commit-eligible.
- [ ] 4.5 Integration test: graceful shutdown with in-flight gaps does not over-commit.
- [ ] 4.6 Unit test: stale generation task completion cannot mutate tracker or advance commit.
- [ ] 4.7 Unit test: partition pauses when commit-gap threshold exceeded and resumes below watermark.
- [ ] 4.8 Fault-injection test: crash between processing-start and handled completion does not leave terminal completed marker.

## 5. Rollout
- [ ] 5.1 Deploy with flag off and verify no behavioral drift.
- [ ] 5.2 Enable canary worker in testing.
- [ ] 5.3 Validate metrics/log invariants for at least one traffic cycle.
- [ ] 5.4 Enable across testing workers and run 50+ concurrent-user load test.
- [ ] 5.5 Promote to production after passing SLO and safety checks.
