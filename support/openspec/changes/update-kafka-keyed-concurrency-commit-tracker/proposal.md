## Why
Current consumer logic serializes all records in the same Kafka partition with a partition-wide lock. This protects commit order, but it also throttles throughput when many different users map to one partition. For a 50+ concurrent-user goal, this lock is too coarse.

Naively removing the partition lock is unsafe because the current code commits offsets per record. Out-of-order task completion could commit a later offset before an earlier record is done, which can skip unprocessed messages after restart.

## What Changes
- Replace partition-wide processing lock with key-scoped concurrency (lock by `partition_key`, derived from `chat_guid`/user key).
- Introduce a per-partition contiguous commit tracker so commits advance only through gap-free completed offsets.
- Preserve at-least-once semantics and idempotency behavior with retry/DLQ unchanged.
- Add rebalance/shutdown-safe draining and final commit advancement behavior.
- Add generation-fenced task finalization so stale tasks from revoked assignments cannot advance commit state.
- Add commit-gap backpressure controls (pause/resume by partition) to prevent unbounded memory growth and long-tail stalls.
- Add idempotency/commit coordination rules so terminal idempotency state is not permanently claimed before record handling is actually complete.
- Add metrics/logs for inflight, commit gap depth, key-lock contention, and commit-lag.

## Impact
- Affected specs: `photon-ingest`
- Affected code:
  - `app/integrations/kafka_pipeline.py` (consumer dispatch, locking, commit logic)
  - `app/main.py` (no behavior change expected; observability tags may be expanded)
  - tests for Kafka consumer ordering/commit guarantees
