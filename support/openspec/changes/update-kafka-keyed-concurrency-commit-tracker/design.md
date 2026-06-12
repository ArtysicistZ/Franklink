## Context
Inbound events are already keyed at produce time using:
- `chat_guid` when present
- otherwise `from_number`
- otherwise `message_id/event_id`

This key (`partition_key`) is stable enough to preserve per-conversation ordering while allowing concurrent processing for unrelated users.  
However, current consumer logic applies a single lock per `(topic, partition)`, which serializes all records in that partition and limits throughput under burst traffic.

## Goals
- Allow concurrent processing of different keys within the same partition.
- Preserve strict in-order processing per key.
- Preserve at-least-once delivery safety and prevent offset-skip message loss.
- Keep existing retry/DLQ and idempotency semantics.

## Non-Goals
- Changing topic schema or producer partitioning strategy.
- Replacing Redis idempotency system-wide in this change.
- Reworking retry-tier topology.

## Correctness Constraints (Must Hold)
1. **No offset skip on commit**: never commit an offset if any lower offset in that partition is unfinished/unhandled.
2. **Per-key ordering**: two events with same `partition_key` execute sequentially.
3. **At-least-once**: if processing is not successfully handled (including retry/DLQ route), offset is not committed.
4. **Rebalance safety**: on stop/rebalance, do not commit beyond contiguous completed range.
5. **Generation fencing**: task results from old partition assignments MUST NOT mutate current tracker/commit state.
6. **Bounded gap growth**: uncommitted gap and completed-set growth per partition must be bounded with pause/resume backpressure.
7. **Idempotency safety**: terminal idempotency state MUST NOT be finalized before successful handling path is reached.

## High-Level Approach
Replace partition lock with:
- key lock map: `key -> asyncio.Lock` (ordering boundary)
- partition commit tracker: tracks completion by offset and advances commits only through contiguous offsets

Concurrency boundary becomes:
- same key: serialized
- different keys in same partition: concurrent
- still bounded by global semaphore (`kafka_consumer_max_inflight`)

## Data Structures
For each `TopicPartition` (`tp`), maintain `PartitionTracker`:

- `last_committed: int`
  - Last offset that has been committed for `tp` (initially `-1` or recovered from assignment state).
- `completed: set[int]`
  - Offsets that finished successfully and are commit-eligible.
- `inflight: set[int]`
  - Offsets currently running.
- `key_locks: dict[str, asyncio.Lock]`
  - Per-key lock map for in-partition ordering by key.
- `tracker_lock: asyncio.Lock`
  - Guards tracker state mutation and commit advancement.
- `commit_lock: asyncio.Lock`
  - Serializes broker commit calls for this partition.
- `generation_id: int`
  - Assignment epoch for fencing stale task finalization.

Optional operational helpers:
- `key_last_used: dict[str, float]` for lock-map cleanup.
- `max_key_locks_per_partition` configuration to cap memory.
- `max_commit_gap_per_partition` configuration to trigger pause/resume.
- `paused_for_gap: bool` state flag.

## State Machine Per Record
For record `(tp, offset, key)`:

1. Dispatch:
   - Acquire global semaphore.
   - Add `offset` to `tracker.inflight`.
   - Capture `task_generation = tracker.generation_id`.
   - Spawn task.
2. Execute:
   - Acquire `key_locks[key]`.
   - Decode and process event.
   - On success or handled failure (retry scheduled / DLQ sent): mark as `completed`.
   - On unhandled failure: do not mark completed.
3. Finalize:
   - Verify task generation matches current tracker generation.
   - If generation mismatch: discard finalize mutation (stale task result).
   - Remove `offset` from `inflight`.
   - Try `advance_commit(tp)`.
   - Evaluate gap thresholds and pause/resume partition if needed.
   - Release semaphore.

## Commit Tracker Algorithm (Core)
`advance_commit(tp)`:

1. `candidate = last_committed + 1`
2. While `candidate in completed`:
   - remove candidate from `completed`
   - `candidate += 1`
3. `new_last = candidate - 1`
4. If `new_last > last_committed`:
   - commit Kafka offset `new_last + 1` under `commit_lock`
   - set `last_committed = new_last` only after commit ack

This guarantees commits only move across contiguous completed offsets.  
If offset 101 completes before 100, commit does not pass 99 until 100 is completed.

## Generation Fencing (Rebalance Correctness)
On partition assignment/revocation:
- Increment tracker `generation_id` for that partition.
- New tasks use the new generation.
- Old tasks that complete later detect generation mismatch and skip tracker mutation/commit advancement.

This prevents stale tasks from revoked partitions corrupting commit state.

## Gap Backpressure Strategy
Define per-partition thresholds:
- `max_commit_gap_per_partition`: maximum `(max_seen_offset - last_committed)` or equivalent completed-gap depth.

Behavior:
- If gap exceeds threshold, pause that `TopicPartition` on consumer.
- Continue processing in-flight tasks.
- Resume partition when gap drops below a lower watermark (hysteresis) to prevent pause/resume thrash.

This keeps memory bounded and prevents one pathological partition from destabilizing worker latency.

## Idempotency and Commit Coordination
Key rule:
- Do not write permanent "already processed" state at message-start time.

Required behavior for this feature:
- Use processing lease + completed marker (or equivalent two-state idempotency contract):
  - `processing` marker: short TTL, non-terminal.
  - `completed` marker: terminal duplicate suppression state.
- Set `completed` only on handled path (success or successful retry/DLQ routing), before offset becomes commit-eligible.
- If process crashes before handled path, terminal completed marker must not be present.

Note:
- This change can reuse existing downstream idempotency checks for side-effect safety.
- The exact key schema can be implemented in coordination with `update-kafka-async-hotpath-nonblocking`.

## Pseudocode (Implementation-Oriented)
```python
async def handle_record(record):
    tp = TopicPartition(record.topic, record.partition)
    tracker = get_tracker(tp)
    key = derive_key(record)  # event.partition_key or fallback event_id/offset
    task_generation = tracker.generation_id

    async with tracker.tracker_lock:
        tracker.inflight.add(record.offset)

    key_lock = get_key_lock(tracker, key)
    handled = False
    try:
        async with key_lock:
            event = decode_event(record)
            if event is None:
                handled = True
            else:
                try:
                    await handler(event)
                    handled = True
                except Exception as exc:
                    handled = await handle_failure(event, exc)  # retry/DLQ path
    finally:
        async with tracker.tracker_lock:
            if task_generation != tracker.generation_id:
                tracker.inflight.discard(record.offset)
                return
            tracker.inflight.discard(record.offset)
            if handled:
                tracker.completed.add(record.offset)
            await advance_commit_locked(tp, tracker)  # contiguous commit only
            maybe_pause_or_resume_partition(tp, tracker)
```

## Failure Semantics
- `handled=True` means:
  - normal processing success, or
  - failure successfully routed to retry topic/DLQ.
- `handled=False` means:
  - processing failed and could not be safely routed.
  - offset remains uncommitted and will be redelivered.

## Rebalance / Shutdown Behavior
On stop/restart/rebalance:
1. Stop pulling new records.
2. Bump generation for revoked/closing partitions.
2. Await in-flight tasks up to configured timeout.
3. Run final `advance_commit` per partition.
4. Stop consumer.

If timeout expires, do not force-commit gaps; keep at-least-once behavior.

## Lock Lifecycle / Memory Control
Because key cardinality can grow:
- Track last-used time per key lock.
- Periodically evict idle key locks when not held.
- Keep a conservative cap (`max_key_locks_per_partition`) with best-effort eviction.

This prevents unbounded lock-map growth for large user populations.

## Observability Requirements
Add metrics/logs:
- `kafka_inflight_total`
- `kafka_partition_inflight{tp}`
- `kafka_completed_gap_depth{tp}` = max completed offset span not yet committed
- `kafka_commit_advance_count`
- `kafka_stale_task_dropped_count{tp}`
- `kafka_partition_paused_for_gap{tp}`
- `kafka_key_lock_wait_ms`
- `kafka_key_lock_cardinality{tp}`

Structured log fields:
- `tp`, `offset`, `partition_key`, `event_id`, `handled`, `commit_advanced_to`

## Validation Plan
1. **Safety test (offset gap)**:
   - Force offset N+1 to complete before N.
   - Assert no commit beyond N-1 until N finishes.
2. **Per-key ordering test**:
   - Two same-key records with delayed first must preserve order.
3. **Cross-key concurrency test**:
   - Different keys in same partition overlap in runtime.
4. **Rebalance/stop test**:
   - Simulate shutdown with gaps; assert no skip commit.
5. **Generation fence test**:
   - Complete old-generation tasks after reassignment; assert no state mutation/commit from stale tasks.
6. **Gap backpressure test**:
   - Force long-running low offset and many fast higher offsets; assert partition pause/resume behavior and bounded tracker growth.
7. **Idempotency crash test**:
   - Crash between processing start and completion; assert no terminal completed marker and safe redelivery.
8. **Load test**:
   - Verify improved throughput/latency under 50+ concurrent users with same worker/partition pressure.

## Rollout Strategy
Feature flag:
- `KAFKA_KEYED_CONCURRENCY_ENABLED` (default `false` initially)

Stages:
1. Deploy code with flag off.
2. Enable in testing with canary worker.
3. Validate gap/ordering metrics and latency.
4. Roll out to full testing fleet.
5. Promote to production.

Rollback:
- Disable `KAFKA_KEYED_CONCURRENCY_ENABLED`.
- Revert to partition lock behavior immediately without topology changes.

## Risks and Mitigations
- Risk: commit tracker bug leads to skipped records.
  - Mitigation: invariant tests + canary + commit gap telemetry.
- Risk: stale task finalization after rebalance mutates tracker.
  - Mitigation: generation fencing + stale-task metrics.
- Risk: gap explosion under one stuck low offset.
  - Mitigation: partition pause/resume thresholds + alerts.
- Risk: early terminal idempotency claim causes loss on crash.
  - Mitigation: two-state idempotency contract with completion-only terminal marker.
- Risk: lock-map memory growth.
  - Mitigation: idle lock eviction + cardinality cap + metric alerts.
- Risk: increased complexity.
  - Mitigation: small dedicated tracker class with strict unit tests.
