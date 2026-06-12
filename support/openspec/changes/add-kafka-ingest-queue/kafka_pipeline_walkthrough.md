# Kafka Pipeline Walkthrough (Function-by-Function, Time-Ordered)

This document is a slow, step-by-step walkthrough of the current Kafka message pipeline
as implemented in `app/integrations/kafka_pipeline.py`. It explains every function and
class in the order they matter at runtime.

Two timelines are covered:
1) First time started
2) Every time a new message comes in

All function names below are exact.

---

## 0) What This File Owns

This module builds a Kafka-based ingestion and processing pipeline.

- It prepares Kafka security config (including AWS MSK IAM).
- It builds the message envelope sent to Kafka.
- It manages a producer for inbound photon messages.
- It manages a consumer for processing messages and retry/DLQ.
- It auto-bootstraps topics on startup.

---

## 1) First Time Started (Startup Timeline)

This is the runtime order when the service boots and the Kafka components start.
The exact order depends on which component is initialized first (producer or consumer),
but the steps and functions are the same.

### 1.1. Global constants and module state

These are loaded at import time.

- `_RETRY_DELAYS_SECONDS = (30, 120, 600)`
  - Delay schedule for retry topics.

- `_TOPIC_BOOTSTRAP_LOCK` / `_TOPIC_BOOTSTRAP_DONE`
  - Shared global lock and flag to ensure topic bootstrap happens once per process.

- `MSKAuthTokenProvider` import (optional)
  - If the AWS IAM signer package is missing, the module can still load,
    but IAM auth will raise an error when actually used.

- `_IAM_TOKEN_PROVIDER`
  - Shared singleton token provider for AWS MSK IAM.

These do not run logic yet, but they shape how startup proceeds.

### 1.2. Startup entry points

The pipeline is started via one of these:

- `KafkaProducerClient.start()`
- `KafkaInboundConsumer.start()`

Both call `ensure_kafka_topics()` before creating Kafka clients.

### 1.3. ensure_kafka_topics()

This is the first real runtime step.

Functions called in order:

1) `_kafka_required_topics()`
   - Returns the list of Kafka topics required for the pipeline:
     - inbound
     - retry.30s
     - retry.2m
     - retry.10m
     - dlq

2) `_kafka_security_config()`
   - Builds Kafka client security settings.
   - Uses IAM auth if configured (details below).

3) `AIOKafkaAdminClient(...)`
   - Starts Kafka admin client and calls:
     - `list_topics()`
     - `create_topics(...)` if needed

4) Validates all required topics exist.

If any required topic is missing after creation, it raises an error and startup fails.

### 1.4. _kafka_security_config()

This is called whenever any Kafka client is created (producer, consumer, admin).

Steps inside:

1) Read settings:
   - `kafka_security_protocol`
   - `kafka_sasl_mechanism`

2) If SASL is needed and protocol isn’t SASL yet, upgrade to `SASL_SSL`.

3) If protocol is SSL or SASL_SSL, set `ssl_context`.

4) If SASL is enabled:
   - For AWS MSK IAM:
     - Uses `sasl_mechanism="OAUTHBEARER"`
     - Calls `_get_iam_token_provider()` for token provider.
   - For other SASL (PLAIN, SCRAM, etc):
     - Uses username/password from settings.

### 1.5. AWS MSK IAM token setup

If IAM is used, the following functions execute in this order:

1) `_get_iam_token_provider()`
   - Returns a singleton `_MskIamTokenProvider`.
   - If not created, calls `_resolve_iam_region()`.

2) `_resolve_iam_region()`
   - Attempts to find AWS region in this order:
     - `settings.kafka_iam_region`
     - `AWS_REGION` or `AWS_DEFAULT_REGION`
     - From bootstrap server hostname

3) `_MskIamTokenProvider.token()`
   - Called by the Kafka client when it needs an auth token.
   - Generates a token with `MSKAuthTokenProvider.generate_auth_token(region)`.
   - Caches it until expiry.
   - Logs refresh with expiry timestamp.

### 1.6. Producer startup

When `KafkaProducerClient.start()` runs:

1) `ensure_kafka_topics()`
2) `AIOKafkaProducer(...)` with:
   - `bootstrap_servers`
   - `client_id`
   - idempotence enabled
   - `acks="all"`
   - security config from `_kafka_security_config()`
3) `producer.start()`
4) Logs `[KAFKA] Producer started`

### 1.7. Consumer startup

When `KafkaInboundConsumer.start()` runs:

1) `ensure_kafka_topics()`
2) `_consumer_topics()`
   - Builds the topic list for inbound + retry topics
3) `_build_consumer()`
4) `_build_producer()` (the retry/DLQ producer)
5) Starts both producer + consumer
6) Spawns async task `_run()`
7) Logs `[KAFKA] Consumer started ...`

---

## 2) Every Time a New Message Comes (Per-Message Timeline)

This describes the runtime flow for an inbound photon message.

### 2.1. Inbound message -> build_kafka_event()

The app passes a photon payload into `build_kafka_event(payload)`.

Functions called inside, in order:

1) `_compute_fallback_event_id(payload)`
   - Hash of important fields for dedupe fallback.

2) `_build_idempotency_key(payload, message_id)`
   - Creates a stable idempotency key for dedupe.

3) Assigns fields:
   - `event_id`, `idempotency_key`, `source`, timestamps, content fields,
     `trace_id`, `partition_key`, etc.

The output is a normalized Kafka event dict.

### 2.2. Producer sends event -> KafkaProducerClient.send_event()

The app calls:

```
await producer.send_event(topic=..., event=...)
```

Steps:

1) Validates producer exists.
2) Serializes event to JSON (UTF-8).
3) Calls `send_and_wait(...)` on Kafka producer.
4) If a SASL auth error occurs:
   - `_restart_for_auth_error()` is triggered.
   - Producer is rebuilt.
   - The send is retried once.

### 2.3. Consumer reads events -> KafkaInboundConsumer._run()

The consumer continuously polls Kafka:

1) `getmany(timeout_ms=..., max_records=...)`
2) For each record, it:
   - Acquires a semaphore to limit inflight concurrency
   - Spawns `_handle_record(record)` as a task

If SASL auth errors occur while polling:

- `_restart_for_auth_error()` is called
  - Consumer and retry producer are rebuilt
  - IAM token provider is reset

### 2.4. Record handling -> _handle_record()

For each record (per partition ordering is enforced):

1) Acquire per-partition lock.
2) `_decode_event(record)`
   - Parse JSON; attach `source_topic`.
3) If `retry_after_seconds` is present:
   - `asyncio.sleep(retry_after_seconds)`
4) Run handler:
   - `await self._handler(event)`
5) On success:
   - `_commit(tp, offset)`
6) On error:
   - `_handle_failure(event, exc)`
   - If failure handled (retry or DLQ), commit offset.

### 2.5. Retry + DLQ path -> _handle_failure()

Steps:

1) Build `last_error` and `last_error_at`.
2) `_is_retryable_error(exc)`
3) If retryable and attempts left:
   - Compute next attempt
   - `_retry_delay_seconds(attempt)`
   - `_retry_topic(attempt)`
   - `event["retry_after_seconds"]` set
   - Send to retry topic via `_send_with_auth_retry()`
4) Otherwise:
   - Send to DLQ topic via `_send_with_auth_retry()`

### 2.6. Retry send with auth protection -> _send_with_auth_retry()

This is the safe send path used by retry/DLQ:

1) Sends to Kafka.
2) If SASL auth fails:
   - `_restart_for_auth_error()` is called.
   - The send is retried once.

---

## 3) Function-by-Function, In Timing Order

Below is every function and class, ordered by when it is first relevant in the
startup + message flows above.

### Module-level
- `_RETRY_DELAYS_SECONDS`
- `_TOPIC_BOOTSTRAP_LOCK`
- `_TOPIC_BOOTSTRAP_DONE`
- `MSKAuthTokenProvider` (optional import)
- `_IAM_TOKEN_PROVIDER`

### IAM token + security
- `_resolve_iam_region()`
- `_get_iam_token_provider()`
- `_reset_iam_token_provider()`
- `_MskIamTokenProvider.token()`
- `_kafka_security_config()`

### Utility helpers
- `_now_iso()`
- `_iter_exception_chain()`
- `_is_sasl_auth_error()`
- `_compute_fallback_event_id()`
- `_build_idempotency_key()`
- `build_kafka_event()`

### Producer path
- `KafkaProducerClient.start()`
- `KafkaProducerClient.send_event()`
- `KafkaProducerClient._restart_for_auth_error()`
- `KafkaProducerClient.stop()`

### Consumer path
- `KafkaInboundConsumer.start()`
- `KafkaInboundConsumer._run()`
- `KafkaInboundConsumer._handle_record()`
- `KafkaInboundConsumer._decode_event()`
- `KafkaInboundConsumer._commit()`
- `KafkaInboundConsumer._handle_failure()`
- `KafkaInboundConsumer._send_with_auth_retry()`
- `KafkaInboundConsumer._restart_for_auth_error()`
- `KafkaInboundConsumer.stop()`

### Retry + topic bootstrap
- `_retry_delay_seconds()`
- `_retry_topic()`
- `_is_retryable_error()`
- `_kafka_required_topics()`
- `ensure_kafka_topics()`

---

## 4) Summary of the Two Timelines (Short Form)

### Startup
1) `ensure_kafka_topics()` -> Admin client -> create topics
2) `_kafka_security_config()` -> IAM token provider if configured
3) Producer and/or consumer start
4) Consumer loop begins (`_run()`)

### Message Flow
1) `build_kafka_event(payload)`
2) `KafkaProducerClient.send_event(...)`
3) Consumer `getmany(...)`
4) `_handle_record()` -> handler
5) On success -> commit
6) On failure -> retry topic or DLQ

If auth ever fails:
- Token provider resets
- Clients restart
- Operation retries

---

## 5) What to Watch in Logs

These logs tell you the pipeline is healthy:

- `[KAFKA] Refreshed IAM token ...`
- `[KAFKA] Producer started`
- `[KAFKA] Consumer started topics=...`
- `[KAFKA] Retry scheduled ...`

If you see:
- `SaslAuthenticationFailed`

Then the auto-restart should trigger and recover without manual intervention.
