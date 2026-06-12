# Kafka Pipeline Full Walkthrough (With Code + Big Picture + Detailed Notes)

This document is intentionally very long and slow-paced.
It explains the Kafka message pipeline and all surrounding modules in time order,
and includes full code for each described function/method.
Each method includes at least five sentences with big-picture intent, stack references,
and beginner-friendly system-design notes.

Modules covered:
- `app/integrations/kafka_pipeline.py`
- `app/integrations/kafka_topic_bootstrap.py`
- `app/integrations/photon_listener.py`
- `app/integrations/photon_client.py`
- `app/main.py`
- `app/config.py`
- `app/orchestrator.py`
- `app/agents/queue/async_processor.py`
- `app/agents/queue/handlers.py`
- `app/agents/queue/callbacks.py`
- `app/agents/queue/__init__.py`
- `app/utils/redis_client.py`
- `app/utils/message_chunker.py`

---

## Timeline A: First-time startup (slow, step-by-step)

A1. Process starts, Python imports modules.
A2. `app.main` loads, config and logging are configured.
A3. FastAPI app is created and routes are registered.
A4. Global singletons are initialized (orchestrator, optional listener, Kafka handles).
A5. Startup event triggers background initializers.
A6. Photon listener may connect (listener or kafka ingest mode).
A7. Kafka consumer may start (if consumer mode is enabled).
A8. Async operation processor may start (if not in ingest-only mode).
A9. Profile synthesis scheduler may start (if enabled and not ingest-only mode).

---

## Timeline B: Every inbound message (slow, step-by-step)

B1. Photon emits a `new-message` Socket.IO event.
B2. `PhotonListener._handle_new_message()` normalizes payload and applies idempotency checks.
B3. The payload is forwarded to either `_forward_photon_message` or `_publish_photon_message` based on ingest mode.
B4. If kafka ingest mode, a Kafka event is built and sent to the inbound topic.
B5. Kafka consumer reads the event and invokes `_handle_kafka_event`.
B6. Orchestrator runs, and responses are sent back through Photon APIs.
B7. Optional retries/DLQ are scheduled on failure.

---

## Module: `app/integrations/kafka_pipeline.py`

Role: Core Kafka pipeline (producer, consumer, retries, topic bootstrap, IAM auth).

Imported stack (selected):
- __future__.annotations, aiokafka.AIOKafkaConsumer, aiokafka.AIOKafkaProducer, aiokafka.abc.AbstractTokenProvider, aiokafka.admin.AIOKafkaAdminClient, aiokafka.admin.NewTopic, aiokafka.errors.TopicAlreadyExistsError, aiokafka.structs.TopicPartition, app.config.settings, asyncio, collections.defaultdict, datetime.datetime, hashlib, json, logging, os, re, ssl, time, typing.Any, typing.Awaitable, typing.Callable, typing.Dict, typing.Optional, typing.Tuple, uuid

### Class: `_MskIamTokenProvider`

Big picture:
- Manages Kafka authentication and secure connectivity for MSK IAM.

Purpose:
- No class docstring; see methods below.

When used:
- Support path (utility/config).

Class code:
```python
class _MskIamTokenProvider(AbstractTokenProvider):
    def __init__(self, region: str) -> None:
        self._region = region
        self._token: Optional[str] = None
        self._expires_at: float = 0.0
        self._lock = asyncio.Lock()

    async def token(self) -> str:
        now = time.time()
        if self._token and now < self._expires_at:
            return self._token
        async with self._lock:
            now = time.time()
            if self._token and now < self._expires_at:
                return self._token
            if MSKAuthTokenProvider is None:
                raise RuntimeError("aws-msk-iam-sasl-signer-python is required for AWS_MSK_IAM")
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(self._region)
            self._token = token
            if expiry_ms:
                expiry_seconds = int(expiry_ms) / 1000.0
                if expiry_seconds > now + 60:
                    # expiry_ms is an epoch timestamp in ms
                    self._expires_at = expiry_seconds - 60.0
                else:
                    # expiry_ms appears to be a TTL-like value in seconds
                    self._expires_at = now + max(1.0, expiry_seconds - 60.0)
            else:
                self._expires_at = now + 14 * 60
            logger.info(
                "[KAFKA] Refreshed IAM token region=%s expires_at=%s",
                self._region,
                datetime.utcfromtimestamp(self._expires_at).isoformat(),
            )
            return self._token
```

#### Method: `_MskIamTokenProvider.__init__()`

Big picture and system-design notes (>=5 sentences):
- This constructor initializes the IAM token provider that backs MSK IAM authentication for Kafka clients in this module.
- It depends on the async stack (`asyncio.Lock`) and the `AbstractTokenProvider` base from `aiokafka` to fit the SASL/OAUTHBEARER interface.
- The `region`, `_token`, and `_expires_at` fields are stored so later calls to `token()` can decide whether to reuse or refresh credentials.
- A key system-design point here is that auth state is encapsulated in a dedicated object instead of leaking across unrelated modules.
- For beginners, this is a clean example of setting up shared mutable state with a lock to make concurrent access safe.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
def __init__(self, region: str) -> None:
        self._region = region
        self._token: Optional[str] = None
        self._expires_at: float = 0.0
        self._lock = asyncio.Lock()
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `_MskIamTokenProvider.token()`

Big picture and system-design notes (>=5 sentences):
- This method is the heart of MSK IAM auth: it returns a valid OAUTHBEARER token to Kafka clients.
- It uses `time.time()` for cache freshness, `asyncio.Lock` to prevent concurrent refreshes, and `MSKAuthTokenProvider` from the AWS signer library to generate tokens.
- The double-check inside the lock prevents redundant refreshes when multiple coroutines arrive at the same time, which is a classic concurrency pattern.
- It also logs the new expiry using `datetime.utcfromtimestamp`, which is helpful for observability and debugging auth flaps.
- For beginners, this shows how to combine caching, locking, and external auth APIs to build a reliable credential provider.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def token(self) -> str:
        now = time.time()
        if self._token and now < self._expires_at:
            return self._token
        async with self._lock:
            now = time.time()
            if self._token and now < self._expires_at:
                return self._token
            if MSKAuthTokenProvider is None:
                raise RuntimeError("aws-msk-iam-sasl-signer-python is required for AWS_MSK_IAM")
            token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(self._region)
            self._token = token
            if expiry_ms:
                expiry_seconds = int(expiry_ms) / 1000.0
                if expiry_seconds > now + 60:
                    # expiry_ms is an epoch timestamp in ms
                    self._expires_at = expiry_seconds - 60.0
                else:
                    # expiry_ms appears to be a TTL-like value in seconds
                    self._expires_at = now + max(1.0, expiry_seconds - 60.0)
            else:
                self._expires_at = now + 14 * 60
            logger.info(
                "[KAFKA] Refreshed IAM token region=%s expires_at=%s",
                self._region,
                datetime.utcfromtimestamp(self._expires_at).isoformat(),
            )
            return self._token
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

### Function: `_resolve_iam_region()`

Big picture and system-design notes (>=5 sentences):
- This function determines the AWS region used for MSK IAM authentication, which is critical for token signing.
- It reads from `settings` first, then `os.getenv` (AWS_REGION / AWS_DEFAULT_REGION), and finally parses the region from Kafka bootstrap hostnames using `re`.
- It is called by `_get_iam_token_provider()` during startup or first Kafka auth, so region resolution happens before any token is generated.
- The design pattern here is layered configuration: explicit config overrides environment, and environment overrides heuristic parsing.
- For beginners, it demonstrates defensive configuration practices that keep services running even when explicit settings are missing.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _resolve_iam_region() -> Optional[str]:
    explicit = str(getattr(settings, "kafka_iam_region", "") or "").strip()
    if explicit:
        return explicit
    env_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if env_region:
        return env_region.strip()
    servers = str(getattr(settings, "kafka_bootstrap_servers", "") or "")
    match = re.search(r"\.(us-[a-z]+-\d)\.", servers)
    if match:
        return match.group(1)
    return None
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_get_iam_token_provider()`

Big picture and system-design notes (>=5 sentences):
- This function is the singleton factory for the IAM token provider used by Kafka clients.
- It calls `_resolve_iam_region()` and raises a clear error if the region cannot be determined, preventing silent misconfiguration.
- The global `_IAM_TOKEN_PROVIDER` cache keeps token generation centralized, which avoids creating multiple token providers with inconsistent state.
- From a design perspective, this is a lightweight service locator pattern scoped to Kafka auth.
- For beginners, it shows how to lazily initialize a shared dependency only when it is actually needed.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _get_iam_token_provider() -> _MskIamTokenProvider:
    global _IAM_TOKEN_PROVIDER
    if _IAM_TOKEN_PROVIDER is not None:
        return _IAM_TOKEN_PROVIDER
    region = _resolve_iam_region()
    if not region:
        raise RuntimeError("Kafka IAM auth requires AWS region (set KAFKA_IAM_REGION or AWS_REGION)")
    _IAM_TOKEN_PROVIDER = _MskIamTokenProvider(region)
    return _IAM_TOKEN_PROVIDER
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_reset_iam_token_provider()`

Big picture and system-design notes (>=5 sentences):
- This helper resets the cached IAM token provider so future calls create a fresh instance.
- It is used after SASL authentication failures to force a new token flow, which is a recovery technique for auth edge cases.
- The only dependency here is the module-level `_IAM_TOKEN_PROVIDER` variable, so the logic is intentionally minimal and safe.
- From a system-design angle, this is a “circuit breaker reset” concept applied to auth state.
- For beginners, it highlights that sometimes the safest fix after auth failure is to drop cached state and rebuild cleanly.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _reset_iam_token_provider() -> None:
    global _IAM_TOKEN_PROVIDER
    _IAM_TOKEN_PROVIDER = None
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_now_iso()`

Big picture and system-design notes (>=5 sentences):
- This utility generates an ISO-8601 UTC timestamp for event metadata.
- It uses `datetime.utcnow()` from the standard library, keeping timestamps consistent across the pipeline.
- It is called when building Kafka events and retry metadata, so every event has a canonical time marker.
- From a system-design perspective, centralized timestamp formatting avoids subtle differences across producers.
- For beginners, it shows why using a single helper for time formatting makes logs and data easier to correlate.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _now_iso() -> str:
    return datetime.utcnow().isoformat()
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_iter_exception_chain()`

Big picture and system-design notes (>=5 sentences):
- This helper walks a Python exception’s causal chain to surface root causes.
- It relies on the standard exception attributes `__cause__` and `__context__`, and guards against cycles with an `id()` set.
- It is used by `_is_sasl_auth_error()` to detect auth failures even when wrapped inside other exceptions.
- The system-design lesson here is to build robust error classification rather than matching only top-level exception types.
- For beginners, it demonstrates how complex errors can be layered and why traversing the chain improves observability and recovery logic.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _iter_exception_chain(exc: BaseException):
    seen = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_is_sasl_auth_error()`

Big picture and system-design notes (>=5 sentences):
- This function determines whether an exception indicates SASL authentication failure in Kafka.
- It uses `_iter_exception_chain()` to inspect nested causes and then applies string/type checks to catch wrapped errors.
- The result is consumed by producer and consumer paths to decide when to reset IAM tokens and restart clients.
- From a system-design view, this is an error classifier that converts low-level failures into actionable recovery signals.
- For beginners, it highlights why resilience sometimes requires inspecting exception messages when strong types are not available.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _is_sasl_auth_error(exc: BaseException) -> bool:
    for err in _iter_exception_chain(exc):
        name = type(err).__name__
        message = str(err)
        if name in {"SaslAuthenticationFailedError", "SaslAuthenticationFailed"}:
            return True
        if "SaslAuthenticationFailed" in message:
            return True
        if "Access denied" in message and "Sasl" in message:
            return True
    return False
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_kafka_security_config()`

Big picture and system-design notes (>=5 sentences):
- This function builds the Kafka client security configuration from `settings`.
- It uses `ssl.create_default_context()` for SSL/TLS and maps AWS_MSK_IAM to SASL_SSL with an OAUTHBEARER token provider.
- Every Kafka admin, producer, and consumer calls this, so security settings are centralized and consistent.
- From a system-design standpoint, centralizing auth config avoids drift between components and makes security audits easier.
- For beginners, this shows how to separate “how to connect securely” from “what business logic to run.”

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _kafka_security_config() -> Dict[str, Any]:
    config: Dict[str, Any] = {}
    protocol = str(getattr(settings, "kafka_security_protocol", "") or "").strip()
    mechanism = str(getattr(settings, "kafka_sasl_mechanism", "") or "").strip()
    mechanism_upper = mechanism.upper()
    protocol_upper = protocol.upper()

    if mechanism_upper in {"AWS_MSK_IAM", "OAUTHBEARER"} and not protocol_upper.startswith("SASL"):
        protocol = "SASL_SSL"
        protocol_upper = "SASL_SSL"

    if protocol:
        config["security_protocol"] = protocol
    if protocol_upper in {"SSL", "SASL_SSL"}:
        config["ssl_context"] = ssl.create_default_context()
    if protocol_upper.startswith("SASL"):
        if mechanism_upper in {"AWS_MSK_IAM", "OAUTHBEARER"}:
            config["sasl_mechanism"] = "OAUTHBEARER"
            config["sasl_oauth_token_provider"] = _get_iam_token_provider()
        else:
            if mechanism:
                config["sasl_mechanism"] = mechanism
            username = getattr(settings, "kafka_username", None)
            password = getattr(settings, "kafka_password", None)
            if username:
                config["sasl_plain_username"] = username
            if password:
                config["sasl_plain_password"] = password
    return config
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_compute_fallback_event_id()`

Big picture and system-design notes (>=5 sentences):
- This function creates a deterministic fallback ID when a Photon message lacks a usable GUID.
- It uses `hashlib.sha256` to hash a minimal fingerprint (sender, chat, content, media) so the same message yields the same ID.
- The fallback is used by `_build_idempotency_key()` and `build_kafka_event()` to keep dedupe stable across retries and re-deliveries.
- From a system-design lens, this is a practical way to enforce idempotency without a database lookup.
- For beginners, it illustrates how hashing can be used to generate stable identifiers from content.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _compute_fallback_event_id(payload: Dict[str, Any]) -> str:
    fingerprint_payload = "|".join(
        [
            str(payload.get("from_number") or "unknown"),
            str(payload.get("chat_guid") or ""),
            str(payload.get("content") or ""),
            str(payload.get("media_url") or ""),
        ]
    )
    return hashlib.sha256(fingerprint_payload.encode("utf-8")).hexdigest()[:16]
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_build_idempotency_key()`

Big picture and system-design notes (>=5 sentences):
- This function computes the Redis idempotency key used to prevent duplicate processing.
- It uses `settings.app_env` to prefix keys in development, which avoids collisions between dev and prod workloads.
- If a real message GUID is present, it is used directly; otherwise it falls back to a content hash from `_compute_fallback_event_id()`.
- The design principle is to make dedupe keys stable across transport retries and message replays.
- For beginners, it shows how to build idempotency into the pipeline without coupling to the database schema.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _build_idempotency_key(payload: Dict[str, Any], message_id: str) -> str:
    env_prefix = "dev_" if settings.app_env == "development" else ""
    message_id = str(message_id or "").strip()
    if message_id and not message_id.startswith("photon_hash:"):
        return f"{env_prefix}photon_msg:{message_id}"
    if message_id.startswith("photon_hash:"):
        return f"{env_prefix}photon_msg_hash:{message_id.split('photon_hash:', 1)[1]}"
    fallback = _compute_fallback_event_id(payload)
    return f"{env_prefix}photon_msg_hash:{fallback}"
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `build_kafka_event()`

Big picture and system-design notes (>=5 sentences):
- This function constructs the canonical Kafka event envelope from a Photon payload.
- It calls `_build_idempotency_key()` and `_now_iso()`, and adds tracing fields like `trace_id`, `partition_key`, and `attempt`.
- The resulting event is the contract between ingestion and processing, so downstream consumers rely on this schema for correctness.
- From a system-design perspective, this is how you decouple upstream input formats from downstream processing logic.
- For beginners, it’s a concrete example of designing a message schema that supports retries, tracing, and ordering.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def build_kafka_event(payload: Dict[str, Any]) -> Dict[str, Any]:
    message_id = str(payload.get("message_id") or "").strip()
    event_id = message_id or f"photon_hash:{_compute_fallback_event_id(payload)}"
    idempotency_key = _build_idempotency_key(payload, message_id or event_id)
    chat_guid = payload.get("chat_guid")
    from_number = payload.get("from_number")
    partition_key = str(chat_guid or from_number or message_id or event_id)
    is_group = bool(chat_guid and (";+;" in str(chat_guid) or str(chat_guid).startswith("chat")))

    event: Dict[str, Any] = {
        "schema_version": 1,
        "event_id": event_id,
        "idempotency_key": idempotency_key,
        "source": "photon",
        "received_at": _now_iso(),
        "payload_timestamp": payload.get("timestamp"),
        "from_number": from_number,
        "to_number": payload.get("to_number"),
        "content": payload.get("content"),
        "media_url": payload.get("media_url"),
        "message_id": message_id or None,
        "chat_guid": chat_guid,
        "is_group": is_group,
        "attempt": int(payload.get("attempt") or 0),
        "trace_id": payload.get("trace_id") or uuid.uuid4().hex,
        "partition_key": partition_key,
    }
    test_run = str(getattr(settings, "latency_test_run", "") or "").strip()
    if test_run:
        event["test_run"] = test_run

    raw_payload = payload.get("raw_payload")
    if raw_payload is not None:
        event["raw_payload"] = raw_payload

    return event
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Class: `KafkaProducerClient`

Big picture:
- Core Kafka pipeline logic.

Purpose:
- No class docstring; see methods below.

When used:
- Support path (utility/config).

Class code:
```python
class KafkaProducerClient:
    def __init__(self) -> None:
        self._producer: Optional[AIOKafkaProducer] = None
        self._restart_lock = asyncio.Lock()

    async def start(self) -> None:
        if self._producer is not None:
            return
        await ensure_kafka_topics()
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.kafka_client_id}-producer",
            enable_idempotence=bool(getattr(settings, "kafka_producer_idempotence", True)),
            acks="all",
            **_kafka_security_config(),
        )
        await self._producer.start()
        logger.info("[KAFKA] Producer started")

    async def stop(self) -> None:
        if self._producer is None:
            return
        await self._producer.stop()
        self._producer = None
        logger.info("[KAFKA] Producer stopped")

    async def _restart_for_auth_error(self, exc: BaseException) -> None:
        async with self._restart_lock:
            logger.warning("[KAFKA] Restarting producer after auth error: %s", exc)
            if self._producer is not None:
                try:
                    await self._producer.stop()
                except Exception:
                    logger.exception("[KAFKA] Failed to stop producer during restart")
                self._producer = None
            _reset_iam_token_provider()
            await asyncio.sleep(1)
            await self.start()

    async def send_event(self, *, topic: str, event: Dict[str, Any], key: Optional[str] = None) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer not started")
        key_value = key or str(event.get("partition_key") or event.get("event_id") or "")
        payload = json.dumps(event, ensure_ascii=True).encode("utf-8")
        try:
            await self._producer.send_and_wait(topic, value=payload, key=key_value.encode("utf-8"))
        except Exception as exc:
            if _is_sasl_auth_error(exc):
                await self._restart_for_auth_error(exc)
                if self._producer is None:
                    raise
                await self._producer.send_and_wait(topic, value=payload, key=key_value.encode("utf-8"))
                return
            raise
```

#### Method: `KafkaProducerClient.__init__()`

Big picture and system-design notes (>=5 sentences):
- This constructor initializes the producer wrapper state and does not perform any network I/O.
- It uses `asyncio.Lock` to serialize restarts and uses the `AIOKafkaProducer` type annotation for clarity.
- It is invoked when the app first needs to publish to Kafka (via `_publish_photon_message` in `app.main`).
- The design idea is to keep lifecycle management encapsulated so start/stop logic is centralized and safe.
- For beginners, it shows a clean pattern: constructors should set up state only, not do slow operations.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
def __init__(self) -> None:
        self._producer: Optional[AIOKafkaProducer] = None
        self._restart_lock = asyncio.Lock()
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaProducerClient.start()`

Big picture and system-design notes (>=5 sentences):
- This method lazily starts the Kafka producer and ensures required topics exist before publishing.
- It uses `AIOKafkaProducer` from `aiokafka`, `ensure_kafka_topics()`, and `_kafka_security_config()` to build a correct client.
- It is called on first publish, so it keeps startup fast while still making sure Kafka is ready.
- The system-design insight is that lazy initialization improves robustness in distributed systems where dependencies may start slowly.
- For beginners, this shows a safe “check -> init -> start” pattern with explicit logging.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def start(self) -> None:
        if self._producer is not None:
            return
        await ensure_kafka_topics()
        self._producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.kafka_client_id}-producer",
            enable_idempotence=bool(getattr(settings, "kafka_producer_idempotence", True)),
            acks="all",
            **_kafka_security_config(),
        )
        await self._producer.start()
        logger.info("[KAFKA] Producer started")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaProducerClient.stop()`

Big picture and system-design notes (>=5 sentences):
- This method gracefully shuts down the producer and releases Kafka resources.
- It calls `AIOKafkaProducer.stop()` and clears the internal reference to prevent accidental reuse.
- It is used during application shutdown and during auth recovery restarts.
- The design principle here is clean resource management to avoid socket leaks and half-open connections.
- For beginners, it demonstrates that teardown logic should be idempotent and safe to call repeatedly.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def stop(self) -> None:
        if self._producer is None:
            return
        await self._producer.stop()
        self._producer = None
        logger.info("[KAFKA] Producer stopped")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaProducerClient._restart_for_auth_error()`

Big picture and system-design notes (>=5 sentences):
- This method handles Kafka SASL auth failures by restarting the producer in a controlled way.
- It uses `asyncio.Lock` to serialize restarts, calls `_reset_iam_token_provider()`, and then re-runs `start()`.
- It is invoked from `send_event()` only when `_is_sasl_auth_error()` indicates a credential failure.
- The system-design point is self-healing: you recover the producer without taking the whole service down.
- For beginners, this is a practical example of safe recovery with locks, cleanup, and retry.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def _restart_for_auth_error(self, exc: BaseException) -> None:
        async with self._restart_lock:
            logger.warning("[KAFKA] Restarting producer after auth error: %s", exc)
            if self._producer is not None:
                try:
                    await self._producer.stop()
                except Exception:
                    logger.exception("[KAFKA] Failed to stop producer during restart")
                self._producer = None
            _reset_iam_token_provider()
            await asyncio.sleep(1)
            await self.start()
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaProducerClient.send_event()`

Big picture and system-design notes (>=5 sentences):
- This method serializes a Kafka event to JSON and publishes it to a topic with a stable key.
- It uses `json.dumps`, `AIOKafkaProducer.send_and_wait`, and the event’s partition key to preserve ordering.
- On SASL auth errors, it calls `_restart_for_auth_error()` and retries the send once.
- The system-design idea is to keep the publishing API simple while embedding reliability at the edge.
- For beginners, this shows how to build a safe “send with retry” path without double-sending in steady state.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def send_event(self, *, topic: str, event: Dict[str, Any], key: Optional[str] = None) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer not started")
        key_value = key or str(event.get("partition_key") or event.get("event_id") or "")
        payload = json.dumps(event, ensure_ascii=True).encode("utf-8")
        try:
            await self._producer.send_and_wait(topic, value=payload, key=key_value.encode("utf-8"))
        except Exception as exc:
            if _is_sasl_auth_error(exc):
                await self._restart_for_auth_error(exc)
                if self._producer is None:
                    raise
                await self._producer.send_and_wait(topic, value=payload, key=key_value.encode("utf-8"))
                return
            raise
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

### Class: `KafkaInboundConsumer`

Big picture:
- Core Kafka pipeline logic.

Purpose:
- No class docstring; see methods below.

When used:
- Support path (utility/config).

Class code:
```python
class KafkaInboundConsumer:
    def __init__(self, handler: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        self._handler = handler
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._task: Optional[asyncio.Task] = None
        self._closing = asyncio.Event()
        self._inflight: set[asyncio.Task] = set()
        self._semaphore = asyncio.Semaphore(int(getattr(settings, "kafka_consumer_max_inflight", 20) or 20))
        self._partition_locks: Dict[Tuple[str, int], asyncio.Lock] = defaultdict(asyncio.Lock)
        self._restart_lock = asyncio.Lock()
        self._restart_delay_seconds = 5

    def _consumer_topics(self) -> list[str]:
        return [
            settings.kafka_topic_inbound,
            settings.kafka_topic_retry_30s,
            settings.kafka_topic_retry_2m,
            settings.kafka_topic_retry_10m,
        ]

    def _build_consumer(self) -> AIOKafkaConsumer:
        topics = self._consumer_topics()
        return AIOKafkaConsumer(
            *topics,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.kafka_client_id}-consumer",
            group_id=settings.kafka_group_id,
            enable_auto_commit=False,
            auto_offset_reset=str(getattr(settings, "kafka_consumer_auto_offset_reset", "latest") or "latest"),
            **_kafka_security_config(),
        )

    def _build_producer(self) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.kafka_client_id}-consumer-producer",
            enable_idempotence=bool(getattr(settings, "kafka_producer_idempotence", True)),
            acks="all",
            **_kafka_security_config(),
        )

    async def start(self) -> None:
        if self._consumer is not None:
            return
        await ensure_kafka_topics()
        self._consumer = self._build_consumer()
        self._producer = self._build_producer()
        await self._producer.start()
        await self._consumer.start()
        self._task = asyncio.create_task(self._run(), name="kafka-inbound-consumer")
        logger.info("[KAFKA] Consumer started topics=%s group=%s", self._consumer_topics(), settings.kafka_group_id)

    async def stop(self) -> None:
        self._closing.set()
        if self._task is not None:
            await self._task
            self._task = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
        logger.info("[KAFKA] Consumer stopped")

    async def _run(self) -> None:
        assert self._consumer is not None
        poll_ms = int(getattr(settings, "kafka_consumer_poll_ms", 1000) or 1000)
        max_batch = int(getattr(settings, "kafka_consumer_max_batch", 50) or 50)
        while not self._closing.is_set():
            try:
                batch = await self._consumer.getmany(timeout_ms=poll_ms, max_records=max_batch)
            except Exception as exc:
                if _is_sasl_auth_error(exc):
                    try:
                        await self._restart_for_auth_error(exc)
                    except Exception:
                        logger.exception("[KAFKA] Consumer restart failed")
                        await asyncio.sleep(self._restart_delay_seconds)
                    continue
                logger.error("[KAFKA] Consumer getmany failed: %s", exc, exc_info=True)
                await asyncio.sleep(1)
                continue
            if not batch:
                await asyncio.sleep(0)
                continue
            for _, messages in batch.items():
                for record in messages:
                    await self._semaphore.acquire()
                    task = asyncio.create_task(self._handle_record(record))
                    self._inflight.add(task)
                    task.add_done_callback(self._on_task_done)

        if self._inflight:
            await asyncio.gather(*self._inflight, return_exceptions=True)

    async def _restart_for_auth_error(self, exc: BaseException) -> None:
        async with self._restart_lock:
            if self._closing.is_set():
                return
            logger.warning("[KAFKA] Restarting consumer after auth error: %s", exc)
            if self._consumer is not None:
                try:
                    await self._consumer.stop()
                except Exception:
                    logger.exception("[KAFKA] Failed to stop consumer during restart")
                self._consumer = None
            if self._producer is not None:
                try:
                    await self._producer.stop()
                except Exception:
                    logger.exception("[KAFKA] Failed to stop retry producer during restart")
                self._producer = None
            _reset_iam_token_provider()
            await asyncio.sleep(self._restart_delay_seconds)
            await ensure_kafka_topics()
            self._consumer = self._build_consumer()
            self._producer = self._build_producer()
            await self._producer.start()
            await self._consumer.start()
            logger.info("[KAFKA] Consumer restarted topics=%s group=%s", self._consumer_topics(), settings.kafka_group_id)

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._inflight.discard(task)
        self._semaphore.release()

    async def _handle_record(self, record) -> None:
        assert self._consumer is not None
        tp = TopicPartition(record.topic, record.partition)
        lock = self._partition_locks[(record.topic, record.partition)]
        async with lock:
            event = self._decode_event(record)
            if event is None:
                await self._commit(tp, record.offset)
                return

            retry_after = int(event.get("retry_after_seconds") or 0)
            if retry_after > 0:
                await asyncio.sleep(retry_after)

            handled = False
            try:
                await self._handler(event)
                handled = True
            except Exception as exc:
                handled = await self._handle_failure(event, exc)
            if handled:
                await self._commit(tp, record.offset)

    def _decode_event(self, record) -> Optional[Dict[str, Any]]:
        try:
            payload = record.value.decode("utf-8") if isinstance(record.value, (bytes, bytearray)) else str(record.value)
            event = json.loads(payload)
            if not isinstance(event, dict):
                raise ValueError("Kafka event payload is not a dict")
            event.setdefault("source_topic", record.topic)
            return event
        except Exception as exc:
            logger.error("[KAFKA] Failed to decode event: %s", exc, exc_info=True)
            return None

    async def _commit(self, tp: TopicPartition, offset: int) -> None:
        assert self._consumer is not None
        await self._consumer.commit({tp: offset + 1})

    async def _send_with_auth_retry(self, *, topic: str, event: Dict[str, Any]) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka retry producer not started")
        payload = json.dumps(event, ensure_ascii=True).encode("utf-8")
        key_value = str(event.get("partition_key") or event.get("event_id") or "").encode("utf-8")
        try:
            await self._producer.send_and_wait(topic, value=payload, key=key_value)
        except Exception as exc:
            if _is_sasl_auth_error(exc):
                try:
                    await self._restart_for_auth_error(exc)
                except Exception:
                    logger.exception("[KAFKA] Retry producer restart failed")
                    raise
                if self._producer is None:
                    raise
                await self._producer.send_and_wait(topic, value=payload, key=key_value)
                return
            raise

    async def _handle_failure(self, event: Dict[str, Any], exc: Exception) -> bool:
        if self._producer is None:
            logger.error("[KAFKA] Producer unavailable for retry; will not commit")
            return False
        attempt = int(event.get("attempt") or 0)
        max_attempts = int(getattr(settings, "kafka_max_attempts", 6) or 6)
        retryable = _is_retryable_error(exc)
        event["last_error"] = f"{type(exc).__name__}:{str(exc)[:240]}"
        event["last_error_at"] = _now_iso()

        if retryable and attempt < max_attempts:
            next_attempt = attempt + 1
            delay_seconds = _retry_delay_seconds(next_attempt)
            topic = _retry_topic(next_attempt)
            event["attempt"] = next_attempt
            event["retry_after_seconds"] = delay_seconds
            event["retry_from_topic"] = event.get("source_topic")
            await self._send_with_auth_retry(topic=topic, event=event)
            logger.warning(
                "[KAFKA] Retry scheduled event_id=%s attempt=%d delay=%ds topic=%s",
                str(event.get("event_id") or "")[:40],
                next_attempt,
                delay_seconds,
                topic,
            )
            return True

        event["attempt"] = attempt + 1
        event["retry_after_seconds"] = 0
        await self._send_with_auth_retry(topic=settings.kafka_topic_dlq, event=event)
        logger.error(
            "[KAFKA] Sent to DLQ event_id=%s attempts=%d err=%s",
            str(event.get("event_id") or "")[:40],
            int(event.get("attempt") or 0),
            str(event.get("last_error") or ""),
        )
        return True
```

#### Method: `KafkaInboundConsumer.__init__()`

Big picture and system-design notes (>=5 sentences):
- This constructor initializes all internal state for the Kafka consumer pipeline, including concurrency controls.
- It uses `asyncio.Event`, `asyncio.Semaphore`, and per-partition `asyncio.Lock` objects to manage inflight processing safely.
- It is called when `_init_kafka_consumer()` creates a consumer during application startup.
- The design pattern here is to set up all coordination primitives up front so the runtime loop can be simple and fast.
- For beginners, it’s a good example of preparing shared state for a concurrent worker before any messages arrive.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
def __init__(self, handler: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        self._handler = handler
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._task: Optional[asyncio.Task] = None
        self._closing = asyncio.Event()
        self._inflight: set[asyncio.Task] = set()
        self._semaphore = asyncio.Semaphore(int(getattr(settings, "kafka_consumer_max_inflight", 20) or 20))
        self._partition_locks: Dict[Tuple[str, int], asyncio.Lock] = defaultdict(asyncio.Lock)
        self._restart_lock = asyncio.Lock()
        self._restart_delay_seconds = 5
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer._consumer_topics()`

Big picture and system-design notes (>=5 sentences):
- This method returns the exact list of Kafka topics the consumer should subscribe to.
- It reads topic names from `settings`, which keeps configuration centralized and environment-specific.
- It is used by `_build_consumer()` and by logging statements, so the same list drives both behavior and observability.
- The design idea is to keep subscription logic in one place so retry topic changes are easy.
- For beginners, it shows how a small helper function can reduce duplicated constants across a class.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
def _consumer_topics(self) -> list[str]:
        return [
            settings.kafka_topic_inbound,
            settings.kafka_topic_retry_30s,
            settings.kafka_topic_retry_2m,
            settings.kafka_topic_retry_10m,
        ]
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer._build_consumer()`

Big picture and system-design notes (>=5 sentences):
- This method constructs the `AIOKafkaConsumer` instance with the correct topics and settings.
- It uses `settings` for group ID, offset reset policy, and bootstrap servers, and `_kafka_security_config()` for auth.
- It is called from `start()` and `_restart_for_auth_error()` to ensure consistent configuration on every rebuild.
- The system-design benefit is that a single builder avoids configuration drift between initial start and recovery.
- For beginners, it demonstrates a clean “builder method” pattern for external clients.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
def _build_consumer(self) -> AIOKafkaConsumer:
        topics = self._consumer_topics()
        return AIOKafkaConsumer(
            *topics,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.kafka_client_id}-consumer",
            group_id=settings.kafka_group_id,
            enable_auto_commit=False,
            auto_offset_reset=str(getattr(settings, "kafka_consumer_auto_offset_reset", "latest") or "latest"),
            **_kafka_security_config(),
        )
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer._build_producer()`

Big picture and system-design notes (>=5 sentences):
- This method constructs a dedicated producer used to publish retry and DLQ events.
- It uses `AIOKafkaProducer` with idempotence and `acks="all"` so retry writes are durable.
- It is invoked during `start()` and during auth recovery to keep retry publishing available.
- The design insight is separation of concerns: the consumer uses a separate producer for retries to avoid coupling to ingestion.
- For beginners, it shows how multiple clients can coexist for different roles in a pipeline.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
def _build_producer(self) -> AIOKafkaProducer:
        return AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.kafka_client_id}-consumer-producer",
            enable_idempotence=bool(getattr(settings, "kafka_producer_idempotence", True)),
            acks="all",
            **_kafka_security_config(),
        )
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer.start()`

Big picture and system-design notes (>=5 sentences):
- This method starts the consumer pipeline by ensuring topics, creating clients, and launching the poll loop.
- It uses `ensure_kafka_topics()`, `_build_consumer()`, and `_build_producer()` before starting both clients.
- It spawns `_run()` via `asyncio.create_task` so the polling loop does not block startup.
- The system-design point is that consumer startup is staged and observable, which makes failures easier to diagnose.
- For beginners, it demonstrates how to start background tasks in an async service safely.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def start(self) -> None:
        if self._consumer is not None:
            return
        await ensure_kafka_topics()
        self._consumer = self._build_consumer()
        self._producer = self._build_producer()
        await self._producer.start()
        await self._consumer.start()
        self._task = asyncio.create_task(self._run(), name="kafka-inbound-consumer")
        logger.info("[KAFKA] Consumer started topics=%s group=%s", self._consumer_topics(), settings.kafka_group_id)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer.stop()`

Big picture and system-design notes (>=5 sentences):
- This method shuts down the consumer pipeline in an orderly way.
- It signals the loop to stop via `_closing`, waits for the background task to finish, then stops the consumer and retry producer.
- It is called on application shutdown and during controlled restarts.
- The design lesson is graceful shutdown: finish inflight work when possible, then release resources.
- For beginners, it shows the importance of canceling background loops before closing clients.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def stop(self) -> None:
        self._closing.set()
        if self._task is not None:
            await self._task
            self._task = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
        logger.info("[KAFKA] Consumer stopped")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer._run()`

Big picture and system-design notes (>=5 sentences):
- This is the main polling loop that continuously reads batches from Kafka.
- It uses `AIOKafkaConsumer.getmany`, `asyncio.create_task`, and a semaphore to control concurrency and avoid overload.
- SASL authentication failures are intercepted and trigger `_restart_for_auth_error()` for self-recovery.
- The system-design insight here is explicit backpressure: you cap inflight work while still maximizing throughput.
- For beginners, this method shows how to build a resilient async loop with error handling and task fan-out.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def _run(self) -> None:
        assert self._consumer is not None
        poll_ms = int(getattr(settings, "kafka_consumer_poll_ms", 1000) or 1000)
        max_batch = int(getattr(settings, "kafka_consumer_max_batch", 50) or 50)
        while not self._closing.is_set():
            try:
                batch = await self._consumer.getmany(timeout_ms=poll_ms, max_records=max_batch)
            except Exception as exc:
                if _is_sasl_auth_error(exc):
                    try:
                        await self._restart_for_auth_error(exc)
                    except Exception:
                        logger.exception("[KAFKA] Consumer restart failed")
                        await asyncio.sleep(self._restart_delay_seconds)
                    continue
                logger.error("[KAFKA] Consumer getmany failed: %s", exc, exc_info=True)
                await asyncio.sleep(1)
                continue
            if not batch:
                await asyncio.sleep(0)
                continue
            for _, messages in batch.items():
                for record in messages:
                    await self._semaphore.acquire()
                    task = asyncio.create_task(self._handle_record(record))
                    self._inflight.add(task)
                    task.add_done_callback(self._on_task_done)

        if self._inflight:
            await asyncio.gather(*self._inflight, return_exceptions=True)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer._restart_for_auth_error()`

Big picture and system-design notes (>=5 sentences):
- This method restarts the consumer and retry producer after a SASL authentication failure.
- It uses a restart lock, stops both clients, resets IAM token state, sleeps, and then rebuilds fresh clients.
- It is invoked from both the polling loop and retry publishing, so recovery is centralized.
- The system-design point is coordinated recovery across dependent components to avoid partial failure states.
- For beginners, it demonstrates the importance of serialized restarts to prevent restart storms.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def _restart_for_auth_error(self, exc: BaseException) -> None:
        async with self._restart_lock:
            if self._closing.is_set():
                return
            logger.warning("[KAFKA] Restarting consumer after auth error: %s", exc)
            if self._consumer is not None:
                try:
                    await self._consumer.stop()
                except Exception:
                    logger.exception("[KAFKA] Failed to stop consumer during restart")
                self._consumer = None
            if self._producer is not None:
                try:
                    await self._producer.stop()
                except Exception:
                    logger.exception("[KAFKA] Failed to stop retry producer during restart")
                self._producer = None
            _reset_iam_token_provider()
            await asyncio.sleep(self._restart_delay_seconds)
            await ensure_kafka_topics()
            self._consumer = self._build_consumer()
            self._producer = self._build_producer()
            await self._producer.start()
            await self._consumer.start()
            logger.info("[KAFKA] Consumer restarted topics=%s group=%s", self._consumer_topics(), settings.kafka_group_id)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer._on_task_done()`

Big picture and system-design notes (>=5 sentences):
- This callback removes a completed task from the inflight set and releases a semaphore permit.
- It is attached to each task spawned in `_run()` to ensure backpressure is relieved reliably.
- It uses `asyncio.Semaphore` semantics to cap concurrency without blocking the event loop.
- The system-design lesson is that concurrency limits are only safe if you always release permits on completion.
- For beginners, it shows a common pattern for tracking and cleaning up async tasks.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
def _on_task_done(self, task: asyncio.Task) -> None:
        self._inflight.discard(task)
        self._semaphore.release()
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer._handle_record()`

Big picture and system-design notes (>=5 sentences):
- This method processes one Kafka record while preserving order within its partition.
- It uses a per-partition `asyncio.Lock`, `_decode_event()` for parsing, and `_handle_failure()` for retry/DLQ decisions.
- Successful handling (or handled failure) results in a commit via `_commit()` so offsets move forward safely.
- The system-design focus is at-least-once processing with ordered execution per partition key.
- For beginners, it shows how locks, retries, and commits combine into a reliable handler.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def _handle_record(self, record) -> None:
        assert self._consumer is not None
        tp = TopicPartition(record.topic, record.partition)
        lock = self._partition_locks[(record.topic, record.partition)]
        async with lock:
            event = self._decode_event(record)
            if event is None:
                await self._commit(tp, record.offset)
                return

            retry_after = int(event.get("retry_after_seconds") or 0)
            if retry_after > 0:
                await asyncio.sleep(retry_after)

            handled = False
            try:
                await self._handler(event)
                handled = True
            except Exception as exc:
                handled = await self._handle_failure(event, exc)
            if handled:
                await self._commit(tp, record.offset)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer._decode_event()`

Big picture and system-design notes (>=5 sentences):
- This method parses a Kafka record payload into a Python dict and tags it with the source topic.
- It uses `json.loads` and validates that the decoded payload is a dictionary, logging errors when parsing fails.
- It is called early in `_handle_record()` so downstream processing assumes a consistent event shape.
- The system-design principle is “validate at the boundary” to prevent malformed messages from propagating.
- For beginners, it shows how to handle untrusted input safely and observably.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
def _decode_event(self, record) -> Optional[Dict[str, Any]]:
        try:
            payload = record.value.decode("utf-8") if isinstance(record.value, (bytes, bytearray)) else str(record.value)
            event = json.loads(payload)
            if not isinstance(event, dict):
                raise ValueError("Kafka event payload is not a dict")
            event.setdefault("source_topic", record.topic)
            return event
        except Exception as exc:
            logger.error("[KAFKA] Failed to decode event: %s", exc, exc_info=True)
            return None
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer._commit()`

Big picture and system-design notes (>=5 sentences):
- This method commits the consumer offset for a specific topic-partition.
- It uses `AIOKafkaConsumer.commit` with an explicit offset+1 to acknowledge processing.
- It is called after successful handling or after a retry/DLQ handoff so progress is only recorded when safe.
- The system-design insight is explicit offset control, which underpins at-least-once delivery guarantees.
- For beginners, it shows why commit timing matters for avoiding message loss or duplicate processing.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def _commit(self, tp: TopicPartition, offset: int) -> None:
        assert self._consumer is not None
        await self._consumer.commit({tp: offset + 1})
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer._send_with_auth_retry()`

Big picture and system-design notes (>=5 sentences):
- This method publishes retry or DLQ events using the internal retry producer.
- It serializes events with `json.dumps` and uses `AIOKafkaProducer.send_and_wait` for durable writes.
- If SASL auth fails, it calls `_restart_for_auth_error()` and retries once after recovery.
- The system-design point is centralized reliability logic for retry publishing, which reduces duplication and drift.
- For beginners, it demonstrates wrapping I/O with error recovery to keep pipelines resilient.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def _send_with_auth_retry(self, *, topic: str, event: Dict[str, Any]) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka retry producer not started")
        payload = json.dumps(event, ensure_ascii=True).encode("utf-8")
        key_value = str(event.get("partition_key") or event.get("event_id") or "").encode("utf-8")
        try:
            await self._producer.send_and_wait(topic, value=payload, key=key_value)
        except Exception as exc:
            if _is_sasl_auth_error(exc):
                try:
                    await self._restart_for_auth_error(exc)
                except Exception:
                    logger.exception("[KAFKA] Retry producer restart failed")
                    raise
                if self._producer is None:
                    raise
                await self._producer.send_and_wait(topic, value=payload, key=key_value)
                return
            raise
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `KafkaInboundConsumer._handle_failure()`

Big picture and system-design notes (>=5 sentences):
- This method decides whether a failed event should be retried or sent to the DLQ.
- It updates the event with error metadata and uses `_is_retryable_error()`, `_retry_delay_seconds()`, and `_retry_topic()` to choose the next step.
- It publishes the updated event via `_send_with_auth_retry()` so retry publishing is resilient to auth glitches.
- The system-design lesson is bounded retries with explicit delay tiers and a terminal dead-letter sink.
- For beginners, it’s a concrete example of error classification driving retry behavior.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
async def _handle_failure(self, event: Dict[str, Any], exc: Exception) -> bool:
        if self._producer is None:
            logger.error("[KAFKA] Producer unavailable for retry; will not commit")
            return False
        attempt = int(event.get("attempt") or 0)
        max_attempts = int(getattr(settings, "kafka_max_attempts", 6) or 6)
        retryable = _is_retryable_error(exc)
        event["last_error"] = f"{type(exc).__name__}:{str(exc)[:240]}"
        event["last_error_at"] = _now_iso()

        if retryable and attempt < max_attempts:
            next_attempt = attempt + 1
            delay_seconds = _retry_delay_seconds(next_attempt)
            topic = _retry_topic(next_attempt)
            event["attempt"] = next_attempt
            event["retry_after_seconds"] = delay_seconds
            event["retry_from_topic"] = event.get("source_topic")
            await self._send_with_auth_retry(topic=topic, event=event)
            logger.warning(
                "[KAFKA] Retry scheduled event_id=%s attempt=%d delay=%ds topic=%s",
                str(event.get("event_id") or "")[:40],
                next_attempt,
                delay_seconds,
                topic,
            )
            return True

        event["attempt"] = attempt + 1
        event["retry_after_seconds"] = 0
        await self._send_with_auth_retry(topic=settings.kafka_topic_dlq, event=event)
        logger.error(
            "[KAFKA] Sent to DLQ event_id=%s attempts=%d err=%s",
            str(event.get("event_id") or "")[:40],
            int(event.get("attempt") or 0),
            str(event.get("last_error") or ""),
        )
        return True
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

### Function: `_retry_delay_seconds()`

Big picture and system-design notes (>=5 sentences):
- This function maps a retry attempt number to a delay in seconds.
- It uses the `_RETRY_DELAYS_SECONDS` tuple to keep retry timing deterministic and bounded.
- It is called by `_handle_failure()` to schedule the next retry topic delay.
- The system-design idea is to implement tiered backoff without external dependencies.
- For beginners, it shows how to encode retry policy in a simple, testable helper.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _retry_delay_seconds(attempt: int) -> int:
    idx = min(max(1, attempt) - 1, len(_RETRY_DELAYS_SECONDS) - 1)
    return int(_RETRY_DELAYS_SECONDS[idx])
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_retry_topic()`

Big picture and system-design notes (>=5 sentences):
- This function maps a retry attempt number to the correct retry topic.
- It uses retry topic names from `settings`, ensuring environment-specific topic routing.
- It is used by `_handle_failure()` so retries go to the correct delay tier (30s, 2m, 10m).
- The system-design idea is explicit retry lanes rather than implicit sleep-based retries in consumers.
- For beginners, it demonstrates how routing by topic can implement delayed retries.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _retry_topic(attempt: int) -> str:
    topics = (
        settings.kafka_topic_retry_30s,
        settings.kafka_topic_retry_2m,
        settings.kafka_topic_retry_10m,
    )
    idx = min(max(1, attempt) - 1, len(topics) - 1)
    return topics[idx]
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_is_retryable_error()`

Big picture and system-design notes (>=5 sentences):
- This function classifies whether an exception should trigger a retry.
- It treats timeouts, connection errors, and OS errors as retryable, while `ValueError` is explicitly non-retryable.
- It is used by `_handle_failure()` to decide whether to send to a retry topic or DLQ.
- The system-design lesson is to build a clear error taxonomy to prevent infinite retries on bad data.
- For beginners, it shows how to codify retry policy in a central function.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _is_retryable_error(exc: Exception) -> bool:
    retryable_types = (
        TimeoutError,
        ConnectionError,
        OSError,
        asyncio.TimeoutError,
    )
    if isinstance(exc, retryable_types):
        return True
    if isinstance(exc, ValueError):
        return False
    return True
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_kafka_required_topics()`

Big picture and system-design notes (>=5 sentences):
- This function returns the canonical list of Kafka topics required by the pipeline.
- It pulls topic names from `settings`, which keeps config centralized and environment-specific.
- It is called by `ensure_kafka_topics()` to know which topics to create and verify.
- The system-design point is a single source of truth for required topics, avoiding mismatches across producers/consumers.
- For beginners, it shows how a tiny helper can prevent configuration drift.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _kafka_required_topics() -> list[str]:
    return [
        settings.kafka_topic_inbound,
        settings.kafka_topic_retry_30s,
        settings.kafka_topic_retry_2m,
        settings.kafka_topic_retry_10m,
        settings.kafka_topic_dlq,
    ]
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `ensure_kafka_topics()`

Big picture and system-design notes (>=5 sentences):
- This function ensures all required Kafka topics exist before producers or consumers run.
- It uses `AIOKafkaAdminClient`, `NewTopic`, and a global lock/flag to make the operation idempotent per process.
- It lists existing topics, creates missing ones, and verifies that required topics are present afterward.
- The system-design insight is to front-load infrastructure validation so runtime failures are reduced.
- For beginners, it demonstrates safe bootstrap logic with locks, retries, and explicit verification.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Startup sequence (Kafka initialization).

Function code:
```python
async def ensure_kafka_topics() -> None:
    global _TOPIC_BOOTSTRAP_DONE
    if _TOPIC_BOOTSTRAP_DONE:
        return
    async with _TOPIC_BOOTSTRAP_LOCK:
        if _TOPIC_BOOTSTRAP_DONE:
            return
        topics_needed = _kafka_required_topics()
        admin = AIOKafkaAdminClient(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            client_id=f"{settings.kafka_client_id}-topic-bootstrap",
            **_kafka_security_config(),
        )
        await admin.start()
        try:
            existing = set(await admin.list_topics())
            to_create = [
                NewTopic(
                    name=name,
                    num_partitions=int(getattr(settings, "kafka_topic_partitions", 12) or 12),
                    replication_factor=int(getattr(settings, "kafka_topic_replication_factor", 3) or 3),
                )
                for name in topics_needed
                if name not in existing
            ]
            if to_create:
                try:
                    await admin.create_topics(to_create, validate_only=False)
                except TopicAlreadyExistsError:
                    pass
                logger.info("[KAFKA] Topic bootstrap created topics=%s", [t.name for t in to_create])
            existing_after = set(await admin.list_topics())
            missing = [name for name in topics_needed if name not in existing_after]
            if missing:
                raise RuntimeError(f"Kafka topics missing after bootstrap: {missing}")
            _TOPIC_BOOTSTRAP_DONE = True
        finally:
            await admin.close()
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

## Module: `app/integrations/kafka_topic_bootstrap.py`

Role: Standalone topic bootstrap runner for Kafka.

Imported stack (selected):
- __future__.annotations, app.config.settings, app.integrations.kafka_pipeline.ensure_kafka_topics, asyncio

### Function: `ensure_topics()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Standalone entrypoint to bootstrap Kafka topics. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
async def ensure_topics() -> list[str]:
    await ensure_kafka_topics()
    return [
        settings.kafka_topic_inbound,
        settings.kafka_topic_retry_30s,
        settings.kafka_topic_retry_2m,
        settings.kafka_topic_retry_10m,
        settings.kafka_topic_dlq,
    ]
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_main()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Standalone entrypoint to bootstrap Kafka topics. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
async def _main() -> None:
    created = await ensure_topics()
    print("Created topics:", created)
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

## Module: `app/integrations/photon_listener.py`

Role: Photon Socket.IO listener that feeds inbound messages into the app.

Module docstring:
- Socket.IO listener that streams Photon iMessage events into our FastAPI app.

Imported stack (selected):
- __future__.annotations, asyncio, cachetools.TTLCache, datetime.datetime, logging, os, socketio, time, typing.Any, typing.Awaitable, typing.Callable, typing.Dict, typing.Optional

### Function: `_in_memory_dedupe()`

Big picture and system-design notes (>=5 sentences):
- This function provides a fast, in-process dedupe check for inbound Photon events.
- It uses `cachetools.TTLCache` and `time.time()` to avoid repeated processing during Socket.IO reconnect storms.
- It is called in `PhotonListener._handle_new_message()` before Redis idempotency to reduce load and prevent duplicates.
- The system-design benefit is a lightweight “first line of defense” that is bounded in size to avoid memory blowups.
- For beginners, it shows how to use TTL caches to mitigate duplicate events without external storage.

What it does (docstring if available):
- Check if a message key has been seen recently.
- 
- Uses a TTLCache with bounded size to prevent memory exhaustion.
- Note: ttl_seconds parameter is ignored (TTLCache uses fixed TTL), kept for API compatibility.
- 
- Args:
-     key: Unique message identifier
-     ttl_seconds: Ignored - using TTLCache's fixed TTL
- 
- Returns:
-     True if this is a new message, False if duplicate

When used:
- Per-message sequence (Photon inbound).

Function code:
```python
def _in_memory_dedupe(key: str, *, ttl_seconds: int = _INBOUND_DEDUPE_TTL_SECONDS) -> bool:
    """Check if a message key has been seen recently.

    Uses a TTLCache with bounded size to prevent memory exhaustion.
    Note: ttl_seconds parameter is ignored (TTLCache uses fixed TTL), kept for API compatibility.

    Args:
        key: Unique message identifier
        ttl_seconds: Ignored - using TTLCache's fixed TTL

    Returns:
        True if this is a new message, False if duplicate
    """
    if not key:
        return True

    if key in _INBOUND_DEDUPE_CACHE:
        return False

    _INBOUND_DEDUPE_CACHE[key] = time.time()
    return True
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_get_memory_mb()`

Big picture and system-design notes (>=5 sentences):
- This function reports process RSS memory in MB for crash/health diagnostics.
- It uses `psutil.Process(os.getpid())` if available, and returns -1.0 if psutil is missing.
- It is called in `_handle_new_message()` to add memory info to crash-detection logs.
- The system-design point is optional instrumentation that does not break the main flow when dependencies are absent.
- For beginners, it demonstrates a safe “best-effort” observability helper.

What it does (docstring if available):
- Get current process memory usage in MB.

When used:
- Per-message sequence (Photon inbound).

Function code:
```python
def _get_memory_mb() -> float:
    """Get current process memory usage in MB."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    except Exception:
        return -1.0
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_is_group_chat()`

Big picture and system-design notes (>=5 sentences):
- This function identifies whether a chat GUID represents a group chat.
- It uses simple string patterns (`;+;` or `chat` prefix) to classify the GUID.
- It is used by `PhotonListener` to route group messages differently from DMs.
- The system-design insight is that small parsing helpers keep domain rules centralized and consistent.
- For beginners, it shows how lightweight string checks can drive significant control flow decisions.

What it does (docstring if available):
- Determine if a chat GUID represents a group chat.
- 
- Chat GUID formats:
- - DM: "any;-;+12152073992" or "iMessage;-;email@example.com" (contains ";-;")
- - Group: "any;+;chat123456789" (contains ";+;")

When used:
- Per-message sequence (Photon inbound).

Function code:
```python
def _is_group_chat(chat_guid: str) -> bool:
    """
    Determine if a chat GUID represents a group chat.

    Chat GUID formats:
    - DM: "any;-;+12152073992" or "iMessage;-;email@example.com" (contains ";-;")
    - Group: "any;+;chat123456789" (contains ";+;")
    """
    if not chat_guid:
        return False
    guid = str(chat_guid)
    return ";+;" in guid or guid.startswith("chat")
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Class: `PhotonListener`

Big picture:
- Manages inbound Photon Socket.IO connectivity.

Purpose:
- Connects to Photon Socket.IO server and forwards inbound messages to a callback.
- 
- Listens for 'new-message' events from Photon's Socket.IO gateway and
- transforms them into a format compatible with our application's webhook handler.

When used:
- Per-message sequence (Photon inbound).

Class code:
```python
class PhotonListener:
    """
    Connects to Photon Socket.IO server and forwards inbound messages to a callback.

    Listens for 'new-message' events from Photon's Socket.IO gateway and
    transforms them into a format compatible with our application's webhook handler.
    """

    def __init__(
        self,
        server_url: str,
        default_number: str,
        message_callback: Callable[[Dict[str, Any]], Awaitable[None]],
        api_key: Optional[str] = None,
    ):
        if not server_url:
            raise ValueError("Photon server URL is required")

        self.server_url = server_url.rstrip("/")
        if not self.server_url.startswith("http"):
            self.server_url = f"https://{self.server_url}"

        self.default_number = default_number
        self._callback = message_callback
        self.api_key = api_key
        self._client = socketio.AsyncClient(
            reconnection=True,
            reconnection_attempts=0,  # Infinite reconnection attempts
        )
        # Register event handlers
        self._client.on("connect", self._on_connect)
        self._client.on("disconnect", self._on_disconnect)
        self._client.on("new-message", self._handle_new_message)
        self._client.on("message-send-error", self._handle_error)
        self._client.on("error", self._handle_error)

        # Debug: Log ALL Socket.IO events
        @self._client.event
        async def __generic_event(event, *args):
            logger.debug(f"[PHOTON] Received event: {event}, args: {args}")

    async def start(self) -> None:
        """Connect to Photon server."""
        logger.info("[PHOTON] Connecting to %s", self.server_url)
        try:
            # Include API key in auth if provided (Photon v1.2.1+)
            auth = {"apiKey": self.api_key} if self.api_key else None
            await self._client.connect(self.server_url, transports=["websocket"], auth=auth)
            logger.info("[PHOTON] ✅ Successfully connected to Photon server")
        except Exception as e:
            logger.error(f"[PHOTON] ❌ Failed to connect to Photon server: {e}")
            raise

    async def stop(self) -> None:
        """Disconnect from Photon server."""
        if self._client.connected:
            await self._client.disconnect()
            logger.info("[PHOTON] Disconnected from server")

    async def _on_connect(self) -> None:
        logger.info("[PHOTON] Socket connected successfully")

    async def _on_disconnect(self) -> None:
        logger.warning("[PHOTON] Socket disconnected - will attempt to reconnect")

    async def _handle_error(self, data: Any) -> None:
        logger.error(f"[PHOTON] Socket error: {data}")

    async def _handle_new_message(self, message: Dict[str, Any]) -> None:
        """
        Handle inbound message from Photon and forward to callback.

        Transforms Photon's message format into a format compatible with
        our application's SendBlue webhook handler.

        Args:
            message: Raw message data from Photon Socket.IO event
        """
        # Crash detection: Log at entry point
        pid = os.getpid()
        mem_mb = _get_memory_mb()
        message_guid = message.get("guid", "NO_GUID")
        logger.info(f"[CRASH DETECT] MESSAGE START - PID={pid}, Memory={mem_mb:.1f}MB, GUID={message_guid[:30] if message_guid != 'NO_GUID' else 'NO_GUID'}")

        logger.info(f"[PHOTON] Received new-message event: {message}")
        try:
            if not message:
                logger.debug("[PHOTON] Dropping empty message")
                return

            # Skip messages from ourselves
            if message.get("isFromMe"):
                logger.debug("[PHOTON] Ignoring message from self")
                return  # Ignore echoes

            # Extract sender information
            handle = message.get("handle") or {}
            from_number = handle.get("address") or handle.get("id")
            if not from_number:
                logger.warning("[PHOTON] Dropping message without handle: %s", message)
                return

            # Extract message text
            text = message.get("text")
            if not text:
                # Fallback to attributed body if available
                attributed = message.get("attributedBody")
                if isinstance(attributed, list) and attributed:
                    text = attributed[0].get("string")

            # Extract attachments
            # Photon attachments have a 'guid' field - use sdk.attachments.downloadAttachment(guid) to get the file
            media_url = None
            attachments = message.get("attachments")
            if isinstance(attachments, list) and attachments:
                first_attachment = attachments[0]
                logger.info(f"[PHOTON] Found attachment: {first_attachment}")

                if isinstance(first_attachment, dict):
                    # Primary: use attachment guid (Photon's identifier)
                    attachment_guid = first_attachment.get("guid")
                    if attachment_guid:
                        media_url = f"photon-attachment:{attachment_guid}"
                        logger.info(f"[PHOTON] Found attachment guid: {attachment_guid}")
                    else:
                        # Fallback: try other field names
                        media_url = (
                            first_attachment.get("path")
                            or first_attachment.get("filename")
                            or first_attachment.get("filePath")
                            or first_attachment.get("transferName")
                        )
                        # If still no media_url but attachment exists, mark it as present
                        if not media_url and first_attachment:
                            mime = first_attachment.get("mime") or first_attachment.get("mimeType") or "unknown"
                            media_url = f"attachment:{mime}"
                            logger.info(f"[PHOTON] Using placeholder media_url: {media_url}")
                elif isinstance(first_attachment, str):
                    media_url = first_attachment

                if media_url:
                    logger.info(f"[PHOTON] Extracted media_url: {media_url}")

            if not text and not media_url:
                logger.debug("[PHOTON] Dropping empty message from %s", from_number)
                return

            # Extract chat GUID
            chat_guid = (
                message.get("chatGuid")
                or message.get("chat_guid")
                or message.get("chatGUID")
                or None
            )
            chats = message.get("chats") or message.get("chat")
            if not chat_guid:
                if isinstance(chats, list) and chats and isinstance(chats[0], dict):
                    chat_guid = chats[0].get("guid") or chats[0].get("chatGuid") or chats[0].get("chat_guid")
                elif isinstance(chats, dict):
                    chat_guid = chats.get("guid") or chats.get("chatGuid") or chats.get("chat_guid")

            # Forward group chat messages too; the orchestrator will decide how to handle them.
            if chat_guid and _is_group_chat(chat_guid):
                logger.info(f"[PHOTON] Received group chat message from {from_number} in {chat_guid[:40]}...")

            # Idempotency check - prevent duplicate message processing.
            # Socket.IO may re-deliver messages during reconnection, but for group chats we prefer
            # to be fail-open (never block a legitimate user reply), so duplicates only log.
            import hashlib

            is_group = bool(chat_guid and _is_group_chat(chat_guid))

            fingerprint_payload = "|".join(
                [
                    str(from_number or "unknown"),
                    str(chat_guid or ""),
                    str(text or ""),
                    str(media_url or ""),
                ]
            )
            content_fingerprint = hashlib.sha256(fingerprint_payload.encode("utf-8")).hexdigest()[:16]

            # In-process dedupe for all inbound events in case Redis is down or Photon re-delivers with a new GUID.
            if not _in_memory_dedupe(f"photon_in:{from_number}:{content_fingerprint}"):
                logger.info("[PHOTON] In-memory dedupe skip sender=%s", from_number)
                return

            message_guid = message.get("guid")
            # Use environment-based prefix to prevent local/production key conflicts
            from app.config import settings
            env_prefix = "dev_" if settings.app_env == "development" else ""
            if message_guid:
                idempotency_key = f"{env_prefix}photon_msg:{message_guid}"
            else:
                idempotency_key = f"{env_prefix}photon_msg_hash:{content_fingerprint}"
                logger.warning(f"[PHOTON] No GUID in message, using content hash: {idempotency_key}")

            ingest_mode = str(getattr(settings, "photon_ingest_mode", "listener") or "").strip().lower()
            if ingest_mode != "kafka":
                logger.info(f"[CRASH DETECT] BEFORE IDEMPOTENCY CHECK - PID={pid}")
                logger.info(f"[PHOTON] Checking idempotency for: {idempotency_key}")
                from app.utils.redis_client import redis_client
                is_new = redis_client.check_idempotency(idempotency_key, ttl=300)
                logger.info(f"[PHOTON] Idempotency check result: is_new={is_new}")
                logger.info(f"[CRASH DETECT] AFTER IDEMPOTENCY CHECK - PID={pid}, is_new={is_new}")
                if not is_new:
                    logger.info(f"[PHOTON] Skipping duplicate message: {idempotency_key}")
                    return  # Already processed within TTL
            else:
                logger.info("[PHOTON] Skipping Redis idempotency in kafka ingest mode")

            # Cache the real chat GUID from Apple for this sender (DMs only).
            # This enables typing indicators to work correctly for phone numbers
            # by using Apple's actual GUID (which includes iMessage;-; or SMS;-; prefix).
            # Do NOT cache group chat GUIDs under a sender handle (would break DM typing indicators).
            if chat_guid and from_number and not _is_group_chat(chat_guid):
                from app.utils.redis_client import redis_client
                redis_client.cache_chat_guid(from_number, chat_guid, ttl=86400)
                logger.debug(f"[PHOTON] Cached chat GUID for {from_number}: {chat_guid[:30]}...")

            # Transform to SendBlue-compatible format for our webhook handler
            payload = {
                "from_number": from_number,
                "to_number": self.default_number,
                "content": text,
                "message_id": message_guid or f"photon_hash:{content_fingerprint}",
                "timestamp": datetime.utcnow().isoformat(),
                "chat_guid": chat_guid,
                "is_outbound": False,  # Inbound message
                "status": "received",
                "media_url": media_url,  # Pass extracted media URL
            }

            logger.info(f"[PHOTON] Forwarding message to callback - from: {from_number}, text: {text[:50]}...")
            logger.debug(f"[PHOTON] Full payload: {payload}")

            # Forward to callback and await it so exceptions are properly caught
            logger.info(f"[CRASH DETECT] BEFORE CALLBACK - PID={pid}, Memory={_get_memory_mb():.1f}MB")
            await self._callback(payload)
            logger.info(f"[CRASH DETECT] AFTER CALLBACK - PID={pid}, Memory={_get_memory_mb():.1f}MB")

        except SystemExit as exc:
            logger.critical(f"[CRASH DETECT] SystemExit caught in message handler - PID={pid}: {exc}", exc_info=True)
            raise  # Re-raise to preserve exit behavior
        except KeyboardInterrupt as exc:
            logger.critical(f"[CRASH DETECT] KeyboardInterrupt caught in message handler - PID={pid}: {exc}", exc_info=True)
            raise  # Re-raise to preserve interrupt behavior
        except Exception as exc:
            logger.error(f"[CRASH DETECT] Exception in message handler - PID={pid}: {exc}", exc_info=True)
            logger.error("[PHOTON] Failed to process inbound message: %s", exc, exc_info=True)
```

#### Method: `PhotonListener.__init__()`

Big picture and system-design notes (>=5 sentences):
- This constructor prepares the Socket.IO client that listens to Photon inbound events.
- It normalizes the server URL, stores the callback, and wires event handlers for connect/disconnect/new-message/error.
- It uses `socketio.AsyncClient` with automatic reconnection, which keeps the listener resilient to transient network drops.
- The system-design pattern is event-driven ingestion: you register handlers once and let the client push events to you.
- For beginners, it shows how to sanitize input configuration and set up callbacks for an async event source.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Per-message sequence (Photon inbound).

Method code:
```python
def __init__(
        self,
        server_url: str,
        default_number: str,
        message_callback: Callable[[Dict[str, Any]], Awaitable[None]],
        api_key: Optional[str] = None,
    ):
        if not server_url:
            raise ValueError("Photon server URL is required")

        self.server_url = server_url.rstrip("/")
        if not self.server_url.startswith("http"):
            self.server_url = f"https://{self.server_url}"

        self.default_number = default_number
        self._callback = message_callback
        self.api_key = api_key
        self._client = socketio.AsyncClient(
            reconnection=True,
            reconnection_attempts=0,  # Infinite reconnection attempts
        )
        # Register event handlers
        self._client.on("connect", self._on_connect)
        self._client.on("disconnect", self._on_disconnect)
        self._client.on("new-message", self._handle_new_message)
        self._client.on("message-send-error", self._handle_error)
        self._client.on("error", self._handle_error)

        # Debug: Log ALL Socket.IO events
        @self._client.event
        async def __generic_event(event, *args):
            logger.debug(f"[PHOTON] Received event: {event}, args: {args}")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonListener.start()`

Big picture and system-design notes (>=5 sentences):
- This method connects the Socket.IO client to the Photon server and begins listening for events.
- It uses `socketio.AsyncClient.connect` with the websocket transport and optional API-key auth.
- It is called during FastAPI startup when the ingest mode enables the listener.
- The system-design point is explicit connection management with logging and error propagation for visibility.
- For beginners, it shows how to connect to an async event source and handle failures early.

What it does (docstring if available):
- Connect to Photon server.

When used:
- Per-message sequence (Photon inbound).

Method code:
```python
async def start(self) -> None:
        """Connect to Photon server."""
        logger.info("[PHOTON] Connecting to %s", self.server_url)
        try:
            # Include API key in auth if provided (Photon v1.2.1+)
            auth = {"apiKey": self.api_key} if self.api_key else None
            await self._client.connect(self.server_url, transports=["websocket"], auth=auth)
            logger.info("[PHOTON] ✅ Successfully connected to Photon server")
        except Exception as e:
            logger.error(f"[PHOTON] ❌ Failed to connect to Photon server: {e}")
            raise
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonListener.stop()`

Big picture and system-design notes (>=5 sentences):
- This method disconnects from the Photon Socket.IO server if the client is connected.
- It uses `AsyncClient.disconnect` to close the websocket cleanly and logs the shutdown.
- It is called during application shutdown to avoid lingering network connections.
- The system-design lesson is that graceful teardown is part of reliability, not an afterthought.
- For beginners, it shows how to guard disconnects with a connection check.

What it does (docstring if available):
- Disconnect from Photon server.

When used:
- Per-message sequence (Photon inbound).

Method code:
```python
async def stop(self) -> None:
        """Disconnect from Photon server."""
        if self._client.connected:
            await self._client.disconnect()
            logger.info("[PHOTON] Disconnected from server")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonListener._on_connect()`

Big picture and system-design notes (>=5 sentences):
- This event handler runs when the Socket.IO client connects successfully.
- It logs the connection for observability and troubleshooting.
- It is registered in the constructor and invoked by the Socket.IO library, not manually.
- The system-design value is simple, reliable signals that make uptime and reconnection visible.
- For beginners, it illustrates how callbacks are wired to network events.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Per-message sequence (Photon inbound).

Method code:
```python
async def _on_connect(self) -> None:
        logger.info("[PHOTON] Socket connected successfully")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonListener._on_disconnect()`

Big picture and system-design notes (>=5 sentences):
- This handler fires when the Socket.IO connection drops.
- It logs a warning; reconnection is handled by the `socketio.AsyncClient` configuration.
- It helps operators spot unstable connectivity without adding heavy recovery logic in the handler.
- The system-design takeaway is to keep event handlers lightweight and rely on client reconnection policy.
- For beginners, it shows how to react to disconnect events without blocking the event loop.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Per-message sequence (Photon inbound).

Method code:
```python
async def _on_disconnect(self) -> None:
        logger.warning("[PHOTON] Socket disconnected - will attempt to reconnect")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonListener._handle_error()`

Big picture and system-design notes (>=5 sentences):
- This handler logs Socket.IO error events reported by the Photon client.
- It provides a centralized place to surface errors without crashing the listener.
- It is invoked by the Socket.IO library whenever an error event is emitted.
- The system-design principle is to keep error handling lightweight and observable.
- For beginners, it highlights why logging errors matters in event-driven systems.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Per-message sequence (Photon inbound).

Method code:
```python
async def _handle_error(self, data: Any) -> None:
        logger.error(f"[PHOTON] Socket error: {data}")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonListener._handle_new_message()`

Big picture and system-design notes (>=5 sentences):
- This is the core inbound handler that normalizes Photon events into the app’s webhook payload format.
- It performs validation, skips self-messages, extracts text/attachments, and detects group chats using `_is_group_chat()`.
- It applies in-memory dedupe and optionally Redis idempotency, then calls the configured callback (Kafka publish or direct handling).
- The system-design focus is boundary hygiene: normalize input, enforce idempotency, and keep side effects controlled.
- For beginners, it’s a full example of turning raw network events into a clean, safe internal message.

What it does (docstring if available):
- Handle inbound message from Photon and forward to callback.
- 
- Transforms Photon's message format into a format compatible with
- our application's SendBlue webhook handler.
- 
- Args:
-     message: Raw message data from Photon Socket.IO event

When used:
- Per-message sequence (Photon inbound).

Method code:
```python
async def _handle_new_message(self, message: Dict[str, Any]) -> None:
        """
        Handle inbound message from Photon and forward to callback.

        Transforms Photon's message format into a format compatible with
        our application's SendBlue webhook handler.

        Args:
            message: Raw message data from Photon Socket.IO event
        """
        # Crash detection: Log at entry point
        pid = os.getpid()
        mem_mb = _get_memory_mb()
        message_guid = message.get("guid", "NO_GUID")
        logger.info(f"[CRASH DETECT] MESSAGE START - PID={pid}, Memory={mem_mb:.1f}MB, GUID={message_guid[:30] if message_guid != 'NO_GUID' else 'NO_GUID'}")

        logger.info(f"[PHOTON] Received new-message event: {message}")
        try:
            if not message:
                logger.debug("[PHOTON] Dropping empty message")
                return

            # Skip messages from ourselves
            if message.get("isFromMe"):
                logger.debug("[PHOTON] Ignoring message from self")
                return  # Ignore echoes

            # Extract sender information
            handle = message.get("handle") or {}
            from_number = handle.get("address") or handle.get("id")
            if not from_number:
                logger.warning("[PHOTON] Dropping message without handle: %s", message)
                return

            # Extract message text
            text = message.get("text")
            if not text:
                # Fallback to attributed body if available
                attributed = message.get("attributedBody")
                if isinstance(attributed, list) and attributed:
                    text = attributed[0].get("string")

            # Extract attachments
            # Photon attachments have a 'guid' field - use sdk.attachments.downloadAttachment(guid) to get the file
            media_url = None
            attachments = message.get("attachments")
            if isinstance(attachments, list) and attachments:
                first_attachment = attachments[0]
                logger.info(f"[PHOTON] Found attachment: {first_attachment}")

                if isinstance(first_attachment, dict):
                    # Primary: use attachment guid (Photon's identifier)
                    attachment_guid = first_attachment.get("guid")
                    if attachment_guid:
                        media_url = f"photon-attachment:{attachment_guid}"
                        logger.info(f"[PHOTON] Found attachment guid: {attachment_guid}")
                    else:
                        # Fallback: try other field names
                        media_url = (
                            first_attachment.get("path")
                            or first_attachment.get("filename")
                            or first_attachment.get("filePath")
                            or first_attachment.get("transferName")
                        )
                        # If still no media_url but attachment exists, mark it as present
                        if not media_url and first_attachment:
                            mime = first_attachment.get("mime") or first_attachment.get("mimeType") or "unknown"
                            media_url = f"attachment:{mime}"
                            logger.info(f"[PHOTON] Using placeholder media_url: {media_url}")
                elif isinstance(first_attachment, str):
                    media_url = first_attachment

                if media_url:
                    logger.info(f"[PHOTON] Extracted media_url: {media_url}")

            if not text and not media_url:
                logger.debug("[PHOTON] Dropping empty message from %s", from_number)
                return

            # Extract chat GUID
            chat_guid = (
                message.get("chatGuid")
                or message.get("chat_guid")
                or message.get("chatGUID")
                or None
            )
            chats = message.get("chats") or message.get("chat")
            if not chat_guid:
                if isinstance(chats, list) and chats and isinstance(chats[0], dict):
                    chat_guid = chats[0].get("guid") or chats[0].get("chatGuid") or chats[0].get("chat_guid")
                elif isinstance(chats, dict):
                    chat_guid = chats.get("guid") or chats.get("chatGuid") or chats.get("chat_guid")

            # Forward group chat messages too; the orchestrator will decide how to handle them.
            if chat_guid and _is_group_chat(chat_guid):
                logger.info(f"[PHOTON] Received group chat message from {from_number} in {chat_guid[:40]}...")

            # Idempotency check - prevent duplicate message processing.
            # Socket.IO may re-deliver messages during reconnection, but for group chats we prefer
            # to be fail-open (never block a legitimate user reply), so duplicates only log.
            import hashlib

            is_group = bool(chat_guid and _is_group_chat(chat_guid))

            fingerprint_payload = "|".join(
                [
                    str(from_number or "unknown"),
                    str(chat_guid or ""),
                    str(text or ""),
                    str(media_url or ""),
                ]
            )
            content_fingerprint = hashlib.sha256(fingerprint_payload.encode("utf-8")).hexdigest()[:16]

            # In-process dedupe for all inbound events in case Redis is down or Photon re-delivers with a new GUID.
            if not _in_memory_dedupe(f"photon_in:{from_number}:{content_fingerprint}"):
                logger.info("[PHOTON] In-memory dedupe skip sender=%s", from_number)
                return

            message_guid = message.get("guid")
            # Use environment-based prefix to prevent local/production key conflicts
            from app.config import settings
            env_prefix = "dev_" if settings.app_env == "development" else ""
            if message_guid:
                idempotency_key = f"{env_prefix}photon_msg:{message_guid}"
            else:
                idempotency_key = f"{env_prefix}photon_msg_hash:{content_fingerprint}"
                logger.warning(f"[PHOTON] No GUID in message, using content hash: {idempotency_key}")

            ingest_mode = str(getattr(settings, "photon_ingest_mode", "listener") or "").strip().lower()
            if ingest_mode != "kafka":
                logger.info(f"[CRASH DETECT] BEFORE IDEMPOTENCY CHECK - PID={pid}")
                logger.info(f"[PHOTON] Checking idempotency for: {idempotency_key}")
                from app.utils.redis_client import redis_client
                is_new = redis_client.check_idempotency(idempotency_key, ttl=300)
                logger.info(f"[PHOTON] Idempotency check result: is_new={is_new}")
                logger.info(f"[CRASH DETECT] AFTER IDEMPOTENCY CHECK - PID={pid}, is_new={is_new}")
                if not is_new:
                    logger.info(f"[PHOTON] Skipping duplicate message: {idempotency_key}")
                    return  # Already processed within TTL
            else:
                logger.info("[PHOTON] Skipping Redis idempotency in kafka ingest mode")

            # Cache the real chat GUID from Apple for this sender (DMs only).
            # This enables typing indicators to work correctly for phone numbers
            # by using Apple's actual GUID (which includes iMessage;-; or SMS;-; prefix).
            # Do NOT cache group chat GUIDs under a sender handle (would break DM typing indicators).
            if chat_guid and from_number and not _is_group_chat(chat_guid):
                from app.utils.redis_client import redis_client
                redis_client.cache_chat_guid(from_number, chat_guid, ttl=86400)
                logger.debug(f"[PHOTON] Cached chat GUID for {from_number}: {chat_guid[:30]}...")

            # Transform to SendBlue-compatible format for our webhook handler
            payload = {
                "from_number": from_number,
                "to_number": self.default_number,
                "content": text,
                "message_id": message_guid or f"photon_hash:{content_fingerprint}",
                "timestamp": datetime.utcnow().isoformat(),
                "chat_guid": chat_guid,
                "is_outbound": False,  # Inbound message
                "status": "received",
                "media_url": media_url,  # Pass extracted media URL
            }

            logger.info(f"[PHOTON] Forwarding message to callback - from: {from_number}, text: {text[:50]}...")
            logger.debug(f"[PHOTON] Full payload: {payload}")

            # Forward to callback and await it so exceptions are properly caught
            logger.info(f"[CRASH DETECT] BEFORE CALLBACK - PID={pid}, Memory={_get_memory_mb():.1f}MB")
            await self._callback(payload)
            logger.info(f"[CRASH DETECT] AFTER CALLBACK - PID={pid}, Memory={_get_memory_mb():.1f}MB")

        except SystemExit as exc:
            logger.critical(f"[CRASH DETECT] SystemExit caught in message handler - PID={pid}: {exc}", exc_info=True)
            raise  # Re-raise to preserve exit behavior
        except KeyboardInterrupt as exc:
            logger.critical(f"[CRASH DETECT] KeyboardInterrupt caught in message handler - PID={pid}: {exc}", exc_info=True)
            raise  # Re-raise to preserve interrupt behavior
        except Exception as exc:
            logger.error(f"[CRASH DETECT] Exception in message handler - PID={pid}: {exc}", exc_info=True)
            logger.error("[PHOTON] Failed to process inbound message: %s", exc, exc_info=True)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

## Module: `app/integrations/photon_client.py`

Role: Photon HTTP/Socket.IO client used for outbound messages and chat actions.

Module docstring:
- Photon API client for Frank's iMessage integration.

Imported stack (selected):
- __future__.annotations, app.config.settings, app.utils.phone_validator.get_invalid_phone_reason, app.utils.phone_validator.is_valid_phone_number, app.utils.phone_validator.normalize_phone_number, asyncio, httpx, logging, random, socketio, tenacity.retry, tenacity.stop_after_attempt, tenacity.wait_exponential, typing.Any, typing.Dict, typing.List, typing.Optional, urllib.parse.quote

### Function: `_get_socketio_client()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Get or create a Socket.IO client for Photon server.

When used:
- Per-message sequence (outbound messaging/typing).

Function code:
```python
async def _get_socketio_client(server_url: str, api_key: Optional[str] = None) -> socketio.AsyncClient:
    """Get or create a Socket.IO client for Photon server."""
    global _socketio_client, _socketio_connected

    if _socketio_client is None:
        _socketio_client = socketio.AsyncClient(
            reconnection=True,
            reconnection_attempts=3,
        )

        @_socketio_client.on("connect")
        async def on_connect():
            global _socketio_connected
            _socketio_connected = True
            logger.info("[PHOTON] Socket.IO client connected for chat operations")

        @_socketio_client.on("disconnect")
        async def on_disconnect():
            global _socketio_connected
            _socketio_connected = False
            logger.warning("[PHOTON] Socket.IO client disconnected")

    if not _socketio_connected:
        url = server_url.rstrip("/")
        if not url.startswith("http"):
            url = f"https://{url}"
        auth = {"apiKey": api_key} if api_key else None
        await _socketio_client.connect(url, transports=["websocket"], auth=auth)
        # Wait for connection to establish
        await asyncio.sleep(0.5)

    return _socketio_client
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Class: `PhotonClientError`

Big picture:
- Outbound Photon API client operations.

Purpose:
- Custom exception raised for Photon API failures.

When used:
- Per-message sequence (outbound messaging/typing).

Class code:
```python
class PhotonClientError(Exception):
    """Custom exception raised for Photon API failures."""
```

### Class: `PhotonClient`

Big picture:
- Outbound Photon API client operations.

Purpose:
- Photon HTTP client for iMessage integration via Advanced iMessage Kit.
- 
- Supports both phone numbers and Apple ID (email) recipients.
- 
- Photon exposes an HTTP + Socket.IO gateway via the Advanced iMessage Kit reference server.
- For sending messages we use the REST API:
-     POST /api/v1/message/text
-     POST /api/v1/chat/{chatGuid}/typing
-     DELETE /api/v1/chat/{chatGuid}/typing

When used:
- Per-message sequence (outbound messaging/typing).

Class code:
```python
class PhotonClient:
    """
    Photon HTTP client for iMessage integration via Advanced iMessage Kit.

    Supports both phone numbers and Apple ID (email) recipients.

    Photon exposes an HTTP + Socket.IO gateway via the Advanced iMessage Kit reference server.
    For sending messages we use the REST API:
        POST /api/v1/message/text
        POST /api/v1/chat/{chatGuid}/typing
        DELETE /api/v1/chat/{chatGuid}/typing
    """

    def __init__(
        self,
        server_url: Optional[str] = None,
        default_number: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        base_url = server_url or settings.photon_server_url
        if not base_url:
            raise PhotonClientError("Photon server URL is not configured")

        base_url = base_url.rstrip("/")
        if not base_url.startswith("http"):
            base_url = f"https://{base_url}"

        self.base_url = base_url
        self.default_number = default_number or settings.photon_default_number
        self.api_key = api_key or settings.photon_api_key

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def send_message(
        self,
        to_number: str,
        content: str,
        *,
        from_number: Optional[str] = None,  # For API compatibility (not used by Photon)
        chat_guid: Optional[str] = None,
        effect_id: Optional[str] = None,
        subject: Optional[str] = None,
        media_url: Optional[str] = None,  # For future implementation
        send_style: Optional[str] = None,  # For future implementation
        group_id: Optional[str] = None,  # For future implementation
    ) -> Dict[str, Any]:
        """
        Send a text message via Photon.

        Args:
            to_number: Recipient phone number or email (Apple ID)
            content: Message text
            from_number: Unused (for API compatibility with SendBlue)
            chat_guid: Optional chat GUID (auto-generated if not provided)
            effect_id: Optional iMessage effect (e.g., "com.apple.messages.effect.CKConfettiEffect")
            subject: Optional message subject line
            media_url: Optional media attachment URL (not yet implemented)
            send_style: Optional send style (not yet implemented)
            group_id: Optional group ID (not yet implemented)

        Returns:
            Dict containing messageId and response data

        Raises:
            PhotonClientError: If validation or API call fails
        """
        if not to_number or not content:
            raise PhotonClientError("Missing required to_number or content")

        # Determine if recipient is email or phone number
        is_email = "@" in to_number

        if is_email:
            # Basic email validation
            if not self._is_valid_email(to_number):
                raise PhotonClientError(f"Invalid email address: {to_number}")
            normalized = to_number.lower().strip()
            logger.info(f"[PHOTON] Validated email recipient: {normalized}")
        else:
            # Phone number validation
            if not is_valid_phone_number(to_number):
                reason = get_invalid_phone_reason(to_number)
                raise PhotonClientError(f"Invalid phone number: {reason}")

            normalized = normalize_phone_number(to_number)
            if not normalized:
                raise PhotonClientError("Failed to normalize phone number")
            logger.info(f"[PHOTON] Validated phone recipient: {normalized}")

        # Photon Advanced iMessage Kit requires "message" (can be empty). Keep it minimal to avoid 400s.
        payload: Dict[str, Any] = {
            "chatGuid": chat_guid or self._build_chat_guid(normalized, is_email=is_email),
            "message": content or "",  # required by Photon validation
        }

        if effect_id:
            payload["effectId"] = effect_id
        if subject:
            payload["subject"] = subject

        logger.info(f"[PHOTON] Sending message to {normalized}: {content[:50]}...")
        logger.debug(f"[PHOTON] Full payload: {payload}")

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
            response = await client.post("/api/v1/message/text", json=payload)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[PHOTON] API error %s - %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                raise PhotonClientError(f"Photon send_message failed: {exc}") from exc

            data = response.json().get("data") if response.content else None
            message_id = (data or {}).get("guid")

            logger.info("✅ Message sent successfully to %s", normalized)
            logger.info("📧 Message ID: %s", message_id)
            logger.info("🔍 Full Photon response: %s", data)

            return {
                "messageId": message_id,
                "data": data,
            }

    async def start_typing(self, to_number: str, *, chat_guid: Optional[str] = None) -> None:
        """Send 'start typing' indicator for the chat."""
        # Try to get cached chat GUID from Apple first (includes correct iMessage/SMS prefix)
        if not chat_guid:
            from app.utils.redis_client import redis_client
            chat_guid = redis_client.get_cached_chat_guid(to_number)
            logger.debug(f"[PHOTON] Cached GUID for {to_number}: {chat_guid}")

        # Fall back to building GUID if no cache hit
        guid = chat_guid or self._build_chat_guid_from_number(to_number)
        if not guid:
            return

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        # URL-encode the GUID to handle special chars like + in phone numbers
        encoded_guid = quote(guid, safe="")

        async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0, headers=headers) as client:
            try:
                await client.post(f"/api/v1/chat/{encoded_guid}/typing")
                logger.debug(f"[PHOTON] Started typing indicator for {guid[:30]}...")
            except Exception as exc:
                # Log at info level to help debug iMessage vs SMS issues
                logger.info(f"[PHOTON] Failed to start typing indicator for {guid[:30]}: {exc}")

    async def stop_typing(self, to_number: str, *, chat_guid: Optional[str] = None) -> None:
        """Send 'stop typing' indicator."""
        # Try to get cached chat GUID from Apple first (includes correct iMessage/SMS prefix)
        if not chat_guid:
            from app.utils.redis_client import redis_client
            chat_guid = redis_client.get_cached_chat_guid(to_number)
            logger.debug(f"[PHOTON] Cached GUID for {to_number}: {chat_guid}")

        # Fall back to building GUID if no cache hit
        guid = chat_guid or self._build_chat_guid_from_number(to_number)
        if not guid:
            return

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        # URL-encode the GUID to handle special chars like + in phone numbers
        encoded_guid = quote(guid, safe="")

        async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0, headers=headers) as client:
            try:
                await client.delete(f"/api/v1/chat/{encoded_guid}/typing")
                logger.debug(f"[PHOTON] Stopped typing indicator for {guid[:30]}...")
            except Exception as exc:
                logger.debug(f"[PHOTON] Failed to stop typing indicator for {guid[:30]}: {exc}")

    async def mark_chat_read(self, chat_guid: str) -> None:
        """Mark a chat as read."""
        if not chat_guid:
            return

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        encoded_guid = quote(chat_guid, safe="")

        async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0, headers=headers) as client:
            try:
                await client.post(f"/api/v1/chat/{encoded_guid}/read")
                logger.debug(f"[PHOTON] Marked chat as read: {chat_guid[:30]}...")
            except Exception as exc:
                logger.debug(f"[PHOTON] Failed to mark chat as read: {exc}")

    async def mark_chat_unread(self, chat_guid: str) -> None:
        """Mark a chat as unread."""
        if not chat_guid:
            return

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        encoded_guid = quote(chat_guid, safe="")

        async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0, headers=headers) as client:
            try:
                await client.post(f"/api/v1/chat/{encoded_guid}/unread")
                logger.debug(f"[PHOTON] Marked chat as unread: {chat_guid[:30]}...")
            except Exception as exc:
                logger.debug(f"[PHOTON] Failed to mark chat as unread: {exc}")

    async def send_typing_indicator(
        self,
        to_number: str,
        duration: float = 1.0,
        *,
        chat_guid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Show typing indicator for a short duration.

        Args:
            to_number: Recipient phone number or email
            duration: How long to show typing (in seconds)
            chat_guid: Optional chat GUID

        Returns:
            Empty dict (for API compatibility with SendBlue)
        """
        try:
            await self.start_typing(to_number, chat_guid=chat_guid)
            await asyncio.sleep(duration)
            return {}
        except Exception as e:
            logger.error(f"Error sending typing indicator: {str(e)}")
            # Don't raise - typing indicator is not critical
            return {}
        finally:
            try:
                await self.stop_typing(to_number, chat_guid=chat_guid)
            except Exception:
                pass  # Typing indicator stop is non-critical

    async def send_reaction(
        self,
        to_number: str,
        message_guid: str,
        reaction: str,
        *,
        chat_guid: Optional[str] = None,
        part_index: int = 0
    ) -> Dict[str, Any]:
        """
        Send a tapback reaction to a user's message.

        This adds an emoji reaction (like ❤️, 😂, !!, etc.) to a specific message,
        making the bot feel more human and engaged.

        Args:
            to_number: Recipient phone number or email (Apple ID)
            message_guid: GUID of the message to react to
            reaction: Reaction type - one of:
                - "love" (❤️ heart)
                - "like" (👍 thumbs up)
                - "dislike" (👎 thumbs down)
                - "laugh" (😂 haha)
                - "emphasize" (!! exclamation marks)
                - "question" (?? question marks)
            chat_guid: Optional chat GUID (auto-detected if not provided)
            part_index: Message part index for multi-part messages (default: 0)

        Returns:
            Dict containing response data, or empty dict on failure

        Example:
            >>> await client.send_reaction(
            ...     to_number="+1234567890",
            ...     message_guid="p:0/ABC-123-XYZ",
            ...     reaction="love"
            ... )
        """
        if not message_guid:
            logger.warning(f"[PHOTON] Cannot send reaction: missing message_guid")
            return {}

        # Validate reaction type
        valid_reactions = ["love", "like", "dislike", "laugh", "emphasize", "question"]
        if reaction not in valid_reactions:
            logger.warning(f"[PHOTON] Invalid reaction type: {reaction}. Must be one of {valid_reactions}")
            return {}

        # Try to get cached chat GUID from Apple first
        if not chat_guid:
            from app.utils.redis_client import redis_client
            chat_guid = redis_client.get_cached_chat_guid(to_number)

        # Fall back to building GUID if no cache hit
        if not chat_guid:
            chat_guid = self._build_chat_guid_from_number(to_number)
            if not chat_guid:
                logger.warning(f"[PHOTON] Cannot send reaction: failed to determine chat GUID")
                return {}

        payload = {
            "chatGuid": chat_guid,
            "selectedMessageGuid": message_guid,  # Photon uses 'selectedMessageGuid', not 'messageGuid'
            "reaction": reaction,
            "partIndex": part_index
        }

        logger.info(f"[PHOTON] Sending '{reaction}' reaction to message {message_guid[:20]}...")
        logger.debug(f"[PHOTON] Reaction payload: {payload}")

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
            try:
                # Photon API endpoint: /api/v1/message/react (verified from source code)
                response = await client.post("/api/v1/message/react", json=payload)
                response.raise_for_status()

                data = response.json().get("data") if response.content else None
                logger.info(f"✅ Reaction '{reaction}' sent successfully")
                logger.debug(f"🔍 Reaction response: {data}")

                return {
                    "success": True,
                    "data": data
                }

            except httpx.HTTPStatusError as exc:
                logger.warning(
                    f"[PHOTON] Reaction API error {exc.response.status_code} - {exc.response.text}"
                )
                # Don't raise - reactions are nice-to-have, not critical
                return {}
            except Exception as exc:
                logger.warning(f"[PHOTON] Failed to send reaction: {exc}")
                # Don't raise - reactions are nice-to-have, not critical
                return {}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
    )
    async def send_message_to_chat(
        self,
        chat_guid: str,
        content: str,
        *,
        effect_id: Optional[str] = None,
        subject: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send a message to an existing chat (1:1 or group) by chat GUID.

        This avoids needing a `to_number` for group chats and matches how services
        in this repo address group conversations.
        """
        if not chat_guid:
            raise PhotonClientError("Missing chat_guid")
        if not content:
            raise PhotonClientError("Missing content")

        payload: Dict[str, Any] = {
            "chatGuid": chat_guid,
            "message": content,
        }
        if effect_id:
            payload["effectId"] = effect_id
        if subject:
            payload["subject"] = subject

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
            response = await client.post("/api/v1/message/text", json=payload)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[PHOTON] send_message_to_chat failed %s - %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                raise PhotonClientError(f"Photon send_message_to_chat failed: {exc}") from exc

            data = response.json().get("data") if response.content else None
            return {"data": data}

    async def create_poll(self, chat_guid: str, *, title: str, options: List[str]) -> Dict[str, Any]:
        """
        Create a native iMessage poll in an existing chat.

        Backed by Photon Advanced iMessage Kit:
            POST /api/v1/poll/create
        """
        if not chat_guid:
            raise PhotonClientError("Missing chat_guid")

        cleaned_options = [(o or "").strip() for o in (options or [])]
        cleaned_options = [o for o in cleaned_options if o]
        if len(cleaned_options) < 2:
            raise PhotonClientError("Poll must have at least 2 non-empty options")

        payload: Dict[str, Any] = {
            "chatGuid": chat_guid,
            "options": cleaned_options,
        }
        cleaned_title = (title or "").strip()
        if cleaned_title:
            payload["title"] = cleaned_title

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
            response = await client.post("/api/v1/poll/create", json=payload)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[PHOTON] create_poll failed %s - %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                if exc.response.status_code == 404:
                    raise PhotonClientError("Server does not support polls (404 /api/v1/poll/create)") from exc
                raise PhotonClientError(f"Photon create_poll failed: {exc}") from exc

            data = response.json().get("data") if response.content else None
            return {"data": data}

    async def send_chunked_messages(
        self,
        to_number: str,
        message_chunks: List[str],
        from_number: Optional[str] = None,
        delay_range: tuple = (1.0, 2.5),
        show_typing: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Send multiple message chunks with human-like delays and typing indicators.

        This makes the bot feel more natural, like a real person texting in bursts
        rather than sending one giant wall of text.

        Args:
            to_number: Recipient phone number or email (Apple ID)
            message_chunks: List of message chunks to send in sequence
            from_number: Unused (for API compatibility with SendBlue)
            delay_range: Tuple of (min_delay, max_delay) in seconds between chunks
            show_typing: Whether to show typing indicator before each chunk

        Returns:
            List of API response dictionaries for each chunk sent

        Example:
            >>> chunks = ["Yo! Found 2 opportunities for you",
            ...           "1. Google SWE Intern - deadline March 15",
            ...           "2. Meta ML Intern - deadline March 20"]
            >>> await client.send_chunked_messages("+1234567890", chunks)
        """
        if not message_chunks:
            logger.warning("send_chunked_messages called with empty chunks list")
            return []

        results = []
        min_delay, max_delay = delay_range

        # Determine chat_guid once for all chunks
        is_email = "@" in to_number
        if is_email:
            normalized = to_number.lower().strip()
        else:
            normalized = normalize_phone_number(to_number)
            if not normalized:
                logger.error(f"Failed to normalize phone number: {to_number}")
                return []

        chat_guid = self._build_chat_guid(normalized, is_email=is_email)

        for i, chunk in enumerate(message_chunks):
            try:
                # Show typing indicator before sending each chunk (including first one)
                if show_typing:
                    try:
                        # Short typing duration for natural feel
                        typing_duration = min(len(chunk) / 100, 1.5)  # Faster "typing" speed
                        await self.send_typing_indicator(
                            to_number=to_number,
                            duration=typing_duration,
                            chat_guid=chat_guid
                        )
                    except Exception as e:
                        logger.warning(f"Could not send typing indicator: {str(e)}")
                        # Continue anyway - typing is nice-to-have

                # Send the chunk
                logger.info(f"Sending chunk {i+1}/{len(message_chunks)} to {to_number}: {chunk[:50]}...")
                result = await self.send_message(
                    to_number=to_number,
                    content=chunk,
                    chat_guid=chat_guid
                )

                results.append({
                    "success": True,
                    "chunk_index": i,
                    "result": result
                })

                logger.info(f"Successfully sent chunk {i+1}/{len(message_chunks)}")

                # Add human-like delay before next chunk (except after last chunk)
                if i < len(message_chunks) - 1:
                    delay = random.uniform(min_delay, max_delay)
                    logger.debug(f"Waiting {delay:.1f}s before next chunk...")
                    await asyncio.sleep(delay)

            except PhotonClientError as e:
                logger.error(f"Failed to send chunk {i+1}/{len(message_chunks)}: {str(e)}")
                results.append({
                    "success": False,
                    "chunk_index": i,
                    "error": str(e)
                })
                # Continue sending remaining chunks even if one fails

            except Exception as e:
                logger.error(f"Unexpected error sending chunk {i+1}: {str(e)}", exc_info=True)
                results.append({
                    "success": False,
                    "chunk_index": i,
                    "error": str(e)
                })

        successful_chunks = sum(1 for r in results if r.get("success"))
        logger.info(f"Sent {successful_chunks}/{len(message_chunks)} chunks successfully")

        return results

    def _build_chat_guid_from_number(self, to_number: str) -> Optional[str]:
        """
        Normalize the number/email and convert to Photon chat GUID.

        Args:
            to_number: Phone number or email address

        Returns:
            Chat GUID or None if invalid
        """
        if not to_number:
            return None

        is_email = "@" in to_number

        if is_email:
            normalized = to_number.lower().strip()
        else:
            normalized = normalize_phone_number(to_number)
            if not normalized:
                return None

        return self._build_chat_guid(normalized, is_email=is_email)

    @staticmethod
    def _build_chat_guid(normalized_identifier: str, is_email: bool = False) -> str:
        """
        Build Apple iMessage chat GUID for 1:1 conversations.

        Format based on Apple's iMessage database structure:
        - Phone: iMessage;-;+<E.164 number>
        - Email: iMessage;-;<email@example.com>

        Args:
            normalized_identifier: Phone number (E.164) or email address (lowercase)
            is_email: Whether the identifier is an email address

        Returns:
            Properly formatted chat GUID
        """
        if is_email:
            # Email format: iMessage;-;email@example.com
            return f"iMessage;-;{normalized_identifier}"
        else:
            # Phone format: iMessage;-;+1234567890
            if not normalized_identifier.startswith("+"):
                normalized_identifier = f"+{normalized_identifier}"
            return f"iMessage;-;{normalized_identifier}"

    @staticmethod
    def _is_valid_email(email: str) -> bool:
        """
        Basic email validation.

        Args:
            email: Email address to validate

        Returns:
            True if email format is valid
        """
        import re
        # Basic email regex pattern
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def create_group_chat(
        self,
        addresses: List[str],
        message: str,
        *,
        service: str = "iMessage",
        method: str = "private-api"
    ) -> Dict[str, Any]:
        """
        Create a group chat with multiple participants.

        Uses the Photon Advanced iMessage Kit to create a new group chat
        with the specified participants and sends an initial message.

        Args:
            addresses: List of phone numbers or emails (2+ participants)
            message: Initial message to send to the group
            service: Service type ("iMessage" or "SMS")
            method: Sending method ("private-api" for group creation)

        Returns:
            Dict containing the chat_guid and response data

        Raises:
            PhotonClientError: If validation or API call fails
        """
        if not addresses or len(addresses) < 2:
            raise PhotonClientError("Group chat requires at least 2 participants")

        if not message:
            raise PhotonClientError("Initial message is required for group creation")

        # Normalize all addresses
        normalized_addresses = []
        for addr in addresses:
            if "@" in addr:
                if not self._is_valid_email(addr):
                    raise PhotonClientError(f"Invalid email address: {addr}")
                normalized_addresses.append(addr.lower().strip())
            else:
                if not is_valid_phone_number(addr):
                    reason = get_invalid_phone_reason(addr)
                    raise PhotonClientError(f"Invalid phone number {addr}: {reason}")
                normalized = normalize_phone_number(addr)
                if not normalized:
                    raise PhotonClientError(f"Failed to normalize phone number: {addr}")
                normalized_addresses.append(normalized)

        logger.info(
            f"[PHOTON] Creating group chat with {len(normalized_addresses)} participants"
        )

        # Use the message/text endpoint with addresses array for group chat creation
        # This mirrors how the TypeScript SDK's createChat works internally
        payload = {
            "addresses": normalized_addresses,
            "message": message,
            "service": service,
            "method": method
        }

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        logger.info(f"[PHOTON] Sending group chat creation request to /api/v1/chat/new")
        logger.debug(f"[PHOTON] Group chat payload: {payload}")

        async with httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30.0,
            headers=headers
        ) as client:
            response = await client.post("/api/v1/chat/new", json=payload)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[PHOTON] Group chat creation failed %s - %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                raise PhotonClientError(
                    f"Photon create_group_chat failed: {exc}"
                ) from exc

            data = response.json().get("data") if response.content else None

            # Extract chat GUID from response - for group chats it might be in different fields
            chat_guid = None
            if data:
                chat_guid = data.get("chatGuid") or data.get("guid")
                # If sending to multiple addresses, check the chats array
                chats = data.get("chats", [])
                if chats and not chat_guid:
                    chat_guid = chats[0].get("guid") if chats else None

            logger.info(f"[PHOTON] Group chat created successfully: {chat_guid}")
            logger.debug(f"[PHOTON] Full response: {data}")

            return {
                "chat_guid": chat_guid,
                "data": data
            }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def update_chat(
        self,
        chat_guid: str,
        *,
        display_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update a chat's metadata (e.g., rename a group chat).

        Photon Advanced iMessage Kit:
          PUT /api/v1/chat/:guid  { displayName: "New Name" }
        """
        guid = str(chat_guid or "").strip()
        if not guid:
            raise PhotonClientError("Missing chat GUID")

        payload: Dict[str, Any] = {}
        if display_name is not None:
            name = str(display_name).strip()
            if name:
                payload["displayName"] = name

        if not payload:
            raise PhotonClientError("No valid chat updates provided")

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        encoded_guid = quote(guid, safe="")

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
            response = await client.put(f"/api/v1/chat/{encoded_guid}", json=payload)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[PHOTON] Chat update failed %s - %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                raise PhotonClientError(f"Photon update_chat failed: {exc}") from exc

            data = response.json().get("data") if response.content else None
            return {
                "chat_guid": guid,
                "data": data,
            }

    async def should_share_contact(self, chat_guid: str) -> bool:
        """
        Check whether the SDK recommends sharing your contact card in this chat.

        Returns:
        - true: sharing is recommended (typically when the other side shared theirs
                and you haven't shared yours yet)
        - false: NOT recommended (e.g. you've already shared, OR the other side
                 hasn't shared theirs yet)

        Args:
            chat_guid: The chat identifier (e.g. the guid field from chat APIs/events)

        Returns:
            bool indicating whether contact card sharing is recommended
        """
        if not chat_guid:
            raise PhotonClientError("Missing chat GUID for should_share_contact check")

        logger.info(f"[PHOTON] Checking should_share_contact for {chat_guid[:30]}...")

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        encoded_guid = quote(str(chat_guid), safe="")

        try:
            async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0, headers=headers) as client:
                response = await client.get(f"/api/v1/chat/{encoded_guid}/share/contact/status")
                response.raise_for_status()
                data = response.json()
                # Response format: { data: { data: boolean } }
                should_share = data.get("data", {}).get("data", False) if isinstance(data.get("data"), dict) else data.get("data", False)
                logger.info(f"[PHOTON] should_share_contact for {chat_guid[:30]}...: {should_share}")
                return bool(should_share)
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "[PHOTON] should_share_contact check failed %s - %s",
                exc.response.status_code,
                exc.response.text,
            )
            # If the endpoint doesn't exist or fails, default to False (don't share)
            return False
        except Exception as exc:
            logger.warning(f"[PHOTON] should_share_contact error: {exc}")
            # If there's an error, default to False (don't share)
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def add_participant(
        self,
        chat_guid: str,
        address: str,
    ) -> Dict[str, Any]:
        """
        Add a participant to an existing group chat.

        Used for adding late joiners to multi-person group chats.

        Args:
            chat_guid: The existing group chat GUID
            address: Phone number or email of the participant to add

        Returns:
            Dict containing the response data

        Raises:
            PhotonClientError: If the operation fails
        """
        if not chat_guid:
            raise PhotonClientError("Missing chat_guid for add_participant")
        if not address:
            raise PhotonClientError("Missing address for add_participant")

        # Normalize the address
        is_email = "@" in address
        if is_email:
            if not self._is_valid_email(address):
                raise PhotonClientError(f"Invalid email address: {address}")
            normalized = address.lower().strip()
        else:
            if not is_valid_phone_number(address):
                reason = get_invalid_phone_reason(address)
                raise PhotonClientError(f"Invalid phone number: {reason}")
            normalized = normalize_phone_number(address)
            if not normalized:
                raise PhotonClientError("Failed to normalize phone number")

        logger.info(f"[PHOTON] Adding participant {normalized} to chat {chat_guid[:30]}...")

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        encoded_guid = quote(chat_guid, safe="")

        # Photon SDK: sdk.chats.addParticipant(chatGuid, address)
        # Maps to: POST /api/v1/chat/{guid}/participant
        payload = {"address": normalized}

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
            response = await client.post(
                f"/api/v1/chat/{encoded_guid}/participant",
                json=payload
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[PHOTON] add_participant failed %s - %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                raise PhotonClientError(f"Photon add_participant failed: {exc}") from exc

            data = response.json().get("data") if response.content else None
            logger.info(f"[PHOTON] Successfully added participant {normalized} to chat")
            return {
                "chat_guid": chat_guid,
                "added_address": normalized,
                "data": data,
            }

    async def share_contact_card(self, chat_guid: str) -> Dict[str, Any]:
        """
        Share your contact card (iMessage "Share Name and Photo") to the specified chat.

        This sends Franklink's contact information to the user via HTTP API,
        allowing them to save it to their contacts.

        Args:
            chat_guid: The chat identifier where to share the contact card

        Returns:
            Dict containing the response data

        Raises:
            PhotonClientError: If the operation fails
        """
        if not chat_guid:
            raise PhotonClientError("Missing chat GUID for share_contact_card")

        logger.info(f"[PHOTON] Sharing contact card to chat: {chat_guid[:30]}...")

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        encoded_guid = quote(str(chat_guid), safe="")

        try:
            async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
                response = await client.post(f"/api/v1/chat/{encoded_guid}/share/contact")
                response.raise_for_status()
                data = response.json() if response.content else None
                logger.info(f"✅ Contact card shared successfully to {chat_guid[:30]}...")
                return {
                    "chat_guid": chat_guid,
                    "data": data,
                }
        except httpx.HTTPStatusError as exc:
            logger.error(
                "[PHOTON] share_contact_card failed %s - %s",
                exc.response.status_code,
                exc.response.text,
            )
            raise PhotonClientError(f"Photon share_contact_card failed: {exc}") from exc
        except Exception as exc:
            logger.error(f"[PHOTON] share_contact_card failed: {exc}")
            raise PhotonClientError(f"Photon share_contact_card failed: {exc}") from exc
```

#### Method: `PhotonClient.__init__()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
def __init__(
        self,
        server_url: Optional[str] = None,
        default_number: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        base_url = server_url or settings.photon_server_url
        if not base_url:
            raise PhotonClientError("Photon server URL is not configured")

        base_url = base_url.rstrip("/")
        if not base_url.startswith("http"):
            base_url = f"https://{base_url}"

        self.base_url = base_url
        self.default_number = default_number or settings.photon_default_number
        self.api_key = api_key or settings.photon_api_key
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.send_message()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Sends outbound messages via Photon HTTP API. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Send a text message via Photon.
- 
- Args:
-     to_number: Recipient phone number or email (Apple ID)
-     content: Message text
-     from_number: Unused (for API compatibility with SendBlue)
-     chat_guid: Optional chat GUID (auto-generated if not provided)
-     effect_id: Optional iMessage effect (e.g., "com.apple.messages.effect.CKConfettiEffect")
-     subject: Optional message subject line
-     media_url: Optional media attachment URL (not yet implemented)
-     send_style: Optional send style (not yet implemented)
-     group_id: Optional group ID (not yet implemented)
- 
- Returns:
-     Dict containing messageId and response data
- 
- Raises:
-     PhotonClientError: If validation or API call fails

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def send_message(
        self,
        to_number: str,
        content: str,
        *,
        from_number: Optional[str] = None,  # For API compatibility (not used by Photon)
        chat_guid: Optional[str] = None,
        effect_id: Optional[str] = None,
        subject: Optional[str] = None,
        media_url: Optional[str] = None,  # For future implementation
        send_style: Optional[str] = None,  # For future implementation
        group_id: Optional[str] = None,  # For future implementation
    ) -> Dict[str, Any]:
        """
        Send a text message via Photon.

        Args:
            to_number: Recipient phone number or email (Apple ID)
            content: Message text
            from_number: Unused (for API compatibility with SendBlue)
            chat_guid: Optional chat GUID (auto-generated if not provided)
            effect_id: Optional iMessage effect (e.g., "com.apple.messages.effect.CKConfettiEffect")
            subject: Optional message subject line
            media_url: Optional media attachment URL (not yet implemented)
            send_style: Optional send style (not yet implemented)
            group_id: Optional group ID (not yet implemented)

        Returns:
            Dict containing messageId and response data

        Raises:
            PhotonClientError: If validation or API call fails
        """
        if not to_number or not content:
            raise PhotonClientError("Missing required to_number or content")

        # Determine if recipient is email or phone number
        is_email = "@" in to_number

        if is_email:
            # Basic email validation
            if not self._is_valid_email(to_number):
                raise PhotonClientError(f"Invalid email address: {to_number}")
            normalized = to_number.lower().strip()
            logger.info(f"[PHOTON] Validated email recipient: {normalized}")
        else:
            # Phone number validation
            if not is_valid_phone_number(to_number):
                reason = get_invalid_phone_reason(to_number)
                raise PhotonClientError(f"Invalid phone number: {reason}")

            normalized = normalize_phone_number(to_number)
            if not normalized:
                raise PhotonClientError("Failed to normalize phone number")
            logger.info(f"[PHOTON] Validated phone recipient: {normalized}")

        # Photon Advanced iMessage Kit requires "message" (can be empty). Keep it minimal to avoid 400s.
        payload: Dict[str, Any] = {
            "chatGuid": chat_guid or self._build_chat_guid(normalized, is_email=is_email),
            "message": content or "",  # required by Photon validation
        }

        if effect_id:
            payload["effectId"] = effect_id
        if subject:
            payload["subject"] = subject

        logger.info(f"[PHOTON] Sending message to {normalized}: {content[:50]}...")
        logger.debug(f"[PHOTON] Full payload: {payload}")

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
            response = await client.post("/api/v1/message/text", json=payload)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[PHOTON] API error %s - %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                raise PhotonClientError(f"Photon send_message failed: {exc}") from exc

            data = response.json().get("data") if response.content else None
            message_id = (data or {}).get("guid")

            logger.info("✅ Message sent successfully to %s", normalized)
            logger.info("📧 Message ID: %s", message_id)
            logger.info("🔍 Full Photon response: %s", data)

            return {
                "messageId": message_id,
                "data": data,
            }
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.start_typing()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Controls typing indicators for user experience. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Send 'start typing' indicator for the chat.

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def start_typing(self, to_number: str, *, chat_guid: Optional[str] = None) -> None:
        """Send 'start typing' indicator for the chat."""
        # Try to get cached chat GUID from Apple first (includes correct iMessage/SMS prefix)
        if not chat_guid:
            from app.utils.redis_client import redis_client
            chat_guid = redis_client.get_cached_chat_guid(to_number)
            logger.debug(f"[PHOTON] Cached GUID for {to_number}: {chat_guid}")

        # Fall back to building GUID if no cache hit
        guid = chat_guid or self._build_chat_guid_from_number(to_number)
        if not guid:
            return

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        # URL-encode the GUID to handle special chars like + in phone numbers
        encoded_guid = quote(guid, safe="")

        async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0, headers=headers) as client:
            try:
                await client.post(f"/api/v1/chat/{encoded_guid}/typing")
                logger.debug(f"[PHOTON] Started typing indicator for {guid[:30]}...")
            except Exception as exc:
                # Log at info level to help debug iMessage vs SMS issues
                logger.info(f"[PHOTON] Failed to start typing indicator for {guid[:30]}: {exc}")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.stop_typing()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Controls typing indicators for user experience. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Send 'stop typing' indicator.

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def stop_typing(self, to_number: str, *, chat_guid: Optional[str] = None) -> None:
        """Send 'stop typing' indicator."""
        # Try to get cached chat GUID from Apple first (includes correct iMessage/SMS prefix)
        if not chat_guid:
            from app.utils.redis_client import redis_client
            chat_guid = redis_client.get_cached_chat_guid(to_number)
            logger.debug(f"[PHOTON] Cached GUID for {to_number}: {chat_guid}")

        # Fall back to building GUID if no cache hit
        guid = chat_guid or self._build_chat_guid_from_number(to_number)
        if not guid:
            return

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        # URL-encode the GUID to handle special chars like + in phone numbers
        encoded_guid = quote(guid, safe="")

        async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0, headers=headers) as client:
            try:
                await client.delete(f"/api/v1/chat/{encoded_guid}/typing")
                logger.debug(f"[PHOTON] Stopped typing indicator for {guid[:30]}...")
            except Exception as exc:
                logger.debug(f"[PHOTON] Failed to stop typing indicator for {guid[:30]}: {exc}")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.mark_chat_read()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Mark a chat as read.

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def mark_chat_read(self, chat_guid: str) -> None:
        """Mark a chat as read."""
        if not chat_guid:
            return

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        encoded_guid = quote(chat_guid, safe="")

        async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0, headers=headers) as client:
            try:
                await client.post(f"/api/v1/chat/{encoded_guid}/read")
                logger.debug(f"[PHOTON] Marked chat as read: {chat_guid[:30]}...")
            except Exception as exc:
                logger.debug(f"[PHOTON] Failed to mark chat as read: {exc}")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.mark_chat_unread()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Mark a chat as unread.

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def mark_chat_unread(self, chat_guid: str) -> None:
        """Mark a chat as unread."""
        if not chat_guid:
            return

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        encoded_guid = quote(chat_guid, safe="")

        async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0, headers=headers) as client:
            try:
                await client.post(f"/api/v1/chat/{encoded_guid}/unread")
                logger.debug(f"[PHOTON] Marked chat as unread: {chat_guid[:30]}...")
            except Exception as exc:
                logger.debug(f"[PHOTON] Failed to mark chat as unread: {exc}")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.send_typing_indicator()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Controls typing indicators for user experience. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Show typing indicator for a short duration.
- 
- Args:
-     to_number: Recipient phone number or email
-     duration: How long to show typing (in seconds)
-     chat_guid: Optional chat GUID
- 
- Returns:
-     Empty dict (for API compatibility with SendBlue)

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def send_typing_indicator(
        self,
        to_number: str,
        duration: float = 1.0,
        *,
        chat_guid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Show typing indicator for a short duration.

        Args:
            to_number: Recipient phone number or email
            duration: How long to show typing (in seconds)
            chat_guid: Optional chat GUID

        Returns:
            Empty dict (for API compatibility with SendBlue)
        """
        try:
            await self.start_typing(to_number, chat_guid=chat_guid)
            await asyncio.sleep(duration)
            return {}
        except Exception as e:
            logger.error(f"Error sending typing indicator: {str(e)}")
            # Don't raise - typing indicator is not critical
            return {}
        finally:
            try:
                await self.stop_typing(to_number, chat_guid=chat_guid)
            except Exception:
                pass
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.send_reaction()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Sends tapback reactions. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Send a tapback reaction to a user's message.
- 
- This adds an emoji reaction (like ❤️, 😂, !!, etc.) to a specific message,
- making the bot feel more human and engaged.
- 
- Args:
-     to_number: Recipient phone number or email (Apple ID)
-     message_guid: GUID of the message to react to
-     reaction: Reaction type - one of:
-         - "love" (❤️ heart)
-         - "like" (👍 thumbs up)
-         - "dislike" (👎 thumbs down)
-         - "laugh" (😂 haha)
-         - "emphasize" (!! exclamation marks)
-         - "question" (?? question marks)
-     chat_guid: Optional chat GUID (auto-detected if not provided)
-     part_index: Message part index for multi-part messages (default: 0)
- 
- Returns:
-     Dict containing response data, or empty dict on failure
- 
- Example:
-     >>> await client.send_reaction(
-     ...     to_number="+1234567890",
-     ...     message_guid="p:0/ABC-123-XYZ",
-     ...     reaction="love"
-     ... )

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def send_reaction(
        self,
        to_number: str,
        message_guid: str,
        reaction: str,
        *,
        chat_guid: Optional[str] = None,
        part_index: int = 0
    ) -> Dict[str, Any]:
        """
        Send a tapback reaction to a user's message.

        This adds an emoji reaction (like ❤️, 😂, !!, etc.) to a specific message,
        making the bot feel more human and engaged.

        Args:
            to_number: Recipient phone number or email (Apple ID)
            message_guid: GUID of the message to react to
            reaction: Reaction type - one of:
                - "love" (❤️ heart)
                - "like" (👍 thumbs up)
                - "dislike" (👎 thumbs down)
                - "laugh" (😂 haha)
                - "emphasize" (!! exclamation marks)
                - "question" (?? question marks)
            chat_guid: Optional chat GUID (auto-detected if not provided)
            part_index: Message part index for multi-part messages (default: 0)

        Returns:
            Dict containing response data, or empty dict on failure

        Example:
            >>> await client.send_reaction(
            ...     to_number="+1234567890",
            ...     message_guid="p:0/ABC-123-XYZ",
            ...     reaction="love"
            ... )
        """
        if not message_guid:
            logger.warning(f"[PHOTON] Cannot send reaction: missing message_guid")
            return {}

        # Validate reaction type
        valid_reactions = ["love", "like", "dislike", "laugh", "emphasize", "question"]
        if reaction not in valid_reactions:
            logger.warning(f"[PHOTON] Invalid reaction type: {reaction}. Must be one of {valid_reactions}")
            return {}

        # Try to get cached chat GUID from Apple first
        if not chat_guid:
            from app.utils.redis_client import redis_client
            chat_guid = redis_client.get_cached_chat_guid(to_number)

        # Fall back to building GUID if no cache hit
        if not chat_guid:
            chat_guid = self._build_chat_guid_from_number(to_number)
            if not chat_guid:
                logger.warning(f"[PHOTON] Cannot send reaction: failed to determine chat GUID")
                return {}

        payload = {
            "chatGuid": chat_guid,
            "selectedMessageGuid": message_guid,  # Photon uses 'selectedMessageGuid', not 'messageGuid'
            "reaction": reaction,
            "partIndex": part_index
        }

        logger.info(f"[PHOTON] Sending '{reaction}' reaction to message {message_guid[:20]}...")
        logger.debug(f"[PHOTON] Reaction payload: {payload}")

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
            try:
                # Photon API endpoint: /api/v1/message/react (verified from source code)
                response = await client.post("/api/v1/message/react", json=payload)
                response.raise_for_status()

                data = response.json().get("data") if response.content else None
                logger.info(f"✅ Reaction '{reaction}' sent successfully")
                logger.debug(f"🔍 Reaction response: {data}")

                return {
                    "success": True,
                    "data": data
                }

            except httpx.HTTPStatusError as exc:
                logger.warning(
                    f"[PHOTON] Reaction API error {exc.response.status_code} - {exc.response.text}"
                )
                # Don't raise - reactions are nice-to-have, not critical
                return {}
            except Exception as exc:
                logger.warning(f"[PHOTON] Failed to send reaction: {exc}")
                # Don't raise - reactions are nice-to-have, not critical
                return {}
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.send_message_to_chat()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Sends outbound messages via Photon HTTP API. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Send a message to an existing chat (1:1 or group) by chat GUID.
- 
- This avoids needing a `to_number` for group chats and matches how services
- in this repo address group conversations.

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def send_message_to_chat(
        self,
        chat_guid: str,
        content: str,
        *,
        effect_id: Optional[str] = None,
        subject: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Send a message to an existing chat (1:1 or group) by chat GUID.

        This avoids needing a `to_number` for group chats and matches how services
        in this repo address group conversations.
        """
        if not chat_guid:
            raise PhotonClientError("Missing chat_guid")
        if not content:
            raise PhotonClientError("Missing content")

        payload: Dict[str, Any] = {
            "chatGuid": chat_guid,
            "message": content,
        }
        if effect_id:
            payload["effectId"] = effect_id
        if subject:
            payload["subject"] = subject

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
            response = await client.post("/api/v1/message/text", json=payload)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[PHOTON] send_message_to_chat failed %s - %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                raise PhotonClientError(f"Photon send_message_to_chat failed: {exc}") from exc

            data = response.json().get("data") if response.content else None
            return {"data": data}
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.create_poll()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Create a native iMessage poll in an existing chat.
- 
- Backed by Photon Advanced iMessage Kit:
-     POST /api/v1/poll/create

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def create_poll(self, chat_guid: str, *, title: str, options: List[str]) -> Dict[str, Any]:
        """
        Create a native iMessage poll in an existing chat.

        Backed by Photon Advanced iMessage Kit:
            POST /api/v1/poll/create
        """
        if not chat_guid:
            raise PhotonClientError("Missing chat_guid")

        cleaned_options = [(o or "").strip() for o in (options or [])]
        cleaned_options = [o for o in cleaned_options if o]
        if len(cleaned_options) < 2:
            raise PhotonClientError("Poll must have at least 2 non-empty options")

        payload: Dict[str, Any] = {
            "chatGuid": chat_guid,
            "options": cleaned_options,
        }
        cleaned_title = (title or "").strip()
        if cleaned_title:
            payload["title"] = cleaned_title

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
            response = await client.post("/api/v1/poll/create", json=payload)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[PHOTON] create_poll failed %s - %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                if exc.response.status_code == 404:
                    raise PhotonClientError("Server does not support polls (404 /api/v1/poll/create)") from exc
                raise PhotonClientError(f"Photon create_poll failed: {exc}") from exc

            data = response.json().get("data") if response.content else None
            return {"data": data}
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.send_chunked_messages()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Send multiple message chunks with human-like delays and typing indicators.
- 
- This makes the bot feel more natural, like a real person texting in bursts
- rather than sending one giant wall of text.
- 
- Args:
-     to_number: Recipient phone number or email (Apple ID)
-     message_chunks: List of message chunks to send in sequence
-     from_number: Unused (for API compatibility with SendBlue)
-     delay_range: Tuple of (min_delay, max_delay) in seconds between chunks
-     show_typing: Whether to show typing indicator before each chunk
- 
- Returns:
-     List of API response dictionaries for each chunk sent
- 
- Example:
-     >>> chunks = ["Yo! Found 2 opportunities for you",
-     ...           "1. Google SWE Intern - deadline March 15",
-     ...           "2. Meta ML Intern - deadline March 20"]
-     >>> await client.send_chunked_messages("+1234567890", chunks)

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def send_chunked_messages(
        self,
        to_number: str,
        message_chunks: List[str],
        from_number: Optional[str] = None,
        delay_range: tuple = (1.0, 2.5),
        show_typing: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Send multiple message chunks with human-like delays and typing indicators.

        This makes the bot feel more natural, like a real person texting in bursts
        rather than sending one giant wall of text.

        Args:
            to_number: Recipient phone number or email (Apple ID)
            message_chunks: List of message chunks to send in sequence
            from_number: Unused (for API compatibility with SendBlue)
            delay_range: Tuple of (min_delay, max_delay) in seconds between chunks
            show_typing: Whether to show typing indicator before each chunk

        Returns:
            List of API response dictionaries for each chunk sent

        Example:
            >>> chunks = ["Yo! Found 2 opportunities for you",
            ...           "1. Google SWE Intern - deadline March 15",
            ...           "2. Meta ML Intern - deadline March 20"]
            >>> await client.send_chunked_messages("+1234567890", chunks)
        """
        if not message_chunks:
            logger.warning("send_chunked_messages called with empty chunks list")
            return []

        results = []
        min_delay, max_delay = delay_range

        # Determine chat_guid once for all chunks
        is_email = "@" in to_number
        if is_email:
            normalized = to_number.lower().strip()
        else:
            normalized = normalize_phone_number(to_number)
            if not normalized:
                logger.error(f"Failed to normalize phone number: {to_number}")
                return []

        chat_guid = self._build_chat_guid(normalized, is_email=is_email)

        for i, chunk in enumerate(message_chunks):
            try:
                # Show typing indicator before sending each chunk (including first one)
                if show_typing:
                    try:
                        # Short typing duration for natural feel
                        typing_duration = min(len(chunk) / 100, 1.5)  # Faster "typing" speed
                        await self.send_typing_indicator(
                            to_number=to_number,
                            duration=typing_duration,
                            chat_guid=chat_guid
                        )
                    except Exception as e:
                        logger.warning(f"Could not send typing indicator: {str(e)}")
                        # Continue anyway - typing is nice-to-have

                # Send the chunk
                logger.info(f"Sending chunk {i+1}/{len(message_chunks)} to {to_number}: {chunk[:50]}...")
                result = await self.send_message(
                    to_number=to_number,
                    content=chunk,
                    chat_guid=chat_guid
                )

                results.append({
                    "success": True,
                    "chunk_index": i,
                    "result": result
                })

                logger.info(f"Successfully sent chunk {i+1}/{len(message_chunks)}")

                # Add human-like delay before next chunk (except after last chunk)
                if i < len(message_chunks) - 1:
                    delay = random.uniform(min_delay, max_delay)
                    logger.debug(f"Waiting {delay:.1f}s before next chunk...")
                    await asyncio.sleep(delay)

            except PhotonClientError as e:
                logger.error(f"Failed to send chunk {i+1}/{len(message_chunks)}: {str(e)}")
                results.append({
                    "success": False,
                    "chunk_index": i,
                    "error": str(e)
                })
                # Continue sending remaining chunks even if one fails

            except Exception as e:
                logger.error(f"Unexpected error sending chunk {i+1}: {str(e)}", exc_info=True)
                results.append({
                    "success": False,
                    "chunk_index": i,
                    "error": str(e)
                })

        successful_chunks = sum(1 for r in results if r.get("success"))
        logger.info(f"Sent {successful_chunks}/{len(message_chunks)} chunks successfully")

        return results
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient._build_chat_guid_from_number()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Normalize the number/email and convert to Photon chat GUID.
- 
- Args:
-     to_number: Phone number or email address
- 
- Returns:
-     Chat GUID or None if invalid

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
def _build_chat_guid_from_number(self, to_number: str) -> Optional[str]:
        """
        Normalize the number/email and convert to Photon chat GUID.

        Args:
            to_number: Phone number or email address

        Returns:
            Chat GUID or None if invalid
        """
        if not to_number:
            return None

        is_email = "@" in to_number

        if is_email:
            normalized = to_number.lower().strip()
        else:
            normalized = normalize_phone_number(to_number)
            if not normalized:
                return None

        return self._build_chat_guid(normalized, is_email=is_email)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient._build_chat_guid()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Build Apple iMessage chat GUID for 1:1 conversations.
- 
- Format based on Apple's iMessage database structure:
- - Phone: iMessage;-;+<E.164 number>
- - Email: iMessage;-;<email@example.com>
- 
- Args:
-     normalized_identifier: Phone number (E.164) or email address (lowercase)
-     is_email: Whether the identifier is an email address
- 
- Returns:
-     Properly formatted chat GUID

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
def _build_chat_guid(normalized_identifier: str, is_email: bool = False) -> str:
        """
        Build Apple iMessage chat GUID for 1:1 conversations.

        Format based on Apple's iMessage database structure:
        - Phone: iMessage;-;+<E.164 number>
        - Email: iMessage;-;<email@example.com>

        Args:
            normalized_identifier: Phone number (E.164) or email address (lowercase)
            is_email: Whether the identifier is an email address

        Returns:
            Properly formatted chat GUID
        """
        if is_email:
            # Email format: iMessage;-;email@example.com
            return f"iMessage;-;{normalized_identifier}"
        else:
            # Phone format: iMessage;-;+1234567890
            if not normalized_identifier.startswith("+"):
                normalized_identifier = f"+{normalized_identifier}"
            return f"iMessage;-;{normalized_identifier}"
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient._is_valid_email()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Basic email validation.
- 
- Args:
-     email: Email address to validate
- 
- Returns:
-     True if email format is valid

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
def _is_valid_email(email: str) -> bool:
        """
        Basic email validation.

        Args:
            email: Email address to validate

        Returns:
            True if email format is valid
        """
        import re
        # Basic email regex pattern
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.create_group_chat()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Create a group chat with multiple participants.
- 
- Uses the Photon Advanced iMessage Kit to create a new group chat
- with the specified participants and sends an initial message.
- 
- Args:
-     addresses: List of phone numbers or emails (2+ participants)
-     message: Initial message to send to the group
-     service: Service type ("iMessage" or "SMS")
-     method: Sending method ("private-api" for group creation)
- 
- Returns:
-     Dict containing the chat_guid and response data
- 
- Raises:
-     PhotonClientError: If validation or API call fails

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def create_group_chat(
        self,
        addresses: List[str],
        message: str,
        *,
        service: str = "iMessage",
        method: str = "private-api"
    ) -> Dict[str, Any]:
        """
        Create a group chat with multiple participants.

        Uses the Photon Advanced iMessage Kit to create a new group chat
        with the specified participants and sends an initial message.

        Args:
            addresses: List of phone numbers or emails (2+ participants)
            message: Initial message to send to the group
            service: Service type ("iMessage" or "SMS")
            method: Sending method ("private-api" for group creation)

        Returns:
            Dict containing the chat_guid and response data

        Raises:
            PhotonClientError: If validation or API call fails
        """
        if not addresses or len(addresses) < 2:
            raise PhotonClientError("Group chat requires at least 2 participants")

        if not message:
            raise PhotonClientError("Initial message is required for group creation")

        # Normalize all addresses
        normalized_addresses = []
        for addr in addresses:
            if "@" in addr:
                if not self._is_valid_email(addr):
                    raise PhotonClientError(f"Invalid email address: {addr}")
                normalized_addresses.append(addr.lower().strip())
            else:
                if not is_valid_phone_number(addr):
                    reason = get_invalid_phone_reason(addr)
                    raise PhotonClientError(f"Invalid phone number {addr}: {reason}")
                normalized = normalize_phone_number(addr)
                if not normalized:
                    raise PhotonClientError(f"Failed to normalize phone number: {addr}")
                normalized_addresses.append(normalized)

        logger.info(
            f"[PHOTON] Creating group chat with {len(normalized_addresses)} participants"
        )

        # Use the message/text endpoint with addresses array for group chat creation
        # This mirrors how the TypeScript SDK's createChat works internally
        payload = {
            "addresses": normalized_addresses,
            "message": message,
            "service": service,
            "method": method
        }

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        logger.info(f"[PHOTON] Sending group chat creation request to /api/v1/chat/new")
        logger.debug(f"[PHOTON] Group chat payload: {payload}")

        async with httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30.0,
            headers=headers
        ) as client:
            response = await client.post("/api/v1/chat/new", json=payload)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[PHOTON] Group chat creation failed %s - %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                raise PhotonClientError(
                    f"Photon create_group_chat failed: {exc}"
                ) from exc

            data = response.json().get("data") if response.content else None

            # Extract chat GUID from response - for group chats it might be in different fields
            chat_guid = None
            if data:
                chat_guid = data.get("chatGuid") or data.get("guid")
                # If sending to multiple addresses, check the chats array
                chats = data.get("chats", [])
                if chats and not chat_guid:
                    chat_guid = chats[0].get("guid") if chats else None

            logger.info(f"[PHOTON] Group chat created successfully: {chat_guid}")
            logger.debug(f"[PHOTON] Full response: {data}")

            return {
                "chat_guid": chat_guid,
                "data": data
            }
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.update_chat()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Update a chat's metadata (e.g., rename a group chat).
- 
- Photon Advanced iMessage Kit:
-   PUT /api/v1/chat/:guid  { displayName: "New Name" }

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def update_chat(
        self,
        chat_guid: str,
        *,
        display_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update a chat's metadata (e.g., rename a group chat).

        Photon Advanced iMessage Kit:
          PUT /api/v1/chat/:guid  { displayName: "New Name" }
        """
        guid = str(chat_guid or "").strip()
        if not guid:
            raise PhotonClientError("Missing chat GUID")

        payload: Dict[str, Any] = {}
        if display_name is not None:
            name = str(display_name).strip()
            if name:
                payload["displayName"] = name

        if not payload:
            raise PhotonClientError("No valid chat updates provided")

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        encoded_guid = quote(guid, safe="")

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
            response = await client.put(f"/api/v1/chat/{encoded_guid}", json=payload)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[PHOTON] Chat update failed %s - %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                raise PhotonClientError(f"Photon update_chat failed: {exc}") from exc

            data = response.json().get("data") if response.content else None
            return {
                "chat_guid": guid,
                "data": data,
            }
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.should_share_contact()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Check whether the SDK recommends sharing your contact card in this chat.
- 
- Returns:
- - true: sharing is recommended (typically when the other side shared theirs
-         and you haven't shared yours yet)
- - false: NOT recommended (e.g. you've already shared, OR the other side
-          hasn't shared theirs yet)
- 
- Args:
-     chat_guid: The chat identifier (e.g. the guid field from chat APIs/events)
- 
- Returns:
-     bool indicating whether contact card sharing is recommended

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def should_share_contact(self, chat_guid: str) -> bool:
        """
        Check whether the SDK recommends sharing your contact card in this chat.

        Returns:
        - true: sharing is recommended (typically when the other side shared theirs
                and you haven't shared yours yet)
        - false: NOT recommended (e.g. you've already shared, OR the other side
                 hasn't shared theirs yet)

        Args:
            chat_guid: The chat identifier (e.g. the guid field from chat APIs/events)

        Returns:
            bool indicating whether contact card sharing is recommended
        """
        if not chat_guid:
            raise PhotonClientError("Missing chat GUID for should_share_contact check")

        logger.info(f"[PHOTON] Checking should_share_contact for {chat_guid[:30]}...")

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        encoded_guid = quote(str(chat_guid), safe="")

        try:
            async with httpx.AsyncClient(base_url=self.base_url, timeout=10.0, headers=headers) as client:
                response = await client.get(f"/api/v1/chat/{encoded_guid}/share/contact/status")
                response.raise_for_status()
                data = response.json()
                # Response format: { data: { data: boolean } }
                should_share = data.get("data", {}).get("data", False) if isinstance(data.get("data"), dict) else data.get("data", False)
                logger.info(f"[PHOTON] should_share_contact for {chat_guid[:30]}...: {should_share}")
                return bool(should_share)
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "[PHOTON] should_share_contact check failed %s - %s",
                exc.response.status_code,
                exc.response.text,
            )
            # If the endpoint doesn't exist or fails, default to False (don't share)
            return False
        except Exception as exc:
            logger.warning(f"[PHOTON] should_share_contact error: {exc}")
            # If there's an error, default to False (don't share)
            return False
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.add_participant()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Add a participant to an existing group chat.
- 
- Used for adding late joiners to multi-person group chats.
- 
- Args:
-     chat_guid: The existing group chat GUID
-     address: Phone number or email of the participant to add
- 
- Returns:
-     Dict containing the response data
- 
- Raises:
-     PhotonClientError: If the operation fails

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def add_participant(
        self,
        chat_guid: str,
        address: str,
    ) -> Dict[str, Any]:
        """
        Add a participant to an existing group chat.

        Used for adding late joiners to multi-person group chats.

        Args:
            chat_guid: The existing group chat GUID
            address: Phone number or email of the participant to add

        Returns:
            Dict containing the response data

        Raises:
            PhotonClientError: If the operation fails
        """
        if not chat_guid:
            raise PhotonClientError("Missing chat_guid for add_participant")
        if not address:
            raise PhotonClientError("Missing address for add_participant")

        # Normalize the address
        is_email = "@" in address
        if is_email:
            if not self._is_valid_email(address):
                raise PhotonClientError(f"Invalid email address: {address}")
            normalized = address.lower().strip()
        else:
            if not is_valid_phone_number(address):
                reason = get_invalid_phone_reason(address)
                raise PhotonClientError(f"Invalid phone number: {reason}")
            normalized = normalize_phone_number(address)
            if not normalized:
                raise PhotonClientError("Failed to normalize phone number")

        logger.info(f"[PHOTON] Adding participant {normalized} to chat {chat_guid[:30]}...")

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        encoded_guid = quote(chat_guid, safe="")

        # Photon SDK: sdk.chats.addParticipant(chatGuid, address)
        # Maps to: POST /api/v1/chat/{guid}/participant
        payload = {"address": normalized}

        async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
            response = await client.post(
                f"/api/v1/chat/{encoded_guid}/participant",
                json=payload
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                logger.error(
                    "[PHOTON] add_participant failed %s - %s",
                    exc.response.status_code,
                    exc.response.text,
                )
                raise PhotonClientError(f"Photon add_participant failed: {exc}") from exc

            data = response.json().get("data") if response.content else None
            logger.info(f"[PHOTON] Successfully added participant {normalized} to chat")
            return {
                "chat_guid": chat_guid,
                "added_address": normalized,
                "data": data,
            }
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `PhotonClient.share_contact_card()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Outbound Photon API client operations. This function sits in the Per-message sequence (outbound messaging/typing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, app, asyncio, httpx, logging, random, socketio, tenacity, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Share your contact card (iMessage "Share Name and Photo") to the specified chat.
- 
- This sends Franklink's contact information to the user via HTTP API,
- allowing them to save it to their contacts.
- 
- Args:
-     chat_guid: The chat identifier where to share the contact card
- 
- Returns:
-     Dict containing the response data
- 
- Raises:
-     PhotonClientError: If the operation fails

When used:
- Per-message sequence (outbound messaging/typing).

Method code:
```python
async def share_contact_card(self, chat_guid: str) -> Dict[str, Any]:
        """
        Share your contact card (iMessage "Share Name and Photo") to the specified chat.

        This sends Franklink's contact information to the user via HTTP API,
        allowing them to save it to their contacts.

        Args:
            chat_guid: The chat identifier where to share the contact card

        Returns:
            Dict containing the response data

        Raises:
            PhotonClientError: If the operation fails
        """
        if not chat_guid:
            raise PhotonClientError("Missing chat GUID for share_contact_card")

        logger.info(f"[PHOTON] Sharing contact card to chat: {chat_guid[:30]}...")

        headers = {}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        encoded_guid = quote(str(chat_guid), safe="")

        try:
            async with httpx.AsyncClient(base_url=self.base_url, timeout=30.0, headers=headers) as client:
                response = await client.post(f"/api/v1/chat/{encoded_guid}/share/contact")
                response.raise_for_status()
                data = response.json() if response.content else None
                logger.info(f"✅ Contact card shared successfully to {chat_guid[:30]}...")
                return {
                    "chat_guid": chat_guid,
                    "data": data,
                }
        except httpx.HTTPStatusError as exc:
            logger.error(
                "[PHOTON] share_contact_card failed %s - %s",
                exc.response.status_code,
                exc.response.text,
            )
            raise PhotonClientError(f"Photon share_contact_card failed: {exc}") from exc
        except Exception as exc:
            logger.error(f"[PHOTON] share_contact_card failed: {exc}")
            raise PhotonClientError(f"Photon share_contact_card failed: {exc}") from exc
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

## Module: `app/main.py`

Role: FastAPI entrypoint; wires listener -> Kafka -> orchestrator.

Module docstring:
- Main FastAPI application for Frank.

Imported stack (selected):
- app.agents.queue.AsyncOperationProcessor, app.agents.queue.register_all_handlers, app.config.settings, app.integrations.composio_client.ComposioClient, app.integrations.kafka_pipeline.KafkaInboundConsumer, app.integrations.kafka_pipeline.KafkaProducerClient, app.integrations.kafka_pipeline.build_kafka_event, app.integrations.photon_client.PhotonClient, app.integrations.photon_listener.PhotonListener, app.integrations.stripe_client.StripeClient, app.orchestrator.MainOrchestrator, asyncio, datetime.datetime, datetime.timezone, fastapi.FastAPI, fastapi.HTTPException, fastapi.Request, fastapi.middleware.cors.CORSMiddleware, json, logging, pydantic.BaseModel, slowapi.Limiter, slowapi._rate_limit_exceeded_handler, slowapi.errors.RateLimitExceeded, slowapi.util.get_remote_address, typing.Any, typing.Dict, typing.List, typing.Optional, urllib.parse.urlparse

### Class: `PhotonWebhook`

Big picture:
- FastAPI endpoints and startup/shutdown wiring.

Purpose:
- Photon webhook payload model.

When used:
- Support path (utility/config).

Class code:
```python
class PhotonWebhook(BaseModel):
    """Photon webhook payload model."""

    from_number: Optional[str] = None
    to_number: Optional[str] = None
    content: Optional[str] = None
    media_url: Optional[str] = None
    message_id: Optional[str] = None
    timestamp: Optional[str] = None
    chat_guid: Optional[str] = None
    is_outbound: bool = False
    status: Optional[str] = None

    class Config:
        extra = "allow"
```

### Class: `HealthResponse`

Big picture:
- FastAPI endpoints and startup/shutdown wiring.

Purpose:
- No class docstring; see methods below.

When used:
- Support path (utility/config).

Class code:
```python
class HealthResponse(BaseModel):
    status: str
    timestamp: str
    version: str = "1.0.0"
```

### Class: `SendPollRequest`

Big picture:
- FastAPI endpoints and startup/shutdown wiring.

Purpose:
- No class docstring; see methods below.

When used:
- Support path (utility/config).

Class code:
```python
class SendPollRequest(BaseModel):
    chat_guid: Optional[str] = None
    to_number: Optional[str] = None
    title: str = ""
    options: List[str]
```

### Function: `root()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
async def root():
    return HealthResponse(status="healthy", timestamp=datetime.utcnow().isoformat())
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `health_check()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
async def health_check():
    return HealthResponse(status="healthy", timestamp=datetime.utcnow().isoformat())
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_require_diagnostics_token()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _require_diagnostics_token(request: Request) -> None:
    token = getattr(settings, "diagnostics_token", None)
    if not token:
        raise HTTPException(status_code=404, detail="Not found")
    provided = request.headers.get("x-diagnostics-token")
    if not provided or provided != token:
        raise HTTPException(status_code=403, detail="Forbidden")
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_safe_url_hint()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _safe_url_hint(url: str) -> str:
    try:
        parsed = urlparse(url)
        path = parsed.path or ""
        hint_path = path[:18] + "…" if len(path) > 18 else path
        return f"{parsed.scheme}://{parsed.netloc}{hint_path}"
    except Exception:
        return "unparseable"
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `debug_composio()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Safe Composio diagnostics endpoint.
- Requires `DIAGNOSTICS_TOKEN` to be set and passed as header `X-Diagnostics-Token`.

When used:
- Support path (utility/config).

Function code:
```python
async def debug_composio(request: Request, generate_link: bool = False):
    """
    Safe Composio diagnostics endpoint.
    Requires `DIAGNOSTICS_TOKEN` to be set and passed as header `X-Diagnostics-Token`.
    """
    _require_diagnostics_token(request)

    client = ComposioClient()
    payload: Dict[str, Any] = {
        "composio_available": client.is_available(),
        "api_key_present": bool(getattr(client, "api_key", None)),
        "base_url_set": bool(getattr(client, "base_url", None)),
        "provider": getattr(client, "provider", None),
        "entity_prefix": getattr(client, "entity_prefix", None),
        "auth_config_id_present": bool(getattr(client, "auth_config_id", None)),
        "callback_url_present": bool(getattr(client, "callback_url", None)),
        "gmail_toolkit_version": getattr(client, "gmail_toolkit_version", None),
        "timestamp": datetime.utcnow().isoformat(),
    }

    try:
        import importlib.metadata as md

        payload["composio_version"] = md.version("composio")
    except Exception:
        payload["composio_version"] = None

    try:
        resolved = await client._resolve_auth_config_id(force_lookup=True)  # noqa
        payload["resolved_auth_config_id_prefix"] = f"{resolved[:6]}..." if resolved else None
    except Exception as exc:
        payload["resolved_auth_config_error"] = f"{type(exc).__name__}: {exc}"

    if generate_link:
        try:
            link = await client.initiate_gmail_connect(user_id="diagnostics")
            payload["auth_link_generated"] = bool(link)
            payload["last_error_code"] = client.get_last_connect_error_code()
            if link:
                payload["auth_link_hint"] = _safe_url_hint(link)
        except Exception as exc:
            payload["auth_link_generated"] = False
            payload["last_error_code"] = client.get_last_connect_error_code()
            payload["auth_link_error"] = f"{type(exc).__name__}: {exc}"

    return payload
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `debug_webhook()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
async def debug_webhook(request: Request):
    body = await request.body()
    json_body = await request.json()
    logger.info(f"Headers: {dict(request.headers)}")
    logger.info(f"Raw body: {body.decode() if body else 'Empty'}")
    logger.info(f"JSON body: {json.dumps(json_body, indent=2)}")
    return {"status": "received", "debug": True}
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `send_message()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
async def send_message(
    request: Request,
    to_number: str,
    content: str,
    media_url: Optional[str] = None,
):
    try:
        client = PhotonClient(
            server_url=settings.photon_server_url,
            default_number=settings.photon_default_number,
        )
        result = await client.send_message(
            to_number=to_number,
            content=content,
            media_url=media_url,
        )
        return {"status": "sent", "result": result}
    except Exception as e:
        logger.error(f"Error sending message: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `send_poll()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
async def send_poll(request: Request, payload: SendPollRequest):
    try:
        client = PhotonClient(
            server_url=settings.photon_server_url,
            default_number=settings.photon_default_number,
        )

        chat_guid = payload.chat_guid
        if not chat_guid and payload.to_number:
            chat_guid = client._build_chat_guid_from_number(payload.to_number)

        if not chat_guid:
            raise HTTPException(status_code=400, detail="Provide chat_guid or to_number")

        result = await client.create_poll(chat_guid, title=payload.title, options=payload.options)
        return {"status": "sent", "chat_guid": chat_guid, "result": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error sending poll: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `photon_webhook()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Primary inbound webhook endpoint.

When used:
- Support path (utility/config).

Function code:
```python
async def photon_webhook(webhook: PhotonWebhook):
    """Primary inbound webhook endpoint."""
    try:
        await orchestrator.handle_message(webhook)
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"[PHOTON] Webhook error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `stripe_webhook()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Handle Stripe webhook events and send iMessage notifications on payment.

When used:
- Support path (utility/config).

Function code:
```python
async def stripe_webhook(request: Request):
    """Handle Stripe webhook events and send iMessage notifications on payment."""
    try:
        payload = await request.body()
        signature = request.headers.get("stripe-signature")
        if not signature:
            raise HTTPException(status_code=400, detail="Missing signature")

        import json

        event_data = json.loads(payload)
        event_id = event_data.get("id")
        if not event_id:
            raise HTTPException(status_code=400, detail="Missing event ID")

        from app.utils.redis_client import redis_client

        idempotency_key = f"stripe_webhook:{event_id}"
        if not redis_client.check_idempotency(idempotency_key):
            logger.warning(f"[STRIPE] Duplicate webhook detected: {event_id}")
            return {"status": "success", "duplicate": True, "message": "Event already processed"}

        # Process the webhook event
        stripe_client = StripeClient()
        try:
            result = await stripe_client.process_webhook_event(payload, signature)
            logger.info(f"[STRIPE] Webhook processed: {event_id}, result: {result}")

            # Send iMessage notification on intro fee payment completion
            if result.get("action") == "intro_payment_completed":
                phone_number = result.get("phone_number")
                if phone_number:
                    try:
                        from app.integrations.photon_client import PhotonClient
                        photon = PhotonClient(
                            server_url=settings.photon_server_url,
                            default_number=settings.photon_default_number,
                            api_key=settings.photon_api_key,
                        )
                        await photon.send_message(
                            to_number=phone_number,
                            content="payment received, you're all set! text me whenever you want to make a connection",
                        )
                        logger.info(f"[STRIPE] Payment confirmation iMessage sent to {phone_number}")
                    except Exception as msg_error:
                        logger.error(f"[STRIPE] Failed to send payment confirmation iMessage: {msg_error}")

            return {"status": "success", "event_id": event_id, "result": result}

        except ValueError as sig_error:
            logger.error(f"[STRIPE] Invalid signature during processing: {sig_error}")
            raise HTTPException(status_code=400, detail="Invalid signature")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[STRIPE] Webhook error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `payment_success()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
async def payment_success(session_id: str = None):
    logger.info(f"[STRIPE] Payment success redirect - session_id: {session_id}")
    try:
        if not session_id:
            return {"status": "success", "message": "Payment completed successfully! Return to iMessage to continue."}

        stripe_client = StripeClient()
        import stripe

        stripe.api_key = settings.stripe_api_key
        session = stripe.checkout.Session.retrieve(session_id)

        if session.payment_status == "paid":
            user_id = session.metadata.get("user_id")
            tier = session.metadata.get("tier", "premium")
            return {
                "status": "success",
                "message": f"Payment confirmed! You're now on {tier.upper()} tier.",
                "details": {
                    "session_id": session_id,
                    "user_id": user_id,
                    "tier": tier,
                    "amount_paid": session.amount_total / 100,
                    "currency": session.currency,
                },
                "next_step": "Return to iMessage to continue.",
            }
        else:
            return {
                "status": "pending",
                "message": "Payment is still processing. Please wait a moment and refresh.",
                "payment_status": session.payment_status,
            }
    except Exception as e:
        logger.error(f"[STRIPE] Error verifying payment: {e}", exc_info=True)
        return {"status": "success", "message": "Payment completed! Please return to your iMessage conversation."}
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `payment_cancel()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
async def payment_cancel():
    return {
        "status": "canceled",
        "message": "Payment was canceled. No charges were made.",
        "options": {
            "continue_free": "You can continue using the FREE tier with limited features.",
            "retry_payment": "Return to your iMessage conversation to try upgrading again.",
            "contact_support": "Need help? Reply 'help' in iMessage.",
        },
        "next_step": "Please return to your iMessage conversation to continue.",
    }
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `startup_event()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
async def startup_event():
    logger.info("Starting Frank API...")

    async def _init_services():
        try:
            # Start Photon Socket.IO listener for inbound messages
            global photon_listener
            ingest_mode = str(getattr(settings, "photon_ingest_mode", "listener") or "").strip().lower()
            if ingest_mode in {"listener", "kafka"}:
                callback = _forward_photon_message
                if ingest_mode == "kafka":
                    callback = _publish_photon_message
                photon_listener = PhotonListener(
                    server_url=settings.photon_server_url,
                    default_number=settings.photon_default_number,
                    api_key=settings.photon_api_key,
                    message_callback=callback,
                )
                await photon_listener.start()
                logger.info("[PHOTON] Listener initialized and connected mode=%s", ingest_mode)
            else:
                logger.info("[PHOTON] Listener disabled (mode=%s)", ingest_mode)
        except Exception as e:
            logger.warning(f"Startup background init failed: {e}")

    async def _init_async_processor():
        try:
            ingest_mode = str(getattr(settings, "photon_ingest_mode", "listener") or "").strip().lower()
            if ingest_mode == "kafka":
                logger.info("[ASYNC_QUEUE] Skipping async processor in ingest-only mode")
                return
            # Start async operation processor for long-running tasks
            global async_processor
            async_processor = AsyncOperationProcessor()
            # Register handlers for group chat creation, multi-match invitations, etc.
            register_all_handlers(async_processor)
            asyncio.create_task(async_processor.start_processing())
            logger.info("[ASYNC_QUEUE] Operation processor started")
        except Exception as e:
            logger.warning(f"Async processor init failed: {e}")

    async def _init_profile_synthesis_scheduler():
        """Run profile synthesis job periodically."""
        from app.jobs.user_profile_synthesis import run_profile_synthesis_job
        ingest_mode = str(getattr(settings, "photon_ingest_mode", "listener") or "").strip().lower()
        if ingest_mode == "kafka":
            logger.info("[PROFILE_SYNTHESIS] Skipping scheduler in ingest-only mode")
            return

        if not getattr(settings, "profile_synthesis_enabled", True):
            logger.info("[PROFILE_SYNTHESIS] Job disabled via settings")
            return

        # Run initial job after 60 second delay to allow other services to start
        await asyncio.sleep(60)

        while True:
            try:
                logger.info("[PROFILE_SYNTHESIS] Starting scheduled job run")
                # Add timeout to prevent hanging jobs (1 hour max)
                stats = await asyncio.wait_for(
                    run_profile_synthesis_job(
                        batch_size=getattr(settings, "profile_synthesis_batch_size", 50),
                        stale_days=getattr(settings, "profile_synthesis_stale_days", 7),
                        rate_limit_seconds=getattr(settings, "profile_synthesis_rate_limit", 2.0),
                    ),
                    timeout=3600,
                )
                logger.info(f"[PROFILE_SYNTHESIS] Job completed: {stats}")
            except asyncio.TimeoutError:
                logger.error("[PROFILE_SYNTHESIS] Job timed out after 1 hour")
            except asyncio.CancelledError:
                logger.info("[PROFILE_SYNTHESIS] Job cancelled, shutting down")
                break
            except Exception as e:
                logger.error(f"[PROFILE_SYNTHESIS] Job failed: {e}", exc_info=True)

            # Run every 6 hours
            await asyncio.sleep(6 * 60 * 60)

    asyncio.create_task(_init_services())
    asyncio.create_task(_init_kafka_consumer())
    asyncio.create_task(_init_async_processor())
    asyncio.create_task(_init_profile_synthesis_scheduler())

    logger.info("Frank API started successfully")
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `shutdown_event()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
async def shutdown_event():
    logger.info("Shutting down Frank API...")
    try:
        if photon_listener:
            await photon_listener.stop()
            logger.info("[PHOTON] Listener stopped")
    except Exception as e:
        logger.warning(f"[PHOTON] Failed to stop listener: {e}")

    try:
        if kafka_consumer:
            await kafka_consumer.stop()
            logger.info("[KAFKA] Consumer stopped")
        if kafka_producer:
            await kafka_producer.stop()
            logger.info("[KAFKA] Producer stopped")
    except Exception as e:
        logger.warning(f"[KAFKA] Failed to stop Kafka components: {e}")

    try:
        if async_processor:
            await async_processor.stop_processing()
            logger.info("[ASYNC_QUEUE] Operation processor stopped")
    except Exception as e:
        logger.warning(f"[ASYNC_QUEUE] Failed to stop processor: {e}")

    logger.info("Frank API shut down successfully")
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_forward_photon_message()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Direct (non-Kafka) path into the orchestrator. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Bridge PhotonListener inbound payloads into orchestrator.handle_message.

When used:
- Support path (utility/config).

Function code:
```python
async def _forward_photon_message(payload: Dict[str, Any]) -> None:
    """
    Bridge PhotonListener inbound payloads into orchestrator.handle_message.
    """
    from types import SimpleNamespace

    # PhotonListener builds a dict compatible with our webhook model
    webhook_obj = SimpleNamespace(**payload)
    await orchestrator.handle_message(webhook_obj)
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_publish_photon_message()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Bridges Photon inbound payloads into Kafka. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Publish PhotonListener payloads to Kafka for downstream processing.

When used:
- Support path (utility/config).

Function code:
```python
async def _publish_photon_message(payload: Dict[str, Any]) -> None:
    """
    Publish PhotonListener payloads to Kafka for downstream processing.
    """
    global kafka_producer
    global kafka_producer_lock
    if kafka_producer is None:
        if kafka_producer_lock is None:
            kafka_producer_lock = asyncio.Lock()
        async with kafka_producer_lock:
            if kafka_producer is None:
                kafka_producer = KafkaProducerClient()
                await kafka_producer.start()
    event = build_kafka_event(payload)
    await kafka_producer.send_event(topic=settings.kafka_topic_inbound, event=event)
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_parse_event_epoch_ms()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: FastAPI endpoints and startup/shutdown wiring. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _parse_event_epoch_ms(event: Dict[str, Any]) -> Optional[int]:
    value = event.get("payload_timestamp") or event.get("received_at")
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            n = float(value)
            return int(n if n > 1_000_000_000_000 else n * 1000)
        s = str(value).strip()
        if not s:
            return None
        if s.replace(".", "", 1).isdigit():
            n = float(s)
            return int(n if n > 1_000_000_000_000 else n * 1000)
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_init_kafka_consumer()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Bootstraps background services on startup. This function sits in the Startup sequence (FastAPI startup event). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Startup sequence (FastAPI startup event).

Function code:
```python
async def _init_kafka_consumer() -> None:
    mode = str(getattr(settings, "photon_consumer_mode", "off") or "").strip().lower()
    if mode != "consumer":
        logger.info("[KAFKA] Consumer disabled (mode=%s)", mode)
        return

    global kafka_consumer
    retry_delay = 1
    while True:
        try:
            if kafka_consumer is None:
                kafka_consumer = KafkaInboundConsumer(handler=_handle_kafka_event)
            await kafka_consumer.start()
            logger.info("[KAFKA] Consumer initialized and running")
            return
        except Exception as e:
            logger.warning("[KAFKA] Consumer init failed: %s (retry in %ss)", e, retry_delay)
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 10)
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_handle_kafka_event()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Consumes Kafka events and invokes the orchestrator. This function sits in the Per-message sequence (Kafka event processing). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, datetime, fastapi, json, logging, pydantic, slowapi, typing, urllib.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Per-message sequence (Kafka event processing).

Function code:
```python
async def _handle_kafka_event(event: Dict[str, Any]) -> None:
    from types import SimpleNamespace
    from app.utils.redis_client import redis_client

    event_id = str(event.get("event_id") or event.get("message_id") or "").strip()
    idempotency_key = str(event.get("idempotency_key") or "").strip()
    if not event_id or not idempotency_key:
        logger.warning("[KAFKA] Missing event_id/idempotency_key; dropping event")
        return
    ttl = int(getattr(settings, "photon_kafka_idempotency_ttl", settings.redis_idempotency_ttl) or settings.redis_idempotency_ttl)
    is_new = redis_client.check_idempotency(idempotency_key, ttl=ttl)
    if not is_new:
        logger.info("[KAFKA] Duplicate event skipped: %s", idempotency_key)
        return

    payload = {
        "from_number": event.get("from_number"),
        "to_number": event.get("to_number"),
        "content": event.get("content"),
        "message_id": event.get("message_id") or event_id,
        "timestamp": event.get("payload_timestamp") or event.get("received_at"),
        "chat_guid": event.get("chat_guid"),
        "is_outbound": False,
        "status": "received",
        "media_url": event.get("media_url"),
    }
    webhook_obj = SimpleNamespace(**payload)
    loop = asyncio.get_running_loop()
    start = loop.time()
    event_epoch_ms = _parse_event_epoch_ms(event)
    test_run = str(event.get("test_run") or getattr(settings, "latency_test_run", "") or "default")
    trace_id = str(event.get("trace_id") or "")
    try:
        await orchestrator.handle_message(webhook_obj)
    except Exception:
        processing_ms = int((loop.time() - start) * 1000)
        end_to_end_ms = processing_ms
        if event_epoch_ms:
            end_to_end_ms = max(0, int(datetime.now(timezone.utc).timestamp() * 1000) - event_epoch_ms)
        logger.info(
            "LATENCY test_run=%s status=fail latency_ms=%d processing_ms=%d trace_id=%s event_id=%s is_group=%s",
            test_run,
            end_to_end_ms,
            processing_ms,
            trace_id,
            event_id,
            bool(event.get("is_group")),
        )
        raise
    processing_ms = int((loop.time() - start) * 1000)
    end_to_end_ms = processing_ms
    if event_epoch_ms:
        end_to_end_ms = max(0, int(datetime.now(timezone.utc).timestamp() * 1000) - event_epoch_ms)
    logger.info(
        "LATENCY test_run=%s status=ok latency_ms=%d processing_ms=%d trace_id=%s event_id=%s is_group=%s",
        test_run,
        end_to_end_ms,
        processing_ms,
        trace_id,
        event_id,
        bool(event.get("is_group")),
    )
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

## Module: `app/config.py`

Role: Central configuration and settings for Kafka, Photon, Redis, etc.

Module docstring:
- Configuration settings for Frank application.

Imported stack (selected):
- pathlib.Path, pydantic_settings.BaseSettings, typing.Optional

### Class: `Settings`

Big picture:
- Central configuration for all services and integrations.

Purpose:
- Application settings.

When used:
- Support path (utility/config).

Class code:
```python
class Settings(BaseSettings):
    """Application settings."""

    # Photon Configuration
    photon_server_url: str
    photon_default_number: str  # must be provided via environment
    photon_enable_listener: bool = True
    photon_api_key: Optional[str] = None
    photon_ingest_mode: str = "listener"  # listener|kafka|off
    photon_consumer_mode: str = "off"  # consumer|off

    # Kafka Configuration
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: Optional[str] = None
    kafka_username: Optional[str] = None
    kafka_password: Optional[str] = None
    kafka_iam_region: Optional[str] = None
    kafka_client_id: str = "franklink"
    kafka_group_id: str = "frank-worker"
    kafka_topic_inbound: str = "photon.inbound.v1"
    kafka_topic_retry_30s: str = "photon.inbound.retry.30s"
    kafka_topic_retry_2m: str = "photon.inbound.retry.2m"
    kafka_topic_retry_10m: str = "photon.inbound.retry.10m"
    kafka_topic_dlq: str = "photon.inbound.dlq.v1"
    kafka_topic_partitions: int = 12
    kafka_topic_replication_factor: int = 3
    kafka_max_attempts: int = 6
    kafka_consumer_max_inflight: int = 20
    kafka_consumer_max_batch: int = 50
    kafka_consumer_poll_ms: int = 1000
    kafka_consumer_auto_offset_reset: str = "latest"
    kafka_producer_idempotence: bool = True
    photon_kafka_idempotency_ttl: int = 86400
    latency_test_run: str = ""

    # Azure OpenAI Configuration
    azure_openai_api_key: str
    azure_openai_endpoint: str
    azure_openai_api_version: str = "2025-01-01-preview"
    azure_openai_deployment_name: str
    azure_openai_reasoning_deployment_name: str
    azure_openai_embedding_deployment: str = "text-embedding-3-small"

    # Supabase Configuration
    supabase_url: str
    supabase_key: str
    supabase_service_key: Optional[str] = None

    # Franklink Resources Database
    resources_supabase_url: str
    resources_supabase_key: str
    resources_supabase_service_key: Optional[str] = None
    resources_news_table: str = "google_news_articles"

    # Group chat icebreaker (post-intro)
    icebreaker_enabled: bool = True
    icebreaker_poll_options: int = 4
    icebreaker_poll_backup_text_enabled: bool = False

    # Group chat summarization (background job)
    groupchat_summary_enabled: bool = True
    groupchat_summary_inactivity_minutes: int = 120
    groupchat_summary_model: str = "gpt-4o-mini"
    groupchat_summary_worker_max_jobs: int = 5
    groupchat_summary_worker_stale_minutes: int = 20

    # Group chat behavior
    groupchat_icebreaker_followup_opinion_enabled: bool = False
    groupchat_meeting_default_minutes: int = 30
    groupchat_meeting_send_updates: bool = True
    groupchat_meeting_create_meeting_room: bool = False
    groupchat_meeting_calendar_id: str = "primary"

    # Group chat inactivity follow-up (background job)
    groupchat_followup_enabled: bool = True
    groupchat_followup_inactivity_minutes: int = 10080
    groupchat_followup_summary_window_days: int = 14
    groupchat_followup_model: str = "gpt-4o-mini"
    groupchat_followup_worker_max_jobs: int = 5
    groupchat_followup_worker_stale_minutes: int = 20
    groupchat_followup_poll_seconds: int = 10
    groupchat_followup_worker_max_attempts: int = 6

    # Tapback reactions (Photon)
    reactions_enabled: bool = True
    reactions_llm_enabled: bool = True
    reactions_model: str = "gpt-4o-mini"

    # Redis Configuration
    redis_url: str = "redis://localhost:6379/0"
    redis_max_connections: int = 50
    redis_idempotency_ttl: int = 86400
    redis_cache_ttl: int = 300
    redis_rate_limit_window: int = 60

    # FastAPI Configuration
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_env: str = "development"
    debug: bool = True
    app_log_level: str = "INFO"
    diagnostics_token: Optional[str] = None

    # CORS
    cors_allowed_origins: str = "*"

    # Sentry
    sentry_dsn: Optional[str] = None

    # Rate Limiting
    rate_limit_per_minute: int = 60
    rate_limit_per_hour: int = 1000

    # Proactive Daily Email Worker
    daily_email_worker_enabled: bool = True

    # Proactive Outreach Worker
    proactive_outreach_worker_enabled: bool = False

    # Zep Memory
    zep_api_key: Optional[str] = None
    zep_base_url: str = "https://api.getzep.com"
    zep_enabled: bool = True

    # Zep Graph (knowledge graph for email context)
    zep_graph_enabled: bool = True
    zep_graph_chunk_size: int = 9000  # Max chars per graph.add call (limit is 10k)
    zep_graph_sync_emails: bool = True  # Sync emails to user's graph
    zep_graph_sync_signals: bool = True  # Sync networking signals to graph
    zep_graph_fallback_to_supabase: bool = True  # Fallback to Supabase on Zep failure
    zep_graph_enrich_candidates: bool = True  # Enrich match candidates with Zep facts
    zep_graph_max_facts_per_candidate: int = 3  # Max Zep facts per candidate

    # User Profile Synthesis (holistic user understanding)
    profile_synthesis_enabled: bool = True  # Enable profile synthesis job
    profile_synthesis_stale_days: int = 7  # Resynthesize profiles older than this
    profile_synthesis_min_facts: int = 3  # Min Zep facts required for synthesis
    profile_synthesis_batch_size: int = 50  # Max users per job run
    profile_synthesis_rate_limit: float = 2.0  # Seconds between users (API rate limit)
    profile_synthesis_model: str = "gpt-4o"  # Model for synthesis (use quality model)
    profile_synthesis_use_in_matching: bool = True  # Use holistic profiles in matching

    # Composio (email context)
    composio_api_key: Optional[str] = None
    composio_base_url: Optional[str] = None
    composio_entity_prefix: str = "franklink"
    composio_gmail_provider: str = "gmail"
    composio_gmail_toolkit_slug: str = "gmail"
    composio_auth_config_id: Optional[str] = None
    composio_gmail_toolkit_version: Optional[str] = None
    composio_callback_url: Optional[str] = None

    # Composio (calendar)
    composio_calendar_provider: str = "googlecalendar"
    composio_calendar_toolkit_slug: str = "googlecalendar"
    composio_calendar_auth_config_id: Optional[str] = None
    composio_calendar_toolkit_version: Optional[str] = None
    composio_calendar_create_tool: str = "GOOGLECALENDAR_CREATE_EVENT"

    # Login page URL for wrapping OAuth links (shows franklink.ai/login instead of raw Composio URL)
    login_page_url: Optional[str] = None  # e.g., "https://franklink.ai/login"

    # Email context signals (derived from inbox)
    email_context_query: str = "newer_than:90d"
    email_context_max_threads: int = 30
    email_context_max_evidence: int = 100  # 50 received + 50 sent emails
    email_context_refresh_days: int = 14

    # Stripe Payment
    stripe_api_key: Optional[str] = None
    stripe_webhook_secret: Optional[str] = None
    stripe_success_url: str = "http://localhost:8000/payment/success"
    stripe_cancel_url: str = "http://localhost:8000/payment/cancel"

    # Legal
    privacy_policy_url: str = "https://franklink.ai/privacy"
    terms_of_service_url: str = "https://franklink.ai/terms"
    data_deletion_url: str = "https://franklink.ai/data-deletion"

    @property
    def cors_origins_list(self):
        return [o.strip() for o in self.cors_allowed_origins.split(",") if o.strip()]
```

#### Method: `Settings.cors_origins_list()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Central configuration for all services and integrations. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: pathlib, pydantic_settings, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No method docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Method code:
```python
def cors_origins_list(self):
        return [o.strip() for o in self.cors_allowed_origins.split(",") if o.strip()]
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

## Module: `app/orchestrator.py`

Role: Main orchestrator; handles inbound messages and sends responses.

Module docstring:
- Main orchestrator agent for handling conversations via InteractionAgent.

Imported stack (selected):
- app.config.settings, app.database.client.DatabaseClient, app.integrations.azure_openai_client.AzureOpenAIClient, app.integrations.photon_client.PhotonClient, app.utils.message_chunker.chunk_message, asyncio, logging, typing.Any, typing.Dict, typing.Optional

### Class: `MainOrchestrator`

Big picture:
- Coordinates user, agent, and response sending.

Purpose:
- Main orchestrator agent that coordinates conversation handling.
- 
- This class serves as the entry point for all messages, routing them through
- the InteractionAgent and handling response delivery.

When used:
- Per-message sequence (message orchestration).

Class code:
```python
class MainOrchestrator:
    """
    Main orchestrator agent that coordinates conversation handling.

    This class serves as the entry point for all messages, routing them through
    the InteractionAgent and handling response delivery.
    """

    def __init__(self):
        """Initialize the orchestrator with required clients."""
        self.db = DatabaseClient()
        self.photon = PhotonClient(
            server_url=settings.photon_server_url,
            default_number=settings.photon_default_number
        )
        self.openai = AzureOpenAIClient()

        # Initialize interaction agent (lazy-loaded on first message)
        self.interaction_agent = None

    async def handle_message(self, webhook: Any) -> None:
        """
        Handle an incoming message from Photon webhook.

        Args:
            webhook: The webhook data from Photon
        """
        import os
        pid = os.getpid()
        logger.info(f"[ORCHESTRATOR] Handling message pid={pid} from={webhook.from_number} to={webhook.to_number}")

        try:
            # 1. Get or create user profile
            logger.info(f"[ORCHESTRATOR] Getting/creating user for {webhook.from_number}")
            user = await self.db.get_or_create_user(webhook.from_number)
            logger.info(f"[ORCHESTRATOR] Processing message for user {user['id']}")

            # Store the incoming user message in conversation history
            try:
                await self.db.store_message(
                    user_id=user['id'],
                    content=webhook.content,
                    message_type="user",
                    metadata={
                        "message_id": getattr(webhook, "message_id", None),
                        "chat_guid": getattr(webhook, "chat_guid", None),
                    }
                )
                logger.debug(f"[ORCHESTRATOR] Stored user message for {user['id']}")
            except Exception as e:
                logger.warning(f"[ORCHESTRATOR] Failed to store user message: {e}")

            # Group chat messages are handled separately (never DM-reply to group messages).
            chat_guid = getattr(webhook, "chat_guid", None)
            if chat_guid and (";+;" in str(chat_guid) or str(chat_guid).startswith("chat")):
                try:
                    from app.groupchat.runtime.router import GroupChatRouter

                    router = GroupChatRouter(
                        db=self.db,
                        photon=self.photon,
                        openai=self.openai,
                    )
                    handled = await router.handle_inbound(webhook, sender_user_id=str(user.get("id") or ""))
                    logger.info(
                        "[ORCHESTRATOR] Group chat routed handled=%s chat_guid=%s msg_id=%s sender_user_id=%s",
                        handled,
                        str(chat_guid)[:40],
                        str(getattr(webhook, "message_id", "") or "")[:18],
                        str(user.get("id") or "")[:8],
                    )
                except Exception as e:
                    logger.error(f"[ORCHESTRATOR] Group chat handler failed: {e}", exc_info=True)
                return

            # 2. Process via InteractionAgent
            logger.info("[ORCHESTRATOR] Processing message via InteractionAgent")

            if self.interaction_agent is None:
                from app.agents.interaction import get_interaction_agent
                self.interaction_agent = get_interaction_agent(
                    db=self.db,
                    photon=self.photon,
                    openai=self.openai,
                )
                logger.info("[ORCHESTRATOR] InteractionAgent initialized")

            # Mark chat as read before processing
            try:
                await self.photon.mark_chat_read(chat_guid)
            except Exception as e:
                logger.debug(f"[ORCHESTRATOR] Failed to mark chat as read: {e}")

            # Show typing indicator while processing (typically 3-4 seconds)
            try:
                await self.photon.start_typing(webhook.from_number, chat_guid=chat_guid)
                logger.info(f"[ORCHESTRATOR] Started typing indicator for {webhook.from_number}")
            except Exception as e:
                logger.warning(f"[ORCHESTRATOR] Failed to start typing indicator: {e}")

            result = None
            try:
                webhook_data = {
                    "message_id": getattr(webhook, "message_id", None),
                    "timestamp": getattr(webhook, "timestamp", None),
                    "media_url": getattr(webhook, "media_url", None),
                    "chat_guid": chat_guid,
                }

                # Filter user profile to only include necessary fields to reduce context size
                filtered_user = {
                    "id": user.get("id"),
                    "phone_number": user.get("phone_number"),
                    "name": user.get("name"),
                    "email": user.get("email"),
                    "university": user.get("university"),
                    "location": user.get("location"),
                    "major": user.get("major"),
                    "year": user.get("year"),
                    "career_interests": user.get("career_interests"),
                    "networking_clarification": user.get("networking_clarification"),
                    "is_onboarded": user.get("is_onboarded"),
                    # Networking-required fields
                    "latest_demand": user.get("latest_demand"),
                    "all_demand": user.get("all_demand"),
                    "all_value": user.get("all_value"),
                    # Onboarding-required fields (stores email_connect status, eval states)
                    "personal_facts": user.get("personal_facts"),
                    "onboarding_stage": user.get("onboarding_stage"),
                    "linkedin_url": user.get("linkedin_url"),
                    "demand_history": user.get("demand_history"),
                    "value_history": user.get("value_history"),
                    "intro_fee_cents": user.get("intro_fee_cents"),
                    "needs": user.get("needs"),
                    "career_goals": user.get("career_goals"),
                    "networking_limitation": user.get("networking_limitation"),
                }

                result = await self.interaction_agent.process_message(
                    phone_number=webhook.from_number,
                    message_content=webhook.content,
                    user=filtered_user,
                    webhook_data=webhook_data,
                )
            finally:
                # Always stop typing indicator when processing completes
                try:
                    await self.photon.stop_typing(webhook.from_number, chat_guid=chat_guid)
                    logger.info(f"[ORCHESTRATOR] Stopped typing indicator for {webhook.from_number}")
                except Exception as e:
                    logger.debug(f"[ORCHESTRATOR] Failed to stop typing indicator: {e}")

            # Handle response
            if result["success"]:
                responses = result.get("responses")
                if isinstance(responses, list) and responses:
                    inbound_guid = str(getattr(webhook, "message_id", "") or "").strip()
                    legacy_outbound_ok = None
                    for idx, item in enumerate(responses):
                        response_text = str(item.get("response_text") or "").strip()
                        task_intent = item.get("intent")
                        task_name = item.get("task")

                        if response_text:
                            # Don't chunk URLs - they break when split
                            if response_text.startswith("http://") or response_text.startswith("https://"):
                                await self.photon.send_message(to_number=webhook.from_number, content=response_text)
                            else:
                                await self._send_message_chunks(
                                    phone_number=webhook.from_number,
                                    response=response_text,
                                    user_id=user['id']
                                )

                            await self.db.store_message(
                                user_id=user['id'],
                                content=response_text,
                                message_type="bot",
                                metadata={
                                    "intent": task_intent,
                                    "task": task_name,
                                    "task_index": idx,
                                }
                            )

                            # Sync conversation to Zep knowledge graph (background task)
                            asyncio.create_task(self._sync_conversation_to_zep(
                                user_id=user['id'],
                                user_message=webhook.content,
                                bot_response=response_text,
                                user_name=user.get('name'),
                                intent=task_intent or task_name,
                            ))

                        resource_urls = item.get("resource_urls", []) or []
                        if resource_urls:
                            urls_only = [r.get("url", "") for r in resource_urls if r.get("url")]
                            if urls_only:
                                url_message = "\n".join(urls_only)
                                await self._send_message_chunks(
                                    phone_number=webhook.from_number,
                                    response=url_message,
                                    user_id=user['id']
                                )
                                await self.db.store_message(
                                    user_id=user['id'],
                                    content=url_message,
                                    message_type="bot",
                                    metadata={
                                        "intent": task_intent,
                                        "task": task_name,
                                        "message_part": "urls",
                                        "task_index": idx,
                                    }
                                )

                        outbound = item.get("outbound_messages", []) or []
                        if isinstance(outbound, list) and outbound:
                            redis_client = None
                            if inbound_guid:
                                try:
                                    from app.utils.redis_client import redis_client as redis_client
                                except Exception:
                                    redis_client = None

                            if inbound_guid and redis_client and legacy_outbound_ok is None:
                                try:
                                    legacy_outbound_ok = redis_client.check_idempotency(
                                        f"outbound_messages:v1:{inbound_guid}",
                                        ttl=60 * 60 * 24 * 30
                                    )
                                except Exception:
                                    legacy_outbound_ok = True
                            should_send_extras = True
                            if inbound_guid:
                                if legacy_outbound_ok is False:
                                    should_send_extras = False
                                elif redis_client:
                                    try:
                                        should_send_extras = redis_client.check_idempotency(
                                            f"outbound_messages:v2:{inbound_guid}:{idx}",
                                            ttl=60 * 60 * 24 * 30
                                        )
                                    except Exception:
                                        should_send_extras = True

                            if should_send_extras:
                                for i, text in enumerate(outbound[:3]):
                                    msg = str(text or "").strip()
                                    if not msg:
                                        continue
                                    # Don't chunk URLs - they break when split
                                    if msg.startswith("http://") or msg.startswith("https://"):
                                        await self.photon.send_message(to_number=webhook.from_number, content=msg)
                                    else:
                                        await self._send_message_chunks(
                                            phone_number=webhook.from_number,
                                            response=msg,
                                            user_id=user['id']
                                        )
                                    await self.db.store_message(
                                        user_id=user['id'],
                                        content=msg,
                                        message_type="bot",
                                        metadata={
                                            "intent": task_intent,
                                            "task": task_name,
                                            "message_part": f"outbound_{i}",
                                            "task_index": idx,
                                        }
                                    )

                    # Maybe send a lightweight reaction based on the last task
                    last = responses[-1] if responses else {}
                    asyncio.create_task(self._maybe_send_reaction(
                        phone_number=webhook.from_number,
                        chat_guid=getattr(webhook, "chat_guid", None),
                        message_guid=getattr(webhook, "message_id", None),
                        message_content=webhook.content,
                        context={
                            "intent": last.get("intent"),
                            "task": last.get("task"),
                            "onboarding_stage": (result.get("state", {}) or {}).get("user_profile", {}).get("onboarding_stage"),
                        },
                    ))

                    logger.info(
                        "[ORCHESTRATOR] Multi-task processing complete responses=%s",
                        len(responses),
                    )
                    return

                # Fallback: single response_text without responses list
                if result.get("response_text"):
                    response_text = result["response_text"]
                    # Don't chunk URLs - they break when split
                    if response_text.startswith("http://") or response_text.startswith("https://"):
                        await self.photon.send_message(to_number=webhook.from_number, content=response_text)
                    else:
                        await self._send_message_chunks(
                            phone_number=webhook.from_number,
                            response=response_text,
                            user_id=user['id']
                        )

                    await self.db.store_message(
                        user_id=user['id'],
                        content=response_text,
                        message_type="bot",
                        metadata={
                            "intent": result.get("intent"),
                            "task": result.get("intent"),
                        }
                    )

                    logger.info(f"[ORCHESTRATOR] Processing complete - Task: {result.get('intent')}")
                    return

                # No response generated - send a fallback
                logger.warning("[ORCHESTRATOR] Success but no response generated, sending fallback")
                fallback_response = "hey! what can i help you with?"
                await self._send_message_chunks(
                    phone_number=webhook.from_number,
                    response=fallback_response,
                    user_id=user['id']
                )
                await self.db.store_message(
                    user_id=user['id'],
                    content=fallback_response,
                    message_type="bot",
                    metadata={
                        "intent": result.get("intent"),
                        "fallback": True
                    }
                )
                return

            else:
                # Processing failed - send simple error message
                logger.error(f"[ORCHESTRATOR] Processing failed: {result.get('error', 'Unknown error')}")

                await self._send_message_chunks(
                    phone_number=webhook.from_number,
                    response="Sorry, I'm having technical difficulties right now. Please try again in a moment!",
                    user_id=user['id']
                )
                return

        except SystemExit as e:
            logger.critical(f"[CRASH DETECT] SystemExit in orchestrator - PID={pid}: {e}", exc_info=True)
            raise  # Re-raise to preserve exit behavior
        except KeyboardInterrupt as e:
            logger.critical(f"[CRASH DETECT] KeyboardInterrupt in orchestrator - PID={pid}: {e}", exc_info=True)
            raise  # Re-raise to preserve interrupt behavior
        except Exception as e:
            logger.error(f"[CRASH DETECT] Exception in orchestrator - PID={pid}: {e}", exc_info=True)
            logger.error(f"[ORCHESTRATOR] Critical error in orchestrator: {str(e)}", exc_info=True)
            # Send a fallback error message to user
            try:
                await self._send_message_chunks(
                    phone_number=webhook.from_number,
                    response="Sorry, something went wrong. Please try again later!",
                    user_id=user.get('id') if user else None
                )
            except Exception as send_error:
                logger.error(f"[ORCHESTRATOR] Failed to send error message: {send_error}", exc_info=True)

    async def _send_message_chunks(
        self,
        phone_number: str,
        response: str,
        user_id: str | None,
        max_length: int = 280
    ) -> None:
        """
        Send a response message in chunks if it's too long.

        First checks for natural bubble separators (\n\n), then falls back to smart chunking.

        Args:
            phone_number: Recipient's phone number or email (Apple ID)
            response: The full response text to send
            user_id: User's UUID for logging (optional)
            max_length: Maximum characters per chunk (default 280)
        """
        try:
            # Check if response has natural bubble separators (\n\n)
            if "\n\n" in response:
                # Split on double newlines to get natural bubbles
                natural_bubbles = [bubble.strip() for bubble in response.split("\n\n") if bubble.strip()]

                if natural_bubbles:
                    logger.info(f"[ORCHESTRATOR] Sending {len(natural_bubbles)} natural bubbles to {phone_number}")
                    results = await self.photon.send_chunked_messages(
                        to_number=phone_number,
                        message_chunks=natural_bubbles,
                        delay_range=(0.3, 0.3),  # 0.3s delay between bubbles
                        show_typing=True
                    )
                    failed = [r for r in results if not r.get("success")]
                    if failed:
                        logger.error(f"[ORCHESTRATOR] Failed to send {len(failed)}/{len(natural_bubbles)} bubbles to {phone_number}")
                    else:
                        logger.info(f"[ORCHESTRATOR] Successfully sent all bubbles to {phone_number}")
                    return

            # Fallback: original chunking logic for single-bubble or too-long messages
            # If short enough, send as a single message
            if len(response) <= max_length:
                await self.photon.send_message(to_number=phone_number, content=response)
                return

            # For iMessage, keep responses to at most 2 bubbles (best-effort) to stay natural.
            chunks = chunk_message(response, max_length=max_length, max_chunks=2)
            if not chunks:
                logger.warning(f"[ORCHESTRATOR] No chunks generated for message to {phone_number}")
                return

            logger.info(f"[ORCHESTRATOR] Sending message in {len(chunks)} chunk(s) to {phone_number}")
            results = await self.photon.send_chunked_messages(
                to_number=phone_number,
                message_chunks=chunks,
                delay_range=(0.5, 1.0),
                show_typing=True
            )
            failed = [r for r in results if not r.get("success")]
            if failed:
                logger.error(f"[ORCHESTRATOR] Failed to send {len(failed)}/{len(chunks)} chunks to {phone_number}")
            else:
                logger.info(f"[ORCHESTRATOR] Successfully sent all chunks to {phone_number}")
        except Exception as e:
            logger.error(f"[ORCHESTRATOR] Error in _send_message_chunks: {e}", exc_info=True)
            try:
                await self.photon.send_message(
                    to_number=phone_number,
                    content="Sorry, I had trouble sending that message. Please try again!"
                )
            except Exception:
                pass

    async def _maybe_send_reaction(
        self,
        phone_number: str,
        message_guid: str | None,
        message_content: str,
        chat_guid: str | None = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Send an optional tapback reaction to the user's message (LLM-decided).
        """
        if not message_guid:
            return

        try:
            from app.reactions.service import ReactionService

            await ReactionService(photon=self.photon, openai=self.openai).maybe_send_reaction(
                to_number=phone_number,
                message_guid=message_guid,
                message_content=message_content,
                chat_guid=chat_guid,
                context=context or {},
            )
        except Exception as e:
            logger.debug(f"[REACTION] Failed to send reaction: {e}")

    async def _sync_conversation_to_zep(
        self,
        user_id: str,
        user_message: str,
        bot_response: str,
        user_name: Optional[str] = None,
        intent: Optional[str] = None,
    ) -> None:
        """
        Sync a conversation exchange to Zep's knowledge graph.

        Runs as a background task so it doesn't slow down response delivery.
        Enriches Zep with conversation context for better understanding.
        """
        try:
            from app.agents.tools.conversation_zep_sync import sync_conversation_to_zep

            result = await sync_conversation_to_zep(
                user_id=user_id,
                user_message=user_message,
                bot_response=bot_response,
                user_name=user_name,
                intent=intent,
            )

            if result.get("synced"):
                logger.debug(
                    "[ORCHESTRATOR] Synced conversation to Zep user=%s intent=%s",
                    user_id[:8] if user_id else "unknown",
                    intent or "unknown",
                )
        except Exception as e:
            # Don't let Zep sync failures affect the main flow
            logger.debug(f"[ORCHESTRATOR] Zep conversation sync failed: {e}")
```

#### Method: `MainOrchestrator.__init__()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Coordinates user, agent, and response sending. This function sits in the Per-message sequence (message orchestration). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, logging, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Initialize the orchestrator with required clients.

When used:
- Per-message sequence (message orchestration).

Method code:
```python
def __init__(self):
        """Initialize the orchestrator with required clients."""
        self.db = DatabaseClient()
        self.photon = PhotonClient(
            server_url=settings.photon_server_url,
            default_number=settings.photon_default_number
        )
        self.openai = AzureOpenAIClient()

        # Initialize interaction agent (lazy-loaded on first message)
        self.interaction_agent = None
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `MainOrchestrator.handle_message()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Primary message processing path for DMs and group chats. This function sits in the Per-message sequence (message orchestration). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, logging, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Handle an incoming message from Photon webhook.
- 
- Args:
-     webhook: The webhook data from Photon

When used:
- Per-message sequence (message orchestration).

Method code:
```python
async def handle_message(self, webhook: Any) -> None:
        """
        Handle an incoming message from Photon webhook.

        Args:
            webhook: The webhook data from Photon
        """
        import os
        pid = os.getpid()
        logger.info(f"[ORCHESTRATOR] Handling message pid={pid} from={webhook.from_number} to={webhook.to_number}")

        try:
            # 1. Get or create user profile
            logger.info(f"[ORCHESTRATOR] Getting/creating user for {webhook.from_number}")
            user = await self.db.get_or_create_user(webhook.from_number)
            logger.info(f"[ORCHESTRATOR] Processing message for user {user['id']}")

            # Store the incoming user message in conversation history
            try:
                await self.db.store_message(
                    user_id=user['id'],
                    content=webhook.content,
                    message_type="user",
                    metadata={
                        "message_id": getattr(webhook, "message_id", None),
                        "chat_guid": getattr(webhook, "chat_guid", None),
                    }
                )
                logger.debug(f"[ORCHESTRATOR] Stored user message for {user['id']}")
            except Exception as e:
                logger.warning(f"[ORCHESTRATOR] Failed to store user message: {e}")

            # Group chat messages are handled separately (never DM-reply to group messages).
            chat_guid = getattr(webhook, "chat_guid", None)
            if chat_guid and (";+;" in str(chat_guid) or str(chat_guid).startswith("chat")):
                try:
                    from app.groupchat.runtime.router import GroupChatRouter

                    router = GroupChatRouter(
                        db=self.db,
                        photon=self.photon,
                        openai=self.openai,
                    )
                    handled = await router.handle_inbound(webhook, sender_user_id=str(user.get("id") or ""))
                    logger.info(
                        "[ORCHESTRATOR] Group chat routed handled=%s chat_guid=%s msg_id=%s sender_user_id=%s",
                        handled,
                        str(chat_guid)[:40],
                        str(getattr(webhook, "message_id", "") or "")[:18],
                        str(user.get("id") or "")[:8],
                    )
                except Exception as e:
                    logger.error(f"[ORCHESTRATOR] Group chat handler failed: {e}", exc_info=True)
                return

            # 2. Process via InteractionAgent
            logger.info("[ORCHESTRATOR] Processing message via InteractionAgent")

            if self.interaction_agent is None:
                from app.agents.interaction import get_interaction_agent
                self.interaction_agent = get_interaction_agent(
                    db=self.db,
                    photon=self.photon,
                    openai=self.openai,
                )
                logger.info("[ORCHESTRATOR] InteractionAgent initialized")

            # Mark chat as read before processing
            try:
                await self.photon.mark_chat_read(chat_guid)
            except Exception as e:
                logger.debug(f"[ORCHESTRATOR] Failed to mark chat as read: {e}")

            # Show typing indicator while processing (typically 3-4 seconds)
            try:
                await self.photon.start_typing(webhook.from_number, chat_guid=chat_guid)
                logger.info(f"[ORCHESTRATOR] Started typing indicator for {webhook.from_number}")
            except Exception as e:
                logger.warning(f"[ORCHESTRATOR] Failed to start typing indicator: {e}")

            result = None
            try:
                webhook_data = {
                    "message_id": getattr(webhook, "message_id", None),
                    "timestamp": getattr(webhook, "timestamp", None),
                    "media_url": getattr(webhook, "media_url", None),
                    "chat_guid": chat_guid,
                }

                # Filter user profile to only include necessary fields to reduce context size
                filtered_user = {
                    "id": user.get("id"),
                    "phone_number": user.get("phone_number"),
                    "name": user.get("name"),
                    "email": user.get("email"),
                    "university": user.get("university"),
                    "location": user.get("location"),
                    "major": user.get("major"),
                    "year": user.get("year"),
                    "career_interests": user.get("career_interests"),
                    "networking_clarification": user.get("networking_clarification"),
                    "is_onboarded": user.get("is_onboarded"),
                    # Networking-required fields
                    "latest_demand": user.get("latest_demand"),
                    "all_demand": user.get("all_demand"),
                    "all_value": user.get("all_value"),
                    # Onboarding-required fields (stores email_connect status, eval states)
                    "personal_facts": user.get("personal_facts"),
                    "onboarding_stage": user.get("onboarding_stage"),
                    "linkedin_url": user.get("linkedin_url"),
                    "demand_history": user.get("demand_history"),
                    "value_history": user.get("value_history"),
                    "intro_fee_cents": user.get("intro_fee_cents"),
                    "needs": user.get("needs"),
                    "career_goals": user.get("career_goals"),
                    "networking_limitation": user.get("networking_limitation"),
                }

                result = await self.interaction_agent.process_message(
                    phone_number=webhook.from_number,
                    message_content=webhook.content,
                    user=filtered_user,
                    webhook_data=webhook_data,
                )
            finally:
                # Always stop typing indicator when processing completes
                try:
                    await self.photon.stop_typing(webhook.from_number, chat_guid=chat_guid)
                    logger.info(f"[ORCHESTRATOR] Stopped typing indicator for {webhook.from_number}")
                except Exception as e:
                    logger.debug(f"[ORCHESTRATOR] Failed to stop typing indicator: {e}")

            # Handle response
            if result["success"]:
                responses = result.get("responses")
                if isinstance(responses, list) and responses:
                    inbound_guid = str(getattr(webhook, "message_id", "") or "").strip()
                    legacy_outbound_ok = None
                    for idx, item in enumerate(responses):
                        response_text = str(item.get("response_text") or "").strip()
                        task_intent = item.get("intent")
                        task_name = item.get("task")

                        if response_text:
                            # Don't chunk URLs - they break when split
                            if response_text.startswith("http://") or response_text.startswith("https://"):
                                await self.photon.send_message(to_number=webhook.from_number, content=response_text)
                            else:
                                await self._send_message_chunks(
                                    phone_number=webhook.from_number,
                                    response=response_text,
                                    user_id=user['id']
                                )

                            await self.db.store_message(
                                user_id=user['id'],
                                content=response_text,
                                message_type="bot",
                                metadata={
                                    "intent": task_intent,
                                    "task": task_name,
                                    "task_index": idx,
                                }
                            )

                            # Sync conversation to Zep knowledge graph (background task)
                            asyncio.create_task(self._sync_conversation_to_zep(
                                user_id=user['id'],
                                user_message=webhook.content,
                                bot_response=response_text,
                                user_name=user.get('name'),
                                intent=task_intent or task_name,
                            ))

                        resource_urls = item.get("resource_urls", []) or []
                        if resource_urls:
                            urls_only = [r.get("url", "") for r in resource_urls if r.get("url")]
                            if urls_only:
                                url_message = "\n".join(urls_only)
                                await self._send_message_chunks(
                                    phone_number=webhook.from_number,
                                    response=url_message,
                                    user_id=user['id']
                                )
                                await self.db.store_message(
                                    user_id=user['id'],
                                    content=url_message,
                                    message_type="bot",
                                    metadata={
                                        "intent": task_intent,
                                        "task": task_name,
                                        "message_part": "urls",
                                        "task_index": idx,
                                    }
                                )

                        outbound = item.get("outbound_messages", []) or []
                        if isinstance(outbound, list) and outbound:
                            redis_client = None
                            if inbound_guid:
                                try:
                                    from app.utils.redis_client import redis_client as redis_client
                                except Exception:
                                    redis_client = None

                            if inbound_guid and redis_client and legacy_outbound_ok is None:
                                try:
                                    legacy_outbound_ok = redis_client.check_idempotency(
                                        f"outbound_messages:v1:{inbound_guid}",
                                        ttl=60 * 60 * 24 * 30
                                    )
                                except Exception:
                                    legacy_outbound_ok = True
                            should_send_extras = True
                            if inbound_guid:
                                if legacy_outbound_ok is False:
                                    should_send_extras = False
                                elif redis_client:
                                    try:
                                        should_send_extras = redis_client.check_idempotency(
                                            f"outbound_messages:v2:{inbound_guid}:{idx}",
                                            ttl=60 * 60 * 24 * 30
                                        )
                                    except Exception:
                                        should_send_extras = True

                            if should_send_extras:
                                for i, text in enumerate(outbound[:3]):
                                    msg = str(text or "").strip()
                                    if not msg:
                                        continue
                                    # Don't chunk URLs - they break when split
                                    if msg.startswith("http://") or msg.startswith("https://"):
                                        await self.photon.send_message(to_number=webhook.from_number, content=msg)
                                    else:
                                        await self._send_message_chunks(
                                            phone_number=webhook.from_number,
                                            response=msg,
                                            user_id=user['id']
                                        )
                                    await self.db.store_message(
                                        user_id=user['id'],
                                        content=msg,
                                        message_type="bot",
                                        metadata={
                                            "intent": task_intent,
                                            "task": task_name,
                                            "message_part": f"outbound_{i}",
                                            "task_index": idx,
                                        }
                                    )

                    # Maybe send a lightweight reaction based on the last task
                    last = responses[-1] if responses else {}
                    asyncio.create_task(self._maybe_send_reaction(
                        phone_number=webhook.from_number,
                        chat_guid=getattr(webhook, "chat_guid", None),
                        message_guid=getattr(webhook, "message_id", None),
                        message_content=webhook.content,
                        context={
                            "intent": last.get("intent"),
                            "task": last.get("task"),
                            "onboarding_stage": (result.get("state", {}) or {}).get("user_profile", {}).get("onboarding_stage"),
                        },
                    ))

                    logger.info(
                        "[ORCHESTRATOR] Multi-task processing complete responses=%s",
                        len(responses),
                    )
                    return

                # Fallback: single response_text without responses list
                if result.get("response_text"):
                    response_text = result["response_text"]
                    # Don't chunk URLs - they break when split
                    if response_text.startswith("http://") or response_text.startswith("https://"):
                        await self.photon.send_message(to_number=webhook.from_number, content=response_text)
                    else:
                        await self._send_message_chunks(
                            phone_number=webhook.from_number,
                            response=response_text,
                            user_id=user['id']
                        )

                    await self.db.store_message(
                        user_id=user['id'],
                        content=response_text,
                        message_type="bot",
                        metadata={
                            "intent": result.get("intent"),
                            "task": result.get("intent"),
                        }
                    )

                    logger.info(f"[ORCHESTRATOR] Processing complete - Task: {result.get('intent')}")
                    return

                # No response generated - send a fallback
                logger.warning("[ORCHESTRATOR] Success but no response generated, sending fallback")
                fallback_response = "hey! what can i help you with?"
                await self._send_message_chunks(
                    phone_number=webhook.from_number,
                    response=fallback_response,
                    user_id=user['id']
                )
                await self.db.store_message(
                    user_id=user['id'],
                    content=fallback_response,
                    message_type="bot",
                    metadata={
                        "intent": result.get("intent"),
                        "fallback": True
                    }
                )
                return

            else:
                # Processing failed - send simple error message
                logger.error(f"[ORCHESTRATOR] Processing failed: {result.get('error', 'Unknown error')}")

                await self._send_message_chunks(
                    phone_number=webhook.from_number,
                    response="Sorry, I'm having technical difficulties right now. Please try again in a moment!",
                    user_id=user['id']
                )
                return

        except SystemExit as e:
            logger.critical(f"[CRASH DETECT] SystemExit in orchestrator - PID={pid}: {e}", exc_info=True)
            raise  # Re-raise to preserve exit behavior
        except KeyboardInterrupt as e:
            logger.critical(f"[CRASH DETECT] KeyboardInterrupt in orchestrator - PID={pid}: {e}", exc_info=True)
            raise  # Re-raise to preserve interrupt behavior
        except Exception as e:
            logger.error(f"[CRASH DETECT] Exception in orchestrator - PID={pid}: {e}", exc_info=True)
            logger.error(f"[ORCHESTRATOR] Critical error in orchestrator: {str(e)}", exc_info=True)
            # Send a fallback error message to user
            try:
                await self._send_message_chunks(
                    phone_number=webhook.from_number,
                    response="Sorry, something went wrong. Please try again later!",
                    user_id=user.get('id') if user else None
                )
            except Exception as send_error:
                logger.error(f"[ORCHESTRATOR] Failed to send error message: {send_error}", exc_info=True)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `MainOrchestrator._send_message_chunks()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Coordinates user, agent, and response sending. This function sits in the Per-message sequence (message orchestration). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, logging, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
-         Send a response message in chunks if it's too long.
- 
-         First checks for natural bubble separators (
- 
- ), then falls back to smart chunking.
- 
-         Args:
-             phone_number: Recipient's phone number or email (Apple ID)
-             response: The full response text to send
-             user_id: User's UUID for logging (optional)
-             max_length: Maximum characters per chunk (default 280)
-         

When used:
- Per-message sequence (message orchestration).

Method code:
```python
async def _send_message_chunks(
        self,
        phone_number: str,
        response: str,
        user_id: str | None,
        max_length: int = 280
    ) -> None:
        """
        Send a response message in chunks if it's too long.

        First checks for natural bubble separators (\n\n), then falls back to smart chunking.

        Args:
            phone_number: Recipient's phone number or email (Apple ID)
            response: The full response text to send
            user_id: User's UUID for logging (optional)
            max_length: Maximum characters per chunk (default 280)
        """
        try:
            # Check if response has natural bubble separators (\n\n)
            if "\n\n" in response:
                # Split on double newlines to get natural bubbles
                natural_bubbles = [bubble.strip() for bubble in response.split("\n\n") if bubble.strip()]

                if natural_bubbles:
                    logger.info(f"[ORCHESTRATOR] Sending {len(natural_bubbles)} natural bubbles to {phone_number}")
                    results = await self.photon.send_chunked_messages(
                        to_number=phone_number,
                        message_chunks=natural_bubbles,
                        delay_range=(0.3, 0.3),  # 0.3s delay between bubbles
                        show_typing=True
                    )
                    failed = [r for r in results if not r.get("success")]
                    if failed:
                        logger.error(f"[ORCHESTRATOR] Failed to send {len(failed)}/{len(natural_bubbles)} bubbles to {phone_number}")
                    else:
                        logger.info(f"[ORCHESTRATOR] Successfully sent all bubbles to {phone_number}")
                    return

            # Fallback: original chunking logic for single-bubble or too-long messages
            # If short enough, send as a single message
            if len(response) <= max_length:
                await self.photon.send_message(to_number=phone_number, content=response)
                return

            # For iMessage, keep responses to at most 2 bubbles (best-effort) to stay natural.
            chunks = chunk_message(response, max_length=max_length, max_chunks=2)
            if not chunks:
                logger.warning(f"[ORCHESTRATOR] No chunks generated for message to {phone_number}")
                return

            logger.info(f"[ORCHESTRATOR] Sending message in {len(chunks)} chunk(s) to {phone_number}")
            results = await self.photon.send_chunked_messages(
                to_number=phone_number,
                message_chunks=chunks,
                delay_range=(0.5, 1.0),
                show_typing=True
            )
            failed = [r for r in results if not r.get("success")]
            if failed:
                logger.error(f"[ORCHESTRATOR] Failed to send {len(failed)}/{len(chunks)} chunks to {phone_number}")
            else:
                logger.info(f"[ORCHESTRATOR] Successfully sent all chunks to {phone_number}")
        except Exception as e:
            logger.error(f"[ORCHESTRATOR] Error in _send_message_chunks: {e}", exc_info=True)
            try:
                await self.photon.send_message(
                    to_number=phone_number,
                    content="Sorry, I had trouble sending that message. Please try again!"
                )
            except Exception:
                pass
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `MainOrchestrator._maybe_send_reaction()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Coordinates user, agent, and response sending. This function sits in the Per-message sequence (message orchestration). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, logging, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Send an optional tapback reaction to the user's message (LLM-decided).

When used:
- Per-message sequence (message orchestration).

Method code:
```python
async def _maybe_send_reaction(
        self,
        phone_number: str,
        message_guid: str | None,
        message_content: str,
        chat_guid: str | None = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Send an optional tapback reaction to the user's message (LLM-decided).
        """
        if not message_guid:
            return

        try:
            from app.reactions.service import ReactionService

            await ReactionService(photon=self.photon, openai=self.openai).maybe_send_reaction(
                to_number=phone_number,
                message_guid=message_guid,
                message_content=message_content,
                chat_guid=chat_guid,
                context=context or {},
            )
        except Exception as e:
            logger.debug(f"[REACTION] Failed to send reaction: {e}")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `MainOrchestrator._sync_conversation_to_zep()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Coordinates user, agent, and response sending. This function sits in the Per-message sequence (message orchestration). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, asyncio, logging, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Sync a conversation exchange to Zep's knowledge graph.
- 
- Runs as a background task so it doesn't slow down response delivery.
- Enriches Zep with conversation context for better understanding.

When used:
- Per-message sequence (message orchestration).

Method code:
```python
async def _sync_conversation_to_zep(
        self,
        user_id: str,
        user_message: str,
        bot_response: str,
        user_name: Optional[str] = None,
        intent: Optional[str] = None,
    ) -> None:
        """
        Sync a conversation exchange to Zep's knowledge graph.

        Runs as a background task so it doesn't slow down response delivery.
        Enriches Zep with conversation context for better understanding.
        """
        try:
            from app.agents.tools.conversation_zep_sync import sync_conversation_to_zep

            result = await sync_conversation_to_zep(
                user_id=user_id,
                user_message=user_message,
                bot_response=bot_response,
                user_name=user_name,
                intent=intent,
            )

            if result.get("synced"):
                logger.debug(
                    "[ORCHESTRATOR] Synced conversation to Zep user=%s intent=%s",
                    user_id[:8] if user_id else "unknown",
                    intent or "unknown",
                )
        except Exception as e:
            # Don't let Zep sync failures affect the main flow
            logger.debug(f"[ORCHESTRATOR] Zep conversation sync failed: {e}")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

## Module: `app/agents/queue/async_processor.py`

Role: Async queue for long-running operations.

Module docstring:
- Async operation processor for long-running tasks.
- 
- Learned from poke-backend's message_processor.py pattern:
- - Fire-and-forget with UUID tracking
- - Background processing loop
- - Status polling for results
- 
- This allows long-running operations (group chat creation, multi-match)
- to complete without blocking webhooks or causing timeouts.
- 
- Example usage:
-     # Queue an operation
-     op_id = await processor.queue_operation(
-         operation_type="group_chat_creation",
-         user_id="user-123",
-         payload={"target_ids": ["user-456", "user-789"]},
-     )
- 
-     # Return immediately to user
-     return "Creating your group chat..."
- 
-     # Later, poll for result
-     result = processor.get_operation_status(op_id)
-     if result["status"] == "completed":
-         group_guid = result["result"]["group_chat_guid"]

Imported stack (selected):
- asyncio, dataclasses.dataclass, dataclasses.field, datetime.datetime, datetime.timedelta, enum.Enum, logging, typing.Any, typing.Awaitable, typing.Callable, typing.Dict, typing.Optional, uuid.uuid4

### Class: `OperationStatus`

Big picture:
- Background task queue for long-running operations.

Purpose:
- Status of a queued operation.

When used:
- Background/async operations path.

Class code:
```python
class OperationStatus(str, Enum):
    """Status of a queued operation."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"
```

### Class: `QueuedOperation`

Big picture:
- Background task queue for long-running operations.

Purpose:
- A queued operation awaiting processing.
- 
- Attributes:
-     operation_id: Unique identifier for tracking
-     operation_type: Type of operation (e.g., "group_chat_creation")
-     user_id: User who initiated the operation
-     payload: Operation-specific data
-     status: Current status
-     result: Operation result (when completed)
-     error: Error message (when failed)
-     created_at: When the operation was queued
-     completed_at: When the operation finished
-     expires_at: When the operation result expires (for cleanup)
-     on_complete: Optional callback invoked when operation finishes

When used:
- Background/async operations path.

Class code:
```python
class QueuedOperation:
    """A queued operation awaiting processing.

    Attributes:
        operation_id: Unique identifier for tracking
        operation_type: Type of operation (e.g., "group_chat_creation")
        user_id: User who initiated the operation
        payload: Operation-specific data
        status: Current status
        result: Operation result (when completed)
        error: Error message (when failed)
        created_at: When the operation was queued
        completed_at: When the operation finished
        expires_at: When the operation result expires (for cleanup)
        on_complete: Optional callback invoked when operation finishes
    """

    operation_id: str
    operation_type: str
    user_id: str
    payload: Dict[str, Any]
    status: OperationStatus = OperationStatus.PENDING
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    expires_at: datetime = field(
        default_factory=lambda: datetime.utcnow() + timedelta(hours=1)
    )
    on_complete: Optional["CompletionCallback"] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "operation_id": self.operation_id,
            "operation_type": self.operation_type,
            "user_id": self.user_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
        }
```

#### Method: `QueuedOperation.to_dict()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Background task queue for long-running operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: asyncio, dataclasses, datetime, enum, logging, typing, uuid.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Convert to dictionary for API responses.

When used:
- Background/async operations path.

Method code:
```python
def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "operation_id": self.operation_id,
            "operation_type": self.operation_type,
            "user_id": self.user_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
        }
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

### Class: `AsyncOperationProcessor`

Big picture:
- Background task queue for long-running operations.

Purpose:
- Fire-and-forget async operation queue for long-running tasks.
- 
- Provides:
- - Immediate operation_id return for tracking
- - Background processing loop
- - Status polling for results
- - Automatic cleanup of expired results
- 
- Example:
-     processor = AsyncOperationProcessor()
-     processor.register_handler("group_chat_creation", create_group_handler)
-     await processor.start_processing()
- 
-     # Queue operation
-     op_id = await processor.queue_operation("group_chat_creation", user_id, payload)
- 
-     # Poll for result
-     status = processor.get_operation_status(op_id)

When used:
- Background/async operations path.

Class code:
```python
class AsyncOperationProcessor:
    """Fire-and-forget async operation queue for long-running tasks.

    Provides:
    - Immediate operation_id return for tracking
    - Background processing loop
    - Status polling for results
    - Automatic cleanup of expired results

    Example:
        processor = AsyncOperationProcessor()
        processor.register_handler("group_chat_creation", create_group_handler)
        await processor.start_processing()

        # Queue operation
        op_id = await processor.queue_operation("group_chat_creation", user_id, payload)

        # Poll for result
        status = processor.get_operation_status(op_id)
    """

    def __init__(self, context: Optional[Any] = None):
        """Initialize the processor.

        Args:
            context: Shared context (db, photon, etc.) passed to handlers
        """
        self.context = context
        self._operations: Dict[str, QueuedOperation] = {}
        self._queue: asyncio.Queue[QueuedOperation] = asyncio.Queue()
        self._handlers: Dict[str, OperationHandler] = {}
        self._processing = False
        self._cleanup_interval = 300  # 5 minutes
        self._cleanup_task: Optional[asyncio.Task[None]] = None

    def register_handler(
        self,
        operation_type: str,
        handler: OperationHandler,
    ) -> None:
        """Register a handler for an operation type.

        Args:
            operation_type: Type of operation (e.g., "group_chat_creation")
            handler: Async function that processes the operation
        """
        self._handlers[operation_type] = handler
        logger.info(f"[ASYNC_QUEUE] Registered handler for {operation_type}")

    async def queue_operation(
        self,
        operation_type: str,
        user_id: str,
        payload: Dict[str, Any],
        on_complete: Optional[CompletionCallback] = None,
    ) -> str:
        """Queue an operation and return its ID immediately.

        This is the "fire" part of fire-and-forget. The caller gets
        an operation_id immediately and can poll for results later.

        Optionally provide an on_complete callback to be notified when
        the operation finishes (success or failure). This enables
        automatic user notifications without polling.

        Args:
            operation_type: Type of operation to queue
            user_id: User who initiated the operation
            payload: Operation-specific data
            on_complete: Optional async callback(operation, result, error)
                        invoked when operation completes

        Returns:
            Operation ID for tracking

        Raises:
            ValueError: If no handler is registered for the operation type
        """
        # Validate that a handler exists for this operation type
        if operation_type not in self._handlers:
            raise ValueError(
                f"No handler registered for operation type: {operation_type}. "
                f"Available types: {list(self._handlers.keys())}"
            )

        op_id = str(uuid4())
        operation = QueuedOperation(
            operation_id=op_id,
            operation_type=operation_type,
            user_id=user_id,
            payload=payload,
            on_complete=on_complete,
        )

        self._operations[op_id] = operation
        await self._queue.put(operation)

        logger.info(
            f"[ASYNC_QUEUE] Queued {operation_type} operation {op_id} for user {user_id}"
        )
        return op_id

    def get_operation_status(self, operation_id: str) -> Dict[str, Any]:
        """Get current status of an operation.

        Args:
            operation_id: Operation ID to check

        Returns:
            Status dictionary with operation details
        """
        operation = self._operations.get(operation_id)
        if not operation:
            return {"status": "not_found", "operation_id": operation_id}

        return operation.to_dict()

    def get_pending_operations_for_user(
        self,
        user_id: str,
        operation_type: Optional[str] = None,
    ) -> list[Dict[str, Any]]:
        """Get all pending operations for a user.

        Args:
            user_id: User ID to check
            operation_type: Optional filter by operation type

        Returns:
            List of pending operation dictionaries
        """
        results = []
        for op in self._operations.values():
            if op.user_id != user_id:
                continue
            if op.status not in (OperationStatus.PENDING, OperationStatus.PROCESSING):
                continue
            if operation_type and op.operation_type != operation_type:
                continue
            results.append(op.to_dict())

        return results

    async def start_processing(self) -> None:
        """Start the background processing loop.

        Call this on application startup. Runs until stop_processing()
        is called.
        """
        self._processing = True
        logger.info("[ASYNC_QUEUE] Starting operation processor...")

        # Start cleanup task and track it for proper shutdown
        self._cleanup_task = asyncio.create_task(self._cleanup_expired())

        while self._processing:
            try:
                operation = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
                await self._process_operation(operation)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[ASYNC_QUEUE] Error in processing loop: {e}")
                await asyncio.sleep(1)

    async def stop_processing(self) -> None:
        """Stop the processing loop gracefully."""
        self._processing = False
        logger.info("[ASYNC_QUEUE] Stopping operation processor...")

        # Cancel the cleanup task to prevent orphaned coroutine
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None

    async def _process_operation(self, operation: QueuedOperation) -> None:
        """Process a single operation.

        After processing (success or failure), invokes the on_complete
        callback if one was provided. This enables automatic user
        notifications without polling.

        Args:
            operation: Operation to process
        """
        handler = self._handlers.get(operation.operation_type)
        if not handler:
            logger.error(
                f"[ASYNC_QUEUE] No handler for operation type: {operation.operation_type}"
            )
            operation.status = OperationStatus.FAILED
            operation.error = f"Unknown operation type: {operation.operation_type}"
            operation.completed_at = datetime.utcnow()
            await self._invoke_callback(operation)
            return

        logger.info(
            f"[ASYNC_QUEUE] Processing {operation.operation_type} "
            f"operation {operation.operation_id}"
        )
        operation.status = OperationStatus.PROCESSING

        try:
            result = await handler(operation, self.context)
            operation.status = OperationStatus.COMPLETED
            operation.result = result
            operation.completed_at = datetime.utcnow()

            logger.info(
                f"[ASYNC_QUEUE] Completed {operation.operation_type} "
                f"operation {operation.operation_id}"
            )

        except Exception as e:
            logger.error(
                f"[ASYNC_QUEUE] Failed {operation.operation_type} "
                f"operation {operation.operation_id}: {e}"
            )
            operation.status = OperationStatus.FAILED
            operation.error = str(e)
            operation.completed_at = datetime.utcnow()

        # Invoke completion callback (for both success and failure)
        await self._invoke_callback(operation)

    async def _invoke_callback(self, operation: QueuedOperation) -> None:
        """Invoke the operation's completion callback if present.

        Catches and logs errors to prevent callback failures from
        affecting the queue processing.

        Args:
            operation: Completed operation
        """
        if not operation.on_complete:
            return

        try:
            await operation.on_complete(
                operation,
                operation.result,
                operation.error,
            )
            logger.info(
                f"[ASYNC_QUEUE] Callback completed for operation {operation.operation_id}"
            )
        except Exception as e:
            logger.error(
                f"[ASYNC_QUEUE] Callback failed for operation {operation.operation_id}: {e}"
            )

    async def _cleanup_expired(self) -> None:
        """Periodically clean up expired operations from memory."""
        while self._processing:
            await asyncio.sleep(self._cleanup_interval)

            now = datetime.utcnow()
            expired_ids = [
                op_id
                for op_id, op in self._operations.items()
                if op.expires_at < now
            ]

            for op_id in expired_ids:
                del self._operations[op_id]

            if expired_ids:
                logger.info(
                    f"[ASYNC_QUEUE] Cleaned up {len(expired_ids)} expired operations"
                )
```

#### Method: `AsyncOperationProcessor.__init__()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Background task queue for long-running operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: asyncio, dataclasses, datetime, enum, logging, typing, uuid.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Initialize the processor.
- 
- Args:
-     context: Shared context (db, photon, etc.) passed to handlers

When used:
- Background/async operations path.

Method code:
```python
def __init__(self, context: Optional[Any] = None):
        """Initialize the processor.

        Args:
            context: Shared context (db, photon, etc.) passed to handlers
        """
        self.context = context
        self._operations: Dict[str, QueuedOperation] = {}
        self._queue: asyncio.Queue[QueuedOperation] = asyncio.Queue()
        self._handlers: Dict[str, OperationHandler] = {}
        self._processing = False
        self._cleanup_interval = 300  # 5 minutes
        self._cleanup_task: Optional[asyncio.Task[None]] = None
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `AsyncOperationProcessor.register_handler()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Background task queue for long-running operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: asyncio, dataclasses, datetime, enum, logging, typing, uuid.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Register a handler for an operation type.
- 
- Args:
-     operation_type: Type of operation (e.g., "group_chat_creation")
-     handler: Async function that processes the operation

When used:
- Background/async operations path.

Method code:
```python
def register_handler(
        self,
        operation_type: str,
        handler: OperationHandler,
    ) -> None:
        """Register a handler for an operation type.

        Args:
            operation_type: Type of operation (e.g., "group_chat_creation")
            handler: Async function that processes the operation
        """
        self._handlers[operation_type] = handler
        logger.info(f"[ASYNC_QUEUE] Registered handler for {operation_type}")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `AsyncOperationProcessor.queue_operation()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Background task queue for long-running operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: asyncio, dataclasses, datetime, enum, logging, typing, uuid.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Queue an operation and return its ID immediately.
- 
- This is the "fire" part of fire-and-forget. The caller gets
- an operation_id immediately and can poll for results later.
- 
- Optionally provide an on_complete callback to be notified when
- the operation finishes (success or failure). This enables
- automatic user notifications without polling.
- 
- Args:
-     operation_type: Type of operation to queue
-     user_id: User who initiated the operation
-     payload: Operation-specific data
-     on_complete: Optional async callback(operation, result, error)
-                 invoked when operation completes
- 
- Returns:
-     Operation ID for tracking
- 
- Raises:
-     ValueError: If no handler is registered for the operation type

When used:
- Background/async operations path.

Method code:
```python
async def queue_operation(
        self,
        operation_type: str,
        user_id: str,
        payload: Dict[str, Any],
        on_complete: Optional[CompletionCallback] = None,
    ) -> str:
        """Queue an operation and return its ID immediately.

        This is the "fire" part of fire-and-forget. The caller gets
        an operation_id immediately and can poll for results later.

        Optionally provide an on_complete callback to be notified when
        the operation finishes (success or failure). This enables
        automatic user notifications without polling.

        Args:
            operation_type: Type of operation to queue
            user_id: User who initiated the operation
            payload: Operation-specific data
            on_complete: Optional async callback(operation, result, error)
                        invoked when operation completes

        Returns:
            Operation ID for tracking

        Raises:
            ValueError: If no handler is registered for the operation type
        """
        # Validate that a handler exists for this operation type
        if operation_type not in self._handlers:
            raise ValueError(
                f"No handler registered for operation type: {operation_type}. "
                f"Available types: {list(self._handlers.keys())}"
            )

        op_id = str(uuid4())
        operation = QueuedOperation(
            operation_id=op_id,
            operation_type=operation_type,
            user_id=user_id,
            payload=payload,
            on_complete=on_complete,
        )

        self._operations[op_id] = operation
        await self._queue.put(operation)

        logger.info(
            f"[ASYNC_QUEUE] Queued {operation_type} operation {op_id} for user {user_id}"
        )
        return op_id
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `AsyncOperationProcessor.get_operation_status()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Background task queue for long-running operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: asyncio, dataclasses, datetime, enum, logging, typing, uuid.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Get current status of an operation.
- 
- Args:
-     operation_id: Operation ID to check
- 
- Returns:
-     Status dictionary with operation details

When used:
- Background/async operations path.

Method code:
```python
def get_operation_status(self, operation_id: str) -> Dict[str, Any]:
        """Get current status of an operation.

        Args:
            operation_id: Operation ID to check

        Returns:
            Status dictionary with operation details
        """
        operation = self._operations.get(operation_id)
        if not operation:
            return {"status": "not_found", "operation_id": operation_id}

        return operation.to_dict()
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `AsyncOperationProcessor.get_pending_operations_for_user()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Background task queue for long-running operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: asyncio, dataclasses, datetime, enum, logging, typing, uuid.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Get all pending operations for a user.
- 
- Args:
-     user_id: User ID to check
-     operation_type: Optional filter by operation type
- 
- Returns:
-     List of pending operation dictionaries

When used:
- Background/async operations path.

Method code:
```python
def get_pending_operations_for_user(
        self,
        user_id: str,
        operation_type: Optional[str] = None,
    ) -> list[Dict[str, Any]]:
        """Get all pending operations for a user.

        Args:
            user_id: User ID to check
            operation_type: Optional filter by operation type

        Returns:
            List of pending operation dictionaries
        """
        results = []
        for op in self._operations.values():
            if op.user_id != user_id:
                continue
            if op.status not in (OperationStatus.PENDING, OperationStatus.PROCESSING):
                continue
            if operation_type and op.operation_type != operation_type:
                continue
            results.append(op.to_dict())

        return results
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `AsyncOperationProcessor.start_processing()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Background task queue for long-running operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: asyncio, dataclasses, datetime, enum, logging, typing, uuid.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Start the background processing loop.
- 
- Call this on application startup. Runs until stop_processing()
- is called.

When used:
- Background/async operations path.

Method code:
```python
async def start_processing(self) -> None:
        """Start the background processing loop.

        Call this on application startup. Runs until stop_processing()
        is called.
        """
        self._processing = True
        logger.info("[ASYNC_QUEUE] Starting operation processor...")

        # Start cleanup task and track it for proper shutdown
        self._cleanup_task = asyncio.create_task(self._cleanup_expired())

        while self._processing:
            try:
                operation = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
                await self._process_operation(operation)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"[ASYNC_QUEUE] Error in processing loop: {e}")
                await asyncio.sleep(1)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `AsyncOperationProcessor.stop_processing()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Background task queue for long-running operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: asyncio, dataclasses, datetime, enum, logging, typing, uuid.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Stop the processing loop gracefully.

When used:
- Background/async operations path.

Method code:
```python
async def stop_processing(self) -> None:
        """Stop the processing loop gracefully."""
        self._processing = False
        logger.info("[ASYNC_QUEUE] Stopping operation processor...")

        # Cancel the cleanup task to prevent orphaned coroutine
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            self._cleanup_task = None
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `AsyncOperationProcessor._process_operation()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Background task queue for long-running operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: asyncio, dataclasses, datetime, enum, logging, typing, uuid.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Process a single operation.
- 
- After processing (success or failure), invokes the on_complete
- callback if one was provided. This enables automatic user
- notifications without polling.
- 
- Args:
-     operation: Operation to process

When used:
- Background/async operations path.

Method code:
```python
async def _process_operation(self, operation: QueuedOperation) -> None:
        """Process a single operation.

        After processing (success or failure), invokes the on_complete
        callback if one was provided. This enables automatic user
        notifications without polling.

        Args:
            operation: Operation to process
        """
        handler = self._handlers.get(operation.operation_type)
        if not handler:
            logger.error(
                f"[ASYNC_QUEUE] No handler for operation type: {operation.operation_type}"
            )
            operation.status = OperationStatus.FAILED
            operation.error = f"Unknown operation type: {operation.operation_type}"
            operation.completed_at = datetime.utcnow()
            await self._invoke_callback(operation)
            return

        logger.info(
            f"[ASYNC_QUEUE] Processing {operation.operation_type} "
            f"operation {operation.operation_id}"
        )
        operation.status = OperationStatus.PROCESSING

        try:
            result = await handler(operation, self.context)
            operation.status = OperationStatus.COMPLETED
            operation.result = result
            operation.completed_at = datetime.utcnow()

            logger.info(
                f"[ASYNC_QUEUE] Completed {operation.operation_type} "
                f"operation {operation.operation_id}"
            )

        except Exception as e:
            logger.error(
                f"[ASYNC_QUEUE] Failed {operation.operation_type} "
                f"operation {operation.operation_id}: {e}"
            )
            operation.status = OperationStatus.FAILED
            operation.error = str(e)
            operation.completed_at = datetime.utcnow()

        # Invoke completion callback (for both success and failure)
        await self._invoke_callback(operation)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `AsyncOperationProcessor._invoke_callback()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Background task queue for long-running operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: asyncio, dataclasses, datetime, enum, logging, typing, uuid.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Invoke the operation's completion callback if present.
- 
- Catches and logs errors to prevent callback failures from
- affecting the queue processing.
- 
- Args:
-     operation: Completed operation

When used:
- Background/async operations path.

Method code:
```python
async def _invoke_callback(self, operation: QueuedOperation) -> None:
        """Invoke the operation's completion callback if present.

        Catches and logs errors to prevent callback failures from
        affecting the queue processing.

        Args:
            operation: Completed operation
        """
        if not operation.on_complete:
            return

        try:
            await operation.on_complete(
                operation,
                operation.result,
                operation.error,
            )
            logger.info(
                f"[ASYNC_QUEUE] Callback completed for operation {operation.operation_id}"
            )
        except Exception as e:
            logger.error(
                f"[ASYNC_QUEUE] Callback failed for operation {operation.operation_id}: {e}"
            )
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `AsyncOperationProcessor._cleanup_expired()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Background task queue for long-running operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: asyncio, dataclasses, datetime, enum, logging, typing, uuid.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Periodically clean up expired operations from memory.

When used:
- Background/async operations path.

Method code:
```python
async def _cleanup_expired(self) -> None:
        """Periodically clean up expired operations from memory."""
        while self._processing:
            await asyncio.sleep(self._cleanup_interval)

            now = datetime.utcnow()
            expired_ids = [
                op_id
                for op_id, op in self._operations.items()
                if op.expires_at < now
            ]

            for op_id in expired_ids:
                del self._operations[op_id]

            if expired_ids:
                logger.info(
                    f"[ASYNC_QUEUE] Cleaned up {len(expired_ids)} expired operations"
                )
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

## Module: `app/agents/queue/handlers.py`

Role: Handlers for async operations (group chat, multi-match).

Module docstring:
- Operation handlers for the async processor.
- 
- Registers handlers for long-running operations:
- - group_chat_creation: Create group chat (single or multi-person)
- - multi_match_invitations: Send invitations to multiple targets
- 
- Each handler receives a QueuedOperation and optional context,
- returning a result dict on success or raising on failure.

Imported stack (selected):
- app.agents.queue.async_processor.QueuedOperation, logging, typing.Any, typing.Dict, typing.Optional

### Function: `handle_group_chat_creation()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Implements long-running operation handlers. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, logging, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Handle group chat creation operation.
- 
- Payload should contain:
-     - connection_request_id: The connection request ID
-     - multi_match_status: Optional dict with multi-match info
- 
- Returns:
-     Dict with chat_guid and creation details

When used:
- Background/async operations path.

Function code:
```python
async def handle_group_chat_creation(
    operation: QueuedOperation,
    context: Optional[Any] = None,
) -> Dict[str, Any]:
    """Handle group chat creation operation.

    Payload should contain:
        - connection_request_id: The connection request ID
        - multi_match_status: Optional dict with multi-match info

    Returns:
        Dict with chat_guid and creation details
    """
    from app.groupchat.features.provisioning import GroupChatService
    from app.agents.execution.networking.utils.handshake_manager import HandshakeManager
    from app.database.client import DatabaseClient

    payload = operation.payload
    connection_request_id = payload.get("connection_request_id")
    multi_match_status = payload.get("multi_match_status")

    if not connection_request_id:
        raise ValueError("connection_request_id is required")

    db = DatabaseClient()
    handshake = HandshakeManager(db=db)

    # Get connection request data
    request_data = await db.get_connection_request(connection_request_id)
    if not request_data:
        raise ValueError(f"Connection request {connection_request_id} not found")

    # Check if this is a multi-match request
    is_multi_match = request_data.get("is_multi_match", False)
    signal_group_id = request_data.get("signal_group_id")

    # Use multi_match_status if provided, otherwise fetch fresh
    if multi_match_status is None and is_multi_match and signal_group_id:
        check_result = await db.check_multi_match_ready_v1(signal_group_id)
        multi_match_status = {
            "is_multi_match": True,
            "signal_group_id": signal_group_id,
            "ready_for_group": check_result.get("ready", False),
            "existing_chat_guid": check_result.get("chat_guid"),
            "accepted_request_ids": check_result.get("accepted_request_ids", []),
        }

    # Handle multi-match group creation
    if is_multi_match and multi_match_status:
        existing_chat = multi_match_status.get("existing_chat_guid")

        if existing_chat:
            # Late joiner - add to existing group
            result = await handshake.add_late_joiner_to_group(
                request_id=connection_request_id,
                existing_chat_guid=existing_chat,
            )
            added_name = result.get("added_user_name", "someone")
            return {
                "chat_guid": existing_chat,
                "added_user_name": added_name,
                "is_late_joiner": True,
                "operation_id": operation.operation_id,
                # User-friendly notification message
                "notification_message": f"🎉 {added_name} has joined the group chat!",
            }

        # Create new multi-person group
        accepted_ids = multi_match_status.get("accepted_request_ids", [])
        if connection_request_id not in accepted_ids:
            accepted_ids.append(connection_request_id)

        result = await handshake.create_multi_person_group(
            signal_group_id=signal_group_id,
            accepted_request_ids=accepted_ids,
        )

        participant_count = len(result.get("participants", []))
        return {
            "chat_guid": result.get("chat_guid"),
            "participant_count": participant_count,
            "is_multi_person": True,
            "operation_id": operation.operation_id,
            # User-friendly notification message
            "notification_message": f"🎉 Your group chat with {participant_count} people is ready! Check your messages.",
        }

    # Standard single-match flow
    initiator_user_id = request_data.get("initiator_user_id")
    target_user_id = request_data.get("target_user_id")
    matching_reasons = request_data.get("matching_reasons", [])

    # Look up both users
    initiator_user = await db.get_user_by_id(initiator_user_id)
    if not initiator_user or not initiator_user.get("phone_number"):
        raise ValueError(f"Could not find phone number for initiator user {initiator_user_id}")
    initiator_phone = initiator_user.get("phone_number")
    initiator_name = initiator_user.get("name", "friend")

    target_user = await db.get_user_by_id(target_user_id)
    if not target_user or not target_user.get("phone_number"):
        raise ValueError(f"Could not find phone number for target user {target_user_id}")
    target_phone = target_user.get("phone_number")
    target_name = target_user.get("name", "friend")

    # Get shared university if any
    university = None
    if initiator_user.get("university") and initiator_user.get("university") == target_user.get("university"):
        university = initiator_user.get("university")

    logger.info(f"[GROUP_CHAT_HANDLER] Creating chat: {initiator_name} <-> {target_name}")

    service = GroupChatService()
    result = await service.create_group(
        user_a_phone=initiator_phone,
        user_b_phone=target_phone,
        user_a_name=initiator_name,
        user_b_name=target_name,
        connection_request_id=connection_request_id,
        user_a_id=initiator_user_id,
        user_b_id=target_user_id,
        university=university,
        matching_reasons=matching_reasons,
    )

    chat_guid = result.get("chat_guid")

    # Mark the connection request as having group created
    await handshake.mark_group_created(connection_request_id, chat_guid)

    return {
        "chat_guid": chat_guid,
        "initiator_name": initiator_name,
        "target_name": target_name,
        "action_type": "group_chat_created",
        "operation_id": operation.operation_id,
        # User-friendly notification message
        "notification_message": f"🎉 Your group chat with {target_name} is ready! Check your messages.",
    }
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `handle_multi_match_invitations()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Implements long-running operation handlers. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, logging, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Handle sending invitations to multiple targets.
- 
- Used when initiator confirms multi-match - sends invitations
- to all matched targets in parallel.
- 
- Payload should contain:
-     - request_ids: List of connection request IDs to send invitations for
-     - initiator_name: Name of the initiator
- 
- Returns:
-     Dict with sent_count and results

When used:
- Background/async operations path.

Function code:
```python
async def handle_multi_match_invitations(
    operation: QueuedOperation,
    context: Optional[Any] = None,
) -> Dict[str, Any]:
    """Handle sending invitations to multiple targets.

    Used when initiator confirms multi-match - sends invitations
    to all matched targets in parallel.

    Payload should contain:
        - request_ids: List of connection request IDs to send invitations for
        - initiator_name: Name of the initiator

    Returns:
        Dict with sent_count and results
    """
    from app.agents.execution.networking.utils.message_generator import (
        generate_invitation_message,
    )
    from app.integrations.photon_client import PhotonClient
    from app.database.client import DatabaseClient
    import asyncio

    payload = operation.payload
    request_ids = payload.get("request_ids", [])
    initiator_name = payload.get("initiator_name", "someone")

    if not request_ids:
        raise ValueError("request_ids is required")

    db = DatabaseClient()
    photon = PhotonClient()
    results = []
    sent_count = 0

    async def send_invitation(request_id: str) -> Dict[str, Any]:
        """Send a single invitation."""
        try:
            # Get request data
            request_data = await db.get_connection_request(request_id)
            if not request_data:
                return {"request_id": request_id, "success": False, "error": "not_found"}

            target_user_id = request_data.get("target_user_id")
            target_user = await db.get_user_by_id(target_user_id)
            if not target_user or not target_user.get("phone_number"):
                return {"request_id": request_id, "success": False, "error": "no_phone"}

            target_phone = target_user.get("phone_number")
            target_name = target_user.get("name", "there")
            matching_reasons = request_data.get("matching_reasons", [])

            # Generate and send message
            message = await generate_invitation_message(
                initiator_name=initiator_name,
                target_name=target_name,
                matching_reasons=matching_reasons,
            )

            await photon.send_message(to_number=target_phone, content=message)

            # Store in target's conversation history
            await db.store_message(
                user_id=target_user_id,
                content=message,
                message_type="bot",
                metadata={
                    "intent": "networking_invitation",
                    "connection_request_id": request_id,
                    "initiator_name": initiator_name,
                },
            )

            return {
                "request_id": request_id,
                "success": True,
                "target_name": target_name,
            }

        except Exception as e:
            logger.error(f"[MULTI_INVITE_HANDLER] Failed to send invitation {request_id}: {e}")
            return {"request_id": request_id, "success": False, "error": str(e)}

    # Send all invitations in parallel
    tasks = [send_invitation(req_id) for req_id in request_ids]
    results = await asyncio.gather(*tasks)

    sent_count = sum(1 for r in results if r.get("success"))

    logger.info(f"[MULTI_INVITE_HANDLER] Sent {sent_count}/{len(request_ids)} invitations")

    # Build list of target names for notification
    target_names = [r.get("target_name") for r in results if r.get("success") and r.get("target_name")]
    names_str = ", ".join(target_names[:3])  # Show first 3 names
    if len(target_names) > 3:
        names_str += f" and {len(target_names) - 3} more"

    return {
        "sent_count": sent_count,
        "total_count": len(request_ids),
        "results": results,
        "operation_id": operation.operation_id,
        # User-friendly notification message
        "notification_message": (
            f"✅ Sent invitations to {names_str}. I'll let you know when they respond!"
            if target_names
            else f"✅ Sent {sent_count} invitation(s). I'll let you know when they respond!"
        ),
    }
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `register_all_handlers()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Implements long-running operation handlers. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, logging, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Register all operation handlers with the processor.
- 
- Call this on application startup after creating the processor.
- 
- Args:
-     processor: The AsyncOperationProcessor instance

When used:
- Background/async operations path.

Function code:
```python
def register_all_handlers(processor: "AsyncOperationProcessor") -> None:
    """Register all operation handlers with the processor.

    Call this on application startup after creating the processor.

    Args:
        processor: The AsyncOperationProcessor instance
    """
    from app.agents.queue.async_processor import AsyncOperationProcessor

    processor.register_handler("group_chat_creation", handle_group_chat_creation)
    processor.register_handler("multi_match_invitations", handle_multi_match_invitations)

    logger.info("[HANDLERS] Registered all operation handlers")
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

## Module: `app/agents/queue/callbacks.py`

Role: Callbacks to notify users when async operations complete.

Module docstring:
- Completion callbacks for async operations.
- 
- Provides callback functions that can be passed to queue_operation()
- to automatically notify users when long-running tasks complete.
- 
- Example usage:
-     from app.agents.queue import AsyncOperationProcessor
-     from app.agents.queue.callbacks import create_user_notification_callback
- 
-     # Create callback that will message the user when done
-     callback = create_user_notification_callback(
-         user_phone="+1234567890",
-         success_message="Your group chat is ready!",
-     )
- 
-     # Queue operation with callback
-     op_id = await processor.queue_operation(
-         operation_type="group_chat_creation",
-         user_id="user-123",
-         payload={...},
-         on_complete=callback,
-     )
- 
-     # User immediately gets: "Creating your group chat..."
-     # Later, when done: "Your group chat is ready!"

Imported stack (selected):
- app.agents.queue.async_processor.QueuedOperation, logging, typing.Any, typing.Awaitable, typing.Callable, typing.Dict, typing.Optional

### Function: `create_user_notification_callback()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: User notification callbacks for async operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, logging, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Create a callback that notifies the user via iMessage when operation completes.
- 
- The callback will send a message to the user's phone number when
- the operation finishes (success or failure).
- 
- Args:
-     user_phone: User's phone number to send notification to
-     success_message: Message to send on success (overrides result message)
-     failure_message: Message to send on failure (default: generic error)
-     use_result_message: If True, use notification_message from result if available
- 
- Returns:
-     Async callback function to pass to queue_operation()
- 
- Example:
-     callback = await create_user_notification_callback(
-         user_phone="+1234567890",
-         failure_message="Sorry, something went wrong. Please try again.",
-     )

When used:
- Background/async operations path.

Function code:
```python
async def create_user_notification_callback(
    user_phone: str,
    success_message: Optional[str] = None,
    failure_message: Optional[str] = None,
    use_result_message: bool = True,
) -> Callable[
    [QueuedOperation, Optional[Dict[str, Any]], Optional[str]], Awaitable[None]
]:
    """Create a callback that notifies the user via iMessage when operation completes.

    The callback will send a message to the user's phone number when
    the operation finishes (success or failure).

    Args:
        user_phone: User's phone number to send notification to
        success_message: Message to send on success (overrides result message)
        failure_message: Message to send on failure (default: generic error)
        use_result_message: If True, use notification_message from result if available

    Returns:
        Async callback function to pass to queue_operation()

    Example:
        callback = await create_user_notification_callback(
            user_phone="+1234567890",
            failure_message="Sorry, something went wrong. Please try again.",
        )
    """
    from app.integrations.photon_client import PhotonClient

    async def callback(
        operation: QueuedOperation,
        result: Optional[Dict[str, Any]],
        error: Optional[str],
    ) -> None:
        """Send notification to user when operation completes."""
        photon = PhotonClient()

        if error:
            # Operation failed
            message = failure_message or (
                "Sorry, something went wrong while processing your request. "
                "Please try again or let me know if you need help!"
            )
            logger.info(
                f"[CALLBACK] Sending failure notification to {user_phone} "
                f"for operation {operation.operation_id}"
            )
        else:
            # Operation succeeded
            # Priority: explicit success_message > result notification_message > generic
            if success_message:
                message = success_message
            elif use_result_message and result and result.get("notification_message"):
                message = result["notification_message"]
            else:
                message = "✅ Your request has been processed!"

            logger.info(
                f"[CALLBACK] Sending success notification to {user_phone} "
                f"for operation {operation.operation_id}"
            )

        try:
            await photon.send_message(to_number=user_phone, content=message)
            logger.info(f"[CALLBACK] Notification sent to {user_phone}")
        except Exception as e:
            logger.error(f"[CALLBACK] Failed to send notification to {user_phone}: {e}")

    return callback
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `make_notification_callback()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: User notification callbacks for async operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, logging, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Synchronous factory for creating user notification callbacks.
- 
- Use this when you need to create the callback synchronously (e.g., in
- non-async context). The callback itself is still async.
- 
- Args:
-     user_phone: User's phone number to send notification to
-     success_message: Message to send on success (overrides result message)
-     failure_message: Message to send on failure (default: generic error)
-     use_result_message: If True, use notification_message from result if available
- 
- Returns:
-     Async callback function to pass to queue_operation()
- 
- Example:
-     callback = make_notification_callback(user_phone="+1234567890")
-     op_id = await processor.queue_operation(..., on_complete=callback)

When used:
- Background/async operations path.

Function code:
```python
def make_notification_callback(
    user_phone: str,
    success_message: Optional[str] = None,
    failure_message: Optional[str] = None,
    use_result_message: bool = True,
) -> Callable[
    [QueuedOperation, Optional[Dict[str, Any]], Optional[str]], Awaitable[None]
]:
    """Synchronous factory for creating user notification callbacks.

    Use this when you need to create the callback synchronously (e.g., in
    non-async context). The callback itself is still async.

    Args:
        user_phone: User's phone number to send notification to
        success_message: Message to send on success (overrides result message)
        failure_message: Message to send on failure (default: generic error)
        use_result_message: If True, use notification_message from result if available

    Returns:
        Async callback function to pass to queue_operation()

    Example:
        callback = make_notification_callback(user_phone="+1234567890")
        op_id = await processor.queue_operation(..., on_complete=callback)
    """
    from app.integrations.photon_client import PhotonClient

    async def callback(
        operation: QueuedOperation,
        result: Optional[Dict[str, Any]],
        error: Optional[str],
    ) -> None:
        """Send notification to user when operation completes."""
        photon = PhotonClient()

        if error:
            message = failure_message or (
                "Sorry, something went wrong while processing your request. "
                "Please try again or let me know if you need help!"
            )
            logger.info(
                f"[CALLBACK] Sending failure notification to {user_phone} "
                f"for operation {operation.operation_id}"
            )
        else:
            if success_message:
                message = success_message
            elif use_result_message and result and result.get("notification_message"):
                message = result["notification_message"]
            else:
                message = "✅ Your request has been processed!"

            logger.info(
                f"[CALLBACK] Sending success notification to {user_phone} "
                f"for operation {operation.operation_id}"
            )

        try:
            await photon.send_message(to_number=user_phone, content=message)
            logger.info(f"[CALLBACK] Notification sent to {user_phone}")
        except Exception as e:
            logger.error(f"[CALLBACK] Failed to send notification to {user_phone}: {e}")

    return callback
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `make_db_notification_callback()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: User notification callbacks for async operations. This function sits in the Background/async operations path. path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, logging, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Create callback that looks up user phone from DB and sends notification.
- 
- Use this when you have user_id but not phone number. The callback
- will look up the user's phone number from the database.
- 
- Args:
-     user_id: User ID to look up phone number for
-     success_message: Message to send on success (overrides result message)
-     failure_message: Message to send on failure (default: generic error)
-     use_result_message: If True, use notification_message from result if available
- 
- Returns:
-     Async callback function to pass to queue_operation()
- 
- Example:
-     callback = make_db_notification_callback(user_id="user-123")
-     op_id = await processor.queue_operation(..., on_complete=callback)

When used:
- Background/async operations path.

Function code:
```python
def make_db_notification_callback(
    user_id: str,
    success_message: Optional[str] = None,
    failure_message: Optional[str] = None,
    use_result_message: bool = True,
) -> Callable[
    [QueuedOperation, Optional[Dict[str, Any]], Optional[str]], Awaitable[None]
]:
    """Create callback that looks up user phone from DB and sends notification.

    Use this when you have user_id but not phone number. The callback
    will look up the user's phone number from the database.

    Args:
        user_id: User ID to look up phone number for
        success_message: Message to send on success (overrides result message)
        failure_message: Message to send on failure (default: generic error)
        use_result_message: If True, use notification_message from result if available

    Returns:
        Async callback function to pass to queue_operation()

    Example:
        callback = make_db_notification_callback(user_id="user-123")
        op_id = await processor.queue_operation(..., on_complete=callback)
    """
    from app.database.client import DatabaseClient
    from app.integrations.photon_client import PhotonClient

    async def callback(
        operation: QueuedOperation,
        result: Optional[Dict[str, Any]],
        error: Optional[str],
    ) -> None:
        """Look up user phone and send notification."""
        db = DatabaseClient()
        photon = PhotonClient()

        # Look up user phone
        user = await db.get_user_by_id(user_id)
        if not user or not user.get("phone_number"):
            logger.warning(
                f"[CALLBACK] Cannot send notification: no phone for user {user_id}"
            )
            return

        user_phone = user["phone_number"]

        if error:
            message = failure_message or (
                "Sorry, something went wrong while processing your request. "
                "Please try again or let me know if you need help!"
            )
            logger.info(
                f"[CALLBACK] Sending failure notification to {user_phone} "
                f"for operation {operation.operation_id}"
            )
        else:
            if success_message:
                message = success_message
            elif use_result_message and result and result.get("notification_message"):
                message = result["notification_message"]
            else:
                message = "✅ Your request has been processed!"

            logger.info(
                f"[CALLBACK] Sending success notification to {user_phone} "
                f"for operation {operation.operation_id}"
            )

        try:
            await photon.send_message(to_number=user_phone, content=message)
            logger.info(f"[CALLBACK] Notification sent to {user_phone}")
        except Exception as e:
            logger.error(f"[CALLBACK] Failed to send notification to {user_phone}: {e}")

    return callback
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

## Module: `app/agents/queue/__init__.py`

Role: Exports async queue symbols.

Module docstring:
- Async operation queue for long-running tasks.
- 
- Provides fire-and-forget pattern with polling for results,
- learned from poke-backend's message processor pattern.
- 
- Features:
- - Immediate acknowledgment to user
- - Background processing without blocking webhooks
- - Optional completion callbacks for automatic user notifications
- - Status polling for results
- 
- Example with automatic notification:
-     from app.agents.queue import (
-         AsyncOperationProcessor,
-         make_notification_callback,
-     )
- 
-     # Create callback that messages user when done
-     callback = make_notification_callback(user_phone="+1234567890")
- 
-     # Queue operation - returns immediately
-     op_id = await processor.queue_operation(
-         operation_type="group_chat_creation",
-         user_id="user-123",
-         payload={...},
-         on_complete=callback,  # User gets messaged when complete!
-     )

Imported stack (selected):
- async_processor.AsyncOperationProcessor, async_processor.CompletionCallback, async_processor.OperationStatus, async_processor.QueuedOperation, callbacks.make_db_notification_callback, callbacks.make_notification_callback, handlers.register_all_handlers

## Module: `app/utils/redis_client.py`

Role: Redis client for idempotency, caching, rate limiting, chat GUID cache.

Module docstring:
- Redis Client with Connection Pooling for High Concurrency
- 
- This module provides a singleton Redis client with connection pooling
- optimized for handling 200+ concurrent payment operations.
- 
- Features:
- - Connection pooling with configurable max connections
- - Idempotency checking for webhooks
- - Subscription status caching
- - Rate limiting
- - Circuit breaker pattern for resilience
- 
- Location: app/utils/redis_client.py

Imported stack (selected):
- app.config.settings, datetime.timedelta, functools.wraps, json, logging, redis, redis.connection.ConnectionPool, typing.Any, typing.Optional

### Class: `RedisClient`

Big picture:
- Shared Redis utilities for idempotency, caching, and rate limiting.

Purpose:
- Singleton Redis client with connection pooling.
- 
- This client is designed to handle high concurrency (200+ simultaneous operations)
- without exhausting connections or blocking the event loop.

When used:
- Support path (idempotency/caching/rate limits).

Class code:
```python
class RedisClient:
    """
    Singleton Redis client with connection pooling.

    This client is designed to handle high concurrency (200+ simultaneous operations)
    without exhausting connections or blocking the event loop.
    """

    _instance: Optional['RedisClient'] = None
    _pool: Optional[ConnectionPool] = None
    _client: Optional[redis.Redis] = None

    def __new__(cls):
        """Ensure singleton instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize Redis connection pool."""
        try:
            # Create connection pool with configured max connections
            self._pool = ConnectionPool.from_url(
                settings.redis_url,
                max_connections=settings.redis_max_connections,
                decode_responses=True,  # Automatically decode bytes to strings
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30  # Health check every 30s
            )

            self._client = redis.Redis(connection_pool=self._pool)

            # Test connection
            self._client.ping()
            logger.info(
                f"[REDIS] Connection pool initialized: "
                f"max_connections={settings.redis_max_connections}"
            )

        except Exception as e:
            logger.error(f"[REDIS] Failed to initialize connection pool: {e}", exc_info=True)
            raise

    @property
    def client(self) -> redis.Redis:
        """Get Redis client instance."""
        if self._client is None:
            raise RuntimeError("Redis client not initialized")
        return self._client

    def close(self):
        """Close Redis connection pool."""
        if self._pool:
            self._pool.disconnect()
            logger.info("[REDIS] Connection pool closed")

    # ==================== IDEMPOTENCY ====================

    def check_idempotency(self, key: str, ttl: Optional[int] = None) -> bool:
        """
        Check if an operation has already been processed (idempotency).

        Args:
            key: Unique identifier for the operation (e.g., "stripe_event:evt_123")
            ttl: Time-to-live in seconds (default: from settings)

        Returns:
            True if operation is new (not seen before)
            False if operation is duplicate (already processed)

        Example:
            >>> redis_client = RedisClient()
            >>> if not redis_client.check_idempotency("stripe_event:evt_123"):
            >>>     return {"status": "duplicate"}
            >>> # Process the event...
        """
        try:
            ttl = ttl or settings.redis_idempotency_ttl

            # SET NX (set if not exists) returns True if key was set, None if key already exists
            # In redis-py 4.x, None means the key exists (not False!)
            is_new = self.client.set(key, "processed", nx=True, ex=ttl)

            # DEBUG: Log what Redis actually returned
            logger.critical(f"[REDIS DEBUG] Raw return: {repr(is_new)}, type={type(is_new).__name__}, key={key[:60]}")

            # Convert to boolean: True = new operation, None/False = duplicate
            is_new_bool = (is_new is True)

            if is_new_bool:
                logger.info(f"[REDIS IDEMPOTENCY] New operation: {key}")
            else:
                logger.warning(f"[REDIS IDEMPOTENCY] Duplicate detected: {key}")

            return is_new_bool

        except Exception as e:
            logger.error(f"[REDIS IDEMPOTENCY] Error checking {key}: {e}", exc_info=True)
            # On Redis failure, allow operation to proceed (fail open)
            return True

    def mark_processed(self, key: str, ttl: Optional[int] = None) -> bool:
        """
        Mark an operation as processed.

        Args:
            key: Unique identifier
            ttl: Time-to-live in seconds

        Returns:
            True if successfully marked
        """
        try:
            ttl = ttl or settings.redis_idempotency_ttl
            self.client.setex(key, ttl, "processed")
            return True
        except Exception as e:
            logger.error(f"[REDIS IDEMPOTENCY] Error marking processed {key}: {e}")
            return False

    # ==================== CACHING ====================

    def get_cached(self, key: str) -> Optional[Any]:
        """
        Get cached value.

        Args:
            key: Cache key

        Returns:
            Cached value (deserialized from JSON) or None if not found
        """
        try:
            value = self.client.get(key)
            if value:
                logger.debug(f"[REDIS CACHE] Hit: {key}")
                return json.loads(value)
            logger.debug(f"[REDIS CACHE] Miss: {key}")
            return None
        except Exception as e:
            logger.error(f"[REDIS CACHE] Error getting {key}: {e}")
            return None

    def set_cached(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set cached value.

        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time-to-live in seconds (default: from settings)

        Returns:
            True if successfully cached
        """
        try:
            ttl = ttl or settings.redis_cache_ttl
            serialized = json.dumps(value)
            self.client.setex(key, ttl, serialized)
            logger.debug(f"[REDIS CACHE] Set: {key} (TTL={ttl}s)")
            return True
        except Exception as e:
            logger.error(f"[REDIS CACHE] Error setting {key}: {e}")
            return False

    def invalidate_cache(self, key: str) -> bool:
        """
        Invalidate cached value.

        Args:
            key: Cache key to delete

        Returns:
            True if key was deleted
        """
        try:
            deleted = self.client.delete(key)
            if deleted:
                logger.debug(f"[REDIS CACHE] Invalidated: {key}")
            return bool(deleted)
        except Exception as e:
            logger.error(f"[REDIS CACHE] Error invalidating {key}: {e}")
            return False

    # ==================== RATE LIMITING ====================

    def check_rate_limit(
        self,
        key: str,
        max_requests: int,
        window_seconds: Optional[int] = None
    ) -> tuple[bool, int]:
        """
        Check rate limit using sliding window.

        Args:
            key: Rate limit key (e.g., "stripe_api:user_123")
            max_requests: Maximum requests allowed in window
            window_seconds: Time window in seconds (default: from settings)

        Returns:
            Tuple of (allowed: bool, current_count: int)

        Example:
            >>> allowed, count = redis_client.check_rate_limit("stripe_api:create_payment", 100, 60)
            >>> if not allowed:
            >>>     raise RateLimitExceeded(f"Rate limit exceeded: {count}/{max_requests}")
        """
        try:
            window_seconds = window_seconds or settings.redis_rate_limit_window

            # Increment counter
            pipe = self.client.pipeline()
            pipe.incr(key)
            pipe.expire(key, window_seconds)
            result = pipe.execute()

            current_count = result[0]
            allowed = current_count <= max_requests

            if not allowed:
                logger.warning(
                    f"[REDIS RATE LIMIT] Exceeded: {key} "
                    f"({current_count}/{max_requests} in {window_seconds}s)"
                )

            return allowed, current_count

        except Exception as e:
            logger.error(f"[REDIS RATE LIMIT] Error checking {key}: {e}", exc_info=True)
            # On Redis failure, allow request (fail open)
            return True, 0

    # ==================== SUBSCRIPTION CACHING ====================

    def get_subscription_status(self, user_id: str) -> Optional[dict]:
        """
        Get cached subscription status for a user.

        Args:
            user_id: User UUID

        Returns:
            Dict with subscription info or None if not cached
        """
        key = f"subscription:{user_id}"
        return self.get_cached(key)

    def cache_subscription_status(
        self,
        user_id: str,
        subscription_data: dict,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Cache subscription status for a user.

        Args:
            user_id: User UUID
            subscription_data: Subscription info to cache
            ttl: Cache TTL in seconds

        Returns:
            True if successfully cached
        """
        key = f"subscription:{user_id}"
        return self.set_cached(key, subscription_data, ttl)

    def invalidate_subscription_cache(self, user_id: str) -> bool:
        """
        Invalidate subscription cache when status changes.

        Args:
            user_id: User UUID

        Returns:
            True if cache was invalidated
        """
        key = f"subscription:{user_id}"
        return self.invalidate_cache(key)

    # ==================== CHAT GUID CACHING ====================

    def cache_chat_guid(self, phone_number: str, chat_guid: str, ttl: int = 86400) -> bool:
        """
        Cache the real chat GUID from Apple for a phone number or email.

        This stores the actual chat GUID that Apple provides, which includes
        the correct service prefix (iMessage;-; or SMS;-;). This ensures
        typing indicators work correctly for phone numbers.

        Args:
            phone_number: Phone number or email address
            chat_guid: The actual chat GUID from Apple's iMessage system
            ttl: Time-to-live in seconds (default: 24 hours)

        Returns:
            True if successfully cached
        """
        try:
            key = f"chat_guid:{phone_number}"
            self.client.setex(key, ttl, chat_guid)
            logger.debug(f"[REDIS CHAT GUID] Cached: {phone_number} → {chat_guid[:20]}...")
            return True
        except Exception as e:
            logger.error(f"[REDIS CHAT GUID] Error caching {phone_number}: {e}")
            return False

    def get_cached_chat_guid(self, phone_number: str) -> Optional[str]:
        """
        Retrieve cached chat GUID for a phone number or email.

        Args:
            phone_number: Phone number or email address

        Returns:
            The cached chat GUID string, or None if not found
        """
        try:
            key = f"chat_guid:{phone_number}"
            value = self.client.get(key)
            if value:
                logger.debug(f"[REDIS CHAT GUID] Hit: {phone_number} → {value[:20]}...")
                return value
            logger.debug(f"[REDIS CHAT GUID] Miss: {phone_number}")
            return None
        except Exception as e:
            logger.error(f"[REDIS CHAT GUID] Error getting {phone_number}: {e}")
            return None

    # ==================== CIRCUIT BREAKER ====================

    def get_circuit_breaker_status(self, service: str) -> Optional[str]:
        """
        Get circuit breaker status for a service.

        Args:
            service: Service name (e.g., "stripe_api")

        Returns:
            Status: "open", "closed", "half_open", or None
        """
        key = f"circuit_breaker:{service}"
        return self.client.get(key)

    def open_circuit_breaker(self, service: str, ttl: int = 60) -> bool:
        """
        Open circuit breaker (stop calling failing service).

        Args:
            service: Service name
            ttl: How long to keep circuit open (seconds)

        Returns:
            True if circuit was opened
        """
        try:
            key = f"circuit_breaker:{service}"
            self.client.setex(key, ttl, "open")
            logger.warning(f"[REDIS CIRCUIT BREAKER] Opened: {service} (TTL={ttl}s)")
            return True
        except Exception as e:
            logger.error(f"[REDIS CIRCUIT BREAKER] Error opening {service}: {e}")
            return False

    def close_circuit_breaker(self, service: str) -> bool:
        """
        Close circuit breaker (service recovered).

        Args:
            service: Service name

        Returns:
            True if circuit was closed
        """
        try:
            key = f"circuit_breaker:{service}"
            deleted = self.client.delete(key)
            if deleted:
                logger.info(f"[REDIS CIRCUIT BREAKER] Closed: {service}")
            return bool(deleted)
        except Exception as e:
            logger.error(f"[REDIS CIRCUIT BREAKER] Error closing {service}: {e}")
            return False
```

#### Method: `RedisClient.__new__()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Ensure singleton instance.

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def __new__(cls):
        """Ensure singleton instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient._initialize()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Initialize Redis connection pool.

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def _initialize(self):
        """Initialize Redis connection pool."""
        try:
            # Create connection pool with configured max connections
            self._pool = ConnectionPool.from_url(
                settings.redis_url,
                max_connections=settings.redis_max_connections,
                decode_responses=True,  # Automatically decode bytes to strings
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30  # Health check every 30s
            )

            self._client = redis.Redis(connection_pool=self._pool)

            # Test connection
            self._client.ping()
            logger.info(
                f"[REDIS] Connection pool initialized: "
                f"max_connections={settings.redis_max_connections}"
            )

        except Exception as e:
            logger.error(f"[REDIS] Failed to initialize connection pool: {e}", exc_info=True)
            raise
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.client()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Get Redis client instance.

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def client(self) -> redis.Redis:
        """Get Redis client instance."""
        if self._client is None:
            raise RuntimeError("Redis client not initialized")
        return self._client
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.close()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Close Redis connection pool.

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def close(self):
        """Close Redis connection pool."""
        if self._pool:
            self._pool.disconnect()
            logger.info("[REDIS] Connection pool closed")
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.check_idempotency()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Check if an operation has already been processed (idempotency).
- 
- Args:
-     key: Unique identifier for the operation (e.g., "stripe_event:evt_123")
-     ttl: Time-to-live in seconds (default: from settings)
- 
- Returns:
-     True if operation is new (not seen before)
-     False if operation is duplicate (already processed)
- 
- Example:
-     >>> redis_client = RedisClient()
-     >>> if not redis_client.check_idempotency("stripe_event:evt_123"):
-     >>>     return {"status": "duplicate"}
-     >>> # Process the event...

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def check_idempotency(self, key: str, ttl: Optional[int] = None) -> bool:
        """
        Check if an operation has already been processed (idempotency).

        Args:
            key: Unique identifier for the operation (e.g., "stripe_event:evt_123")
            ttl: Time-to-live in seconds (default: from settings)

        Returns:
            True if operation is new (not seen before)
            False if operation is duplicate (already processed)

        Example:
            >>> redis_client = RedisClient()
            >>> if not redis_client.check_idempotency("stripe_event:evt_123"):
            >>>     return {"status": "duplicate"}
            >>> # Process the event...
        """
        try:
            ttl = ttl or settings.redis_idempotency_ttl

            # SET NX (set if not exists) returns True if key was set, None if key already exists
            # In redis-py 4.x, None means the key exists (not False!)
            is_new = self.client.set(key, "processed", nx=True, ex=ttl)

            # DEBUG: Log what Redis actually returned
            logger.critical(f"[REDIS DEBUG] Raw return: {repr(is_new)}, type={type(is_new).__name__}, key={key[:60]}")

            # Convert to boolean: True = new operation, None/False = duplicate
            is_new_bool = (is_new is True)

            if is_new_bool:
                logger.info(f"[REDIS IDEMPOTENCY] New operation: {key}")
            else:
                logger.warning(f"[REDIS IDEMPOTENCY] Duplicate detected: {key}")

            return is_new_bool

        except Exception as e:
            logger.error(f"[REDIS IDEMPOTENCY] Error checking {key}: {e}", exc_info=True)
            # On Redis failure, allow operation to proceed (fail open)
            return True
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.mark_processed()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Mark an operation as processed.
- 
- Args:
-     key: Unique identifier
-     ttl: Time-to-live in seconds
- 
- Returns:
-     True if successfully marked

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def mark_processed(self, key: str, ttl: Optional[int] = None) -> bool:
        """
        Mark an operation as processed.

        Args:
            key: Unique identifier
            ttl: Time-to-live in seconds

        Returns:
            True if successfully marked
        """
        try:
            ttl = ttl or settings.redis_idempotency_ttl
            self.client.setex(key, ttl, "processed")
            return True
        except Exception as e:
            logger.error(f"[REDIS IDEMPOTENCY] Error marking processed {key}: {e}")
            return False
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.get_cached()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Get cached value.
- 
- Args:
-     key: Cache key
- 
- Returns:
-     Cached value (deserialized from JSON) or None if not found

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def get_cached(self, key: str) -> Optional[Any]:
        """
        Get cached value.

        Args:
            key: Cache key

        Returns:
            Cached value (deserialized from JSON) or None if not found
        """
        try:
            value = self.client.get(key)
            if value:
                logger.debug(f"[REDIS CACHE] Hit: {key}")
                return json.loads(value)
            logger.debug(f"[REDIS CACHE] Miss: {key}")
            return None
        except Exception as e:
            logger.error(f"[REDIS CACHE] Error getting {key}: {e}")
            return None
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.set_cached()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Set cached value.
- 
- Args:
-     key: Cache key
-     value: Value to cache (will be JSON serialized)
-     ttl: Time-to-live in seconds (default: from settings)
- 
- Returns:
-     True if successfully cached

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def set_cached(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set cached value.

        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time-to-live in seconds (default: from settings)

        Returns:
            True if successfully cached
        """
        try:
            ttl = ttl or settings.redis_cache_ttl
            serialized = json.dumps(value)
            self.client.setex(key, ttl, serialized)
            logger.debug(f"[REDIS CACHE] Set: {key} (TTL={ttl}s)")
            return True
        except Exception as e:
            logger.error(f"[REDIS CACHE] Error setting {key}: {e}")
            return False
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.invalidate_cache()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Invalidate cached value.
- 
- Args:
-     key: Cache key to delete
- 
- Returns:
-     True if key was deleted

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def invalidate_cache(self, key: str) -> bool:
        """
        Invalidate cached value.

        Args:
            key: Cache key to delete

        Returns:
            True if key was deleted
        """
        try:
            deleted = self.client.delete(key)
            if deleted:
                logger.debug(f"[REDIS CACHE] Invalidated: {key}")
            return bool(deleted)
        except Exception as e:
            logger.error(f"[REDIS CACHE] Error invalidating {key}: {e}")
            return False
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.check_rate_limit()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Check rate limit using sliding window.
- 
- Args:
-     key: Rate limit key (e.g., "stripe_api:user_123")
-     max_requests: Maximum requests allowed in window
-     window_seconds: Time window in seconds (default: from settings)
- 
- Returns:
-     Tuple of (allowed: bool, current_count: int)
- 
- Example:
-     >>> allowed, count = redis_client.check_rate_limit("stripe_api:create_payment", 100, 60)
-     >>> if not allowed:
-     >>>     raise RateLimitExceeded(f"Rate limit exceeded: {count}/{max_requests}")

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def check_rate_limit(
        self,
        key: str,
        max_requests: int,
        window_seconds: Optional[int] = None
    ) -> tuple[bool, int]:
        """
        Check rate limit using sliding window.

        Args:
            key: Rate limit key (e.g., "stripe_api:user_123")
            max_requests: Maximum requests allowed in window
            window_seconds: Time window in seconds (default: from settings)

        Returns:
            Tuple of (allowed: bool, current_count: int)

        Example:
            >>> allowed, count = redis_client.check_rate_limit("stripe_api:create_payment", 100, 60)
            >>> if not allowed:
            >>>     raise RateLimitExceeded(f"Rate limit exceeded: {count}/{max_requests}")
        """
        try:
            window_seconds = window_seconds or settings.redis_rate_limit_window

            # Increment counter
            pipe = self.client.pipeline()
            pipe.incr(key)
            pipe.expire(key, window_seconds)
            result = pipe.execute()

            current_count = result[0]
            allowed = current_count <= max_requests

            if not allowed:
                logger.warning(
                    f"[REDIS RATE LIMIT] Exceeded: {key} "
                    f"({current_count}/{max_requests} in {window_seconds}s)"
                )

            return allowed, current_count

        except Exception as e:
            logger.error(f"[REDIS RATE LIMIT] Error checking {key}: {e}", exc_info=True)
            # On Redis failure, allow request (fail open)
            return True, 0
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.get_subscription_status()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Get cached subscription status for a user.
- 
- Args:
-     user_id: User UUID
- 
- Returns:
-     Dict with subscription info or None if not cached

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def get_subscription_status(self, user_id: str) -> Optional[dict]:
        """
        Get cached subscription status for a user.

        Args:
            user_id: User UUID

        Returns:
            Dict with subscription info or None if not cached
        """
        key = f"subscription:{user_id}"
        return self.get_cached(key)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.cache_subscription_status()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Cache subscription status for a user.
- 
- Args:
-     user_id: User UUID
-     subscription_data: Subscription info to cache
-     ttl: Cache TTL in seconds
- 
- Returns:
-     True if successfully cached

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def cache_subscription_status(
        self,
        user_id: str,
        subscription_data: dict,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Cache subscription status for a user.

        Args:
            user_id: User UUID
            subscription_data: Subscription info to cache
            ttl: Cache TTL in seconds

        Returns:
            True if successfully cached
        """
        key = f"subscription:{user_id}"
        return self.set_cached(key, subscription_data, ttl)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.invalidate_subscription_cache()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Invalidate subscription cache when status changes.
- 
- Args:
-     user_id: User UUID
- 
- Returns:
-     True if cache was invalidated

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def invalidate_subscription_cache(self, user_id: str) -> bool:
        """
        Invalidate subscription cache when status changes.

        Args:
            user_id: User UUID

        Returns:
            True if cache was invalidated
        """
        key = f"subscription:{user_id}"
        return self.invalidate_cache(key)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.cache_chat_guid()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Cache the real chat GUID from Apple for a phone number or email.
- 
- This stores the actual chat GUID that Apple provides, which includes
- the correct service prefix (iMessage;-; or SMS;-;). This ensures
- typing indicators work correctly for phone numbers.
- 
- Args:
-     phone_number: Phone number or email address
-     chat_guid: The actual chat GUID from Apple's iMessage system
-     ttl: Time-to-live in seconds (default: 24 hours)
- 
- Returns:
-     True if successfully cached

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def cache_chat_guid(self, phone_number: str, chat_guid: str, ttl: int = 86400) -> bool:
        """
        Cache the real chat GUID from Apple for a phone number or email.

        This stores the actual chat GUID that Apple provides, which includes
        the correct service prefix (iMessage;-; or SMS;-;). This ensures
        typing indicators work correctly for phone numbers.

        Args:
            phone_number: Phone number or email address
            chat_guid: The actual chat GUID from Apple's iMessage system
            ttl: Time-to-live in seconds (default: 24 hours)

        Returns:
            True if successfully cached
        """
        try:
            key = f"chat_guid:{phone_number}"
            self.client.setex(key, ttl, chat_guid)
            logger.debug(f"[REDIS CHAT GUID] Cached: {phone_number} → {chat_guid[:20]}...")
            return True
        except Exception as e:
            logger.error(f"[REDIS CHAT GUID] Error caching {phone_number}: {e}")
            return False
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.get_cached_chat_guid()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Retrieve cached chat GUID for a phone number or email.
- 
- Args:
-     phone_number: Phone number or email address
- 
- Returns:
-     The cached chat GUID string, or None if not found

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def get_cached_chat_guid(self, phone_number: str) -> Optional[str]:
        """
        Retrieve cached chat GUID for a phone number or email.

        Args:
            phone_number: Phone number or email address

        Returns:
            The cached chat GUID string, or None if not found
        """
        try:
            key = f"chat_guid:{phone_number}"
            value = self.client.get(key)
            if value:
                logger.debug(f"[REDIS CHAT GUID] Hit: {phone_number} → {value[:20]}...")
                return value
            logger.debug(f"[REDIS CHAT GUID] Miss: {phone_number}")
            return None
        except Exception as e:
            logger.error(f"[REDIS CHAT GUID] Error getting {phone_number}: {e}")
            return None
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.get_circuit_breaker_status()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Get circuit breaker status for a service.
- 
- Args:
-     service: Service name (e.g., "stripe_api")
- 
- Returns:
-     Status: "open", "closed", "half_open", or None

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def get_circuit_breaker_status(self, service: str) -> Optional[str]:
        """
        Get circuit breaker status for a service.

        Args:
            service: Service name (e.g., "stripe_api")

        Returns:
            Status: "open", "closed", "half_open", or None
        """
        key = f"circuit_breaker:{service}"
        return self.client.get(key)
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.open_circuit_breaker()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Open circuit breaker (stop calling failing service).
- 
- Args:
-     service: Service name
-     ttl: How long to keep circuit open (seconds)
- 
- Returns:
-     True if circuit was opened

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def open_circuit_breaker(self, service: str, ttl: int = 60) -> bool:
        """
        Open circuit breaker (stop calling failing service).

        Args:
            service: Service name
            ttl: How long to keep circuit open (seconds)

        Returns:
            True if circuit was opened
        """
        try:
            key = f"circuit_breaker:{service}"
            self.client.setex(key, ttl, "open")
            logger.warning(f"[REDIS CIRCUIT BREAKER] Opened: {service} (TTL={ttl}s)")
            return True
        except Exception as e:
            logger.error(f"[REDIS CIRCUIT BREAKER] Error opening {service}: {e}")
            return False
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

#### Method: `RedisClient.close_circuit_breaker()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Close circuit breaker (service recovered).
- 
- Args:
-     service: Service name
- 
- Returns:
-     True if circuit was closed

When used:
- Support path (idempotency/caching/rate limits).

Method code:
```python
def close_circuit_breaker(self, service: str) -> bool:
        """
        Close circuit breaker (service recovered).

        Args:
            service: Service name

        Returns:
            True if circuit was closed
        """
        try:
            key = f"circuit_breaker:{service}"
            deleted = self.client.delete(key)
            if deleted:
                logger.info(f"[REDIS CIRCUIT BREAKER] Closed: {service}")
            return bool(deleted)
        except Exception as e:
            logger.error(f"[REDIS CIRCUIT BREAKER] Error closing {service}: {e}")
            return False
```

Detailed step-by-step (expanded):
- Step 1: Enter the method and read its parameters.
- Step 2: Read any class fields used by this method.
- Step 3: Evaluate early-exit guards (if any).
- Step 4: Build or normalize inputs for downstream calls.
- Step 5: Call helper functions (if present) in the order they appear.
- Step 6: Handle conditional branches based on runtime state.
- Step 7: Perform the primary side effect (network call, Kafka I/O, Redis, etc.).
- Step 8: Handle exceptions (log or re-raise) according to code flow.
- Step 9: Update in-memory state or caches if needed.
- Step 10: Return the final value (or await completes).

### Function: `with_idempotency()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Decorator to add idempotency checking to a function.
- 
- Args:
-     key_prefix: Prefix for idempotency key
- 
- Example:
-     >>> @with_idempotency("stripe_webhook")
-     >>> async def process_webhook(event_id: str):
-     >>>     # Will automatically check if event_id was already processed
-     >>>     pass

When used:
- Support path (idempotency/caching/rate limits).

Function code:
```python
def with_idempotency(key_prefix: str):
    """
    Decorator to add idempotency checking to a function.

    Args:
        key_prefix: Prefix for idempotency key

    Example:
        >>> @with_idempotency("stripe_webhook")
        >>> async def process_webhook(event_id: str):
        >>>     # Will automatically check if event_id was already processed
        >>>     pass
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract event ID from first argument (assumes it's event_id or similar)
            event_id = args[0] if args else kwargs.get('event_id', 'unknown')
            idempotency_key = f"{key_prefix}:{event_id}"

            # Check if already processed
            if not redis_client.check_idempotency(idempotency_key):
                logger.warning(f"[IDEMPOTENCY] Skipping duplicate: {idempotency_key}")
                return {"status": "duplicate", "processed": False}

            # Process the function
            result = await func(*args, **kwargs)
            return result

        return wrapper
    return decorator
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `with_rate_limit()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Shared Redis utilities for idempotency, caching, and rate limiting. This function sits in the Support path (idempotency/caching/rate limits). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: app, datetime, functools, json, logging, redis, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Decorator to add rate limiting to a function.
- 
- Args:
-     key_prefix: Prefix for rate limit key
-     max_requests: Max requests allowed in window
-     window_seconds: Time window in seconds
- 
- Example:
-     >>> @with_rate_limit("stripe_create_payment", 100, 60)
-     >>> async def create_payment_link(user_id: str):
-     >>>     # Will be rate limited to 100 calls per minute
-     >>>     pass

When used:
- Support path (idempotency/caching/rate limits).

Function code:
```python
def with_rate_limit(key_prefix: str, max_requests: int, window_seconds: int = 60):
    """
    Decorator to add rate limiting to a function.

    Args:
        key_prefix: Prefix for rate limit key
        max_requests: Max requests allowed in window
        window_seconds: Time window in seconds

    Example:
        >>> @with_rate_limit("stripe_create_payment", 100, 60)
        >>> async def create_payment_link(user_id: str):
        >>>     # Will be rate limited to 100 calls per minute
        >>>     pass
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Use function name as part of key
            rate_limit_key = f"{key_prefix}:{func.__name__}"

            allowed, count = redis_client.check_rate_limit(
                rate_limit_key,
                max_requests,
                window_seconds
            )

            if not allowed:
                logger.error(f"[RATE LIMIT] Exceeded for {rate_limit_key}: {count}/{max_requests}")
                raise Exception(f"Rate limit exceeded: {count}/{max_requests} in {window_seconds}s")

            return await func(*args, **kwargs)

        return wrapper
    return decorator
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

## Module: `app/utils/message_chunker.py`

Role: Chunking helper for long responses.

Module docstring:
- Message chunker to split long responses for sending.
- 
- We prefer natural boundaries (newlines, then sentence ends, then whitespace) and we
- preserve newlines in the returned chunks.

Imported stack (selected):
- __future__.annotations, typing.List

### Function: `_trim_boundary()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Splits long responses into message-sized chunks. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _trim_boundary(text: str) -> str:
    return (text or "").strip()
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_find_break_index()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Splits long responses into message-sized chunks. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Pick a natural break index <= max_length.
- 
- Preference order:
-   1) Newline boundaries
-   2) Sentence boundaries (., !, …)
-   3) Clause boundaries (;, :, ,)
-   4) Whitespace
-   5) Hard cut

When used:
- Support path (utility/config).

Function code:
```python
def _find_break_index(text: str, max_length: int) -> int:
    """
    Pick a natural break index <= max_length.

    Preference order:
      1) Newline boundaries
      2) Sentence boundaries (., !, …)
      3) Clause boundaries (;, :, ,)
      4) Whitespace
      5) Hard cut
    """
    if max_length <= 0:
        return 0
    if len(text) <= max_length:
        return len(text)

    min_idx = int(max_length * 0.55)
    min_idx = max(0, min(min_idx, max_length - 1))

    def _search_window(start: int, end: int) -> int:
        # 1) Newlines (break *before* the newline)
        for sep in ("\n\n", "\n"):
            idx = text.rfind(sep, start, end)
            if idx != -1:
                return idx

        # 2) Sentence ends (include punctuation in the left chunk)
        for sep in (".\n", "!\n", "…\n", ". ", "! ", "… "):
            idx = text.rfind(sep, start, end)
            if idx != -1:
                return idx + 1

        # 3) Clause boundaries
        for sep in (";\n", ":\n", ",\n", "; ", ": ", ", "):
            idx = text.rfind(sep, start, end)
            if idx != -1:
                return idx + 1

        # 4) Whitespace
        idx = text.rfind(" ", start, end)
        if idx != -1:
            return idx

        return -1

    idx = _search_window(min_idx, max_length + 1)
    if idx != -1:
        return idx

    idx = _search_window(0, max_length + 1)
    if idx != -1:
        return idx

    return max_length
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `_truncate()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Splits long responses into message-sized chunks. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- No function docstring; behavior described by name and inline code.

When used:
- Support path (utility/config).

Function code:
```python
def _truncate(text: str, max_length: int, *, suffix: str = "…") -> str:
    if max_length <= 0:
        return ""
    s = _trim_boundary(text)
    if len(s) <= max_length:
        return s
    if max_length <= len(suffix):
        return suffix[:max_length]

    cut_limit = max_length - len(suffix)
    idx = _find_break_index(s, cut_limit)
    if idx <= 0:
        idx = cut_limit
    left = s[:idx].rstrip()
    return (left + suffix).strip()
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

### Function: `chunk_message()`

Big picture and system-design notes (>=5 sentences):
- Big-picture role: Splits long responses into message-sized chunks. This function sits in the Support path (utility/config). path and shapes how the system behaves at that stage.
- It uses the module?s imported stack directly or indirectly, which here includes: __future__, typing.
- From a system-design perspective, pay attention to how this method isolates responsibilities, such as separating transport (Kafka/HTTP/Socket.IO) from domain logic.
- Concurrency and reliability concerns are handled via async/await, locks, retries, or idempotency checks, depending on the function?s responsibility.
- For beginners, the key learning is to follow the data flow: inputs are validated, normalized, side effects happen, and outputs are either returned or logged for observability.

What it does (docstring if available):
- Split a message into chunks no longer than max_length.
- 
- If max_chunks is set, returns at most that many chunks (best-effort).

When used:
- Support path (utility/config).

Function code:
```python
def chunk_message(message: str, max_length: int = 280, *, max_chunks: int | None = None) -> List[str]:
    """
    Split a message into chunks no longer than max_length.

    If max_chunks is set, returns at most that many chunks (best-effort).
    """
    text = _trim_boundary(message)
    if not text:
        return []

    if max_length <= 0:
        return [text]

    if len(text) <= max_length:
        return [text]

    if max_chunks is not None and max_chunks <= 1:
        return [_truncate(text, max_length)]

    if max_chunks == 2:
        idx = _find_break_index(text, max_length)
        left = _trim_boundary(text[:idx])
        right = _trim_boundary(text[idx:])
        if not left:
            left = _trim_boundary(text[:max_length])
            right = _trim_boundary(text[max_length:])
        if not right:
            return [left]
        if len(right) > max_length:
            right = _truncate(right, max_length)
        return [left, right]

    chunks: List[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= max_length:
            chunks.append(remaining)
            break

        idx = _find_break_index(remaining, max_length)
        piece = _trim_boundary(remaining[:idx])
        remaining = _trim_boundary(remaining[idx:])
        if not piece:
            piece = _trim_boundary(remaining[:max_length])
            remaining = _trim_boundary(remaining[max_length:])
        chunks.append(piece)

        if max_chunks is not None and len(chunks) >= max_chunks:
            if remaining:
                chunks[-1] = _truncate(chunks[-1] + " " + remaining, max_length)
            break

    return [c for c in chunks if c]
```

Detailed step-by-step (expanded):
- Step 1: Enter the function and read its parameters.
- Step 2: Normalize or validate inputs if needed.
- Step 3: Build any intermediate values or configuration objects.
- Step 4: Call dependent helpers in the order they appear.
- Step 5: Apply conditional logic and branching.
- Step 6: Perform the primary side effect or computation.
- Step 7: Handle exceptions and log appropriately.
- Step 8: Return the function?s output.

---

## Expanded Timeline Appendix (micro-steps)

This appendix repeats the end-to-end pipeline at very fine granularity.
Each line is a single, small step to make the timing explicit.

0001. Process boot begins.
0002. Python imports `app.main` and config.
0003. Settings are loaded from environment and .env.
0004. Logging is configured.
0005. FastAPI app object is created.
0006. Global orchestrator instance is created.
0007. Kafka globals are initialized to None.
0008. Startup event is registered with FastAPI.
0009. Startup event fires when server starts.
0010. Startup task `_init_services` is scheduled.
0011. Startup task `_init_kafka_consumer` is scheduled.
0012. Startup task `_init_async_processor` is scheduled.
0013. Startup task `_init_profile_synthesis_scheduler` is scheduled.
0014. Photon listener decides ingest mode.
0015. Photon listener connects if enabled.
0016. Kafka consumer start loop begins if enabled.
0017. Kafka consumer calls `ensure_kafka_topics`.
0018. Kafka admin client lists topics.
0019. Kafka admin client creates missing topics.
0020. Kafka consumer constructs security config.
0021. Kafka consumer starts and begins polling.
0022. Kafka retry producer starts.
0023. Async processor is initialized if enabled.
0024. Async handlers are registered.
0025. Async processor background loop starts.
0026. Profile synthesis scheduler enters sleep before first run.
0027. --- Message cycle 1 begins ---
0028. Photon emits a Socket.IO `new-message` event.
0029. Photon listener receives event and logs entry.
0030. Listener drops empty or self messages.
0031. Listener extracts sender handle and text.
0032. Listener extracts attachment info if present.
0033. Listener computes chat GUID and group status.
0034. Listener runs in-memory dedupe check.
0035. Listener computes idempotency key.
0036. Listener checks Redis idempotency (unless kafka ingest).
0037. Listener caches chat GUID for DMs.
0038. Listener builds SendBlue-compatible payload.
0039. Listener forwards payload to callback.
0040. Callback chooses Kafka publish if ingest mode is kafka.
0041. Kafka event is built from payload.
0042. Kafka producer ensures topics exist.
0043. Kafka producer sends event to inbound topic.
0044. Kafka consumer polls for messages.
0045. Kafka consumer decodes event JSON.
0046. Kafka consumer applies retry delay if present.
0047. Kafka consumer invokes `_handle_kafka_event`.
0048. Kafka event idempotency check runs in Redis.
0049. Orchestrator handles the message.
0050. Orchestrator decides group chat vs DM flow.
0051. Interaction agent processes DM messages.
0052. Responses are sent via Photon client.
0053. Orchestrator logs latency metrics.
0054. On error, Kafka pipeline schedules retry or DLQ.
0055. --- Message cycle 1 ends ---
0056. --- Message cycle 2 begins ---
0057. Photon emits a Socket.IO `new-message` event.
0058. Photon listener receives event and logs entry.
0059. Listener drops empty or self messages.
0060. Listener extracts sender handle and text.
0061. Listener extracts attachment info if present.
0062. Listener computes chat GUID and group status.
0063. Listener runs in-memory dedupe check.
0064. Listener computes idempotency key.
0065. Listener checks Redis idempotency (unless kafka ingest).
0066. Listener caches chat GUID for DMs.
0067. Listener builds SendBlue-compatible payload.
0068. Listener forwards payload to callback.
0069. Callback chooses Kafka publish if ingest mode is kafka.
0070. Kafka event is built from payload.
0071. Kafka producer ensures topics exist.
0072. Kafka producer sends event to inbound topic.
0073. Kafka consumer polls for messages.
0074. Kafka consumer decodes event JSON.
0075. Kafka consumer applies retry delay if present.
0076. Kafka consumer invokes `_handle_kafka_event`.
0077. Kafka event idempotency check runs in Redis.
0078. Orchestrator handles the message.
0079. Orchestrator decides group chat vs DM flow.
0080. Interaction agent processes DM messages.
0081. Responses are sent via Photon client.
0082. Orchestrator logs latency metrics.
0083. On error, Kafka pipeline schedules retry or DLQ.
0084. --- Message cycle 2 ends ---
0085. --- Message cycle 3 begins ---
0086. Photon emits a Socket.IO `new-message` event.
0087. Photon listener receives event and logs entry.
0088. Listener drops empty or self messages.
0089. Listener extracts sender handle and text.
0090. Listener extracts attachment info if present.
0091. Listener computes chat GUID and group status.
0092. Listener runs in-memory dedupe check.
0093. Listener computes idempotency key.
0094. Listener checks Redis idempotency (unless kafka ingest).
0095. Listener caches chat GUID for DMs.
0096. Listener builds SendBlue-compatible payload.
0097. Listener forwards payload to callback.
0098. Callback chooses Kafka publish if ingest mode is kafka.
0099. Kafka event is built from payload.
0100. Kafka producer ensures topics exist.
0101. Kafka producer sends event to inbound topic.
0102. Kafka consumer polls for messages.
0103. Kafka consumer decodes event JSON.
0104. Kafka consumer applies retry delay if present.
0105. Kafka consumer invokes `_handle_kafka_event`.
0106. Kafka event idempotency check runs in Redis.
0107. Orchestrator handles the message.
0108. Orchestrator decides group chat vs DM flow.
0109. Interaction agent processes DM messages.
0110. Responses are sent via Photon client.
0111. Orchestrator logs latency metrics.
0112. On error, Kafka pipeline schedules retry or DLQ.
0113. --- Message cycle 3 ends ---
0114. --- Message cycle 4 begins ---
0115. Photon emits a Socket.IO `new-message` event.
0116. Photon listener receives event and logs entry.
0117. Listener drops empty or self messages.
0118. Listener extracts sender handle and text.
0119. Listener extracts attachment info if present.
0120. Listener computes chat GUID and group status.
0121. Listener runs in-memory dedupe check.
0122. Listener computes idempotency key.
0123. Listener checks Redis idempotency (unless kafka ingest).
0124. Listener caches chat GUID for DMs.
0125. Listener builds SendBlue-compatible payload.
0126. Listener forwards payload to callback.
0127. Callback chooses Kafka publish if ingest mode is kafka.
0128. Kafka event is built from payload.
0129. Kafka producer ensures topics exist.
0130. Kafka producer sends event to inbound topic.
0131. Kafka consumer polls for messages.
0132. Kafka consumer decodes event JSON.
0133. Kafka consumer applies retry delay if present.
0134. Kafka consumer invokes `_handle_kafka_event`.
0135. Kafka event idempotency check runs in Redis.
0136. Orchestrator handles the message.
0137. Orchestrator decides group chat vs DM flow.
0138. Interaction agent processes DM messages.
0139. Responses are sent via Photon client.
0140. Orchestrator logs latency metrics.
0141. On error, Kafka pipeline schedules retry or DLQ.
0142. --- Message cycle 4 ends ---
0143. --- Message cycle 5 begins ---
0144. Photon emits a Socket.IO `new-message` event.
0145. Photon listener receives event and logs entry.
0146. Listener drops empty or self messages.
0147. Listener extracts sender handle and text.
0148. Listener extracts attachment info if present.
0149. Listener computes chat GUID and group status.
0150. Listener runs in-memory dedupe check.
0151. Listener computes idempotency key.
0152. Listener checks Redis idempotency (unless kafka ingest).
0153. Listener caches chat GUID for DMs.
0154. Listener builds SendBlue-compatible payload.
0155. Listener forwards payload to callback.
0156. Callback chooses Kafka publish if ingest mode is kafka.
0157. Kafka event is built from payload.
0158. Kafka producer ensures topics exist.
0159. Kafka producer sends event to inbound topic.
0160. Kafka consumer polls for messages.
0161. Kafka consumer decodes event JSON.
0162. Kafka consumer applies retry delay if present.
0163. Kafka consumer invokes `_handle_kafka_event`.
0164. Kafka event idempotency check runs in Redis.
0165. Orchestrator handles the message.
0166. Orchestrator decides group chat vs DM flow.
0167. Interaction agent processes DM messages.
0168. Responses are sent via Photon client.
0169. Orchestrator logs latency metrics.
0170. On error, Kafka pipeline schedules retry or DLQ.
0171. --- Message cycle 5 ends ---
0172. --- Message cycle 6 begins ---
0173. Photon emits a Socket.IO `new-message` event.
0174. Photon listener receives event and logs entry.
0175. Listener drops empty or self messages.
0176. Listener extracts sender handle and text.
0177. Listener extracts attachment info if present.
0178. Listener computes chat GUID and group status.
0179. Listener runs in-memory dedupe check.
0180. Listener computes idempotency key.
0181. Listener checks Redis idempotency (unless kafka ingest).
0182. Listener caches chat GUID for DMs.
0183. Listener builds SendBlue-compatible payload.
0184. Listener forwards payload to callback.
0185. Callback chooses Kafka publish if ingest mode is kafka.
0186. Kafka event is built from payload.
0187. Kafka producer ensures topics exist.
0188. Kafka producer sends event to inbound topic.
0189. Kafka consumer polls for messages.
0190. Kafka consumer decodes event JSON.
0191. Kafka consumer applies retry delay if present.
0192. Kafka consumer invokes `_handle_kafka_event`.
0193. Kafka event idempotency check runs in Redis.
0194. Orchestrator handles the message.
0195. Orchestrator decides group chat vs DM flow.
0196. Interaction agent processes DM messages.
0197. Responses are sent via Photon client.
0198. Orchestrator logs latency metrics.
0199. On error, Kafka pipeline schedules retry or DLQ.
0200. --- Message cycle 6 ends ---
0201. --- Message cycle 7 begins ---
0202. Photon emits a Socket.IO `new-message` event.
0203. Photon listener receives event and logs entry.
0204. Listener drops empty or self messages.
0205. Listener extracts sender handle and text.
0206. Listener extracts attachment info if present.
0207. Listener computes chat GUID and group status.
0208. Listener runs in-memory dedupe check.
0209. Listener computes idempotency key.
0210. Listener checks Redis idempotency (unless kafka ingest).
0211. Listener caches chat GUID for DMs.
0212. Listener builds SendBlue-compatible payload.
0213. Listener forwards payload to callback.
0214. Callback chooses Kafka publish if ingest mode is kafka.
0215. Kafka event is built from payload.
0216. Kafka producer ensures topics exist.
0217. Kafka producer sends event to inbound topic.
0218. Kafka consumer polls for messages.
0219. Kafka consumer decodes event JSON.
0220. Kafka consumer applies retry delay if present.
0221. Kafka consumer invokes `_handle_kafka_event`.
0222. Kafka event idempotency check runs in Redis.
0223. Orchestrator handles the message.
0224. Orchestrator decides group chat vs DM flow.
0225. Interaction agent processes DM messages.
0226. Responses are sent via Photon client.
0227. Orchestrator logs latency metrics.
0228. On error, Kafka pipeline schedules retry or DLQ.
0229. --- Message cycle 7 ends ---
0230. --- Message cycle 8 begins ---
0231. Photon emits a Socket.IO `new-message` event.
0232. Photon listener receives event and logs entry.
0233. Listener drops empty or self messages.
0234. Listener extracts sender handle and text.
0235. Listener extracts attachment info if present.
0236. Listener computes chat GUID and group status.
0237. Listener runs in-memory dedupe check.
0238. Listener computes idempotency key.
0239. Listener checks Redis idempotency (unless kafka ingest).
0240. Listener caches chat GUID for DMs.
0241. Listener builds SendBlue-compatible payload.
0242. Listener forwards payload to callback.
0243. Callback chooses Kafka publish if ingest mode is kafka.
0244. Kafka event is built from payload.
0245. Kafka producer ensures topics exist.
0246. Kafka producer sends event to inbound topic.
0247. Kafka consumer polls for messages.
0248. Kafka consumer decodes event JSON.
0249. Kafka consumer applies retry delay if present.
0250. Kafka consumer invokes `_handle_kafka_event`.
0251. Kafka event idempotency check runs in Redis.
0252. Orchestrator handles the message.
0253. Orchestrator decides group chat vs DM flow.
0254. Interaction agent processes DM messages.
0255. Responses are sent via Photon client.
0256. Orchestrator logs latency metrics.
0257. On error, Kafka pipeline schedules retry or DLQ.
0258. --- Message cycle 8 ends ---
0259. --- Message cycle 9 begins ---
0260. Photon emits a Socket.IO `new-message` event.
0261. Photon listener receives event and logs entry.
0262. Listener drops empty or self messages.
0263. Listener extracts sender handle and text.
0264. Listener extracts attachment info if present.
0265. Listener computes chat GUID and group status.
0266. Listener runs in-memory dedupe check.
0267. Listener computes idempotency key.
0268. Listener checks Redis idempotency (unless kafka ingest).
0269. Listener caches chat GUID for DMs.
0270. Listener builds SendBlue-compatible payload.
0271. Listener forwards payload to callback.
0272. Callback chooses Kafka publish if ingest mode is kafka.
0273. Kafka event is built from payload.
0274. Kafka producer ensures topics exist.
0275. Kafka producer sends event to inbound topic.
0276. Kafka consumer polls for messages.
0277. Kafka consumer decodes event JSON.
0278. Kafka consumer applies retry delay if present.
0279. Kafka consumer invokes `_handle_kafka_event`.
0280. Kafka event idempotency check runs in Redis.
0281. Orchestrator handles the message.
0282. Orchestrator decides group chat vs DM flow.
0283. Interaction agent processes DM messages.
0284. Responses are sent via Photon client.
0285. Orchestrator logs latency metrics.
0286. On error, Kafka pipeline schedules retry or DLQ.
0287. --- Message cycle 9 ends ---
0288. --- Message cycle 10 begins ---
0289. Photon emits a Socket.IO `new-message` event.
0290. Photon listener receives event and logs entry.
0291. Listener drops empty or self messages.
0292. Listener extracts sender handle and text.
0293. Listener extracts attachment info if present.
0294. Listener computes chat GUID and group status.
0295. Listener runs in-memory dedupe check.
0296. Listener computes idempotency key.
0297. Listener checks Redis idempotency (unless kafka ingest).
0298. Listener caches chat GUID for DMs.
0299. Listener builds SendBlue-compatible payload.
0300. Listener forwards payload to callback.
0301. Callback chooses Kafka publish if ingest mode is kafka.
0302. Kafka event is built from payload.
0303. Kafka producer ensures topics exist.
0304. Kafka producer sends event to inbound topic.
0305. Kafka consumer polls for messages.
0306. Kafka consumer decodes event JSON.
0307. Kafka consumer applies retry delay if present.
0308. Kafka consumer invokes `_handle_kafka_event`.
0309. Kafka event idempotency check runs in Redis.
0310. Orchestrator handles the message.
0311. Orchestrator decides group chat vs DM flow.
0312. Interaction agent processes DM messages.
0313. Responses are sent via Photon client.
0314. Orchestrator logs latency metrics.
0315. On error, Kafka pipeline schedules retry or DLQ.
0316. --- Message cycle 10 ends ---
0317. --- Message cycle 11 begins ---
0318. Photon emits a Socket.IO `new-message` event.
0319. Photon listener receives event and logs entry.
0320. Listener drops empty or self messages.
0321. Listener extracts sender handle and text.
0322. Listener extracts attachment info if present.
0323. Listener computes chat GUID and group status.
0324. Listener runs in-memory dedupe check.
0325. Listener computes idempotency key.
0326. Listener checks Redis idempotency (unless kafka ingest).
0327. Listener caches chat GUID for DMs.
0328. Listener builds SendBlue-compatible payload.
0329. Listener forwards payload to callback.
0330. Callback chooses Kafka publish if ingest mode is kafka.
0331. Kafka event is built from payload.
0332. Kafka producer ensures topics exist.
0333. Kafka producer sends event to inbound topic.
0334. Kafka consumer polls for messages.
0335. Kafka consumer decodes event JSON.
0336. Kafka consumer applies retry delay if present.
0337. Kafka consumer invokes `_handle_kafka_event`.
0338. Kafka event idempotency check runs in Redis.
0339. Orchestrator handles the message.
0340. Orchestrator decides group chat vs DM flow.
0341. Interaction agent processes DM messages.
0342. Responses are sent via Photon client.
0343. Orchestrator logs latency metrics.
0344. On error, Kafka pipeline schedules retry or DLQ.
0345. --- Message cycle 11 ends ---
0346. --- Message cycle 12 begins ---
0347. Photon emits a Socket.IO `new-message` event.
0348. Photon listener receives event and logs entry.
0349. Listener drops empty or self messages.
0350. Listener extracts sender handle and text.
0351. Listener extracts attachment info if present.
0352. Listener computes chat GUID and group status.
0353. Listener runs in-memory dedupe check.
0354. Listener computes idempotency key.
0355. Listener checks Redis idempotency (unless kafka ingest).
0356. Listener caches chat GUID for DMs.
0357. Listener builds SendBlue-compatible payload.
0358. Listener forwards payload to callback.
0359. Callback chooses Kafka publish if ingest mode is kafka.
0360. Kafka event is built from payload.
0361. Kafka producer ensures topics exist.
0362. Kafka producer sends event to inbound topic.
0363. Kafka consumer polls for messages.
0364. Kafka consumer decodes event JSON.
0365. Kafka consumer applies retry delay if present.
0366. Kafka consumer invokes `_handle_kafka_event`.
0367. Kafka event idempotency check runs in Redis.
0368. Orchestrator handles the message.
0369. Orchestrator decides group chat vs DM flow.
0370. Interaction agent processes DM messages.
0371. Responses are sent via Photon client.
0372. Orchestrator logs latency metrics.
0373. On error, Kafka pipeline schedules retry or DLQ.
0374. --- Message cycle 12 ends ---
0375. --- Message cycle 13 begins ---
0376. Photon emits a Socket.IO `new-message` event.
0377. Photon listener receives event and logs entry.
0378. Listener drops empty or self messages.
0379. Listener extracts sender handle and text.
0380. Listener extracts attachment info if present.
0381. Listener computes chat GUID and group status.
0382. Listener runs in-memory dedupe check.
0383. Listener computes idempotency key.
0384. Listener checks Redis idempotency (unless kafka ingest).
0385. Listener caches chat GUID for DMs.
0386. Listener builds SendBlue-compatible payload.
0387. Listener forwards payload to callback.
0388. Callback chooses Kafka publish if ingest mode is kafka.
0389. Kafka event is built from payload.
0390. Kafka producer ensures topics exist.
0391. Kafka producer sends event to inbound topic.
0392. Kafka consumer polls for messages.
0393. Kafka consumer decodes event JSON.
0394. Kafka consumer applies retry delay if present.
0395. Kafka consumer invokes `_handle_kafka_event`.
0396. Kafka event idempotency check runs in Redis.
0397. Orchestrator handles the message.
0398. Orchestrator decides group chat vs DM flow.
0399. Interaction agent processes DM messages.
0400. Responses are sent via Photon client.
0401. Orchestrator logs latency metrics.
0402. On error, Kafka pipeline schedules retry or DLQ.
0403. --- Message cycle 13 ends ---
0404. --- Message cycle 14 begins ---
0405. Photon emits a Socket.IO `new-message` event.
0406. Photon listener receives event and logs entry.
0407. Listener drops empty or self messages.
0408. Listener extracts sender handle and text.
0409. Listener extracts attachment info if present.
0410. Listener computes chat GUID and group status.
0411. Listener runs in-memory dedupe check.
0412. Listener computes idempotency key.
0413. Listener checks Redis idempotency (unless kafka ingest).
0414. Listener caches chat GUID for DMs.
0415. Listener builds SendBlue-compatible payload.
0416. Listener forwards payload to callback.
0417. Callback chooses Kafka publish if ingest mode is kafka.
0418. Kafka event is built from payload.
0419. Kafka producer ensures topics exist.
0420. Kafka producer sends event to inbound topic.
0421. Kafka consumer polls for messages.
0422. Kafka consumer decodes event JSON.
0423. Kafka consumer applies retry delay if present.
0424. Kafka consumer invokes `_handle_kafka_event`.
0425. Kafka event idempotency check runs in Redis.
0426. Orchestrator handles the message.
0427. Orchestrator decides group chat vs DM flow.
0428. Interaction agent processes DM messages.
0429. Responses are sent via Photon client.
0430. Orchestrator logs latency metrics.
0431. On error, Kafka pipeline schedules retry or DLQ.
0432. --- Message cycle 14 ends ---
0433. --- Message cycle 15 begins ---
0434. Photon emits a Socket.IO `new-message` event.
0435. Photon listener receives event and logs entry.
0436. Listener drops empty or self messages.
0437. Listener extracts sender handle and text.
0438. Listener extracts attachment info if present.
0439. Listener computes chat GUID and group status.
0440. Listener runs in-memory dedupe check.
0441. Listener computes idempotency key.
0442. Listener checks Redis idempotency (unless kafka ingest).
0443. Listener caches chat GUID for DMs.
0444. Listener builds SendBlue-compatible payload.
0445. Listener forwards payload to callback.
0446. Callback chooses Kafka publish if ingest mode is kafka.
0447. Kafka event is built from payload.
0448. Kafka producer ensures topics exist.
0449. Kafka producer sends event to inbound topic.
0450. Kafka consumer polls for messages.
0451. Kafka consumer decodes event JSON.
0452. Kafka consumer applies retry delay if present.
0453. Kafka consumer invokes `_handle_kafka_event`.
0454. Kafka event idempotency check runs in Redis.
0455. Orchestrator handles the message.
0456. Orchestrator decides group chat vs DM flow.
0457. Interaction agent processes DM messages.
0458. Responses are sent via Photon client.
0459. Orchestrator logs latency metrics.
0460. On error, Kafka pipeline schedules retry or DLQ.
0461. --- Message cycle 15 ends ---
0462. --- Message cycle 16 begins ---
0463. Photon emits a Socket.IO `new-message` event.
0464. Photon listener receives event and logs entry.
0465. Listener drops empty or self messages.
0466. Listener extracts sender handle and text.
0467. Listener extracts attachment info if present.
0468. Listener computes chat GUID and group status.
0469. Listener runs in-memory dedupe check.
0470. Listener computes idempotency key.
0471. Listener checks Redis idempotency (unless kafka ingest).
0472. Listener caches chat GUID for DMs.
0473. Listener builds SendBlue-compatible payload.
0474. Listener forwards payload to callback.
0475. Callback chooses Kafka publish if ingest mode is kafka.
0476. Kafka event is built from payload.
0477. Kafka producer ensures topics exist.
0478. Kafka producer sends event to inbound topic.
0479. Kafka consumer polls for messages.
0480. Kafka consumer decodes event JSON.
0481. Kafka consumer applies retry delay if present.
0482. Kafka consumer invokes `_handle_kafka_event`.
0483. Kafka event idempotency check runs in Redis.
0484. Orchestrator handles the message.
0485. Orchestrator decides group chat vs DM flow.
0486. Interaction agent processes DM messages.
0487. Responses are sent via Photon client.
0488. Orchestrator logs latency metrics.
0489. On error, Kafka pipeline schedules retry or DLQ.
0490. --- Message cycle 16 ends ---
0491. --- Message cycle 17 begins ---
0492. Photon emits a Socket.IO `new-message` event.
0493. Photon listener receives event and logs entry.
0494. Listener drops empty or self messages.
0495. Listener extracts sender handle and text.
0496. Listener extracts attachment info if present.
0497. Listener computes chat GUID and group status.
0498. Listener runs in-memory dedupe check.
0499. Listener computes idempotency key.
0500. Listener checks Redis idempotency (unless kafka ingest).
0501. Listener caches chat GUID for DMs.
0502. Listener builds SendBlue-compatible payload.
0503. Listener forwards payload to callback.
0504. Callback chooses Kafka publish if ingest mode is kafka.
0505. Kafka event is built from payload.
0506. Kafka producer ensures topics exist.
0507. Kafka producer sends event to inbound topic.
0508. Kafka consumer polls for messages.
0509. Kafka consumer decodes event JSON.
0510. Kafka consumer applies retry delay if present.
0511. Kafka consumer invokes `_handle_kafka_event`.
0512. Kafka event idempotency check runs in Redis.
0513. Orchestrator handles the message.
0514. Orchestrator decides group chat vs DM flow.
0515. Interaction agent processes DM messages.
0516. Responses are sent via Photon client.
0517. Orchestrator logs latency metrics.
0518. On error, Kafka pipeline schedules retry or DLQ.
0519. --- Message cycle 17 ends ---
0520. --- Message cycle 18 begins ---
0521. Photon emits a Socket.IO `new-message` event.
0522. Photon listener receives event and logs entry.
0523. Listener drops empty or self messages.
0524. Listener extracts sender handle and text.
0525. Listener extracts attachment info if present.
0526. Listener computes chat GUID and group status.
0527. Listener runs in-memory dedupe check.
0528. Listener computes idempotency key.
0529. Listener checks Redis idempotency (unless kafka ingest).
0530. Listener caches chat GUID for DMs.
0531. Listener builds SendBlue-compatible payload.
0532. Listener forwards payload to callback.
0533. Callback chooses Kafka publish if ingest mode is kafka.
0534. Kafka event is built from payload.
0535. Kafka producer ensures topics exist.
0536. Kafka producer sends event to inbound topic.
0537. Kafka consumer polls for messages.
0538. Kafka consumer decodes event JSON.
0539. Kafka consumer applies retry delay if present.
0540. Kafka consumer invokes `_handle_kafka_event`.
0541. Kafka event idempotency check runs in Redis.
0542. Orchestrator handles the message.
0543. Orchestrator decides group chat vs DM flow.
0544. Interaction agent processes DM messages.
0545. Responses are sent via Photon client.
0546. Orchestrator logs latency metrics.
0547. On error, Kafka pipeline schedules retry or DLQ.
0548. --- Message cycle 18 ends ---
0549. --- Message cycle 19 begins ---
0550. Photon emits a Socket.IO `new-message` event.
0551. Photon listener receives event and logs entry.
0552. Listener drops empty or self messages.
0553. Listener extracts sender handle and text.
0554. Listener extracts attachment info if present.
0555. Listener computes chat GUID and group status.
0556. Listener runs in-memory dedupe check.
0557. Listener computes idempotency key.
0558. Listener checks Redis idempotency (unless kafka ingest).
0559. Listener caches chat GUID for DMs.
0560. Listener builds SendBlue-compatible payload.
0561. Listener forwards payload to callback.
0562. Callback chooses Kafka publish if ingest mode is kafka.
0563. Kafka event is built from payload.
0564. Kafka producer ensures topics exist.
0565. Kafka producer sends event to inbound topic.
0566. Kafka consumer polls for messages.
0567. Kafka consumer decodes event JSON.
0568. Kafka consumer applies retry delay if present.
0569. Kafka consumer invokes `_handle_kafka_event`.
0570. Kafka event idempotency check runs in Redis.
0571. Orchestrator handles the message.
0572. Orchestrator decides group chat vs DM flow.
0573. Interaction agent processes DM messages.
0574. Responses are sent via Photon client.
0575. Orchestrator logs latency metrics.
0576. On error, Kafka pipeline schedules retry or DLQ.
0577. --- Message cycle 19 ends ---
0578. --- Message cycle 20 begins ---
0579. Photon emits a Socket.IO `new-message` event.
0580. Photon listener receives event and logs entry.
0581. Listener drops empty or self messages.
0582. Listener extracts sender handle and text.
0583. Listener extracts attachment info if present.
0584. Listener computes chat GUID and group status.
0585. Listener runs in-memory dedupe check.
0586. Listener computes idempotency key.
0587. Listener checks Redis idempotency (unless kafka ingest).
0588. Listener caches chat GUID for DMs.
0589. Listener builds SendBlue-compatible payload.
0590. Listener forwards payload to callback.
0591. Callback chooses Kafka publish if ingest mode is kafka.
0592. Kafka event is built from payload.
0593. Kafka producer ensures topics exist.
0594. Kafka producer sends event to inbound topic.
0595. Kafka consumer polls for messages.
0596. Kafka consumer decodes event JSON.
0597. Kafka consumer applies retry delay if present.
0598. Kafka consumer invokes `_handle_kafka_event`.
0599. Kafka event idempotency check runs in Redis.
0600. Orchestrator handles the message.
0601. Orchestrator decides group chat vs DM flow.
0602. Interaction agent processes DM messages.
0603. Responses are sent via Photon client.
0604. Orchestrator logs latency metrics.
0605. On error, Kafka pipeline schedules retry or DLQ.
0606. --- Message cycle 20 ends ---
0607. --- Message cycle 21 begins ---
0608. Photon emits a Socket.IO `new-message` event.
0609. Photon listener receives event and logs entry.
0610. Listener drops empty or self messages.
0611. Listener extracts sender handle and text.
0612. Listener extracts attachment info if present.
0613. Listener computes chat GUID and group status.
0614. Listener runs in-memory dedupe check.
0615. Listener computes idempotency key.
0616. Listener checks Redis idempotency (unless kafka ingest).
0617. Listener caches chat GUID for DMs.
0618. Listener builds SendBlue-compatible payload.
0619. Listener forwards payload to callback.
0620. Callback chooses Kafka publish if ingest mode is kafka.
0621. Kafka event is built from payload.
0622. Kafka producer ensures topics exist.
0623. Kafka producer sends event to inbound topic.
0624. Kafka consumer polls for messages.
0625. Kafka consumer decodes event JSON.
0626. Kafka consumer applies retry delay if present.
0627. Kafka consumer invokes `_handle_kafka_event`.
0628. Kafka event idempotency check runs in Redis.
0629. Orchestrator handles the message.
0630. Orchestrator decides group chat vs DM flow.
0631. Interaction agent processes DM messages.
0632. Responses are sent via Photon client.
0633. Orchestrator logs latency metrics.
0634. On error, Kafka pipeline schedules retry or DLQ.
0635. --- Message cycle 21 ends ---
0636. --- Message cycle 22 begins ---
0637. Photon emits a Socket.IO `new-message` event.
0638. Photon listener receives event and logs entry.
0639. Listener drops empty or self messages.
0640. Listener extracts sender handle and text.
0641. Listener extracts attachment info if present.
0642. Listener computes chat GUID and group status.
0643. Listener runs in-memory dedupe check.
0644. Listener computes idempotency key.
0645. Listener checks Redis idempotency (unless kafka ingest).
0646. Listener caches chat GUID for DMs.
0647. Listener builds SendBlue-compatible payload.
0648. Listener forwards payload to callback.
0649. Callback chooses Kafka publish if ingest mode is kafka.
0650. Kafka event is built from payload.
0651. Kafka producer ensures topics exist.
0652. Kafka producer sends event to inbound topic.
0653. Kafka consumer polls for messages.
0654. Kafka consumer decodes event JSON.
0655. Kafka consumer applies retry delay if present.
0656. Kafka consumer invokes `_handle_kafka_event`.
0657. Kafka event idempotency check runs in Redis.
0658. Orchestrator handles the message.
0659. Orchestrator decides group chat vs DM flow.
0660. Interaction agent processes DM messages.
0661. Responses are sent via Photon client.
0662. Orchestrator logs latency metrics.
0663. On error, Kafka pipeline schedules retry or DLQ.
0664. --- Message cycle 22 ends ---
0665. --- Message cycle 23 begins ---
0666. Photon emits a Socket.IO `new-message` event.
0667. Photon listener receives event and logs entry.
0668. Listener drops empty or self messages.
0669. Listener extracts sender handle and text.
0670. Listener extracts attachment info if present.
0671. Listener computes chat GUID and group status.
0672. Listener runs in-memory dedupe check.
0673. Listener computes idempotency key.
0674. Listener checks Redis idempotency (unless kafka ingest).
0675. Listener caches chat GUID for DMs.
0676. Listener builds SendBlue-compatible payload.
0677. Listener forwards payload to callback.
0678. Callback chooses Kafka publish if ingest mode is kafka.
0679. Kafka event is built from payload.
0680. Kafka producer ensures topics exist.
0681. Kafka producer sends event to inbound topic.
0682. Kafka consumer polls for messages.
0683. Kafka consumer decodes event JSON.
0684. Kafka consumer applies retry delay if present.
0685. Kafka consumer invokes `_handle_kafka_event`.
0686. Kafka event idempotency check runs in Redis.
0687. Orchestrator handles the message.
0688. Orchestrator decides group chat vs DM flow.
0689. Interaction agent processes DM messages.
0690. Responses are sent via Photon client.
0691. Orchestrator logs latency metrics.
0692. On error, Kafka pipeline schedules retry or DLQ.
0693. --- Message cycle 23 ends ---
0694. --- Message cycle 24 begins ---
0695. Photon emits a Socket.IO `new-message` event.
0696. Photon listener receives event and logs entry.
0697. Listener drops empty or self messages.
0698. Listener extracts sender handle and text.
0699. Listener extracts attachment info if present.
0700. Listener computes chat GUID and group status.
0701. Listener runs in-memory dedupe check.
0702. Listener computes idempotency key.
0703. Listener checks Redis idempotency (unless kafka ingest).
0704. Listener caches chat GUID for DMs.
0705. Listener builds SendBlue-compatible payload.
0706. Listener forwards payload to callback.
0707. Callback chooses Kafka publish if ingest mode is kafka.
0708. Kafka event is built from payload.
0709. Kafka producer ensures topics exist.
0710. Kafka producer sends event to inbound topic.
0711. Kafka consumer polls for messages.
0712. Kafka consumer decodes event JSON.
0713. Kafka consumer applies retry delay if present.
0714. Kafka consumer invokes `_handle_kafka_event`.
0715. Kafka event idempotency check runs in Redis.
0716. Orchestrator handles the message.
0717. Orchestrator decides group chat vs DM flow.
0718. Interaction agent processes DM messages.
0719. Responses are sent via Photon client.
0720. Orchestrator logs latency metrics.
0721. On error, Kafka pipeline schedules retry or DLQ.
0722. --- Message cycle 24 ends ---
0723. --- Message cycle 25 begins ---
0724. Photon emits a Socket.IO `new-message` event.
0725. Photon listener receives event and logs entry.
0726. Listener drops empty or self messages.
0727. Listener extracts sender handle and text.
0728. Listener extracts attachment info if present.
0729. Listener computes chat GUID and group status.
0730. Listener runs in-memory dedupe check.
0731. Listener computes idempotency key.
0732. Listener checks Redis idempotency (unless kafka ingest).
0733. Listener caches chat GUID for DMs.
0734. Listener builds SendBlue-compatible payload.
0735. Listener forwards payload to callback.
0736. Callback chooses Kafka publish if ingest mode is kafka.
0737. Kafka event is built from payload.
0738. Kafka producer ensures topics exist.
0739. Kafka producer sends event to inbound topic.
0740. Kafka consumer polls for messages.
0741. Kafka consumer decodes event JSON.
0742. Kafka consumer applies retry delay if present.
0743. Kafka consumer invokes `_handle_kafka_event`.
0744. Kafka event idempotency check runs in Redis.
0745. Orchestrator handles the message.
0746. Orchestrator decides group chat vs DM flow.
0747. Interaction agent processes DM messages.
0748. Responses are sent via Photon client.
0749. Orchestrator logs latency metrics.
0750. On error, Kafka pipeline schedules retry or DLQ.
0751. --- Message cycle 25 ends ---
0752. --- Message cycle 26 begins ---
0753. Photon emits a Socket.IO `new-message` event.
0754. Photon listener receives event and logs entry.
0755. Listener drops empty or self messages.
0756. Listener extracts sender handle and text.
0757. Listener extracts attachment info if present.
0758. Listener computes chat GUID and group status.
0759. Listener runs in-memory dedupe check.
0760. Listener computes idempotency key.
0761. Listener checks Redis idempotency (unless kafka ingest).
0762. Listener caches chat GUID for DMs.
0763. Listener builds SendBlue-compatible payload.
0764. Listener forwards payload to callback.
0765. Callback chooses Kafka publish if ingest mode is kafka.
0766. Kafka event is built from payload.
0767. Kafka producer ensures topics exist.
0768. Kafka producer sends event to inbound topic.
0769. Kafka consumer polls for messages.
0770. Kafka consumer decodes event JSON.
0771. Kafka consumer applies retry delay if present.
0772. Kafka consumer invokes `_handle_kafka_event`.
0773. Kafka event idempotency check runs in Redis.
0774. Orchestrator handles the message.
0775. Orchestrator decides group chat vs DM flow.
0776. Interaction agent processes DM messages.
0777. Responses are sent via Photon client.
0778. Orchestrator logs latency metrics.
0779. On error, Kafka pipeline schedules retry or DLQ.
0780. --- Message cycle 26 ends ---
0781. --- Message cycle 27 begins ---
0782. Photon emits a Socket.IO `new-message` event.
0783. Photon listener receives event and logs entry.
0784. Listener drops empty or self messages.
0785. Listener extracts sender handle and text.
0786. Listener extracts attachment info if present.
0787. Listener computes chat GUID and group status.
0788. Listener runs in-memory dedupe check.
0789. Listener computes idempotency key.
0790. Listener checks Redis idempotency (unless kafka ingest).
0791. Listener caches chat GUID for DMs.
0792. Listener builds SendBlue-compatible payload.
0793. Listener forwards payload to callback.
0794. Callback chooses Kafka publish if ingest mode is kafka.
0795. Kafka event is built from payload.
0796. Kafka producer ensures topics exist.
0797. Kafka producer sends event to inbound topic.
0798. Kafka consumer polls for messages.
0799. Kafka consumer decodes event JSON.
0800. Kafka consumer applies retry delay if present.
0801. Kafka consumer invokes `_handle_kafka_event`.
0802. Kafka event idempotency check runs in Redis.
0803. Orchestrator handles the message.
0804. Orchestrator decides group chat vs DM flow.
0805. Interaction agent processes DM messages.
0806. Responses are sent via Photon client.
0807. Orchestrator logs latency metrics.
0808. On error, Kafka pipeline schedules retry or DLQ.
0809. --- Message cycle 27 ends ---
0810. --- Message cycle 28 begins ---
0811. Photon emits a Socket.IO `new-message` event.
0812. Photon listener receives event and logs entry.
0813. Listener drops empty or self messages.
0814. Listener extracts sender handle and text.
0815. Listener extracts attachment info if present.
0816. Listener computes chat GUID and group status.
0817. Listener runs in-memory dedupe check.
0818. Listener computes idempotency key.
0819. Listener checks Redis idempotency (unless kafka ingest).
0820. Listener caches chat GUID for DMs.
0821. Listener builds SendBlue-compatible payload.
0822. Listener forwards payload to callback.
0823. Callback chooses Kafka publish if ingest mode is kafka.
0824. Kafka event is built from payload.
0825. Kafka producer ensures topics exist.
0826. Kafka producer sends event to inbound topic.
0827. Kafka consumer polls for messages.
0828. Kafka consumer decodes event JSON.
0829. Kafka consumer applies retry delay if present.
0830. Kafka consumer invokes `_handle_kafka_event`.
0831. Kafka event idempotency check runs in Redis.
0832. Orchestrator handles the message.
0833. Orchestrator decides group chat vs DM flow.
0834. Interaction agent processes DM messages.
0835. Responses are sent via Photon client.
0836. Orchestrator logs latency metrics.
0837. On error, Kafka pipeline schedules retry or DLQ.
0838. --- Message cycle 28 ends ---
0839. --- Message cycle 29 begins ---
0840. Photon emits a Socket.IO `new-message` event.
0841. Photon listener receives event and logs entry.
0842. Listener drops empty or self messages.
0843. Listener extracts sender handle and text.
0844. Listener extracts attachment info if present.
0845. Listener computes chat GUID and group status.
0846. Listener runs in-memory dedupe check.
0847. Listener computes idempotency key.
0848. Listener checks Redis idempotency (unless kafka ingest).
0849. Listener caches chat GUID for DMs.
0850. Listener builds SendBlue-compatible payload.
0851. Listener forwards payload to callback.
0852. Callback chooses Kafka publish if ingest mode is kafka.
0853. Kafka event is built from payload.
0854. Kafka producer ensures topics exist.
0855. Kafka producer sends event to inbound topic.
0856. Kafka consumer polls for messages.
0857. Kafka consumer decodes event JSON.
0858. Kafka consumer applies retry delay if present.
0859. Kafka consumer invokes `_handle_kafka_event`.
0860. Kafka event idempotency check runs in Redis.
0861. Orchestrator handles the message.
0862. Orchestrator decides group chat vs DM flow.
0863. Interaction agent processes DM messages.
0864. Responses are sent via Photon client.
0865. Orchestrator logs latency metrics.
0866. On error, Kafka pipeline schedules retry or DLQ.
0867. --- Message cycle 29 ends ---
0868. --- Message cycle 30 begins ---
0869. Photon emits a Socket.IO `new-message` event.
0870. Photon listener receives event and logs entry.
0871. Listener drops empty or self messages.
0872. Listener extracts sender handle and text.
0873. Listener extracts attachment info if present.
0874. Listener computes chat GUID and group status.
0875. Listener runs in-memory dedupe check.
0876. Listener computes idempotency key.
0877. Listener checks Redis idempotency (unless kafka ingest).
0878. Listener caches chat GUID for DMs.
0879. Listener builds SendBlue-compatible payload.
0880. Listener forwards payload to callback.
0881. Callback chooses Kafka publish if ingest mode is kafka.
0882. Kafka event is built from payload.
0883. Kafka producer ensures topics exist.
0884. Kafka producer sends event to inbound topic.
0885. Kafka consumer polls for messages.
0886. Kafka consumer decodes event JSON.
0887. Kafka consumer applies retry delay if present.
0888. Kafka consumer invokes `_handle_kafka_event`.
0889. Kafka event idempotency check runs in Redis.
0890. Orchestrator handles the message.
0891. Orchestrator decides group chat vs DM flow.
0892. Interaction agent processes DM messages.
0893. Responses are sent via Photon client.
0894. Orchestrator logs latency metrics.
0895. On error, Kafka pipeline schedules retry or DLQ.
0896. --- Message cycle 30 ends ---
0897. --- Message cycle 31 begins ---
0898. Photon emits a Socket.IO `new-message` event.
0899. Photon listener receives event and logs entry.
0900. Listener drops empty or self messages.
0901. Listener extracts sender handle and text.
0902. Listener extracts attachment info if present.
0903. Listener computes chat GUID and group status.
0904. Listener runs in-memory dedupe check.
0905. Listener computes idempotency key.
0906. Listener checks Redis idempotency (unless kafka ingest).
0907. Listener caches chat GUID for DMs.
0908. Listener builds SendBlue-compatible payload.
0909. Listener forwards payload to callback.
0910. Callback chooses Kafka publish if ingest mode is kafka.
0911. Kafka event is built from payload.
0912. Kafka producer ensures topics exist.
0913. Kafka producer sends event to inbound topic.
0914. Kafka consumer polls for messages.
0915. Kafka consumer decodes event JSON.
0916. Kafka consumer applies retry delay if present.
0917. Kafka consumer invokes `_handle_kafka_event`.
0918. Kafka event idempotency check runs in Redis.
0919. Orchestrator handles the message.
0920. Orchestrator decides group chat vs DM flow.
0921. Interaction agent processes DM messages.
0922. Responses are sent via Photon client.
0923. Orchestrator logs latency metrics.
0924. On error, Kafka pipeline schedules retry or DLQ.
0925. --- Message cycle 31 ends ---
0926. --- Message cycle 32 begins ---
0927. Photon emits a Socket.IO `new-message` event.
0928. Photon listener receives event and logs entry.
0929. Listener drops empty or self messages.
0930. Listener extracts sender handle and text.
0931. Listener extracts attachment info if present.
0932. Listener computes chat GUID and group status.
0933. Listener runs in-memory dedupe check.
0934. Listener computes idempotency key.
0935. Listener checks Redis idempotency (unless kafka ingest).
0936. Listener caches chat GUID for DMs.
0937. Listener builds SendBlue-compatible payload.
0938. Listener forwards payload to callback.
0939. Callback chooses Kafka publish if ingest mode is kafka.
0940. Kafka event is built from payload.
0941. Kafka producer ensures topics exist.
0942. Kafka producer sends event to inbound topic.
0943. Kafka consumer polls for messages.
0944. Kafka consumer decodes event JSON.
0945. Kafka consumer applies retry delay if present.
0946. Kafka consumer invokes `_handle_kafka_event`.
0947. Kafka event idempotency check runs in Redis.
0948. Orchestrator handles the message.
0949. Orchestrator decides group chat vs DM flow.
0950. Interaction agent processes DM messages.
0951. Responses are sent via Photon client.
0952. Orchestrator logs latency metrics.
0953. On error, Kafka pipeline schedules retry or DLQ.
0954. --- Message cycle 32 ends ---
0955. --- Message cycle 33 begins ---
0956. Photon emits a Socket.IO `new-message` event.
0957. Photon listener receives event and logs entry.
0958. Listener drops empty or self messages.
0959. Listener extracts sender handle and text.
0960. Listener extracts attachment info if present.
0961. Listener computes chat GUID and group status.
0962. Listener runs in-memory dedupe check.
0963. Listener computes idempotency key.
0964. Listener checks Redis idempotency (unless kafka ingest).
0965. Listener caches chat GUID for DMs.
0966. Listener builds SendBlue-compatible payload.
0967. Listener forwards payload to callback.
0968. Callback chooses Kafka publish if ingest mode is kafka.
0969. Kafka event is built from payload.
0970. Kafka producer ensures topics exist.
0971. Kafka producer sends event to inbound topic.
0972. Kafka consumer polls for messages.
0973. Kafka consumer decodes event JSON.
0974. Kafka consumer applies retry delay if present.
0975. Kafka consumer invokes `_handle_kafka_event`.
0976. Kafka event idempotency check runs in Redis.
0977. Orchestrator handles the message.
0978. Orchestrator decides group chat vs DM flow.
0979. Interaction agent processes DM messages.
0980. Responses are sent via Photon client.
0981. Orchestrator logs latency metrics.
0982. On error, Kafka pipeline schedules retry or DLQ.
0983. --- Message cycle 33 ends ---
0984. --- Message cycle 34 begins ---
0985. Photon emits a Socket.IO `new-message` event.
0986. Photon listener receives event and logs entry.
0987. Listener drops empty or self messages.
0988. Listener extracts sender handle and text.
0989. Listener extracts attachment info if present.
0990. Listener computes chat GUID and group status.
0991. Listener runs in-memory dedupe check.
0992. Listener computes idempotency key.
0993. Listener checks Redis idempotency (unless kafka ingest).
0994. Listener caches chat GUID for DMs.
0995. Listener builds SendBlue-compatible payload.
0996. Listener forwards payload to callback.
0997. Callback chooses Kafka publish if ingest mode is kafka.
0998. Kafka event is built from payload.
0999. Kafka producer ensures topics exist.
1000. Kafka producer sends event to inbound topic.
1001. Kafka consumer polls for messages.
1002. Kafka consumer decodes event JSON.
1003. Kafka consumer applies retry delay if present.
1004. Kafka consumer invokes `_handle_kafka_event`.
1005. Kafka event idempotency check runs in Redis.
1006. Orchestrator handles the message.
1007. Orchestrator decides group chat vs DM flow.
1008. Interaction agent processes DM messages.
1009. Responses are sent via Photon client.
1010. Orchestrator logs latency metrics.
1011. On error, Kafka pipeline schedules retry or DLQ.
1012. --- Message cycle 34 ends ---
1013. --- Message cycle 35 begins ---
1014. Photon emits a Socket.IO `new-message` event.
1015. Photon listener receives event and logs entry.
1016. Listener drops empty or self messages.
1017. Listener extracts sender handle and text.
1018. Listener extracts attachment info if present.
1019. Listener computes chat GUID and group status.
1020. Listener runs in-memory dedupe check.
1021. Listener computes idempotency key.
1022. Listener checks Redis idempotency (unless kafka ingest).
1023. Listener caches chat GUID for DMs.
1024. Listener builds SendBlue-compatible payload.
1025. Listener forwards payload to callback.
1026. Callback chooses Kafka publish if ingest mode is kafka.
1027. Kafka event is built from payload.
1028. Kafka producer ensures topics exist.
1029. Kafka producer sends event to inbound topic.
1030. Kafka consumer polls for messages.
1031. Kafka consumer decodes event JSON.
1032. Kafka consumer applies retry delay if present.
1033. Kafka consumer invokes `_handle_kafka_event`.
1034. Kafka event idempotency check runs in Redis.
1035. Orchestrator handles the message.
1036. Orchestrator decides group chat vs DM flow.
1037. Interaction agent processes DM messages.
1038. Responses are sent via Photon client.
1039. Orchestrator logs latency metrics.
1040. On error, Kafka pipeline schedules retry or DLQ.
1041. --- Message cycle 35 ends ---
1042. --- Message cycle 36 begins ---
1043. Photon emits a Socket.IO `new-message` event.
1044. Photon listener receives event and logs entry.
1045. Listener drops empty or self messages.
1046. Listener extracts sender handle and text.
1047. Listener extracts attachment info if present.
1048. Listener computes chat GUID and group status.
1049. Listener runs in-memory dedupe check.
1050. Listener computes idempotency key.
1051. Listener checks Redis idempotency (unless kafka ingest).
1052. Listener caches chat GUID for DMs.
1053. Listener builds SendBlue-compatible payload.
1054. Listener forwards payload to callback.
1055. Callback chooses Kafka publish if ingest mode is kafka.
1056. Kafka event is built from payload.
1057. Kafka producer ensures topics exist.
1058. Kafka producer sends event to inbound topic.
1059. Kafka consumer polls for messages.
1060. Kafka consumer decodes event JSON.
1061. Kafka consumer applies retry delay if present.
1062. Kafka consumer invokes `_handle_kafka_event`.
1063. Kafka event idempotency check runs in Redis.
1064. Orchestrator handles the message.
1065. Orchestrator decides group chat vs DM flow.
1066. Interaction agent processes DM messages.
1067. Responses are sent via Photon client.
1068. Orchestrator logs latency metrics.
1069. On error, Kafka pipeline schedules retry or DLQ.
1070. --- Message cycle 36 ends ---
1071. --- Message cycle 37 begins ---
1072. Photon emits a Socket.IO `new-message` event.
1073. Photon listener receives event and logs entry.
1074. Listener drops empty or self messages.
1075. Listener extracts sender handle and text.
1076. Listener extracts attachment info if present.
1077. Listener computes chat GUID and group status.
1078. Listener runs in-memory dedupe check.
1079. Listener computes idempotency key.
1080. Listener checks Redis idempotency (unless kafka ingest).
1081. Listener caches chat GUID for DMs.
1082. Listener builds SendBlue-compatible payload.
1083. Listener forwards payload to callback.
1084. Callback chooses Kafka publish if ingest mode is kafka.
1085. Kafka event is built from payload.
1086. Kafka producer ensures topics exist.
1087. Kafka producer sends event to inbound topic.
1088. Kafka consumer polls for messages.
1089. Kafka consumer decodes event JSON.
1090. Kafka consumer applies retry delay if present.
1091. Kafka consumer invokes `_handle_kafka_event`.
1092. Kafka event idempotency check runs in Redis.
1093. Orchestrator handles the message.
1094. Orchestrator decides group chat vs DM flow.
1095. Interaction agent processes DM messages.
1096. Responses are sent via Photon client.
1097. Orchestrator logs latency metrics.
1098. On error, Kafka pipeline schedules retry or DLQ.
1099. --- Message cycle 37 ends ---
1100. --- Message cycle 38 begins ---
1101. Photon emits a Socket.IO `new-message` event.
1102. Photon listener receives event and logs entry.
1103. Listener drops empty or self messages.
1104. Listener extracts sender handle and text.
1105. Listener extracts attachment info if present.
1106. Listener computes chat GUID and group status.
1107. Listener runs in-memory dedupe check.
1108. Listener computes idempotency key.
1109. Listener checks Redis idempotency (unless kafka ingest).
1110. Listener caches chat GUID for DMs.
1111. Listener builds SendBlue-compatible payload.
1112. Listener forwards payload to callback.
1113. Callback chooses Kafka publish if ingest mode is kafka.
1114. Kafka event is built from payload.
1115. Kafka producer ensures topics exist.
1116. Kafka producer sends event to inbound topic.
1117. Kafka consumer polls for messages.
1118. Kafka consumer decodes event JSON.
1119. Kafka consumer applies retry delay if present.
1120. Kafka consumer invokes `_handle_kafka_event`.
1121. Kafka event idempotency check runs in Redis.
1122. Orchestrator handles the message.
1123. Orchestrator decides group chat vs DM flow.
1124. Interaction agent processes DM messages.
1125. Responses are sent via Photon client.
1126. Orchestrator logs latency metrics.
1127. On error, Kafka pipeline schedules retry or DLQ.
1128. --- Message cycle 38 ends ---
1129. --- Message cycle 39 begins ---
1130. Photon emits a Socket.IO `new-message` event.
1131. Photon listener receives event and logs entry.
1132. Listener drops empty or self messages.
1133. Listener extracts sender handle and text.
1134. Listener extracts attachment info if present.
1135. Listener computes chat GUID and group status.
1136. Listener runs in-memory dedupe check.
1137. Listener computes idempotency key.
1138. Listener checks Redis idempotency (unless kafka ingest).
1139. Listener caches chat GUID for DMs.
1140. Listener builds SendBlue-compatible payload.
1141. Listener forwards payload to callback.
1142. Callback chooses Kafka publish if ingest mode is kafka.
1143. Kafka event is built from payload.
1144. Kafka producer ensures topics exist.
1145. Kafka producer sends event to inbound topic.
1146. Kafka consumer polls for messages.
1147. Kafka consumer decodes event JSON.
1148. Kafka consumer applies retry delay if present.
1149. Kafka consumer invokes `_handle_kafka_event`.
1150. Kafka event idempotency check runs in Redis.
1151. Orchestrator handles the message.
1152. Orchestrator decides group chat vs DM flow.
1153. Interaction agent processes DM messages.
1154. Responses are sent via Photon client.
1155. Orchestrator logs latency metrics.
1156. On error, Kafka pipeline schedules retry or DLQ.
1157. --- Message cycle 39 ends ---
1158. --- Message cycle 40 begins ---
1159. Photon emits a Socket.IO `new-message` event.
1160. Photon listener receives event and logs entry.
1161. Listener drops empty or self messages.
1162. Listener extracts sender handle and text.
1163. Listener extracts attachment info if present.
1164. Listener computes chat GUID and group status.
1165. Listener runs in-memory dedupe check.
1166. Listener computes idempotency key.
1167. Listener checks Redis idempotency (unless kafka ingest).
1168. Listener caches chat GUID for DMs.
1169. Listener builds SendBlue-compatible payload.
1170. Listener forwards payload to callback.
1171. Callback chooses Kafka publish if ingest mode is kafka.
1172. Kafka event is built from payload.
1173. Kafka producer ensures topics exist.
1174. Kafka producer sends event to inbound topic.
1175. Kafka consumer polls for messages.
1176. Kafka consumer decodes event JSON.
1177. Kafka consumer applies retry delay if present.
1178. Kafka consumer invokes `_handle_kafka_event`.
1179. Kafka event idempotency check runs in Redis.
1180. Orchestrator handles the message.
1181. Orchestrator decides group chat vs DM flow.
1182. Interaction agent processes DM messages.
1183. Responses are sent via Photon client.
1184. Orchestrator logs latency metrics.
1185. On error, Kafka pipeline schedules retry or DLQ.
1186. --- Message cycle 40 ends ---
