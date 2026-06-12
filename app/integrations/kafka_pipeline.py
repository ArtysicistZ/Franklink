from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import ssl
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Optional, Set, Tuple

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.abc import AbstractTokenProvider
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from aiokafka.structs import TopicPartition

from app.config import settings

logger = logging.getLogger(__name__)

_RETRY_DELAYS_SECONDS = (30, 120, 600)
_TOPIC_BOOTSTRAP_LOCK = asyncio.Lock()
_TOPIC_BOOTSTRAP_DONE = False


@dataclass
class _PartitionTracker:
    """Tracks per-partition concurrent processing and contiguous commit state."""

    last_committed: int = -1
    initialized: bool = False
    completed: Set[int] = field(default_factory=set)
    inflight: Set[int] = field(default_factory=set)
    key_locks: Dict[str, asyncio.Lock] = field(default_factory=dict)
    key_last_used: Dict[str, float] = field(default_factory=dict)
    max_seen_offset: int = -1
    paused_for_gap: bool = False
    generation_id: int = 0
    tracker_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    commit_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

try:
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
except Exception:  # pragma: no cover - optional dependency for IAM auth
    MSKAuthTokenProvider = None


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


_IAM_TOKEN_PROVIDER: Optional[_MskIamTokenProvider] = None


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


def _get_iam_token_provider() -> _MskIamTokenProvider:
    global _IAM_TOKEN_PROVIDER
    if _IAM_TOKEN_PROVIDER is not None:
        return _IAM_TOKEN_PROVIDER
    region = _resolve_iam_region()
    if not region:
        raise RuntimeError("Kafka IAM auth requires AWS region (set KAFKA_IAM_REGION or AWS_REGION)")
    _IAM_TOKEN_PROVIDER = _MskIamTokenProvider(region)
    return _IAM_TOKEN_PROVIDER


def _reset_iam_token_provider() -> None:
    global _IAM_TOKEN_PROVIDER
    _IAM_TOKEN_PROVIDER = None


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _iter_exception_chain(exc: BaseException):
    seen = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


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


def _build_idempotency_key(payload: Dict[str, Any], message_id: str) -> str:
    env_prefix = "dev_" if settings.app_env == "development" else ""
    message_id = str(message_id or "").strip()
    if message_id and not message_id.startswith("photon_hash:"):
        return f"{env_prefix}photon_msg:{message_id}"
    if message_id.startswith("photon_hash:"):
        return f"{env_prefix}photon_msg_hash:{message_id.split('photon_hash:', 1)[1]}"
    fallback = _compute_fallback_event_id(payload)
    return f"{env_prefix}photon_msg_hash:{fallback}"


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
        "is_location_share": bool(payload.get("is_location_share")),
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


class KafkaInboundConsumer:
    def __init__(self, handler: Callable[[Dict[str, Any]], Awaitable[None]]) -> None:
        self._handler = handler
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._producer: Optional[AIOKafkaProducer] = None
        self._task: Optional[asyncio.Task] = None
        self._closing = asyncio.Event()
        self._inflight: set[asyncio.Task] = set()
        self._semaphore = asyncio.Semaphore(int(getattr(settings, "kafka_consumer_max_inflight", 20) or 20))
        self._keyed_concurrency_enabled = bool(
            getattr(settings, "kafka_keyed_concurrency_enabled", False)
        )
        self._partition_locks: Dict[Tuple[str, int], asyncio.Lock] = defaultdict(asyncio.Lock)
        self._partition_trackers: Dict[TopicPartition, _PartitionTracker] = {}
        self._assignment_snapshot: Set[TopicPartition] = set()
        self._key_lock_idle_ttl_seconds = int(getattr(settings, "kafka_key_lock_idle_ttl_seconds", 600) or 600)
        self._key_locks_max_per_partition = int(
            getattr(settings, "kafka_key_locks_max_per_partition", 2000) or 2000
        )
        self._max_commit_gap_per_partition = int(
            getattr(settings, "kafka_max_commit_gap_per_partition", 500) or 500
        )
        self._commit_gap_resume_watermark = int(
            getattr(settings, "kafka_commit_gap_resume_watermark", 200) or 200
        )
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
        self._assignment_snapshot = set()
        self._partition_trackers = {}
        self._consumer = self._build_consumer()
        self._producer = self._build_producer()
        await self._producer.start()
        await self._consumer.start()
        self._task = asyncio.create_task(self._run(), name="kafka-inbound-consumer")
        logger.info(
            "[KAFKA] Consumer started topics=%s group=%s keyed_concurrency=%s",
            self._consumer_topics(),
            settings.kafka_group_id,
            self._keyed_concurrency_enabled,
        )

    async def stop(self) -> None:
        self._closing.set()
        if self._task is not None:
            await self._task
            self._task = None
        if self._keyed_concurrency_enabled:
            await self._drain_and_finalize_commits()
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
        self._assignment_snapshot = set()
        self._partition_trackers = {}
        logger.info("[KAFKA] Consumer stopped")

    async def _run(self) -> None:
        assert self._consumer is not None
        poll_ms = int(getattr(settings, "kafka_consumer_poll_ms", 1000) or 1000)
        max_batch = int(getattr(settings, "kafka_consumer_max_batch", 50) or 50)
        while not self._closing.is_set():
            if self._keyed_concurrency_enabled:
                await self._reconcile_assignments()
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
            if self._keyed_concurrency_enabled:
                await self._reconcile_assignments()
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
            if self._keyed_concurrency_enabled:
                for tracker in self._partition_trackers.values():
                    async with tracker.tracker_lock:
                        tracker.generation_id += 1
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
            self._assignment_snapshot = set()
            _reset_iam_token_provider()
            await asyncio.sleep(self._restart_delay_seconds)
            await ensure_kafka_topics()
            self._consumer = self._build_consumer()
            self._producer = self._build_producer()
            await self._producer.start()
            await self._consumer.start()
            if self._keyed_concurrency_enabled:
                await self._reconcile_assignments()
            logger.info("[KAFKA] Consumer restarted topics=%s group=%s", self._consumer_topics(), settings.kafka_group_id)

    def _on_task_done(self, task: asyncio.Task) -> None:
        self._inflight.discard(task)
        self._semaphore.release()

    def _get_tracker(self, tp: TopicPartition) -> _PartitionTracker:
        tracker = self._partition_trackers.get(tp)
        if tracker is None:
            tracker = _PartitionTracker()
            self._partition_trackers[tp] = tracker
        return tracker

    async def _reconcile_assignments(self) -> None:
        if not self._keyed_concurrency_enabled or self._consumer is None:
            return
        current = set(self._consumer.assignment() or set())
        if current == self._assignment_snapshot:
            return

        revoked = self._assignment_snapshot - current
        added = current - self._assignment_snapshot

        for tp in revoked:
            tracker = self._partition_trackers.get(tp)
            if tracker is None:
                continue
            async with tracker.tracker_lock:
                tracker.generation_id += 1
                tracker.paused_for_gap = False

        for tp in added:
            tracker = self._get_tracker(tp)
            async with tracker.tracker_lock:
                tracker.generation_id += 1
                tracker.paused_for_gap = False

        self._assignment_snapshot = current
        logger.info(
            "[KAFKA] Assignment changed added=%s revoked=%s",
            [f"{tp.topic}:{tp.partition}" for tp in sorted(added, key=lambda x: (x.topic, x.partition))],
            [f"{tp.topic}:{tp.partition}" for tp in sorted(revoked, key=lambda x: (x.topic, x.partition))],
        )

    def _derive_lock_key(self, event: Optional[Dict[str, Any]], record) -> str:
        if event:
            key = str(
                event.get("partition_key")
                or event.get("chat_guid")
                or event.get("from_number")
                or event.get("event_id")
                or ""
            ).strip()
            if key:
                return key
        return f"offset:{record.topic}:{record.partition}:{record.offset}"

    def _get_key_lock(self, tracker: _PartitionTracker, key: str) -> asyncio.Lock:
        lock = tracker.key_locks.get(key)
        if lock is None:
            if len(tracker.key_locks) >= self._key_locks_max_per_partition:
                self._evict_idle_key_locks(tracker)
            lock = tracker.key_locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                tracker.key_locks[key] = lock
        tracker.key_last_used[key] = time.time()
        return lock

    def _evict_idle_key_locks(self, tracker: _PartitionTracker) -> None:
        cutoff = time.time() - self._key_lock_idle_ttl_seconds
        to_delete = [
            key
            for key, last_used in tracker.key_last_used.items()
            if last_used < cutoff and key in tracker.key_locks and not tracker.key_locks[key].locked()
        ]
        for key in to_delete:
            tracker.key_locks.pop(key, None)
            tracker.key_last_used.pop(key, None)

    def _current_gap_depth(self, tracker: _PartitionTracker) -> int:
        if tracker.max_seen_offset < tracker.last_committed:
            return 0
        return max(0, tracker.max_seen_offset - tracker.last_committed)

    async def _maybe_adjust_partition_flow_locked(self, tp: TopicPartition, tracker: _PartitionTracker) -> None:
        if self._consumer is None:
            return
        if tp not in self._assignment_snapshot:
            return
        gap_depth = self._current_gap_depth(tracker)
        if gap_depth > self._max_commit_gap_per_partition and not tracker.paused_for_gap:
            try:
                self._consumer.pause(tp)
                tracker.paused_for_gap = True
                logger.warning(
                    "[KAFKA] Paused partition topic=%s partition=%s gap_depth=%d max_gap=%d",
                    tp.topic,
                    tp.partition,
                    gap_depth,
                    self._max_commit_gap_per_partition,
                )
            except Exception:
                logger.exception("[KAFKA] Failed to pause partition %s-%s", tp.topic, tp.partition)
            return
        if tracker.paused_for_gap and gap_depth <= self._commit_gap_resume_watermark:
            try:
                self._consumer.resume(tp)
                tracker.paused_for_gap = False
                logger.info(
                    "[KAFKA] Resumed partition topic=%s partition=%s gap_depth=%d resume_at=%d",
                    tp.topic,
                    tp.partition,
                    gap_depth,
                    self._commit_gap_resume_watermark,
                )
            except Exception:
                logger.exception("[KAFKA] Failed to resume partition %s-%s", tp.topic, tp.partition)

    async def _advance_commit_locked(self, tp: TopicPartition, tracker: _PartitionTracker) -> None:
        if self._consumer is None:
            return
        if tp not in self._assignment_snapshot:
            return
        candidate = tracker.last_committed + 1
        while candidate in tracker.completed:
            candidate += 1
        new_last = candidate - 1
        if new_last <= tracker.last_committed:
            return
        try:
            async with tracker.commit_lock:
                await self._consumer.commit({tp: new_last + 1})
        except Exception as exc:
            logger.warning(
                "[KAFKA] Commit advance failed topic=%s partition=%s from=%d to=%d err=%s",
                tp.topic,
                tp.partition,
                tracker.last_committed,
                new_last,
                exc,
            )
            return

        old_last = tracker.last_committed
        tracker.last_committed = new_last
        for done_offset in range(old_last + 1, new_last + 1):
            tracker.completed.discard(done_offset)

    async def _drain_and_finalize_commits(self) -> None:
        if not self._partition_trackers:
            return
        for tp, tracker in list(self._partition_trackers.items()):
            async with tracker.tracker_lock:
                await self._advance_commit_locked(tp, tracker)

    async def _handle_record(self, record) -> None:
        assert self._consumer is not None
        tp = TopicPartition(record.topic, record.partition)
        if not self._keyed_concurrency_enabled:
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
            return

        tracker = self._get_tracker(tp)
        event = self._decode_event(record)
        task_generation = 0
        async with tracker.tracker_lock:
            if not tracker.initialized:
                tracker.last_committed = record.offset - 1
                tracker.initialized = True
            tracker.inflight.add(record.offset)
            tracker.max_seen_offset = max(tracker.max_seen_offset, record.offset)
            task_generation = tracker.generation_id

        handled = False
        key = self._derive_lock_key(event, record)
        key_lock = self._get_key_lock(tracker, key)
        try:
            async with key_lock:
                if event is None:
                    handled = True
                else:
                    retry_after = int(event.get("retry_after_seconds") or 0)
                    if retry_after > 0:
                        await asyncio.sleep(retry_after)

                    try:
                        await self._handler(event)
                        handled = True
                    except Exception as exc:
                        handled = await self._handle_failure(event, exc)
        finally:
            async with tracker.tracker_lock:
                tracker.inflight.discard(record.offset)
                tracker.key_last_used[key] = time.time()
                if task_generation != tracker.generation_id:
                    logger.debug(
                        "[KAFKA] Dropped stale task finalize topic=%s partition=%s offset=%s task_generation=%s generation=%s",
                        tp.topic,
                        tp.partition,
                        record.offset,
                        task_generation,
                        tracker.generation_id,
                    )
                    return
                if handled:
                    tracker.completed.add(record.offset)
                await self._advance_commit_locked(tp, tracker)
                await self._maybe_adjust_partition_flow_locked(tp, tracker)

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


def _retry_delay_seconds(attempt: int) -> int:
    idx = min(max(1, attempt) - 1, len(_RETRY_DELAYS_SECONDS) - 1)
    return int(_RETRY_DELAYS_SECONDS[idx])


def _retry_topic(attempt: int) -> str:
    topics = (
        settings.kafka_topic_retry_30s,
        settings.kafka_topic_retry_2m,
        settings.kafka_topic_retry_10m,
    )
    idx = min(max(1, attempt) - 1, len(topics) - 1)
    return topics[idx]


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


def _kafka_required_topics() -> list[str]:
    return [
        settings.kafka_topic_inbound,
        settings.kafka_topic_retry_30s,
        settings.kafka_topic_retry_2m,
        settings.kafka_topic_retry_10m,
        settings.kafka_topic_dlq,
    ]


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
