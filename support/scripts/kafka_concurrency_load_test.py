"""
Kafka concurrency load test publisher.

Publishes synthetic inbound events to the Kafka inbound topic so you can test
worker concurrency without real end users.

Examples:
  # Baseline: 1 user, 50 messages (local Docker Kafka)
  python support/scripts/kafka_concurrency_load_test.py \
    --bootstrap-servers localhost:29092 \
    --security-protocol PLAINTEXT \
    --users 1 --messages-per-user 50 \
    --test-run baseline_1u

  # Pressure: 50 users, 20 messages each
  python support/scripts/kafka_concurrency_load_test.py \
    --bootstrap-servers localhost:29092 \
    --security-protocol PLAINTEXT \
    --users 50 --messages-per-user 20 \
    --max-inflight 200 \
    --test-run load_50u

  # Hot-key pressure: many users mapped to few keys (contention test)
  python support/scripts/kafka_concurrency_load_test.py \
    --bootstrap-servers localhost:29092 \
    --security-protocol PLAINTEXT \
    --users 50 --messages-per-user 20 \
    --partition-mode hot --hot-key-count 2 \
    --test-run hotkey_50u
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import re
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaProducer
from aiokafka.abc import AbstractTokenProvider

try:
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
except Exception:
    MSKAuthTokenProvider = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_region(explicit: Optional[str], bootstrap_servers: str) -> Optional[str]:
    if explicit:
        return explicit.strip()
    env_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if env_region:
        return env_region.strip()
    match = re.search(r"\.(us-[a-z]+-\d)\.", bootstrap_servers)
    if match:
        return match.group(1)
    return None


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
                    self._expires_at = expiry_seconds - 60.0
                else:
                    self._expires_at = now + max(1.0, expiry_seconds - 60.0)
            else:
                self._expires_at = now + 14 * 60
            return self._token


def _security_config(
    *,
    bootstrap_servers: str,
    security_protocol: str,
    sasl_mechanism: Optional[str],
    username: Optional[str],
    password: Optional[str],
    iam_region: Optional[str],
) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {}
    protocol = (security_protocol or "PLAINTEXT").strip().upper()
    mechanism = (sasl_mechanism or "").strip().upper()

    if mechanism in {"AWS_MSK_IAM", "OAUTHBEARER"} and not protocol.startswith("SASL"):
        protocol = "SASL_SSL"

    cfg["security_protocol"] = protocol

    if protocol in {"SSL", "SASL_SSL"}:
        import ssl

        cfg["ssl_context"] = ssl.create_default_context()

    if protocol.startswith("SASL"):
        if mechanism in {"AWS_MSK_IAM", "OAUTHBEARER"}:
            region = _resolve_region(iam_region, bootstrap_servers)
            if not region:
                raise RuntimeError("Could not resolve AWS region for IAM auth")
            cfg["sasl_mechanism"] = "OAUTHBEARER"
            cfg["sasl_oauth_token_provider"] = _MskIamTokenProvider(region)
        else:
            cfg["sasl_mechanism"] = mechanism or "PLAIN"
            if username:
                cfg["sasl_plain_username"] = username
            if password:
                cfg["sasl_plain_password"] = password

    return cfg


@dataclass
class LoadConfig:
    topic: str
    test_run: str
    users: int
    messages_per_user: int
    max_inflight: int
    send_interval_ms: int
    partition_mode: str
    hot_key_count: int
    shuffle: bool


def _phone_for_user(user_idx: int) -> str:
    # Deterministic synthetic E.164-like number
    return f"+1555{user_idx:07d}"


def _partition_key_for_user(user_idx: int, mode: str, hot_key_count: int) -> str:
    if mode == "hot":
        slot = user_idx % max(1, hot_key_count)
        return f"hot-key-{slot:03d}"
    return f"user-key-{user_idx:05d}"


def _build_event(*, user_idx: int, seq: int, cfg: LoadConfig) -> Dict[str, Any]:
    now_iso = _now_iso()
    from_number = _phone_for_user(user_idx)
    message_id = f"load-{cfg.test_run}-u{user_idx:05d}-m{seq:05d}-{uuid.uuid4().hex[:10]}"
    event_id = message_id
    partition_key = _partition_key_for_user(user_idx, cfg.partition_mode, cfg.hot_key_count)
    idempotency_key = f"load:{cfg.test_run}:{event_id}"

    return {
        "schema_version": 1,
        "event_id": event_id,
        "idempotency_key": idempotency_key,
        "source": "kafka-load-test",
        "received_at": now_iso,
        "payload_timestamp": now_iso,
        "from_number": from_number,
        "to_number": "+10000000000",
        "content": f"[load-test {cfg.test_run}] user={user_idx} seq={seq}",
        "media_url": None,
        "message_id": message_id,
        "chat_guid": f"any;-;{from_number}",
        "is_group": False,
        "attempt": 0,
        "trace_id": f"{cfg.test_run}-{uuid.uuid4().hex[:16]}",
        "partition_key": partition_key,
        "test_run": cfg.test_run,
    }


async def _publish(cfg: LoadConfig, producer: AIOKafkaProducer) -> None:
    total = cfg.users * cfg.messages_per_user
    events = [
        _build_event(user_idx=u, seq=m, cfg=cfg)
        for u in range(1, cfg.users + 1)
        for m in range(1, cfg.messages_per_user + 1)
    ]
    if cfg.shuffle:
        random.shuffle(events)

    semaphore = asyncio.Semaphore(cfg.max_inflight)
    sent = 0
    failures = 0
    started = time.perf_counter()
    print(
        f"[LOAD] Start publish test_run={cfg.test_run} total={total} users={cfg.users} "
        f"messages_per_user={cfg.messages_per_user} mode={cfg.partition_mode}"
    )

    async def send_one(event: Dict[str, Any]) -> None:
        nonlocal sent, failures
        payload = json.dumps(event, ensure_ascii=True).encode("utf-8")
        key = str(event["partition_key"]).encode("utf-8")
        try:
            await producer.send_and_wait(cfg.topic, value=payload, key=key)
            sent += 1
            if sent % 100 == 0 or sent == total:
                elapsed = time.perf_counter() - started
                rate = sent / max(elapsed, 0.001)
                print(f"[LOAD] Progress sent={sent}/{total} failures={failures} rate={rate:.1f} msg/s")
        except Exception as exc:
            failures += 1
            print(f"[LOAD] Send failure event_id={event.get('event_id')} err={exc}", file=sys.stderr)
        finally:
            semaphore.release()

    tasks = []
    for event in events:
        await semaphore.acquire()
        task = asyncio.create_task(send_one(event))
        tasks.append(task)
        if cfg.send_interval_ms > 0:
            await asyncio.sleep(cfg.send_interval_ms / 1000.0)

    await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.perf_counter() - started
    rate = sent / max(elapsed, 0.001)
    print(f"[LOAD] Done sent={sent} failures={failures} elapsed={elapsed:.2f}s rate={rate:.1f} msg/s")


def _arg(name: str, default: Optional[str] = None) -> Optional[str]:
    val = os.getenv(name, default if default is not None else None)
    if val is None:
        return None
    return str(val)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka inbound concurrency load test publisher")
    parser.add_argument("--bootstrap-servers", default=_arg("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--topic", default=_arg("KAFKA_TOPIC_INBOUND", "photon.inbound.v1"))
    parser.add_argument("--client-id", default="franklink-load-test")
    parser.add_argument("--security-protocol", default=_arg("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"))
    parser.add_argument("--sasl-mechanism", default=_arg("KAFKA_SASL_MECHANISM"))
    parser.add_argument("--username", default=_arg("KAFKA_USERNAME"))
    parser.add_argument("--password", default=_arg("KAFKA_PASSWORD"))
    parser.add_argument("--iam-region", default=_arg("KAFKA_IAM_REGION"))

    parser.add_argument("--test-run", required=True)
    parser.add_argument("--users", type=int, default=50)
    parser.add_argument("--messages-per-user", type=int, default=20)
    parser.add_argument("--max-inflight", type=int, default=200)
    parser.add_argument("--send-interval-ms", type=int, default=0)
    parser.add_argument("--partition-mode", choices=["user", "hot"], default="user")
    parser.add_argument("--hot-key-count", type=int, default=2)
    parser.add_argument("--shuffle", action="store_true")
    return parser.parse_args()


async def _main() -> int:
    args = parse_args()
    if args.users <= 0 or args.messages_per_user <= 0:
        print("--users and --messages-per-user must be > 0", file=sys.stderr)
        return 2
    if args.max_inflight <= 0:
        print("--max-inflight must be > 0", file=sys.stderr)
        return 2

    cfg = LoadConfig(
        topic=args.topic,
        test_run=args.test_run,
        users=args.users,
        messages_per_user=args.messages_per_user,
        max_inflight=args.max_inflight,
        send_interval_ms=max(0, args.send_interval_ms),
        partition_mode=args.partition_mode,
        hot_key_count=max(1, args.hot_key_count),
        shuffle=bool(args.shuffle),
    )

    security_cfg = _security_config(
        bootstrap_servers=args.bootstrap_servers,
        security_protocol=args.security_protocol,
        sasl_mechanism=args.sasl_mechanism,
        username=args.username,
        password=args.password,
        iam_region=args.iam_region,
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        client_id=args.client_id,
        acks="all",
        enable_idempotence=True,
        **security_cfg,
    )

    await producer.start()
    try:
        await _publish(cfg, producer)
    finally:
        await producer.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
