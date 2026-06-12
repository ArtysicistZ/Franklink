#!/usr/bin/env python3
"""
Backfill user_email_intent_events from stored user_email_highlights.

Usage:
    python support/scripts/backfill_email_intent_events.py
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Iterator, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.agents.tools.email_intent_events import process_email_intent_events_from_highlights
from app.database.client import DatabaseClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _iter_user_ids_from_highlights(
    db: DatabaseClient,
    *,
    batch_size: int,
    max_users: Optional[int],
) -> Iterator[str]:
    offset = 0
    last_user_id: Optional[str] = None
    yielded = 0

    while True:
        result = (
            db.client.table("user_email_highlights")
            .select("user_id")
            .order("user_id", desc=False)
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        rows = list(result.data or [])
        if not rows:
            break

        for row in rows:
            user_id = str(row.get("user_id") or "").strip()
            if not user_id or user_id == last_user_id:
                continue
            last_user_id = user_id
            yield user_id
            yielded += 1
            if max_users and yielded >= max_users:
                return

        if len(rows) < batch_size:
            break
        offset += batch_size


def _get_user_highlights(db: DatabaseClient, *, user_id: str, limit: int) -> list[dict]:
    result = (
        db.client.table("user_email_highlights")
        .select("subject,body_excerpt,sender,is_from_me,created_at")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    return list(result.data or [])


async def _run(*, batch_size: int, max_users: Optional[int], max_highlights: int) -> int:
    db = DatabaseClient()
    processed = 0
    stored_total = 0
    errors = 0

    for user_id in _iter_user_ids_from_highlights(db, batch_size=batch_size, max_users=max_users):
        try:
            highlights = _get_user_highlights(db, user_id=user_id, limit=max_highlights)
            result = await process_email_intent_events_from_highlights(
                user_id=user_id,
                highlights=highlights,
            )
            processed += 1
            stored = int(result.get("stored") or 0)
            stored_total += stored
            logger.info(
                "processed user=%s stored=%d total=%d",
                user_id[:8],
                stored,
                int(result.get("total") or 0),
            )
        except Exception as exc:
            errors += 1
            logger.error("failed user=%s err=%s", user_id[:8], exc, exc_info=True)

    logger.info("backfill complete users=%d stored=%d errors=%d", processed, stored_total, errors)
    return 0 if errors == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill email intent events from highlights")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-users", type=int, default=None)
    parser.add_argument("--max-highlights", type=int, default=200)
    args = parser.parse_args()

    batch_size = max(1, int(args.batch_size or 500))
    max_users = int(args.max_users) if args.max_users else None
    max_highlights = max(1, int(args.max_highlights or 200))
    return int(asyncio.run(_run(
        batch_size=batch_size,
        max_users=max_users,
        max_highlights=max_highlights,
    )) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
