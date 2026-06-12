#!/usr/bin/env python3
"""
Backfill user_email_highlights from stored user_emails.

Usage:
    python support/scripts/backfill_email_highlights.py
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

from app.database.client import DatabaseClient
from app.agents.tools.email_highlights import process_user_email_highlights
from app.integrations.composio_client import ComposioClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _iter_user_ids_from_user_emails(
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
            db.client.table("user_emails")
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


def _get_user_email(db: DatabaseClient, *, user_id: str) -> Optional[str]:
    result = (
        db.client.table("users")
        .select("email")
        .eq("id", user_id)
        .limit(1)
        .execute()
    )
    if not result.data:
        return None
    return result.data[0].get("email")


async def _run(*, batch_size: int, max_users: Optional[int]) -> int:
    db = DatabaseClient()
    composio = ComposioClient()
    composio_available = composio.is_available()
    if not composio_available:
        logger.warning("Composio client unavailable; skipping email backfill.")

    processed = 0
    stored_total = 0
    errors = 0

    for user_id in _iter_user_ids_from_user_emails(db, batch_size=batch_size, max_users=max_users):
        try:
            if composio_available:
                email = _get_user_email(db, user_id=user_id)
                if not email:
                    connected_email = await composio.get_connected_gmail_address(user_id=user_id)
                    logger.info("resolved composio email user=%s email=%s", user_id[:8], connected_email or "none")
                    if connected_email:
                        await db.update_user_profile(user_id, {"email": connected_email})
                        logger.info("updated user email from Composio user=%s", user_id[:8])

            result = await process_user_email_highlights(user_id=user_id)
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
    parser = argparse.ArgumentParser(description="Backfill email highlights from user_emails")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-users", type=int, default=None)
    args = parser.parse_args()

    batch_size = max(1, int(args.batch_size or 500))
    max_users = int(args.max_users) if args.max_users else None
    return int(asyncio.run(_run(batch_size=batch_size, max_users=max_users)) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
