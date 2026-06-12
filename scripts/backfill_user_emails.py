#!/usr/bin/env python3
"""
Backfill users.email from connected Gmail accounts via Composio.

Usage:
    python support/scripts/backfill_user_emails.py
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Iterator, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.client import DatabaseClient
from app.integrations.composio_client import ComposioClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _iter_users_missing_email(
    db: DatabaseClient,
    *,
    batch_size: int,
    max_users: Optional[int],
) -> Iterator[str]:
    offset = 0
    yielded = 0

    while True:
        result = (
            db.client.table("users")
            .select("id")
            .is_("email", "null")
            .order("created_at", desc=False)
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        rows = list(result.data or [])
        if not rows:
            break

        for row in rows:
            user_id = str(row.get("id") or "").strip()
            if not user_id:
                continue
            yield user_id
            yielded += 1
            if max_users and yielded >= max_users:
                return

        if len(rows) < batch_size:
            break
        offset += batch_size


async def _run(*, batch_size: int, max_users: Optional[int], dry_run: bool) -> int:
    db = DatabaseClient()
    composio = ComposioClient()

    if not composio.is_available():
        logger.error("Composio client unavailable; set COMPOSIO API key before running.")
        return 1

    processed = 0
    updated = 0
    errors = 0

    for user_id in _iter_users_missing_email(db, batch_size=batch_size, max_users=max_users):
        try:
            email = await composio.get_connected_gmail_address(user_id=user_id)
            if not email:
                logger.info("no email resolved user=%s", user_id[:8])
                processed += 1
                continue

            if dry_run:
                logger.info("[DRY_RUN] would update user=%s email=%s", user_id[:8], email)
            else:
                await db.update_user_profile(user_id, {"email": email})
                updated += 1
                logger.info("updated user=%s email=%s", user_id[:8], email)

            processed += 1
        except Exception as exc:
            errors += 1
            logger.error("failed user=%s err=%s", user_id[:8], exc, exc_info=True)

    logger.info("backfill complete users=%d updated=%d errors=%d", processed, updated, errors)
    return 0 if errors == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill users.email via Composio")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--max-users", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    batch_size = max(1, int(args.batch_size or 200))
    max_users = int(args.max_users) if args.max_users else None
    return int(asyncio.run(_run(batch_size=batch_size, max_users=max_users, dry_run=bool(args.dry_run))) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
