#!/usr/bin/env python3
"""
Migrate existing user email highlights from Supabase to Zep knowledge graph.

This script:
1. Fetches all unique user_ids from user_email_highlights table
2. For each user, syncs their unsynced highlights to Zep
3. Marks highlights as synced after successful sync

Note: Only highlight emails (curated, important emails) are synced to Zep,
not all raw emails. This ensures high-quality context in the knowledge graph.

Usage:
    python scripts/migrate_emails_to_zep.py

    # Dry run (just count, don't sync):
    python scripts/migrate_emails_to_zep.py --dry-run

    # Limit to specific number of users:
    python scripts/migrate_emails_to_zep.py --limit 10

    # Skip first N users (for resuming):
    python scripts/migrate_emails_to_zep.py --offset 50
"""

import asyncio
import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def get_users_with_highlights() -> List[Dict[str, Any]]:
    """Get all unique users who have email highlights in the database."""
    from app.database.client import DatabaseClient

    db = DatabaseClient()

    # Query distinct user_ids from user_email_highlights
    logger.info("Fetching user_ids from user_email_highlights table...")

    all_user_ids: List[str] = []
    page_size = 1000
    offset = 0

    while True:
        result = (
            db.client.table("user_email_highlights")
            .select("user_id")
            .range(offset, offset + page_size - 1)
            .execute()
        )

        if not result.data:
            break

        for row in result.data:
            uid = row.get("user_id")
            if uid:
                all_user_ids.append(uid)

        if len(result.data) < page_size:
            break

        offset += page_size
        logger.info(f"  Fetched {offset} records...")

    if not all_user_ids:
        return []

    # Count highlights per user
    user_counts: Dict[str, int] = {}
    for uid in all_user_ids:
        user_counts[uid] = user_counts.get(uid, 0) + 1

    return [{"user_id": uid, "highlight_count": count} for uid, count in user_counts.items()]


async def get_unsynced_highlight_count(user_id: str) -> int:
    """Get count of unsynced highlights for a user."""
    from app.database.client import DatabaseClient

    db = DatabaseClient()

    try:
        result = (
            db.client.table("user_email_highlights")
            .select("id", count="exact")
            .eq("user_id", user_id)
            .is_("zep_synced_at", "null")
            .execute()
        )
        return result.count or 0
    except Exception as e:
        logger.error(f"Error counting unsynced highlights for user {user_id[:8]}...: {e}")
        return 0


async def migrate_user_highlights(user_id: str) -> Dict[str, Any]:
    """
    Migrate a single user's highlights to Zep.

    Args:
        user_id: User identifier

    Returns:
        Migration result dict
    """
    from app.agents.tools.email_zep_sync import sync_unsynced_highlights_to_zep

    return await sync_unsynced_highlights_to_zep(user_id=user_id, max_highlights=500)


async def main(
    dry_run: bool = False,
    limit: int = 0,
    offset: int = 0,
    batch_delay: float = 0.5,
):
    """
    Main migration function.

    Args:
        dry_run: If True, just count without syncing
        limit: Max number of users to process (0 = all)
        offset: Skip first N users
        batch_delay: Delay between users to avoid rate limiting
    """
    print("=" * 70)
    print("Email Highlights Migration: Supabase -> Zep Knowledge Graph")
    print("=" * 70)
    print(f"Mode: {'DRY RUN (no sync)' if dry_run else 'MIGRATION'}")
    print(f"Offset: {offset}, Limit: {limit if limit > 0 else 'unlimited'}")
    print("-" * 70)

    # Check if Zep is configured
    from app.config import settings
    if not settings.zep_graph_enabled:
        print("ERROR: Zep graph is not enabled (ZEP_GRAPH_ENABLED=false)")
        sys.exit(1)

    if not settings.zep_api_key:
        print("ERROR: ZEP_API_KEY not configured")
        sys.exit(1)

    print(f"Zep endpoint: {settings.zep_base_url}")
    print("-" * 70)

    # Get users with highlights
    print("Fetching users with email highlights from Supabase...")
    users = await get_users_with_highlights()

    if not users:
        print("No users with email highlights found.")
        return

    total_users = len(users)
    print(f"Found {total_users} users with email highlights")

    # Apply offset and limit
    if offset > 0:
        users = users[offset:]
        print(f"Skipped first {offset} users")

    if limit > 0:
        users = users[:limit]
        print(f"Processing {len(users)} users (limited)")

    print("-" * 70)

    # Sort by highlight count descending
    users.sort(key=lambda x: x.get("highlight_count", 0), reverse=True)

    # Summary stats
    total_highlights = sum(u.get("highlight_count", 0) for u in users)
    print(f"Total highlights: {total_highlights}")
    print("-" * 70)

    if dry_run:
        print("\nDRY RUN - Users with highlights:")
        for i, user in enumerate(users[:20]):  # Show first 20
            uid = user.get("user_id", "?")
            count = user.get("highlight_count", 0)
            print(f"  {i+1}. {uid[:8]}... ({count} highlights)")

        if len(users) > 20:
            print(f"  ... and {len(users) - 20} more users")

        print(f"\nDRY RUN: Would sync highlights for {len(users)} users")
        return

    # Confirm migration
    print(f"\nAbout to sync highlights for {len(users)} users to Zep.")
    confirm = input("Type 'MIGRATE' to confirm: ")
    if confirm != "MIGRATE":
        print("Aborted.")
        return

    # Migration loop
    print("\nStarting migration...")
    start_time = datetime.now()

    migrated_users = 0
    migrated_highlights = 0
    failed_users = 0
    errors: List[str] = []

    for i, user in enumerate(users):
        user_id = user.get("user_id")
        expected_count = user.get("highlight_count", 0)

        print(f"\n[{i+1}/{len(users)}] User {user_id[:8]}... ({expected_count} total highlights)")

        try:
            # Get unsynced count first
            unsynced_count = await get_unsynced_highlight_count(user_id)

            if unsynced_count == 0:
                print(f"  No unsynced highlights found")
                continue

            print(f"  Found {unsynced_count} unsynced highlights")

            # Sync to Zep
            result = await migrate_user_highlights(user_id)

            if result.get("success"):
                synced = result.get("highlights_synced", 0)
                chunks = result.get("chunks_sent", 0)
                print(f"  Synced {synced} highlights ({chunks} chunks)")
                migrated_users += 1
                migrated_highlights += synced
            else:
                err = result.get("errors", ["Unknown error"])
                print(f"  FAILED: {err[:2]}")
                failed_users += 1
                errors.append(f"{user_id[:8]}: {err[0] if err else 'Unknown'}")

        except Exception as e:
            print(f"  ERROR: {e}")
            failed_users += 1
            errors.append(f"{user_id[:8]}: {str(e)}")

        # Rate limiting delay
        if i < len(users) - 1:
            await asyncio.sleep(batch_delay)

    # Summary
    duration = (datetime.now() - start_time).total_seconds()
    print("\n" + "=" * 70)
    print("MIGRATION COMPLETE")
    print("=" * 70)
    print(f"Duration: {duration:.1f} seconds")
    print(f"Users migrated: {migrated_users}/{len(users)}")
    print(f"Highlights synced: {migrated_highlights}")
    print(f"Failed users: {failed_users}")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for err in errors[:10]:
            print(f"  - {err}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate email highlights from Supabase to Zep")
    parser.add_argument("--dry-run", action="store_true", help="List users without migrating")
    parser.add_argument("--limit", type=int, default=0, help="Max users to process (0=all)")
    parser.add_argument("--offset", type=int, default=0, help="Skip first N users")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between users (seconds)")
    args = parser.parse_args()

    asyncio.run(main(
        dry_run=args.dry_run,
        limit=args.limit,
        offset=args.offset,
        batch_delay=args.delay,
    ))
