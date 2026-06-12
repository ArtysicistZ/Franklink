#!/usr/bin/env python3
"""
Backfill career interest embeddings for existing users.

This script finds all onboarded users without career_interest_embedding
and generates embeddings for them.

Usage:
    python scripts/backfill_embeddings.py
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database.client import DatabaseClient
from app.integrations.azure_openai_client import AzureOpenAIClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def backfill_embeddings():
    """Generate embeddings for all users missing them."""
    db = DatabaseClient()
    openai = AzureOpenAIClient()

    # Get users without embeddings
    users = db.get_users_without_embeddings()

    if not users:
        logger.info("No users need embedding backfill")
        return

    logger.info(f"Found {len(users)} users needing embedding backfill")

    success_count = 0
    error_count = 0

    for user in users:
        user_id = user.get("id")
        name = user.get("name", "Unknown")
        career_interests = user.get("career_interests", [])

        if not career_interests:
            logger.warning(f"Skipping user {name} ({user_id}): no career interests")
            continue

        try:
            interests_text = ", ".join(career_interests)
            logger.info(f"Generating embedding for {name}: {interests_text}")

            embedding = await openai.get_embedding(interests_text)

            if embedding is None:
                logger.error(f"Failed to generate embedding for {name}")
                error_count += 1
                continue

            await db.update_career_interest_embedding(user_id, embedding)
            logger.info(f"Successfully updated embedding for {name}")
            success_count += 1

        except Exception as e:
            logger.error(f"Error processing user {name} ({user_id}): {e}")
            error_count += 1

    logger.info(
        f"Backfill complete: {success_count} success, {error_count} errors"
    )


if __name__ == "__main__":
    asyncio.run(backfill_embeddings())
