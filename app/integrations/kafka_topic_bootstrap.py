from __future__ import annotations

import asyncio

from app.config import settings
from app.integrations.kafka_pipeline import ensure_kafka_topics


async def ensure_topics() -> list[str]:
    await ensure_kafka_topics()
    return [
        settings.kafka_topic_inbound,
        settings.kafka_topic_retry_30s,
        settings.kafka_topic_retry_2m,
        settings.kafka_topic_retry_10m,
        settings.kafka_topic_dlq,
    ]


async def _main() -> None:
    created = await ensure_topics()
    print("Created topics:", created)


if __name__ == "__main__":
    asyncio.run(_main())
