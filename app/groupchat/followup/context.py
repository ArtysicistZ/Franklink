from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.database.client import DatabaseClient
from app.groupchat.summary.utils import parse_timestamp, utc_iso


async def fetch_recent_messages(
    db: DatabaseClient,
    *,
    chat_guid: str,
    limit: int = 120,
) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit or 120), 500))
    return await db.get_group_chat_raw_messages_window_v1(chat_guid=chat_guid, limit=limit)


async def load_participants(
    db: DatabaseClient,
    *,
    chat_guid: str,
) -> Tuple[Optional[Dict[str, Any]], List[str], List[str]]:
    """
    Load participant info for a group chat.

    Returns:
        Tuple of (chat_record, participant_names, participant_modes)
    """
    try:
        chat = await db.get_group_chat_by_guid(chat_guid)
    except Exception:
        chat = None
    if not isinstance(chat, dict):
        return None, [], []

    participant_names: List[str] = []
    participant_modes: List[str] = []
    try:
        # Use unified participants table
        participants = await db.get_group_chat_participants(chat_guid)

        # Get names and modes for all participants
        for i, p in enumerate(participants):
            # Collect mode for all participants
            participant_modes.append(str(p.get("mode") or "active"))

            # Get name for participant
            p_user_id = str(p.get("user_id") or "").strip()
            if p_user_id:
                user = await db.get_user_by_id(p_user_id)
                name = str((user or {}).get("name") or "").strip() or f"user {i + 1}"
            else:
                name = f"user {i + 1}"
            participant_names.append(name)
    except Exception:
        pass
    return chat, participant_names, participant_modes


async def build_summary_segments(
    db: DatabaseClient,
    *,
    chat_guid: str,
    limit: int = 200,
    window_days: int = 7,
    now: Optional[datetime] = None,
) -> List[str]:
    if now is None:
        now = datetime.now(timezone.utc)
    window_days = max(1, int(window_days or 7))
    since = now - timedelta(days=window_days)
    try:
        segments = await db.get_group_chat_summary_segments_v1(
            chat_guid=chat_guid,
            start_at=utc_iso(since),
            limit=limit,
        )
    except Exception:
        segments = []

    parts: List[str] = []
    total = 0
    for seg in segments or []:
        md = str(seg.get("summary_md") or "").strip()
        if not md:
            continue
        end_at = str(seg.get("segment_end_at") or "").strip()
        end_dt = parse_timestamp(end_at) if end_at else None
        if end_dt and end_dt < since:
            continue
        header = f"### segment_end_at={end_at}" if end_at else "### segment"
        block = f"{header}\n{md}".strip()
        if not block:
            continue
        total += len(block)
        if total > 6000 and parts:
            break
        parts.append(block)
    return parts
