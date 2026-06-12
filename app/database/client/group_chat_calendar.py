"""Database client methods for group chat calendar events."""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from postgrest.exceptions import APIError

logger = logging.getLogger(__name__)


class _GroupChatCalendarMethods:
    async def get_group_chat_calendar_event_by_hash(self, request_hash: str) -> Optional[Dict[str, Any]]:
        """Fetch a calendar event record by request hash (idempotency)."""
        if not request_hash:
            return None
        try:
            result = (
                self.client.table("group_chat_calendar_events")
                .select("*")
                .eq("request_hash", request_hash)
                .limit(1)
                .execute()
            )
            rows = list(result.data or [])
            return rows[0] if rows else None
        except APIError as e:
            logger.error("[DB] API error fetching calendar event by hash: %s", e, exc_info=True)
            return None
        except Exception as e:
            logger.error("[DB] Error fetching calendar event by hash: %s", e, exc_info=True)
            return None

    async def create_group_chat_calendar_event(
        self,
        *,
        chat_guid: str,
        organizer_user_id: str,
        event_id: Optional[str],
        title: str,
        start_time: str,
        end_time: str,
        timezone: str,
        attendees: List[str],
        request_hash: str,
        event_link: Optional[str] = None,
        status: str = "created",
    ) -> Optional[Dict[str, Any]]:
        """Insert a new calendar event record."""
        if not (chat_guid and organizer_user_id and request_hash):
            return None
        payload = {
            "chat_guid": chat_guid,
            "organizer_user_id": organizer_user_id,
            "event_id": event_id,
            "title": title,
            "start_time": start_time,
            "end_time": end_time,
            "timezone": timezone,
            "attendees": attendees,
            "request_hash": request_hash,
            "event_link": event_link,
            "status": status,
        }
        try:
            result = self.client.table("group_chat_calendar_events").insert(payload).execute()
            rows = list(result.data or [])
            return rows[0] if rows else payload
        except APIError as e:
            logger.error("[DB] API error creating calendar event: %s", e, exc_info=True)
            return None
        except Exception as e:
            logger.error("[DB] Error creating calendar event: %s", e, exc_info=True)
            return None

    async def update_group_chat_calendar_event_status(
        self,
        *,
        request_hash: str,
        status: str,
    ) -> Optional[Dict[str, Any]]:
        """Update status for a calendar event by request hash."""
        if not request_hash:
            return None
        try:
            result = (
                self.client.table("group_chat_calendar_events")
                .update({"status": status, "updated_at": datetime.utcnow().isoformat()})
                .eq("request_hash", request_hash)
                .execute()
            )
            rows = list(result.data or [])
            return rows[0] if rows else None
        except APIError as e:
            logger.error("[DB] API error updating calendar event status: %s", e, exc_info=True)
            return None
        except Exception as e:
            logger.error("[DB] Error updating calendar event status: %s", e, exc_info=True)
            return None
