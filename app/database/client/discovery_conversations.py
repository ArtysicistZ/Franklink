"""Database client methods for discovery conversations (agent-generated match previews)."""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class _DiscoveryConversationMethods:
    """Mixin for discovery conversation CRUD operations."""

    async def create_discovery_conversation(
        self,
        *,
        slug: str,
        initiator_user_id: str,
        participant_user_ids: List[str],
        turns: List[Dict[str, Any]],
        teaser_summary: str,
        match_metadata: Optional[Dict[str, Any]] = None,
        quality_score: Optional[float] = None,
        flow_type: str = "reactive",
        connection_request_id: Optional[str] = None,
        connection_request_ids: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Insert a new discovery conversation and return it."""
        try:
            payload: Dict[str, Any] = {
                "slug": str(slug),
                "initiator_user_id": str(initiator_user_id),
                "participant_user_ids": [str(uid) for uid in participant_user_ids],
                "turns": turns,
                "teaser_summary": str(teaser_summary),
                "match_metadata": match_metadata or {},
                "flow_type": str(flow_type),
            }
            if quality_score is not None:
                payload["quality_score"] = float(quality_score)
            if connection_request_id:
                payload["connection_request_id"] = str(connection_request_id)
            if connection_request_ids:
                payload["connection_request_ids"] = [
                    str(cid) for cid in connection_request_ids
                ]

            result = (
                self.client.table("discovery_conversations").insert(payload).execute()
            )
            if isinstance(result.data, list) and result.data:
                return result.data[0]
            if isinstance(result.data, dict):
                return result.data
            return None
        except Exception as e:
            logger.error(f"Error creating discovery conversation: {e}", exc_info=True)
            return None

    async def get_discovery_conversation_by_slug(
        self,
        slug: str,
    ) -> Optional[Dict[str, Any]]:
        """Fetch a discovery conversation by its URL slug."""
        try:
            result = (
                self.client.table("discovery_conversations")
                .select("*")
                .eq("slug", str(slug))
                .limit(1)
                .execute()
            )
            if isinstance(result.data, list) and result.data:
                return result.data[0]
            return None
        except Exception as e:
            logger.error(
                f"Error fetching discovery conversation by slug: {e}", exc_info=True
            )
            return None

    async def get_discovery_conversation_by_connection_request_id(
        self,
        connection_request_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Fetch a discovery conversation by connection request ID.

        Checks both the single `connection_request_id` column and the
        `connection_request_ids` array column (used for multi-match).
        """
        try:
            crid = str(connection_request_id)
            result = (
                self.client.table("discovery_conversations")
                .select("*")
                .or_(
                    f"connection_request_id.eq.{crid},"
                    f"connection_request_ids.cs.{{{crid}}}"
                )
                .limit(1)
                .execute()
            )
            if isinstance(result.data, list) and result.data:
                return result.data[0]
            return None
        except Exception as e:
            logger.error(
                "Error fetching discovery conversation by connection_request_id: %s",
                e,
                exc_info=True,
            )
            return None
