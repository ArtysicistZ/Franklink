"""Database client methods for user_networking_opportunities table."""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class _NetworkingOpportunityMethods:
    """Mixin for networking opportunity operations."""

    async def insert_networking_opportunities_batch(
        self,
        *,
        user_id: str,
        source: str,
        opportunities: List[Dict[str, Any]],
    ) -> Optional[str]:
        """
        Insert a batch of ranked networking opportunities.

        Args:
            user_id: User ID
            source: Source of opportunities ('proactive' or 'user_requested')
            opportunities: List of opportunity dicts with fields:
                - purpose: str (required)
                - group_name: str
                - rationale: str
                - evidence: str
                - activity_type: str (academic/event/activity/hobby/social/project/general)
                - event_date: str (YYYY-MM-DD or None)
                - urgency: str (high/medium/low)
                - rank: int
                - match_type: str (single/multi)
                - max_matches: int

        Returns:
            batch_id (uuid) or None on error
        """
        try:
            # Pass the list directly - Supabase client handles JSONB serialization
            result = self.client.rpc(
                "insert_networking_opportunities_batch_v1",
                {
                    "p_user_id": str(user_id),
                    "p_source": str(source),
                    "p_opportunities": opportunities,
                },
            ).execute()

            # RPC returns the batch_id directly
            if result.data:
                return str(result.data)
            return None

        except Exception as e:
            logger.error(
                f"Error inserting networking opportunities batch: {e}",
                exc_info=True,
            )
            return None

    async def get_recent_networking_opportunities(
        self,
        *,
        user_id: str,
        days: int = 7,
        status: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get recent networking opportunities for a user.

        Args:
            user_id: User ID
            days: Number of days to look back (default 7)
            status: Optional status filter (active/used/skipped/expired)

        Returns:
            List of opportunity dicts
        """
        try:
            result = self.client.rpc(
                "get_recent_networking_opportunities_v1",
                {
                    "p_user_id": str(user_id),
                    "p_days": int(days),
                    "p_status": str(status) if status else None,
                },
            ).execute()

            if isinstance(result.data, list):
                return result.data
            return []

        except Exception as e:
            logger.error(
                f"Error getting recent networking opportunities: {e}",
                exc_info=True,
            )
            return []

    async def get_active_opportunities_purposes(
        self,
        *,
        user_id: str,
        days: int = 7,
    ) -> List[str]:
        """
        Get purpose texts from active opportunities for deduplication.

        Args:
            user_id: User ID
            days: Number of days to look back (default 7)

        Returns:
            List of purpose strings
        """
        try:
            result = self.client.rpc(
                "get_active_opportunities_purposes_v1",
                {
                    "p_user_id": str(user_id),
                    "p_days": int(days),
                },
            ).execute()

            if isinstance(result.data, list):
                return [r.get("purpose", "") for r in result.data if r.get("purpose")]
            return []

        except Exception as e:
            logger.error(
                f"Error getting active opportunities purposes: {e}",
                exc_info=True,
            )
            return []

    async def mark_opportunity_used(
        self,
        *,
        opportunity_id: str,
        connection_request_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Mark an opportunity as used and link to connection request.

        Args:
            opportunity_id: Opportunity ID
            connection_request_id: Connection request ID

        Returns:
            Updated opportunity or None
        """
        try:
            result = self.client.rpc(
                "mark_opportunity_used_v1",
                {
                    "p_opportunity_id": str(opportunity_id),
                    "p_connection_request_id": str(connection_request_id),
                },
            ).execute()

            if isinstance(result.data, dict):
                return result.data
            if isinstance(result.data, list) and result.data:
                return result.data[0]
            return None

        except Exception as e:
            logger.error(
                f"Error marking opportunity used: {e}",
                exc_info=True,
            )
            return None

    async def mark_opportunity_skipped(
        self,
        *,
        opportunity_id: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Mark an opportunity as skipped (no match found or duplicate).

        Args:
            opportunity_id: Opportunity ID

        Returns:
            Updated opportunity or None
        """
        try:
            result = self.client.rpc(
                "mark_opportunity_skipped_v1",
                {
                    "p_opportunity_id": str(opportunity_id),
                },
            ).execute()

            if isinstance(result.data, dict):
                return result.data
            if isinstance(result.data, list) and result.data:
                return result.data[0]
            return None

        except Exception as e:
            logger.error(
                f"Error marking opportunity skipped: {e}",
                exc_info=True,
            )
            return None
