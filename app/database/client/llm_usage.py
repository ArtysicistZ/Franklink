"""Database client methods for llm_usage_log table."""

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class _LLMUsageMethods:
    """Mixin for LLM usage logging operations."""

    def log_llm_usage(
        self,
        *,
        trace_label: str,
        deployment: str,
        api_type: str,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
        cost_cents: float,
        duration_ms: Optional[int] = None,
        success: bool = True,
        error_message: Optional[str] = None,
        user_id: Optional[str] = None,
        chat_guid: Optional[str] = None,
        job_type: Optional[str] = None,
        request_metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Log LLM API usage to database.

        This is a synchronous method that performs a direct insert for performance.
        Errors are caught and logged but not raised to avoid failing the main request.

        Args:
            trace_label: Operation identifier (e.g., "interaction_agent", "classify_intent")
            deployment: Model deployment name (e.g., "gpt-4o-mini")
            api_type: "chat" or "embedding"
            prompt_tokens: Number of input tokens
            completion_tokens: Number of output tokens
            total_tokens: Total tokens used
            cost_cents: Calculated cost in USD cents
            duration_ms: API call duration in milliseconds
            success: Whether the API call succeeded
            error_message: Error message if failed (truncated to 500 chars)
            user_id: UUID of the user (optional, may be None for background jobs)
            chat_guid: iMessage chat GUID (optional)
            job_type: Background job identifier (optional)
            request_metadata: Additional debug info (optional)

        Returns:
            The inserted record or None on error
        """
        try:
            data = {
                "trace_label": trace_label,
                "deployment": deployment,
                "api_type": api_type,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": total_tokens,
                "cost_cents": cost_cents,
                "success": success,
                "request_metadata": request_metadata or {},
            }

            # Add optional fields only if present
            if user_id:
                data["user_id"] = user_id
            if chat_guid:
                data["chat_guid"] = chat_guid
            if job_type:
                data["job_type"] = job_type
            if duration_ms is not None:
                data["duration_ms"] = duration_ms
            if error_message:
                data["error_message"] = error_message[:500]

            result = self.client.table("llm_usage_log").insert(data).execute()

            if result.data:
                return result.data[0] if isinstance(result.data, list) else result.data
            return None

        except Exception as e:
            # Log but don't raise - usage logging should never fail the main request
            logger.warning("Failed to log LLM usage: %s", e)
            return None

    def get_user_llm_usage_summary(
        self,
        user_id: str,
        days: int = 30,
    ) -> Dict[str, Any]:
        """
        Get usage summary for a user over the past N days.

        Args:
            user_id: UUID of the user
            days: Number of days to look back (default 30)

        Returns:
            Dictionary with total_calls, total_tokens, total_cost_cents, etc.
        """
        try:
            result = self.client.rpc(
                "get_user_llm_usage_summary_v1",
                {"p_user_id": user_id, "p_days": days}
            ).execute()

            if result.data:
                return result.data[0] if isinstance(result.data, list) else result.data
            return {}

        except Exception as e:
            logger.error("Error getting user LLM usage summary: %s", e, exc_info=True)
            return {}

    def get_daily_llm_usage_stats(
        self,
        days: int = 7,
    ) -> List[Dict[str, Any]]:
        """
        Get daily usage statistics for monitoring dashboard.

        Args:
            days: Number of days to look back (default 7)

        Returns:
            List of daily stats with call counts, tokens, costs, etc.
        """
        try:
            result = self.client.rpc(
                "get_daily_llm_usage_stats_v1",
                {"p_days": days}
            ).execute()

            if result.data:
                return result.data if isinstance(result.data, list) else [result.data]
            return []

        except Exception as e:
            logger.error("Error getting daily LLM usage stats: %s", e, exc_info=True)
            return []
