"""LLM usage tracking service.

This module provides centralized token usage logging for all LLM API calls.
It calculates costs based on model pricing and logs to Supabase for analysis.

Usage:
    from app.integrations.llm_usage_tracker import get_usage_tracker
    from app.context import get_llm_context

    # After an LLM API call:
    usage = response.usage
    if usage:
        ctx = get_llm_context()
        get_usage_tracker().log_usage(
            trace_label="my_operation",
            deployment="gpt-4o-mini",
            api_type="chat",
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            total_tokens=usage.total_tokens,
            duration_ms=123,
            user_id=ctx.get("user_id"),
            chat_guid=ctx.get("chat_guid"),
            job_type=ctx.get("job_type"),
        )
"""

import logging
from typing import Any, Dict, Optional

from app.database.client import DatabaseClient

logger = logging.getLogger(__name__)


class LLMUsageTracker:
    """Tracks LLM API usage for cost analysis and monitoring."""

    # Pricing per 1M tokens (USD)
    # Updated: 2024-01 - Verify current Azure/OpenAI pricing periodically
    PRICING: Dict[str, Dict[str, float]] = {
        # GPT-4o family
        "gpt-4o": {"prompt": 2.50, "completion": 10.00},
        "gpt-4o-mini": {"prompt": 0.15, "completion": 0.60},
        # GPT-5 family (assuming similar to 4o-mini for now)
        "gpt-5-mini": {"prompt": 0.15, "completion": 0.60},
        # Embeddings
        "text-embedding-3-small": {"prompt": 0.02, "completion": 0.0},
        "text-embedding-3-large": {"prompt": 0.13, "completion": 0.0},
        "text-embedding-ada-002": {"prompt": 0.10, "completion": 0.0},
    }

    # Fallback pricing for unknown models
    DEFAULT_PRICING: Dict[str, float] = {"prompt": 0.50, "completion": 1.50}

    def __init__(self, db: Optional[DatabaseClient] = None):
        """
        Initialize the usage tracker.

        Args:
            db: Optional DatabaseClient instance. If not provided, creates one on first use.
        """
        self._db = db

    @property
    def db(self) -> DatabaseClient:
        """Get or create the database client."""
        if self._db is None:
            self._db = DatabaseClient()
        return self._db

    def calculate_cost_cents(
        self,
        deployment: str,
        prompt_tokens: int,
        completion_tokens: int,
    ) -> float:
        """
        Calculate cost in cents (USD) for token usage.

        Args:
            deployment: Model deployment name (e.g., "gpt-4o-mini")
            prompt_tokens: Number of input tokens
            completion_tokens: Number of output tokens

        Returns:
            Cost in USD cents (e.g., 0.045 = $0.00045)
        """
        # Normalize deployment name for pricing lookup (lowercase, strip whitespace)
        deployment_normalized = deployment.lower().strip()

        # Find matching pricing - try exact match first, then partial match
        pricing = self.DEFAULT_PRICING
        for model_name, model_pricing in self.PRICING.items():
            if model_name in deployment_normalized or deployment_normalized in model_name:
                pricing = model_pricing
                break

        # Cost formula: (tokens / 1M) * price_per_1M * 100 (convert to cents)
        prompt_cost = (prompt_tokens / 1_000_000) * pricing["prompt"] * 100
        completion_cost = (completion_tokens / 1_000_000) * pricing["completion"] * 100

        return round(prompt_cost + completion_cost, 4)

    def log_usage(
        self,
        *,
        trace_label: str,
        deployment: str,
        api_type: str = "chat",
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: int = 0,
        duration_ms: Optional[int] = None,
        success: bool = True,
        error_message: Optional[str] = None,
        user_id: Optional[str] = None,
        chat_guid: Optional[str] = None,
        job_type: Optional[str] = None,
        request_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log LLM usage to database.

        This method is fire-and-forget - it catches all exceptions to ensure
        usage logging never fails the main request.

        Args:
            trace_label: Operation identifier (e.g., "interaction_agent", "classify_intent")
            deployment: Model deployment name (e.g., "gpt-4o-mini", "gpt-5-mini")
            api_type: "chat" or "embedding"
            prompt_tokens: Number of input tokens
            completion_tokens: Number of output tokens
            total_tokens: Total tokens (prompt + completion)
            duration_ms: API call duration in milliseconds
            success: Whether the API call succeeded
            error_message: Error message if failed
            user_id: UUID of the user (from context)
            chat_guid: iMessage chat GUID (from context)
            job_type: Background job identifier (from context)
            request_metadata: Additional debug info (e.g., message_count, text_length)
        """
        try:
            cost_cents = self.calculate_cost_cents(
                deployment, prompt_tokens, completion_tokens
            )

            self.db.log_llm_usage(
                trace_label=trace_label,
                deployment=deployment,
                api_type=api_type,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
                cost_cents=cost_cents,
                duration_ms=duration_ms,
                success=success,
                error_message=error_message,
                user_id=user_id,
                chat_guid=chat_guid,
                job_type=job_type,
                request_metadata=request_metadata or {},
            )

            logger.debug(
                "[LLM_USAGE] logged label=%s deployment=%s tokens=%d cost=%.4f cents",
                trace_label,
                deployment,
                total_tokens,
                cost_cents,
            )

        except Exception as e:
            # Never fail the main request due to usage logging errors
            logger.warning("[LLM_USAGE] Failed to log usage: %s", e)


# Singleton instance
_tracker: Optional[LLMUsageTracker] = None


def get_usage_tracker() -> LLMUsageTracker:
    """
    Get the singleton usage tracker instance.

    Returns:
        The shared LLMUsageTracker instance
    """
    global _tracker
    if _tracker is None:
        _tracker = LLMUsageTracker()
    return _tracker
