"""Cooperative cancellation mechanism for gracefully stopping in-progress operations."""

from __future__ import annotations

import asyncio
from typing import Optional


class CancellationToken:
    """
    A cooperative cancellation token for gracefully stopping long-running operations.

    Used by the message coalescing system to cancel in-progress message processing
    when a new message arrives from the same conversation.

    Usage:
        token = CancellationToken()

        # In the processing code, check periodically:
        if token.is_cancelled():
            return early

        # Or raise CancelledError:
        await token.check_or_raise()

        # To cancel from another task:
        token.cancel("New message received")
    """

    def __init__(self):
        self._cancelled = asyncio.Event()
        self._cancel_reason: Optional[str] = None

    def cancel(self, reason: str = "Cancelled") -> None:
        """
        Signal cancellation to all code checking this token.

        Args:
            reason: Human-readable reason for cancellation (for logging)
        """
        self._cancel_reason = reason
        self._cancelled.set()

    def is_cancelled(self) -> bool:
        """Check if cancellation was requested."""
        return self._cancelled.is_set()

    @property
    def cancel_reason(self) -> Optional[str]:
        """Get the reason for cancellation, if any."""
        return self._cancel_reason

    async def check_or_raise(self) -> None:
        """
        Raise CancelledError if cancellation was requested.

        Use this at cancellation checkpoints in async code.

        Raises:
            asyncio.CancelledError: If cancel() was called on this token
        """
        if self.is_cancelled():
            raise asyncio.CancelledError(self._cancel_reason or "Operation cancelled")

    def reset(self) -> None:
        """
        Reset the token for reuse.

        Call this when starting a new operation with the same token.
        """
        self._cancelled.clear()
        self._cancel_reason = None

    async def wait_until_cancelled(self, timeout: Optional[float] = None) -> bool:
        """
        Wait until cancellation is signalled or timeout expires.

        Args:
            timeout: Maximum time to wait in seconds, or None for no timeout

        Returns:
            True if cancelled, False if timeout expired
        """
        try:
            await asyncio.wait_for(self._cancelled.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
