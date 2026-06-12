"""
Message coalescing service for combining rapid sequential messages.

When users send multiple messages quickly (e.g., "yo frank", "find a mentor",
"preferably in AI"), this service combines them into a single message before
processing, providing better context to the AI.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional

from app.services.cancellation import CancellationToken

logger = logging.getLogger(__name__)


@dataclass
class PendingMessage:
    """A message waiting to be coalesced with others."""

    content: str
    timestamp: float
    message_id: str
    from_number: str
    chat_guid: Optional[str] = None
    media_url: Optional[str] = None
    is_location_share: bool = False
    raw_payload: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ConversationState:
    """Tracks coalescing state for a single conversation."""

    conv_key: str
    pending_messages: List[PendingMessage] = field(default_factory=list)
    # Messages currently being processed (restored to pending if cancelled)
    in_flight_messages: List[PendingMessage] = field(default_factory=list)
    active_task: Optional[asyncio.Task] = None
    debounce_task: Optional[asyncio.Task] = None
    cancel_token: CancellationToken = field(default_factory=CancellationToken)
    first_message_at: float = field(default_factory=time.time)


class MessageCoalescer:
    """
    Manages message coalescing for all conversations.

    Key behaviors:
    1. When a message arrives, start a debounce timer (default 1.5s)
    2. If another message arrives before timer expires, reset the timer
    3. If processing is already in progress, cancel it and add new message to queue
    4. When timer expires, combine all pending messages and process as one
    5. Max wait window (default 10s) to prevent indefinite waiting
    """

    def __init__(
        self,
        process_callback: Callable[[Dict[str, Any], CancellationToken], Awaitable[None]],
        debounce_ms: int = 1500,
        max_window_ms: int = 10000,
    ):
        """
        Initialize the coalescer.

        Args:
            process_callback: Async function to call with combined message payload and cancel token
            debounce_ms: How long to wait for more messages (milliseconds)
            max_window_ms: Maximum time to wait before processing (milliseconds)
        """
        self._process_callback = process_callback
        self._debounce_ms = debounce_ms
        self._max_window_ms = max_window_ms
        self._conversations: Dict[str, ConversationState] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def _get_lock(self, conv_key: str) -> asyncio.Lock:
        """Get or create a lock for a conversation."""
        if conv_key not in self._locks:
            self._locks[conv_key] = asyncio.Lock()
        return self._locks[conv_key]

    def _build_conversation_key(self, payload: Dict[str, Any]) -> str:
        """
        Build unique conversation key from payload.

        Uses chat_guid for group chats, from_number for DMs.
        """
        chat_guid = payload.get("chat_guid") or ""
        from_number = payload.get("from_number") or ""

        # Group chat: use chat_guid
        if chat_guid and ";+;" in chat_guid:
            return f"group:{chat_guid}"

        # DM: use from_number
        return f"dm:{from_number}"

    async def enqueue_message(self, payload: Dict[str, Any]) -> None:
        """
        Enqueue a new message for coalescing.

        This is the main entry point called by the PhotonListener callback.
        """
        conv_key = self._build_conversation_key(payload)
        lock = self._get_lock(conv_key)

        async with lock:
            await self._enqueue_message_locked(conv_key, payload)

    async def _enqueue_message_locked(self, conv_key: str, payload: Dict[str, Any]) -> None:
        """Enqueue message (must be called with lock held)."""
        now = time.time()

        # Create pending message
        pending = PendingMessage(
            content=payload.get("content") or "",
            timestamp=now,
            message_id=payload.get("message_id") or "",
            from_number=payload.get("from_number") or "",
            chat_guid=payload.get("chat_guid"),
            media_url=payload.get("media_url"),
            is_location_share=payload.get("is_location_share", False),
            raw_payload=payload,
        )

        # Get or create conversation state
        if conv_key not in self._conversations:
            self._conversations[conv_key] = ConversationState(
                conv_key=conv_key,
                first_message_at=now,
            )

        state = self._conversations[conv_key]

        # Check if processing is in progress
        if state.active_task and not state.active_task.done():
            logger.info(
                "[COALESCER] New message while processing - cancelling current task for %s",
                conv_key[:30],
            )
            # Cancel the active processing
            state.cancel_token.cancel("New message received")
            state.active_task.cancel()

            try:
                await state.active_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.warning("[COALESCER] Error awaiting cancelled task: %s", e)

            # Restore in-flight messages to pending queue (prepend to maintain order)
            if state.in_flight_messages:
                logger.info(
                    "[COALESCER] Restoring %d in-flight messages for %s",
                    len(state.in_flight_messages),
                    conv_key[:30],
                )
                state.pending_messages = state.in_flight_messages + state.pending_messages
                state.in_flight_messages = []

            state.active_task = None

        # Reset first_message_at if this is the start of a fresh batch
        # (no pending messages and no active processing)
        if not state.pending_messages and not state.active_task:
            state.first_message_at = now
            logger.debug(
                "[COALESCER] Starting fresh batch for %s - reset first_message_at",
                conv_key[:30],
            )

        # Add message to pending queue
        state.pending_messages.append(pending)
        logger.info(
            "[COALESCER] Queued message #%d for %s: %s",
            len(state.pending_messages),
            conv_key[:30],
            (pending.content[:50] + "...") if len(pending.content) > 50 else pending.content,
        )

        # Check for immediate triggers (location share, etc.)
        if pending.is_location_share:
            logger.info("[COALESCER] Location share detected - processing immediately")
            await self._process_now(conv_key, state)
            return

        # Start or reset debounce timer
        await self._start_debounce_timer(conv_key, state)

    async def _start_debounce_timer(self, conv_key: str, state: ConversationState) -> None:
        """Start or reset the debounce timer for a conversation."""
        # Cancel existing debounce timer
        if state.debounce_task and not state.debounce_task.done():
            state.debounce_task.cancel()
            try:
                await state.debounce_task
            except asyncio.CancelledError:
                pass

        # Calculate how long we can still wait
        elapsed_ms = (time.time() - state.first_message_at) * 1000
        remaining_window_ms = max(0, self._max_window_ms - elapsed_ms)

        # Use shorter of debounce timeout or remaining window
        wait_ms = min(self._debounce_ms, remaining_window_ms)

        if wait_ms <= 0:
            # Window expired, process immediately
            logger.info("[COALESCER] Max window expired for %s - processing now", conv_key[:30])
            await self._process_now(conv_key, state)
        else:
            # Start debounce timer
            logger.info(
                "[COALESCER] Starting debounce timer (%dms) for %s",
                wait_ms,
                conv_key[:30],
            )
            state.debounce_task = asyncio.create_task(
                self._debounce_wait(conv_key, wait_ms)
            )

    async def _debounce_wait(self, conv_key: str, wait_ms: int) -> None:
        """Wait for debounce period, then trigger processing."""
        try:
            await asyncio.sleep(wait_ms / 1000.0)

            # Acquire lock and process
            lock = self._get_lock(conv_key)
            async with lock:
                state = self._conversations.get(conv_key)
                if state and state.pending_messages:
                    logger.info(
                        "[COALESCER] Debounce timer expired for %s - processing %d messages",
                        conv_key[:30],
                        len(state.pending_messages),
                    )
                    await self._process_now(conv_key, state)

        except asyncio.CancelledError:
            # Timer was reset by new message - this is normal
            logger.debug("[COALESCER] Debounce timer cancelled for %s", conv_key[:30])

    async def _process_now(self, conv_key: str, state: ConversationState) -> None:
        """Combine pending messages and process them."""
        if not state.pending_messages:
            return

        # Move pending messages to in-flight (will be restored if cancelled)
        messages_to_process = state.pending_messages.copy()
        state.in_flight_messages = messages_to_process
        state.pending_messages = []

        # Reset state for next batch
        state.first_message_at = time.time()
        state.cancel_token.reset()

        # Combine messages
        combined_content = self._combine_messages(messages_to_process)

        # Build combined payload (use first message as base)
        first_payload = messages_to_process[0].raw_payload.copy()
        first_payload["content"] = combined_content
        first_payload["_coalesced_count"] = len(messages_to_process)
        first_payload["_coalesced_message_ids"] = [m.message_id for m in messages_to_process]

        # Preserve media from first message that has it
        for msg in messages_to_process:
            if msg.media_url:
                first_payload["media_url"] = msg.media_url
                break

        logger.info(
            "[COALESCER] Processing coalesced message for %s (%d messages combined): %s",
            conv_key[:30],
            len(messages_to_process),
            (combined_content[:80] + "...") if len(combined_content) > 80 else combined_content,
        )

        # Create processing task
        async def _run_processing():
            try:
                await self._process_callback(first_payload, state.cancel_token)
                # Success - clear in-flight messages
                state.in_flight_messages = []
            except asyncio.CancelledError:
                logger.info("[COALESCER] Processing cancelled for %s", conv_key[:30])
                # In-flight messages will be restored by the canceller in _enqueue_message_locked
                raise
            except Exception as e:
                logger.error("[COALESCER] Processing failed for %s: %s", conv_key[:30], e, exc_info=True)
                # On failure, clear in-flight - the message was attempted
                # The orchestrator/agent handles retries internally
                state.in_flight_messages = []

        state.active_task = asyncio.create_task(_run_processing())

        # Don't await the task - let it run in background
        # This allows the coalescer to continue receiving messages

    def _combine_messages(self, messages: List[PendingMessage]) -> str:
        """
        Combine multiple messages into a single coherent message.

        Strategy:
        - Sort by timestamp (chronological order)
        - Deduplicate exact duplicates
        - Join with newlines
        - Filter empty messages
        """
        # Sort by timestamp
        sorted_messages = sorted(messages, key=lambda m: m.timestamp)

        # Deduplicate and filter
        seen = set()
        combined_parts = []

        for msg in sorted_messages:
            content = (msg.content or "").strip()
            if content and content not in seen:
                combined_parts.append(content)
                seen.add(content)

        return "\n".join(combined_parts)

    async def shutdown(self) -> None:
        """Gracefully shutdown the coalescer."""
        logger.info("[COALESCER] Shutting down...")

        # Cancel all debounce timers and active tasks
        for conv_key, state in self._conversations.items():
            if state.debounce_task and not state.debounce_task.done():
                state.debounce_task.cancel()

            if state.active_task and not state.active_task.done():
                state.cancel_token.cancel("Shutdown")
                state.active_task.cancel()

        # Wait for all tasks to complete
        all_tasks = []
        for state in self._conversations.values():
            if state.debounce_task:
                all_tasks.append(state.debounce_task)
            if state.active_task:
                all_tasks.append(state.active_task)

        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

        self._conversations.clear()
        logger.info("[COALESCER] Shutdown complete")
