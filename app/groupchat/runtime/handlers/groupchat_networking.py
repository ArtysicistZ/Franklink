"""Group Chat Networking handler for Frank invocations in group chats.

This handler:
1. Only responds when Frank is explicitly invoked
2. Passes is_group_chat_context=True to InteractionAgent
3. Allows groupchat_maintenance and groupchat_networking tasks
4. Includes group chat participants for context
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Optional
from datetime import datetime, timedelta

from app.groupchat.runtime.deps import GroupChatRuntimeDeps
from app.groupchat.runtime.handlers import GroupChatHandler
from app.groupchat.runtime.types import GroupChatEvent, GroupChatManagedContext
from app.agents.interaction.agent import InteractionAgentNew
from app.agents.memory.task_history import TaskHistorySaver

logger = logging.getLogger(__name__)


def _looks_like_frank_invocation(text: str) -> bool:
    """Check if message looks like an invocation of Frank."""
    msg = (text or "").strip().lower()
    if not msg:
        return False
    # Accept explicit mentions anywhere in the message, not just at the start.
    # This matches cases like "Add more people for this groupchat Frank!"
    if re.search(r"\bfrank\b", msg):
        return True
    if "@frank" in msg:
        return True
    return False


def _strip_invocation(text: str) -> str:
    """Strip the Frank invocation prefix from the message."""
    msg = (text or "").strip()
    if not msg:
        return msg
    patterns = [
        r"^@frank\b[:,]?\s*",
        r"^frank\b[:,]?\s*",
        r"^(hey|hi|yo)\s+frank\b[:,]?\s*",
        r"\s+@frank\b[!?.]*\s*$",
        r"\s+frank\b[!?.]*\s*$",
    ]
    for pattern in patterns:
        cleaned = re.sub(pattern, "", msg, flags=re.IGNORECASE)
        if cleaned != msg:
            return cleaned.strip()
    return msg


@dataclass
class GroupChatNetworkingHandler(GroupChatHandler):
    """Handler for group chat maintenance + networking tasks.

    This handler passes is_group_chat_context=True so the InteractionAgent
    can route between groupchat_maintenance and groupchat_networking.
    """

    name: str = "groupchat_networking"

    async def handle(
        self,
        *,
        event: GroupChatEvent,
        managed: Optional[GroupChatManagedContext],
        deps: GroupChatRuntimeDeps,
    ) -> bool:
        """Handle a group chat event.

        Args:
            event: The group chat event
            managed: Optional managed context for the chat
            deps: Runtime dependencies

        Returns:
            True if the event was handled, False otherwise
        """
        # Handle explicit Frank invocations OR follow-up replies to a pending
        # group chat networking/maintenance request in this chat.
        if not _looks_like_frank_invocation(event.text):
            has_pending = False
            if event.sender_user_id and event.chat_guid:
                has_pending = await self._has_pending_groupchat_request(
                    deps, str(event.sender_user_id), event.chat_guid
                )
                if not has_pending:
                    has_pending = await self._has_pending_groupchat_maintenance(
                        deps, str(event.sender_user_id), event.chat_guid
                    )
            if not has_pending:
                return False

        message_text = _strip_invocation(event.text) if _looks_like_frank_invocation(event.text) else (event.text or "")
        if not message_text:
            message_text = "hey"

        # Get user
        user = None
        if event.sender_user_id:
            user = await deps.db.get_user_by_id(str(event.sender_user_id))
        if not user:
            user = await deps.db.get_or_create_user(str(event.sender_handle or ""))

        # Get participant names for context
        group_chat_participants = await self._get_participant_names(
            deps, event.chat_guid
        )

        # Create agent
        agent = InteractionAgentNew(
            db=deps.db,
            photon=deps.photon,
            openai=deps.openai,
        )

        webhook = SimpleNamespace(
            content=message_text,
            from_number=event.sender_handle,
            message_id=event.message_id or event.event_id,
            timestamp=event.timestamp,
            media_url=event.media_url,
            chat_guid=event.chat_guid,
        )

        # Process with group chat context flag - allows groupchat_maintenance and groupchat_networking
        result = await agent.process_message(
            phone_number=webhook.from_number,
            message_content=webhook.content,
            user=user,
            webhook_data={
                "message_id": webhook.message_id,
                "timestamp": webhook.timestamp,
                "media_url": webhook.media_url,
                "chat_guid": webhook.chat_guid,
                "is_group_chat_context": True,  # KEY: signals group chat mode
                "group_chat_participants": group_chat_participants,
            },
        )

        if not result.get("success"):
            logger.debug("[GROUPCHAT][MAINTENANCE] no response generated")
            return False

        # Extract responses
        responses = result.get("responses") if isinstance(result.get("responses"), list) else []
        if not responses and result.get("response_text"):
            responses = [
                {
                    "response_text": result.get("response_text"),
                    "intent": result.get("intent"),
                    "task": result.get("intent"),
                }
            ]

        # Send responses
        sent_any = False
        for response in responses:
            text = str(response.get("response_text") or "").strip()
            if not text:
                continue
            await deps.sender.send_and_record(
                chat_guid=event.chat_guid,
                content=text,
                metadata={
                    "task": response.get("task"),
                    "intent": response.get("intent"),
                    "handler": self.name,
                },
            )
            sent_any = True

        return sent_any

    async def _has_pending_groupchat_request(
        self,
        deps: GroupChatRuntimeDeps,
        user_id: str,
        chat_guid: str,
    ) -> bool:
        """Check if user has a pending connection request tied to this chat."""
        try:
            initiator_reqs, target_reqs = await asyncio.gather(
                deps.db.list_pending_requests_for_initiator(user_id, limit=5),
                deps.db.list_pending_requests_for_target(user_id, limit=5),
            )
            for req in initiator_reqs + target_reqs:
                if str(req.get("group_chat_guid")) == str(chat_guid):
                    return True
        except Exception as e:
            logger.warning("[GROUPCHAT][NETWORKING] Failed to check pending requests: %s", e)
        return False

    async def _has_pending_groupchat_maintenance(
        self,
        deps: GroupChatRuntimeDeps,
        user_id: str,
        chat_guid: str,
    ) -> bool:
        """Check if user has a pending groupchat_maintenance flow for this chat."""
        waiting_targets = {
            "meeting_time_clarification",
            "meeting_organizer_clarification",
            "meeting_attendee_clarification",
            "calendar_connect",
        }
        try:
            history = TaskHistorySaver(deps.db)
            records = await history.get_recent_tasks(user_id, limit=5)
            for record in records:
                if record.task_name != "groupchat_maintenance":
                    continue
                key_data = record.key_data or {}
                waiting_for = key_data.get("waiting_for")
                if waiting_for not in waiting_targets:
                    continue
                pending_task = key_data.get("pending_task") or {}
                pending_chat = pending_task.get("chat_guid")
                if pending_chat and str(pending_chat) == str(chat_guid):
                    return True

                # If chat_guid is missing but the record is very recent, allow continuation.
                created_at = record.created_at
                if not pending_chat and created_at:
                    try:
                        created_dt = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
                        if datetime.utcnow() - created_dt <= timedelta(hours=2):
                            return True
                    except Exception:
                        pass
        except Exception as e:
            logger.warning("[GROUPCHAT][MAINTENANCE] Failed to check pending maintenance: %s", e)
        return False

    async def _get_participant_names(
        self,
        deps: GroupChatRuntimeDeps,
        chat_guid: str,
    ) -> list:
        """Get names of all participants in the group chat.

        Args:
            deps: Runtime dependencies
            chat_guid: The group chat GUID

        Returns:
            List of participant names
        """
        try:
            participants = await deps.db.get_group_chat_participants(chat_guid)
            names = []
            for p in participants:
                user = await deps.db.get_user_by_id(p.get("user_id"))
                if user and user.get("name"):
                    names.append(user.get("name"))
            return names
        except Exception as e:
            logger.warning(
                "[GROUPCHAT][MAINTENANCE] Failed to get participant names: %s", e
            )
            return []
