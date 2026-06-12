"""Group Chat Maintenance handler for Frank invocations in group chats.

This handler:
1. Only responds when Frank is explicitly invoked
2. Passes is_group_chat_context=True to InteractionAgent
3. Restricts available tasks to groupchat_maintenance only
4. Includes group chat participants for context
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Optional

from app.groupchat.runtime.deps import GroupChatRuntimeDeps
from app.groupchat.runtime.handlers import GroupChatHandler
from app.groupchat.runtime.types import GroupChatEvent, GroupChatManagedContext
from app.agents.interaction.agent import InteractionAgentNew

logger = logging.getLogger(__name__)


def _looks_like_frank_invocation(text: str) -> bool:
    """Check if message looks like an invocation of Frank."""
    msg = (text or "").strip().lower()
    if not msg:
        return False
    if msg.startswith("frank"):
        return True
    if msg.startswith("@frank"):
        return True
    if msg.startswith("hey frank") or msg.startswith("hi frank") or msg.startswith("yo frank"):
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
    ]
    for pattern in patterns:
        cleaned = re.sub(pattern, "", msg, flags=re.IGNORECASE)
        if cleaned != msg:
            return cleaned.strip()
    return msg


@dataclass
class GroupChatMaintenanceHandler(GroupChatHandler):
    """Handler for group chat maintenance tasks.

    This handler replaces InteractionAgentHandler for group chat context,
    passing is_group_chat_context=True to restrict available tasks to
    groupchat_maintenance only (no networking/update tasks).
    """

    name: str = "groupchat_maintenance"

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
        # Only handle Frank invocations
        if not _looks_like_frank_invocation(event.text):
            return False

        message_text = _strip_invocation(event.text)
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

        # Process with group chat context flag - this restricts tasks to groupchat_maintenance
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
