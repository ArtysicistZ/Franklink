from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class GroupChatEvent:
    """
    Normalized group chat inbound event used by the group chat runtime.

    This is intentionally stable and decoupled from Photon payload shape.
    """

    chat_guid: str
    event_id: str
    message_id: Optional[str]
    timestamp: Optional[str]
    sender_handle: Optional[str]
    sender_user_id: Optional[str]
    sender_name: Optional[str]
    resolved_participant: str  # "user_a" | "user_b" | "unknown" | "participant_N"
    text: str
    media_url: Optional[str] = None
    raw_payload: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class GroupChatManagedContext:
    """
    Context about a Franklink-managed group chat loaded from persistent stores.

    Unified model supporting both 2-person and multi-person group chats.
    Uses participant_ids tuple as the source of truth for all participants.
    """

    chat_guid: str
    participant_ids: Tuple[str, ...] = field(default_factory=tuple)
    participant_modes: Dict[str, str] = field(default_factory=dict)
    connection_request_id: Optional[str] = None
    member_count: int = 0

    def get_participant_mode(self, user_id: str) -> Optional[str]:
        """Get mode for any participant by user ID."""
        return self.participant_modes.get(user_id)

    def is_participant(self, user_id: str) -> bool:
        """Check if a user is a participant in this chat."""
        return user_id in self.participant_ids
