"""Frozen dataclasses for agent-generated discovery conversations.

A discovery conversation is a simulated multi-agent dialogue between matched
users' AI agents. Each agent speaks on behalf of its user, surfacing shared
values, complementary skills, and reasons to connect.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class ConversationTurn:
    """A single dialogue turn in a discovery conversation."""

    speaker_name: str
    speaker_user_id: str
    content: str
    turn_index: int


@dataclass(frozen=True)
class DiscoveryConversation:
    """A generated multi-agent conversation between matched users' AI agents."""

    slug: str
    initiator_user_id: str
    participant_user_ids: List[str]
    turns: List[ConversationTurn]
    teaser_summary: str
    quality_score: Optional[float] = None
    flow_type: str = "reactive"
    connection_request_id: Optional[str] = None
    match_metadata: Dict[str, Any] = field(default_factory=dict)

    def to_db_payload(self) -> Dict[str, Any]:
        """Serialize to a dict suitable for database insertion."""
        return {
            "slug": self.slug,
            "initiator_user_id": self.initiator_user_id,
            "participant_user_ids": self.participant_user_ids,
            "turns": [
                {
                    "speaker_name": t.speaker_name,
                    "speaker_user_id": t.speaker_user_id,
                    "content": t.content,
                    "turn_index": t.turn_index,
                }
                for t in self.turns
            ],
            "teaser_summary": self.teaser_summary,
            "quality_score": self.quality_score,
            "flow_type": self.flow_type,
            "connection_request_id": self.connection_request_id,
            "match_metadata": self.match_metadata,
        }


@dataclass(frozen=True)
class ConversationPreviewResult:
    """Lightweight result passed downstream into the match data flow."""

    conversation_url: str
    teaser_summary: str
    quality_score: Optional[float] = None
    slug: str = ""
