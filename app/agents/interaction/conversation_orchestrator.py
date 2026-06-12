"""End-to-end orchestrator for discovery conversation generation.

Ties together graph assembly, LLM generation, persistence, and URL
construction. This is the single integration point called from both
reactive and proactive match flows.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.config import settings

from .conversation_generator import (
    ParticipantInfo,
    generate_discovery_conversation,
    score_conversation_quality,
)
from .conversation_graph import assemble_participant_graphs
from .conversation_preview import ConversationPreviewResult

logger = logging.getLogger(__name__)


def _resolve_name(name: str, user_id: str, fallback_prefix: str = "User") -> str:
    """Return name if truthy, else a readable fallback using user_id prefix."""
    if name and name.strip():
        return name.strip()
    # Handle None or empty user_id defensively
    uid_suffix = str(user_id or "")[:8] or "unknown"
    return f"{fallback_prefix} {uid_suffix}"


async def create_conversation_preview(
    db: Any,
    openai: Any,
    initiator_user_id: str,
    initiator_name: str,
    match_result: Dict[str, Any],
    flow_type: str = "reactive",
    connection_request_id: Optional[str] = None,
) -> Optional[ConversationPreviewResult]:
    """Generate a discovery conversation and return a preview with URL + teaser.

    This is the single function called from both reactive and proactive flows.
    Returns None (graceful degradation) if:
    - conversation_preview_enabled is False
    - Graph data is insufficient
    - LLM generation fails
    - Quality score is below threshold (when filter enabled)

    Args:
        db: DatabaseClient instance.
        openai: AzureOpenAIClient instance.
        initiator_user_id: The requesting user's ID.
        initiator_name: The requesting user's display name.
        match_result: Dict with target_user_id, target_name, matching_reasons,
            mutual_benefit, demand_satisfaction, etc. For multi-match, may
            contain all_matches list.
        flow_type: "reactive" or "proactive".
        connection_request_id: Optional connection request ID.

    Returns:
        ConversationPreviewResult with URL and teaser, or None on failure.
    """
    if not settings.conversation_preview_enabled:
        return None

    try:
        # Collect all participant user IDs and names (with fallback to avoid "Unknown")
        all_user_ids: List[str] = [initiator_user_id]
        user_names: Dict[str, str] = {
            initiator_user_id: _resolve_name(initiator_name, initiator_user_id)
        }

        # Single match
        target_uid = match_result.get("target_user_id")
        target_name = match_result.get("target_name", "")
        if target_uid:
            all_user_ids.append(target_uid)
            user_names[target_uid] = _resolve_name(target_name, target_uid)

        # Multi-match (group)
        all_matches = match_result.get("all_matches", [])
        for m in all_matches:
            uid = m.get("target_user_id")
            name = m.get("target_name", "")
            if uid and uid not in user_names:
                all_user_ids.append(uid)
                user_names[uid] = _resolve_name(name, uid)

        if len(all_user_ids) < 2:
            logger.warning(
                "[CONVERSATION_ORCHESTRATOR] Not enough participants (need >=2, got %d)",
                len(all_user_ids),
            )
            return None

        # Cap participants to keep LLM prompt quality high
        max_participants = 6
        if len(all_user_ids) > max_participants:
            logger.info(
                "[CONVERSATION_ORCHESTRATOR] Capping participants from %d to %d",
                len(all_user_ids),
                max_participants,
            )
            all_user_ids = all_user_ids[:max_participants]

        # Fetch holistic summaries if available
        holistic_summaries: Dict[str, str] = {}
        for uid in all_user_ids:
            try:
                profile = await db.get_user_profile(uid)
                if profile and profile.get("holistic_summary"):
                    holistic_summaries[uid] = profile["holistic_summary"]
            except Exception:
                pass

        # Assemble graph data for all participants
        graph_data = await assemble_participant_graphs(
            db=db,
            user_ids=all_user_ids,
            user_names=user_names,
            holistic_summaries=holistic_summaries,
        )

        # Check if we have meaningful data for at least the initiator
        # (graph edges OR holistic summary — either is sufficient for the LLM)
        initiator_graph = graph_data.get(initiator_user_id)
        if not initiator_graph or (
            not initiator_graph.edges_by_type and not initiator_graph.holistic_summary
        ):
            logger.info(
                "[CONVERSATION_ORCHESTRATOR] Insufficient graph data for initiator %s",
                initiator_user_id[:8],
            )
            return None

        # Build participant info list
        participants: List[ParticipantInfo] = []
        for uid in all_user_ids:
            gd = graph_data.get(uid)
            if gd:
                participants.append(
                    ParticipantInfo(
                        user_id=uid,
                        name=_resolve_name(user_names.get(uid, ""), uid),
                        graph_data=gd,
                        role_label="initiator" if uid == initiator_user_id else "match",
                    )
                )

        # Build match metadata for the generator
        connection_purpose = match_result.get("connection_purpose", "")
        match_metadata: Dict[str, Any] = {
            "matching_reasons": match_result.get("matching_reasons", []),
            "mutual_benefit": match_result.get("mutual_benefit", ""),
            "demand_satisfaction": match_result.get("demand_satisfaction", ""),
            "match_summary": match_result.get("match_summary", ""),
            "match_confidence": match_result.get("match_confidence", 0.0),
            "connection_purpose": connection_purpose,
            "connection_request_id": connection_request_id,
            "flow_type": flow_type,
        }

        logger.info(
            "[CONVERSATION_ORCHESTRATOR] connection_purpose=%s (from match_result keys: %s)",
            connection_purpose[:80] if connection_purpose else "(empty)",
            list(match_result.keys()),
        )

        # Generate the conversation
        conversation = await generate_discovery_conversation(
            initiator_user_id=initiator_user_id,
            initiator_name=initiator_name,
            participants=participants,
            match_metadata=match_metadata,
            openai=openai,
        )

        # Optional quality filtering
        quality_score = None
        if settings.conversation_preview_quality_filter_enabled:
            quality_score = await score_conversation_quality(conversation, openai)
            if quality_score < settings.conversation_preview_quality_threshold:
                logger.info(
                    "[CONVERSATION_ORCHESTRATOR] Conversation quality too low "
                    "(%.2f < %.2f), skipping",
                    quality_score,
                    settings.conversation_preview_quality_threshold,
                )
                return None

        # Persist to database
        db_payload = conversation.to_db_payload()
        if quality_score is not None:
            db_payload["quality_score"] = quality_score

        row = await db.create_discovery_conversation(**db_payload)
        if not row:
            logger.error("[CONVERSATION_ORCHESTRATOR] Failed to persist conversation")
            return None

        slug = conversation.slug
        base_url = settings.conversation_preview_base_url.rstrip("/")
        conversation_url = f"{base_url}/c/{slug}"

        logger.info(
            "[CONVERSATION_ORCHESTRATOR] Created conversation preview: %s",
            conversation_url,
        )

        return ConversationPreviewResult(
            conversation_url=conversation_url,
            teaser_summary=conversation.teaser_summary,
            quality_score=quality_score,
            slug=slug,
        )

    except Exception as e:
        logger.error(
            "[CONVERSATION_ORCHESTRATOR] Failed to create conversation preview: %s",
            e,
            exc_info=True,
        )
        return None
