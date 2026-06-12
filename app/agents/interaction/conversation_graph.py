"""Graph data assembly for discovery conversation generation.

Fetches each participant's subgraph from the cross-user context graph and
formats it as structured text suitable for the LLM conversation prompt.
Includes edge properties (experience level, timelines, roles) and target
node types for richer, more specific conversations.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Edge types grouped by semantic category for prompt formatting
_EDGE_LABEL_MAP: Dict[str, str] = {
    "needs": "Needs (skills they're looking for)",
    "offers": "Has expertise in",
    "attends": "Attends",
    "interested_in": "Passionate about",
    "works_on": "Currently building",
    "seeking_role": "Looking for role as",
    "enrolled_in": "Taking/took courses in",
    "member_of": "Active member of",
    "participating_in": "Participating in",
    "located_in": "Based in",
    "leads": "Leads",
}

# Properties worth surfacing in the prompt
_RELEVANT_PROPERTIES = {
    "level", "experience_level", "role", "type", "description",
    "semester", "season", "timeline", "focus", "status",
}


def _format_edge_target(target_name: str, node_type: str, properties: Dict[str, Any]) -> str:
    """Format a single edge target with type and properties for the prompt."""
    # Clean up the hyphenated name for readability
    display_name = target_name.replace("-", " ").title()

    # Collect relevant property annotations
    annotations: List[str] = []
    for key in _RELEVANT_PROPERTIES:
        val = properties.get(key)
        if val:
            annotations.append(f"{key}: {val}")

    if annotations:
        return f"{display_name} [{node_type}] ({', '.join(annotations)})"
    return f"{display_name} [{node_type}]"


@dataclass(frozen=True)
class EdgeDetail:
    """Rich edge information including target node type and properties."""

    target_name: str
    target_node_type: str
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ParticipantGraphData:
    """Structured graph data for a single participant."""

    user_id: str
    name: str
    edges_by_type: Dict[str, List[EdgeDetail]] = field(default_factory=dict)
    holistic_summary: Optional[str] = None

    def to_prompt_text(self) -> str:
        """Format this participant's graph data as rich LLM prompt text."""
        lines: List[str] = []
        for edge_type, label in _EDGE_LABEL_MAP.items():
            details = self.edges_by_type.get(edge_type, [])
            if details:
                formatted = [
                    _format_edge_target(d.target_name, d.target_node_type, d.properties)
                    for d in details
                ]
                lines.append(f"- {label}: {'; '.join(formatted)}")
        if self.holistic_summary:
            lines.append(f"- Background summary: {self.holistic_summary}")
        if not lines:
            lines.append("- (no graph data available)")
        return "\n".join(lines)


async def assemble_participant_graphs(
    db: Any,
    user_ids: List[str],
    user_names: Optional[Dict[str, str]] = None,
    holistic_summaries: Optional[Dict[str, str]] = None,
) -> Dict[str, ParticipantGraphData]:
    """Fetch graph subgraphs for all participants and return structured data.

    Args:
        db: DatabaseClient instance (has _GraphMethods mixin).
        user_ids: List of user UUID strings to fetch graphs for.
        user_names: Optional mapping of user_id -> display name.
        holistic_summaries: Optional mapping of user_id -> holistic profile summary.

    Returns:
        Dict mapping user_id -> ParticipantGraphData. Users with no graph
        data are included with empty edges.
    """
    if not user_ids:
        return {}

    names = user_names or {}
    summaries = holistic_summaries or {}

    # Batch fetch person nodes for all participants
    person_nodes = await db.get_person_nodes_for_user_ids(user_ids)
    if not person_nodes:
        logger.warning(
            "[CONVERSATION_GRAPH] No person nodes found for %d users", len(user_ids)
        )
        return {
            uid: ParticipantGraphData(
                user_id=uid,
                name=names.get(uid, "Unknown"),
                holistic_summary=summaries.get(uid),
            )
            for uid in user_ids
        }

    # Map person_node_id -> user_id for grouping edges later
    node_id_to_user_id: Dict[str, str] = {}
    for node in person_nodes:
        props = node.get("properties") or {}
        uid = props.get("user_id")
        if uid:
            node_id_to_user_id[node["id"]] = uid

    person_node_ids = list(node_id_to_user_id.keys())

    # Batch fetch all outgoing edges with target node details
    edges = await db.get_subgraph_for_person_nodes(person_node_ids)

    # Group edges by user with full detail
    user_edges: Dict[str, Dict[str, List[EdgeDetail]]] = {uid: {} for uid in user_ids}
    for edge in edges:
        source_node_id = edge.get("source_node_id")
        uid = node_id_to_user_id.get(source_node_id)
        if not uid:
            continue

        edge_type = edge.get("edge_type", "")
        target_node = edge.get("target_node") or {}
        target_name = target_node.get("name", "")
        target_node_type = target_node.get("node_type", "")
        edge_properties = edge.get("properties") or {}
        if not target_name:
            continue

        detail = EdgeDetail(
            target_name=target_name,
            target_node_type=target_node_type,
            properties=edge_properties,
        )

        if edge_type not in user_edges[uid]:
            user_edges[uid][edge_type] = []
        user_edges[uid][edge_type].append(detail)

    # Build ParticipantGraphData for each user
    result: Dict[str, ParticipantGraphData] = {}
    for uid in user_ids:
        result[uid] = ParticipantGraphData(
            user_id=uid,
            name=names.get(uid, "Unknown"),
            edges_by_type=user_edges.get(uid, {}),
            holistic_summary=summaries.get(uid),
        )

    return result
