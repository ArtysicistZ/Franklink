"""Graph database methods for cross-user knowledge graph.

Provides CRUD operations for graph_nodes and graph_edges tables,
plus the match_users_graph_combined RPC for graph-based matching.
"""

import logging
from typing import Any, Dict, List, Optional

from postgrest.exceptions import APIError

logger = logging.getLogger(__name__)


class _GraphMethods:

    async def upsert_graph_node(
        self,
        node_type: str,
        name: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Upsert a node into the knowledge graph.

        Uses ON CONFLICT (node_type, name) to prevent duplicates.
        Shared nodes (skills, orgs, domains) are referenced by multiple users.

        Args:
            node_type: One of 'person', 'skill', 'organization', 'domain', 'project'
            name: Normalized lowercase-hyphenated node name
            properties: Optional JSONB properties (user_id for person nodes, etc.)

        Returns:
            The upserted node record, or None on error
        """
        try:
            result = self.client.table("graph_nodes").upsert(
                {
                    "node_type": node_type,
                    "name": name,
                    "properties": properties or {},
                },
                on_conflict="node_type,name",
            ).execute()

            if result.data:
                return result.data[0]
            return None

        except Exception as e:
            logger.error(f"Error upserting graph node ({node_type}, {name}): {e}")
            return None

    async def upsert_graph_edge(
        self,
        source_node_id: str,
        target_node_id: str,
        edge_type: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Upsert an edge into the knowledge graph.

        Uses ON CONFLICT (source_node_id, target_node_id, edge_type)
        to prevent duplicate edges.

        Args:
            source_node_id: Source node UUID
            target_node_id: Target node UUID
            edge_type: One of 'needs', 'offers', 'attends', 'interested_in', 'works_on', 'seeking_role'
            properties: Optional JSONB properties (urgency, experience_level, etc.)

        Returns:
            The upserted edge record, or None on error
        """
        try:
            result = self.client.table("graph_edges").upsert(
                {
                    "source_node_id": source_node_id,
                    "target_node_id": target_node_id,
                    "edge_type": edge_type,
                    "properties": properties or {},
                },
                on_conflict="source_node_id,target_node_id,edge_type",
            ).execute()

            if result.data:
                return result.data[0]
            return None

        except Exception as e:
            logger.error(
                f"Error upserting graph edge ({edge_type}, {source_node_id} -> {target_node_id}): {e}"
            )
            return None

    async def get_graph_node_by_type_and_name(
        self,
        node_type: str,
        name: str,
    ) -> Optional[Dict[str, Any]]:
        """Look up a graph node by type and name.

        Args:
            node_type: Node type (e.g., 'person', 'skill')
            name: Normalized node name

        Returns:
            Node record or None
        """
        try:
            result = (
                self.client.table("graph_nodes")
                .select("*")
                .eq("node_type", node_type)
                .eq("name", name)
                .execute()
            )
            if result.data:
                return result.data[0]
            return None

        except Exception as e:
            logger.error(f"Error looking up graph node ({node_type}, {name}): {e}")
            return None

    async def get_person_node(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get the person node for a user.

        Args:
            user_id: User UUID string

        Returns:
            Person node record or None
        """
        try:
            result = (
                self.client.table("graph_nodes")
                .select("*")
                .eq("node_type", "person")
                .eq("properties->>user_id", user_id)
                .execute()
            )
            if result.data:
                return result.data[0]
            return None

        except Exception as e:
            logger.error(f"Error looking up person node for {user_id[:8]}...: {e}")
            return None

    async def get_graph_edges_for_user(
        self,
        person_node_id: str,
    ) -> List[Dict[str, Any]]:
        """Get all outgoing edges from a person node.

        Args:
            person_node_id: Person node UUID

        Returns:
            List of edge records
        """
        try:
            result = (
                self.client.table("graph_edges")
                .select("*")
                .eq("source_node_id", person_node_id)
                .execute()
            )
            return result.data or []

        except Exception as e:
            logger.error(f"Error getting edges for node {person_node_id[:8]}...: {e}")
            return []

    async def delete_graph_edges_for_user(
        self,
        person_node_id: str,
    ) -> int:
        """Delete all outgoing edges from a person node.

        Used before re-materialization to ensure a clean rebuild.

        Args:
            person_node_id: Person node UUID

        Returns:
            Number of deleted edges
        """
        try:
            result = (
                self.client.table("graph_edges")
                .delete()
                .eq("source_node_id", person_node_id)
                .execute()
            )
            count = len(result.data) if result.data else 0
            logger.debug(f"Deleted {count} edges for node {person_node_id[:8]}...")
            return count

        except Exception as e:
            logger.error(f"Error deleting edges for node {person_node_id[:8]}...: {e}")
            return 0

    async def get_person_nodes_for_user_ids(
        self,
        user_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """Batch fetch person nodes for multiple user IDs.

        Args:
            user_ids: List of user UUID strings

        Returns:
            List of person node records with id, name, properties
        """
        if not user_ids:
            return []

        try:
            result = (
                self.client.table("graph_nodes")
                .select("id, node_type, name, properties")
                .eq("node_type", "person")
                .in_("properties->>user_id", user_ids)
                .execute()
            )
            return result.data or []

        except Exception as e:
            logger.error(f"Error fetching person nodes for {len(user_ids)} users: {e}")
            return []

    async def get_subgraph_for_person_nodes(
        self,
        person_node_ids: List[str],
    ) -> List[Dict[str, Any]]:
        """Batch fetch all outgoing edges with target node details for multiple person nodes.

        Uses PostgREST resource embedding to join target node data in a single query.

        Args:
            person_node_ids: List of person node UUIDs

        Returns:
            List of edge dicts with nested 'target_node' containing
            {id, node_type, name}. Empty list if none found or on error.
        """
        if not person_node_ids:
            return []

        try:
            result = (
                self.client.table("graph_edges")
                .select(
                    "id, source_node_id, target_node_id, edge_type, properties, "
                    "target_node:graph_nodes!target_node_id(id, node_type, name)"
                )
                .in_("source_node_id", person_node_ids)
                .execute()
            )
            return result.data or []

        except Exception as e:
            logger.error(f"Error fetching subgraph for {len(person_node_ids)} nodes: {e}")
            return []

    async def match_users_graph(
        self,
        user_id: str,
        exclude_user_ids: Optional[List[str]] = None,
        match_count: int = 15,
    ) -> List[Dict[str, Any]]:
        """Find users connected through the knowledge graph.

        Calls the match_users_graph_combined RPC which combines:
        - Shared context (same org/project)
        - Domain bridges (same interest domain)
        - Skill graph paths (they offer what I need)

        Args:
            user_id: Initiator's user UUID
            exclude_user_ids: User IDs to exclude from results
            match_count: Maximum matches to return

        Returns:
            List of matching users with graph_score, shared_context, match_strategies
        """
        try:
            result = self.client.rpc(
                "match_users_graph_combined",
                {
                    "p_user_id": user_id,
                    "p_exclude_user_ids": exclude_user_ids or [],
                    "p_match_count": match_count,
                },
            ).execute()

            matches = result.data or []

            logger.info(
                f"Found {len(matches)} graph matches for user {user_id[:8]}..."
            )
            return matches

        except Exception as e:
            logger.error(f"Error in match_users_graph: {e}", exc_info=True)
            return []
