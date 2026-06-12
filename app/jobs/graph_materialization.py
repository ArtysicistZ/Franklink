"""Materialize Zep facts into the cross-user knowledge graph.

Fetches each user's Zep facts, classifies them into the graph ontology
(Person, Skill, Organization, Domain, Project) using a fast LLM, and
upserts nodes + edges into graph_nodes / graph_edges tables.

This enables multi-hop matching queries like:
  "Find users who attend the same org AND have skills I need"
"""

import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.config import settings
from app.context import set_llm_context, clear_llm_context
from app.database.client import DatabaseClient
from app.integrations.azure_openai_client import AzureOpenAIClient
from app.integrations.zep_graph_client import get_zep_graph_client

logger = logging.getLogger(__name__)


GRAPH_CLASSIFICATION_PROMPT = """You are classifying knowledge graph facts into a structured ontology for a university networking platform.

Given facts about a user, extract graph relationships. Each relationship connects the user to an entity.

## Node Types
- skill: A professional skill or competency (e.g., "ml-engineering", "marketing", "design")
- organization: A university, company, club, or group (e.g., "upenn", "google", "blockchain-club")
- domain: An industry or interest area (e.g., "fintech", "ai-networking", "healthcare")
- project: A specific project, startup, or initiative (e.g., "franklink", "trading-bot")
- event: A campus or professional event (e.g., "pennapps-hackathon", "spring-career-fair", "ai-meetup")
- course: An academic class or seminar (e.g., "cis-121", "econ-101", "machine-learning-seminar")
- location: A physical place where the user is based (e.g., "levine-hall", "philadelphia", "san-francisco")
- role: A career position being sought (e.g., "swe-intern", "co-founder", "product-manager")

## Edge Types
- needs: User needs this skill (seeking help/learning)
- offers: User has this skill (can provide/teach)
- attends: User attends this university or institution
- interested_in: User is interested in this domain
- works_on: User is building/contributing to this project
- seeking_role: User is looking for this career role
- enrolled_in: User is taking or has taken this course
- member_of: User is a member of this club, student org, or team (distinct from attends)
- participating_in: User is attending or competing in this event
- located_in: User is based in or frequently at this location
- leads: User has a leadership role in this organization

## Rules
1. Normalize ALL names: lowercase, hyphenated, no spaces (e.g., "Machine Learning" -> "machine-learning")
2. Be specific: "upenn" not "university", "franklink" not "startup"
3. Only extract relationships with clear evidence in the facts
4. One fact may produce multiple relationships
5. Deduplicate: if multiple facts suggest the same relationship, include it only once
6. For skills, use the taxonomy: marketing, growth-marketing, sales, fundraising, engineering,
   frontend, backend, mobile-dev, ml-engineering, data-science, product-management, design,
   ux-design, finance, accounting, legal, operations, business-development, content-creation,
   research, data-analysis, devops, cybersecurity, blockchain, ai-ml, nlp, robotics, biotech,
   consulting, project-management, public-speaking, writing, trading, quantitative-finance
7. Use "attends" for universities/institutions, "member_of" for clubs/student orgs/teams
8. Use "participating_in" for one-time events, "member_of" for ongoing organizations
9. Use course codes when available (e.g., "cis-121" not "data-structures-class")
10. For locations, be specific: building name > neighborhood > city
11. Include date or season in event properties when mentioned (e.g., {"season": "spring-2026"})

## Output Format
Return ONLY valid JSON (no markdown, no explanation):
{
  "relationships": [
    {"edge_type": "works_on", "target_type": "project", "target_name": "franklink", "properties": {"description": "AI networking platform"}},
    {"edge_type": "attends", "target_type": "organization", "target_name": "upenn", "properties": {"type": "university"}},
    {"edge_type": "interested_in", "target_type": "domain", "target_name": "ai-networking"},
    {"edge_type": "offers", "target_type": "skill", "target_name": "ml-engineering", "properties": {"level": "advanced"}},
    {"edge_type": "enrolled_in", "target_type": "course", "target_name": "cis-121", "properties": {"semester": "fall-2025"}},
    {"edge_type": "participating_in", "target_type": "event", "target_name": "pennapps-hackathon", "properties": {"season": "spring-2026"}},
    {"edge_type": "member_of", "target_type": "organization", "target_name": "blockchain-club", "properties": {"type": "club"}},
    {"edge_type": "leads", "target_type": "organization", "target_name": "ai-club", "properties": {"role": "president"}},
    {"edge_type": "located_in", "target_type": "location", "target_name": "philadelphia"},
    {"edge_type": "seeking_role", "target_type": "role", "target_name": "ml-engineer-intern", "properties": {"timeline": "summer-2026"}}
  ]
}

If no relationships can be extracted, return: {"relationships": []}"""


def _normalize_name(name: str) -> str:
    """Normalize a node name to lowercase hyphenated form."""
    return name.strip().lower().replace(" ", "-").replace("_", "-")


def _format_facts_for_classification(facts: List[Dict[str, Any]]) -> str:
    """Format Zep facts for the classification prompt."""
    if not facts:
        return "No facts available."

    formatted = []
    for fact in facts[:75]:
        fact_text = fact.get("fact", "")
        if fact_text:
            formatted.append(f"- {fact_text}")

    return "\n".join(formatted) if formatted else "No facts available."


async def materialize_user_graph(
    user_id: str,
    db: Optional[DatabaseClient] = None,
    openai: Optional[AzureOpenAIClient] = None,
    filtered_facts: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Materialize a single user's Zep facts into the knowledge graph.

    Args:
        user_id: User UUID
        db: Database client (creates one if not provided)
        openai: OpenAI client (creates one if not provided)
        filtered_facts: Pre-filtered facts (skips Zep fetch + filtering if provided)

    Returns:
        Stats dict with nodes_created, edges_created, etc.
    """
    db = db or DatabaseClient()
    openai = openai or AzureOpenAIClient()

    stats = {
        "user_id": user_id,
        "nodes_upserted": 0,
        "edges_upserted": 0,
        "edges_deleted": 0,
        "facts_processed": 0,
        "error": None,
    }

    if not getattr(settings, "graph_materialization_enabled", True):
        stats["error"] = "disabled"
        return stats

    try:
        # Fetch user for name
        user = await db.get_user_by_id(user_id)
        if not user:
            stats["error"] = "user_not_found"
            return stats

        user_name = user.get("name") or "Unknown"

        # Step 1: Get facts (use provided filtered facts or fetch from Zep)
        if filtered_facts is not None:
            facts = filtered_facts
        else:
            zep = get_zep_graph_client()
            raw_facts = await zep.get_user_facts(user_id, limit=200)

            if not raw_facts:
                # Fallback: search graph edges with broad queries to extract facts
                # Some users have graph data via search but not via the /facts endpoint
                logger.debug(
                    f"No facts via get_user_facts for {user_id[:8]}..., "
                    "trying graph search fallback"
                )
                search_queries = [
                    "skills, interests, and expertise",
                    "organizations, university, and companies",
                    "projects, startups, and work",
                    "events, hackathons, conferences, career fairs, meetups, competitions",
                    "courses, classes, seminars, studying, enrolled, professor",
                    "clubs, student organizations, greek life, sports teams, committees, leadership",
                    "location, city, campus, building, neighborhood, based in",
                    "career, internship, job, recruiting, role, position, applying",
                ]
                seen_facts: set = set()
                raw_facts = []
                for query in search_queries:
                    results = await zep.search_graph(
                        user_id=user_id,
                        query=query,
                        scope="edges",
                        limit=25,
                    )
                    for r in results:
                        if r.fact and r.fact not in seen_facts:
                            seen_facts.add(r.fact)
                            raw_facts.append({"fact": r.fact})

                if not raw_facts:
                    logger.debug(
                        f"No Zep data for {user_id[:8]}..., "
                        "skipping graph materialization"
                    )
                    stats["error"] = "no_facts"
                    return stats

                logger.info(
                    f"Found {len(raw_facts)} facts via graph search fallback "
                    f"for {user_id[:8]}..."
                )

            facts = raw_facts

        stats["facts_processed"] = len(facts)

        # Step 2: Classify facts into ontology via LLM
        facts_text = _format_facts_for_classification(facts)
        user_prompt = f"User: {user_name}\n\nFacts:\n{facts_text}"

        response = await openai.generate_response(
            system_prompt=GRAPH_CLASSIFICATION_PROMPT,
            user_prompt=user_prompt,
            model="gpt-4o-mini",
            temperature=0.0,
            trace_label=f"graph_classify_{user_id[:8]}",
        )

        # Parse LLM response
        cleaned = response.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        classification = json.loads(cleaned)
        relationships = classification.get("relationships", [])

        if not relationships:
            logger.info(f"No relationships extracted for {user_id[:8]}...")
            return stats

        # Step 3: Ensure person node exists for this user
        person_node = await db.upsert_graph_node(
            node_type="person",
            name=_normalize_name(user_name),
            properties={"user_id": user_id},
        )

        if not person_node:
            stats["error"] = "person_node_creation_failed"
            return stats

        person_node_id = person_node["id"]
        stats["nodes_upserted"] += 1

        # Step 4: Delete existing edges for this user (full rebuild)
        deleted = await db.delete_graph_edges_for_user(person_node_id)
        stats["edges_deleted"] = deleted

        # Step 5: Upsert target nodes and edges
        for rel in relationships:
            target_type = rel.get("target_type")
            target_name = rel.get("target_name")
            edge_type = rel.get("edge_type")
            properties = rel.get("properties") or {}

            if not target_type or not target_name or not edge_type:
                continue

            # Normalize target name
            target_name = _normalize_name(target_name)

            # Validate types
            valid_node_types = {"skill", "organization", "domain", "project", "event", "course", "location", "role"}
            valid_edge_types = {
                "needs", "offers", "attends", "interested_in", "works_on", "seeking_role",
                "enrolled_in", "member_of", "participating_in", "located_in", "leads",
            }

            if target_type not in valid_node_types:
                logger.debug(f"Skipping invalid node type: {target_type}")
                continue

            if edge_type not in valid_edge_types:
                logger.debug(f"Skipping invalid edge type: {edge_type}")
                continue

            # Upsert target node (shared across users)
            target_node = await db.upsert_graph_node(
                node_type=target_type,
                name=target_name,
                properties=properties,
            )

            if not target_node:
                continue

            stats["nodes_upserted"] += 1

            # Upsert edge from person to target
            edge = await db.upsert_graph_edge(
                source_node_id=person_node_id,
                target_node_id=target_node["id"],
                edge_type=edge_type,
                properties=properties,
            )

            if edge:
                stats["edges_upserted"] += 1

        logger.info(
            f"Materialized graph for {user_id[:8]}... "
            f"({stats['nodes_upserted']} nodes, {stats['edges_upserted']} edges, "
            f"{stats['edges_deleted']} deleted)"
        )

        return stats

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse graph classification for {user_id[:8]}...: {e}")
        stats["error"] = f"json_parse_error: {e}"
        return stats
    except Exception as e:
        logger.error(f"Graph materialization failed for {user_id[:8]}...: {e}", exc_info=True)
        stats["error"] = str(e)
        return stats


async def run_graph_materialization_job(
    batch_size: int = 50,
    rate_limit_seconds: float = 2.0,
) -> Dict[str, Any]:
    """Batch job to materialize graphs for all users.

    Args:
        batch_size: Max users to process per run
        rate_limit_seconds: Pause between users to avoid API limits

    Returns:
        Dict with job statistics
    """
    if not getattr(settings, "graph_materialization_enabled", True):
        logger.info("Graph materialization job disabled via settings")
        return {"status": "disabled"}

    db = DatabaseClient()
    openai = AzureOpenAIClient()

    stats = {
        "started_at": datetime.utcnow().isoformat(),
        "users_processed": 0,
        "users_materialized": 0,
        "users_skipped": 0,
        "errors": 0,
        "total_nodes": 0,
        "total_edges": 0,
    }

    try:
        # Get all onboarded users (reuse simple query)
        result = db.client.table("users").select(
            "id, name"
        ).eq(
            "is_onboarded", True
        ).not_.is_(
            "phone_number", "null"
        ).limit(batch_size).execute()

        users = result.data or []
        logger.info(f"Graph materialization job starting: {len(users)} users to process")

        for user_record in users:
            user_id = user_record.get("id")

            try:
                # Set LLM context for usage tracking
                set_llm_context(user_id=user_id, job_type="graph_materialization")

                user_stats = await materialize_user_graph(
                    user_id=user_id,
                    db=db,
                    openai=openai,
                )

                stats["users_processed"] += 1

                if user_stats.get("error"):
                    if user_stats["error"] in ("no_facts", "disabled"):
                        stats["users_skipped"] += 1
                    else:
                        stats["errors"] += 1
                else:
                    stats["users_materialized"] += 1
                    stats["total_nodes"] += user_stats.get("nodes_upserted", 0)
                    stats["total_edges"] += user_stats.get("edges_upserted", 0)

            except Exception as e:
                logger.error(f"Error processing {user_id[:8]}...: {e}")
                stats["errors"] += 1
            finally:
                clear_llm_context()

            await asyncio.sleep(rate_limit_seconds)

        stats["completed_at"] = datetime.utcnow().isoformat()
        stats["status"] = "completed"

        logger.info(
            f"Graph materialization job completed: "
            f"{stats['users_materialized']} materialized, "
            f"{stats['users_skipped']} skipped, "
            f"{stats['errors']} errors, "
            f"{stats['total_nodes']} nodes, {stats['total_edges']} edges"
        )

        return stats

    except Exception as e:
        logger.error(f"Graph materialization job failed: {e}", exc_info=True)
        stats["status"] = "failed"
        stats["error"] = str(e)
        return stats
