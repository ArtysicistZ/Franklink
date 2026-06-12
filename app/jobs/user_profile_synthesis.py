"""Background job for synthesizing holistic user profiles from Zep knowledge graph.

This job analyzes a user's Zep knowledge graph (emails, conversations, signals)
and synthesizes a holistic understanding of who they are, including:
- Inferred traits (personality, communication style, work patterns)
- Latent needs (what they actually need vs what they ask for)
- Relationship potential (ideal relationship types, strengths, risks)
- Life trajectory (career stage, motivations, direction)

The synthesized profile is used to enhance matching by understanding users
at a deeper level than explicit demand/value statements.
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.config import settings
from app.context import set_llm_context, clear_llm_context
from app.database.client import DatabaseClient
from app.integrations.azure_openai_client import AzureOpenAIClient
from app.integrations.zep_graph_client import get_zep_graph_client

logger = logging.getLogger(__name__)


# In-process refresh guards to avoid redundant profile refresh storms.
_PROFILE_REFRESH_TASKS: Dict[str, asyncio.Task] = {}
_PROFILE_REFRESH_LAST_RUN: Dict[str, float] = {}


PROFILE_SYNTHESIS_SYSTEM_PROMPT = """You are analyzing a user's professional profile for Franklink, a networking platform for students and early-career professionals.

Given their Zep knowledge graph facts (extracted from emails and conversations) and profile data, synthesize a holistic understanding of who they are.

## CRITICAL: Be Specific, Not Generic

Your analysis MUST include CONCRETE DETAILS from the data. Generic statements like "proactive and ambitious" or "interested in technology" are USELESS for matching.

WRONG (too generic):
- "Eric is proactive and entrepreneurial"
- "Interested in startups and technology"
- "Direct communication style"

RIGHT (specific and actionable):
- "Eric cold-emails industry professionals requesting coffee chats about HFT and ML trading - shows initiative but may come across as transactional"
- "Building Franklink (AI career companion) with real users - has hands-on startup experience, not just interest"
- "Emails are short, direct, always include specific asks (e.g., '15-min call about portfolio construction') - efficient but may miss rapport-building"

## Analysis Instructions

1. **Inferred Traits**: What SPECIFIC patterns do you see?
   - Quote or reference ACTUAL emails/facts: "Sent 3 emails to Chuyue about quant trading" shows persistence
   - What topics do they repeatedly engage with? Name them specifically
   - HOW do they communicate? Short/long emails? Formal/casual? What specific phrases or patterns?

2. **Latent Needs**: What do they ACTUALLY need (with evidence)?
   - Base this on GAPS between their actions and stated goals
   - Example: "Emailing professionals about HFT but studying CS at Penn - may need bridge to finance industry"
   - Example: "Building a startup alone - likely needs co-founder or technical collaborators"

3. **Relationship Potential**: Be specific about WHY
   - Don't just say "would benefit from mentor" - say "needs mentor in quantitative finance given interest in HFT but academic focus on CS"
   - Consider: What specific person would complement them? "Someone with trading desk experience" not just "mentor"

4. **Life Trajectory**: Where SPECIFICALLY are they headed?
   - What career path do the facts suggest? "Exploring quant trading at firms like D.E. Shaw (received recruitment email)"
   - What decisions/transitions are they facing? "Choosing between startup path (Franklink) and finance path (quant interest)"

5. **Holistic Summary**: Make it MATCHABLE
   - Include specific interests: "quant trading, HFT, ML in finance, startup operations"
   - Include specific needs: "needs someone who has worked at a quant fund or trading desk"
   - Include specific offerings: "can offer startup operations experience, product development skills"
   - A good test: Could someone read this and know EXACTLY what kind of person to match them with?

## Output Format

Return ONLY valid JSON (no markdown, no explanation):
{
    "personality_summary": "2-3 sentences with SPECIFIC behavioral evidence from the data",
    "communication_style": "1-2 sentences describing HOW they communicate with examples",
    "work_patterns": "1-2 sentences on work habits with evidence",
    "latent_needs": ["specific_need_1", "specific_need_2", "specific_need_3"],
    "unspoken_gaps": "1-2 sentences on specific gaps between goals and current situation",
    "ideal_relationship_types": ["type1", "type2"],
    "relationship_strengths": "1-2 sentences with specific strengths",
    "relationship_risks": "1-2 sentences with specific risks/challenges",
    "trajectory_summary": "2-3 sentences on specific career direction with evidence",
    "core_motivations": ["specific_motivation_1", "specific_motivation_2"],
    "career_stage": "early_explorer|skill_builder|career_changer|established",
    "holistic_summary": "2-3 paragraphs with SPECIFIC details useful for matching",
    "confidence_score": 0.0-1.0,
    "seeking_skills": ["normalized_skill_1", "normalized_skill_2"],
    "offering_skills": ["normalized_skill_1", "normalized_skill_2"],
    "seeking_relationship_types": ["mentor", "co-founder", "study-partner"],
    "offering_relationship_types": ["technical-advisor", "collaborator"]
}

## Structured Skills Extraction (CRITICAL for matching)

For seeking_skills and offering_skills, extract NORMALIZED skill categories from the data.
Use lowercase, hyphenated terms from this taxonomy (add new ones if needed):

Skills: marketing, growth-marketing, sales, fundraising, engineering, frontend, backend, mobile-dev,
ml-engineering, data-science, product-management, design, ux-design, graphic-design, finance,
accounting, legal, operations, business-development, content-creation, social-media, research,
data-analysis, cloud-infrastructure, devops, cybersecurity, blockchain, ai-ml, nlp, robotics,
hardware, biotech, consulting, project-management, public-speaking, writing, mentorship,
trading, quantitative-finance, supply-chain, hr, recruiting, customer-success, community-management

Relationship types: mentor, mentee, co-founder, collaborator, study-partner, accountability-partner,
advisor, investor, hiring-manager, teammate, peer, industry-contact, domain-expert, technical-advisor

IMPORTANT: Base confidence_score on data richness:
- 0.9+ = Rich email history, clear patterns, consistent signals
- 0.7-0.9 = Good data, some patterns clear
- 0.5-0.7 = Limited data, making reasonable inferences
- <0.5 = Insufficient data, high uncertainty"""


FACT_FILTER_PROMPT = """You are filtering Zep knowledge graph facts for a user profile synthesis.

Your task: From the list of facts below, select ONLY the facts that reveal something meaningful about the USER's:
- Actions they took (emails sent, applications submitted, projects built)
- Interests and goals (what topics they engage with, what they're seeking)
- Professional activities (work, projects, collaborations)
- Communication patterns (how they reach out to people)

EXCLUDE facts that are:
- Generic university announcements or newsletters
- News articles or external events
- Assignment due dates or academic admin
- Marketing/promotional content
- Events the user didn't initiate

Return a JSON array of the INDICES (0-based) of facts to KEEP. Only include facts that reveal something specific about WHO this user is.

Example output: [0, 3, 5, 8, 12]

Facts to filter:
{facts}

Return ONLY the JSON array of indices, nothing else."""


async def _filter_facts_with_llm(
    facts: List[Dict[str, Any]],
    openai: "AzureOpenAIClient",
    user_name: str,
) -> List[Dict[str, Any]]:
    """
    Use a fast LLM to filter Zep facts for high-signal content.

    Args:
        facts: Raw Zep facts
        openai: OpenAI client for LLM filtering
        user_name: User's name to help identify user-specific facts

    Returns:
        Filtered list of high-signal facts
    """
    if not facts:
        return []

    # Format facts for filtering (include index for selection)
    facts_text = "\n".join(
        f"[{i}] {fact.get('fact', '')}"
        for i, fact in enumerate(facts[:100])  # Limit to 100 for cost
    )

    try:
        response = await openai.generate_response(
            system_prompt=FACT_FILTER_PROMPT.format(facts=facts_text),
            user_prompt=f"Filter facts for user: {user_name}",
            model="gpt-4o-mini",  # Fast model for filtering
            temperature=0.0,
            trace_label="fact_filter",
        )

        # Parse response as JSON array
        cleaned = response.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        indices = json.loads(cleaned)

        # Return selected facts
        filtered = [facts[i] for i in indices if 0 <= i < len(facts)]
        logger.info(f"Filtered {len(facts[:100])} facts down to {len(filtered)} high-signal facts")
        return filtered

    except Exception as e:
        logger.warning(f"Fact filtering failed, using first 50 facts: {e}")
        return facts[:50]


def _normalize_skills(skills: list) -> List[str]:
    """Normalize skill strings to lowercase hyphenated form and deduplicate.

    Ensures consistent matching in the complementary matching SQL function,
    which relies on exact string equality via INTERSECT.

    Args:
        skills: Raw skill list from LLM output

    Returns:
        Deduplicated list of normalized skill strings
    """
    seen: set = set()
    normalized: List[str] = []
    for s in skills:
        if not isinstance(s, str):
            continue
        key = s.strip().lower().replace(" ", "-").replace("_", "-")
        if key and key not in seen:
            seen.add(key)
            normalized.append(key)
    return normalized


def _format_zep_facts(facts: List[Dict[str, Any]]) -> str:
    """Format Zep facts for the LLM prompt."""
    if not facts:
        return "No Zep facts available."

    formatted = []
    for fact in facts[:75]:  # Allow more facts since they're filtered
        fact_text = fact.get("fact", "")
        if fact_text:
            created = fact.get("created_at", "")[:10] if fact.get("created_at") else ""
            prefix = f"[{created}] " if created else ""
            formatted.append(f"- {prefix}{fact_text}")

    return "\n".join(formatted) if formatted else "No Zep facts available."


def _format_user_data(
    facts: List[Dict[str, Any]],
    context: Optional[str],
    user: Dict[str, Any],
) -> str:
    """Format all user data for the LLM prompt."""
    from app.utils.demand_value_derived_fields import combine_texts

    demand_history = user.get("demand_history") or []
    value_history = user.get("value_history") or []

    demand_text = combine_texts(demand_history) if demand_history else "Not specified"
    value_text = combine_texts(value_history) if value_history else "Not specified"

    career_interests = user.get("career_interests") or []
    career_interests_str = ", ".join(career_interests) if career_interests else "Not specified"

    return f"""## Zep Knowledge Graph Facts
{_format_zep_facts(facts)}

## Zep User Context Summary
{context or "No context summary available."}

## Existing Profile Data
- Name: {user.get("name") or "Unknown"}
- University: {user.get("university") or "Not specified"}
- Major: {user.get("major") or "Not specified"}
- Year: {user.get("year") or "Not specified"}
- Location: {user.get("location") or "Not specified"}
- Career Interests: {career_interests_str}

## What they've explicitly asked for (demand_history):
{demand_text}

## What they've explicitly offered (value_history):
{value_text}

## Additional Context
- Career Goals: {user.get("career_goals") or "Not specified"}
- LinkedIn Headline: {(user.get("linkedin_data") or {}).get("headline") or "Not available"}
"""


async def synthesize_user_profile(
    user_id: str,
    db: Optional[DatabaseClient] = None,
    openai: Optional[AzureOpenAIClient] = None,
    force: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Synthesize a holistic profile for a single user.

    Args:
        user_id: User UUID
        db: Database client (creates one if not provided)
        openai: OpenAI client (creates one if not provided)
        force: If True, synthesize even if recent profile exists

    Returns:
        Synthesized profile dict or None if insufficient data
    """
    db = db or DatabaseClient()
    openai = openai or AzureOpenAIClient()
    zep = get_zep_graph_client()

    try:
        if not force:
            existing = await db.get_user_profile(user_id)
            if existing and existing.get("computed_at"):
                computed_at = datetime.fromisoformat(
                    existing["computed_at"].replace("Z", "+00:00")
                )
                age_days = (datetime.now(computed_at.tzinfo) - computed_at).days
                if age_days < getattr(settings, "profile_synthesis_stale_days", 7):
                    logger.debug(f"Profile for {user_id[:8]}... is fresh, skipping")
                    return existing

        user = await db.get_user_by_id(user_id)
        if not user:
            logger.warning(f"User {user_id} not found")
            return None

        # Fetch more facts than we need, then filter for signal
        raw_facts = await zep.get_user_facts(user_id, limit=200)
        context = await zep.get_user_context(user_id)

        # Fallback: If /facts endpoint returns empty, try graph search
        # Some Zep deployments have data in edges but not in the /facts endpoint
        if not raw_facts:
            logger.info(
                f"[PROFILE_SYNTHESIS] No facts via /facts endpoint for {user_id[:8]}..., "
                "trying graph search fallback"
            )
            search_queries = [
                "skills, interests, and expertise",
                "organizations, university, and companies",
                "projects, startups, and work",
                "career, internship, job, role, position",
            ]
            seen_facts: set = set()
            for query in search_queries:
                results = await zep.search_graph(
                    user_id=user_id,
                    query=query,
                    scope="edges",
                    limit=50,
                )
                for r in results:
                    if r.fact and r.fact not in seen_facts:
                        seen_facts.add(r.fact)
                        raw_facts.append({"fact": r.fact})

            if raw_facts:
                logger.info(
                    f"[PROFILE_SYNTHESIS] Graph search fallback found {len(raw_facts)} facts "
                    f"for {user_id[:8]}..."
                )

        demand_history = user.get("demand_history") or []
        min_facts = getattr(settings, "profile_synthesis_min_facts", 3)

        # DEBUG: Log data availability for this user
        logger.info(
            f"[PROFILE_SYNTHESIS] Data check for {user_id[:8]}... ({user.get('name', 'Unknown')}):\n"
            f"  - Zep facts: {len(raw_facts)} (min required: {min_facts})\n"
            f"  - Demand history entries: {len(demand_history)}\n"
            f"  - Has Zep context: {bool(context)}"
        )

        if len(raw_facts) < min_facts and not demand_history:
            logger.warning(
                f"[PROFILE_SYNTHESIS] SKIPPING user {user_id[:8]}... - insufficient data.\n"
                f"  - Zep facts: {len(raw_facts)} < {min_facts} minimum\n"
                f"  - Demand history: {len(demand_history)} entries\n"
                f"  This user will have EMPTY seeking_skills/offering_skills and won't match."
            )
            return None

        # Filter facts for high-signal content using fast LLM
        user_name = user.get("name") or "Unknown"
        filtered_facts = await _filter_facts_with_llm(raw_facts, openai, user_name)

        logger.info(
            f"Filtered {len(raw_facts)} raw facts to {len(filtered_facts)} high-signal facts"
        )

        user_data_prompt = _format_user_data(filtered_facts, context, user)

        logger.info(f"Synthesizing profile for {user_id[:8]}... ({len(filtered_facts)} filtered facts)")

        response = await openai.generate_response(
            system_prompt=PROFILE_SYNTHESIS_SYSTEM_PROMPT,
            user_prompt=user_data_prompt,
            model="gpt-4o",
            temperature=0.3,
            trace_label=f"profile_synthesis_{user_id[:8]}",
        )

        cleaned = response.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        profile_data = json.loads(cleaned)

        holistic_summary = profile_data.get("holistic_summary", "")
        holistic_embedding = None
        if holistic_summary:
            holistic_embedding = await openai.get_embedding(holistic_summary)

        profile_data = {
            **profile_data,
            "holistic_embedding": holistic_embedding,
            "zep_facts_count": len(filtered_facts),
            "computed_at": datetime.utcnow().isoformat(),
        }

        result = await db.upsert_user_profile(user_id, profile_data)

        # Persist structured skills to users table for complementary matching
        # DEBUG: Log raw LLM output for skills before normalization
        raw_seeking = profile_data.get("seeking_skills") or []
        raw_offering = profile_data.get("offering_skills") or []
        logger.info(
            f"[PROFILE_SYNTHESIS] LLM extracted skills for {user_id[:8]}...:\n"
            f"  - Raw seeking_skills: {raw_seeking or '(empty)'}\n"
            f"  - Raw offering_skills: {raw_offering or '(empty)'}\n"
            f"  - seeking_relationship_types: {profile_data.get('seeking_relationship_types') or '(empty)'}\n"
            f"  - offering_relationship_types: {profile_data.get('offering_relationship_types') or '(empty)'}"
        )

        structured_update = {}
        if profile_data.get("seeking_skills"):
            structured_update["seeking_skills"] = _normalize_skills(profile_data["seeking_skills"])
        if profile_data.get("offering_skills"):
            structured_update["offering_skills"] = _normalize_skills(profile_data["offering_skills"])
        if profile_data.get("seeking_relationship_types"):
            structured_update["seeking_relationship_types"] = _normalize_skills(profile_data["seeking_relationship_types"])
        if profile_data.get("offering_relationship_types"):
            structured_update["offering_relationship_types"] = _normalize_skills(profile_data["offering_relationship_types"])

        # DEBUG: Log normalized skills
        if structured_update:
            logger.info(
                f"[PROFILE_SYNTHESIS] Normalized skills for {user_id[:8]}...:\n"
                f"  - seeking_skills: {structured_update.get('seeking_skills', [])}\n"
                f"  - offering_skills: {structured_update.get('offering_skills', [])}"
            )

        if structured_update:
            try:
                await db.update_user_profile(user_id, structured_update)
                logger.info(
                    f"[PROFILE_SYNTHESIS] Updated structured skills for {user_id[:8]}... "
                    f"(seeking={len(structured_update.get('seeking_skills', []))}, "
                    f"offering={len(structured_update.get('offering_skills', []))})"
                )
            except Exception as e:
                logger.warning(f"[PROFILE_SYNTHESIS] Failed to update structured skills for {user_id[:8]}...: {e}")
        else:
            logger.warning(
                f"[PROFILE_SYNTHESIS] No structured skills extracted for {user_id[:8]}... "
                "This user will not appear in complementary matching results."
            )

        logger.info(
            f"Synthesized profile for {user_id[:8]}... "
            f"(confidence={profile_data.get('confidence_score', 0):.2f})"
        )

        # Materialize Zep facts into cross-user knowledge graph
        if getattr(settings, "graph_materialization_enabled", True):
            try:
                from app.jobs.graph_materialization import materialize_user_graph

                graph_stats = await materialize_user_graph(
                    user_id=user_id,
                    db=db,
                    openai=openai,
                    filtered_facts=filtered_facts,
                )
                if not graph_stats.get("error"):
                    logger.info(
                        f"Graph materialized for {user_id[:8]}... "
                        f"({graph_stats.get('nodes_upserted', 0)} nodes, "
                        f"{graph_stats.get('edges_upserted', 0)} edges)"
                    )
            except Exception as e:
                logger.warning(f"Graph materialization failed for {user_id[:8]}...: {e}")

        return result

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse LLM response for {user_id[:8]}...: {e}")
        return None
    except Exception as e:
        logger.error(f"Profile synthesis failed for {user_id[:8]}...: {e}", exc_info=True)
        return None


async def run_profile_synthesis_job(
    batch_size: int = 50,
    stale_days: int = 7,
    rate_limit_seconds: float = 2.0,
) -> Dict[str, Any]:
    """
    Background job to synthesize profiles for all eligible users.

    Args:
        batch_size: Max users to process per run
        stale_days: Consider profiles stale after this many days
        rate_limit_seconds: Pause between users to avoid API limits

    Returns:
        Dict with job statistics
    """
    if not getattr(settings, "profile_synthesis_enabled", True):
        logger.info("Profile synthesis job disabled via settings")
        return {"status": "disabled"}

    db = DatabaseClient()
    openai = AzureOpenAIClient()

    stats = {
        "started_at": datetime.utcnow().isoformat(),
        "users_processed": 0,
        "profiles_created": 0,
        "profiles_skipped": 0,
        "errors": 0,
    }

    try:
        users = await db.get_users_needing_profile_synthesis(
            stale_days=stale_days,
            batch_limit=batch_size,
        )

        logger.info(f"Profile synthesis job starting: {len(users)} users to process")

        for user_record in users:
            user_id = user_record.get("user_id")
            reason = user_record.get("reason", "unknown")

            try:
                # Set LLM context for usage tracking
                set_llm_context(user_id=user_id, job_type="profile_synthesis")

                result = await synthesize_user_profile(
                    user_id=user_id,
                    db=db,
                    openai=openai,
                    force=(reason == "stale_profile"),
                )

                stats["users_processed"] += 1

                if result:
                    stats["profiles_created"] += 1
                else:
                    stats["profiles_skipped"] += 1

            except Exception as e:
                logger.error(f"Error processing {user_id[:8]}...: {e}")
                stats["errors"] += 1
            finally:
                clear_llm_context()

            await asyncio.sleep(rate_limit_seconds)

        stats["completed_at"] = datetime.utcnow().isoformat()
        stats["status"] = "completed"

        logger.info(
            f"Profile synthesis job completed: "
            f"{stats['profiles_created']} created, "
            f"{stats['profiles_skipped']} skipped, "
            f"{stats['errors']} errors"
        )

        return stats

    except Exception as e:
        logger.error(f"Profile synthesis job failed: {e}", exc_info=True)
        stats["status"] = "failed"
        stats["error"] = str(e)
        return stats


async def synthesize_profile_after_email_sync(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Trigger profile synthesis after email sync completes.

    Called by email sync job when new emails are added to Zep.
    Forces regeneration so graph materialization reflects newly synced context.

    Args:
        user_id: User UUID

    Returns:
        Synthesized profile or None
    """
    if not getattr(settings, "profile_synthesis_enabled", True):
        return None

    try:
        set_llm_context(user_id=user_id, job_type="profile_synthesis_email_sync")
        return await synthesize_user_profile(user_id, force=True)
    finally:
        clear_llm_context()


def schedule_profile_refresh_after_zep_sync(
    user_id: str,
    *,
    delay_seconds: Optional[float] = None,
    force: bool = True,
) -> bool:
    """Schedule a non-blocking profile refresh after new Zep data is ingested.

    Returns:
        True if a refresh task was scheduled, False if skipped due to settings,
        existing in-flight task, cooldown, or missing event loop.
    """
    if not getattr(settings, "profile_synthesis_enabled", True):
        return False

    if not getattr(settings, "profile_synthesis_refresh_on_zep_sync", True):
        return False

    existing_task = _PROFILE_REFRESH_TASKS.get(user_id)
    if existing_task and not existing_task.done():
        logger.debug(
            "[PROFILE_SYNTHESIS] Refresh already in flight for user=%s",
            user_id[:8] if user_id else "unknown",
        )
        return False

    cooldown_seconds = float(
        getattr(settings, "profile_synthesis_refresh_cooldown_seconds", 900.0)
    )
    now = time.monotonic()
    last_run = _PROFILE_REFRESH_LAST_RUN.get(user_id)
    if last_run is not None and (now - last_run) < cooldown_seconds:
        logger.debug(
            "[PROFILE_SYNTHESIS] Refresh cooldown active user=%s remaining=%.1fs",
            user_id[:8] if user_id else "unknown",
            cooldown_seconds - (now - last_run),
        )
        return False

    delay = (
        float(delay_seconds)
        if delay_seconds is not None
        else float(getattr(settings, "profile_synthesis_refresh_delay_seconds", 20.0))
    )

    async def _run_refresh() -> None:
        try:
            if delay > 0:
                await asyncio.sleep(delay)

            _PROFILE_REFRESH_LAST_RUN[user_id] = time.monotonic()
            await synthesize_user_profile(user_id=user_id, force=force)
            logger.info(
                "[PROFILE_SYNTHESIS] On-demand refresh completed user=%s",
                user_id[:8] if user_id else "unknown",
            )
        except Exception as e:
            logger.warning(
                "[PROFILE_SYNTHESIS] On-demand refresh failed user=%s err=%s",
                user_id[:8] if user_id else "unknown",
                str(e),
                exc_info=True,
            )
        finally:
            _PROFILE_REFRESH_TASKS.pop(user_id, None)

    try:
        task = asyncio.get_running_loop().create_task(_run_refresh())
    except RuntimeError:
        logger.debug(
            "[PROFILE_SYNTHESIS] No running event loop, skip scheduling refresh user=%s",
            user_id[:8] if user_id else "unknown",
        )
        return False

    _PROFILE_REFRESH_TASKS[user_id] = task
    logger.info(
        "[PROFILE_SYNTHESIS] Scheduled on-demand refresh user=%s delay=%.1fs force=%s",
        user_id[:8] if user_id else "unknown",
        delay,
        force,
    )
    return True
