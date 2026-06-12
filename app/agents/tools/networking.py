"""Networking tools for finding matches and managing connections.

These tools handle:
- Checking if networking requests are clear enough to proceed
- Finding value-exchange matches between users
- Managing connection request handshakes
- Sending invitations to potential matches
- Creating group chats for connected users
"""

import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from app.agents.tools.base import tool, ToolResult
from app.database.client import DatabaseClient
from app.database.models import ConnectionRequestStatus
from app.integrations.azure_openai_client import AzureOpenAIClient
from app.utils.demand_value_history import append_history
from app.utils.demand_value_derived_fields import update_demand_value_derived_fields
from app.utils.phone_validator import normalize_phone_number

logger = logging.getLogger(__name__)


def _parse_iso_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO datetime string to datetime object.

    Handles formats like:
    - "2026-01-23T19:07:58.114092Z"
    - "2026-01-20T00:00:00Z"

    Returns datetime or None if parsing fails.
    """
    if not date_str:
        return None
    try:
        # Handle Z suffix and various ISO formats
        cleaned = date_str.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned).replace(tzinfo=None)
    except (ValueError, TypeError):
        return None


def _sort_raw_facts_by_recency(
    raw_facts: List[Dict[str, Any]],
    recent_days: int = 30,
) -> Tuple[List[str], List[str]]:
    """Sort raw Zep facts by valid_at/created_at timestamp into recent vs older.

    Args:
        raw_facts: List of raw fact dicts with 'fact', 'valid_at', 'created_at' keys
        recent_days: Number of days to consider as "recent"

    Returns:
        Tuple of (recent_facts, older_facts) as fact text strings, both sorted newest-first
    """
    now = datetime.now()
    cutoff = now - timedelta(days=recent_days)

    dated_facts = []
    undated_facts = []

    for raw in raw_facts:
        fact_text = raw.get("fact", "")
        if not fact_text:
            continue

        # Prefer valid_from (event date) over created_at (sync date)
        # Note: GraphSearchResult uses valid_from field name
        fact_date = _parse_iso_date(raw.get("valid_from")) or _parse_iso_date(raw.get("created_at"))

        if fact_date:
            # Append date to fact text for LLM context
            date_str = fact_date.strftime("%Y-%m-%d")
            fact_with_date = f"[{date_str}] {fact_text}"
            dated_facts.append((fact_date, fact_with_date))
        else:
            undated_facts.append(fact_text)

    # Sort dated facts newest-first
    dated_facts.sort(key=lambda x: x[0], reverse=True)

    recent = [f for d, f in dated_facts if d >= cutoff]
    older = [f for d, f in dated_facts if d < cutoff]

    # Undated facts go with older (lower priority)
    older.extend(undated_facts)

    return recent, older


def _validate_and_repair_user_id(
    user_id: str, user_profile: Dict[str, Any]
) -> tuple[Optional[str], Optional[str]]:
    """Validate user_id format and auto-repair from user_profile if needed.

    LLMs sometimes corrupt UUIDs during generation (missing segments, wrong chars).
    This function detects corruption and falls back to the authenticated user_id
    from user_profile to ensure the tool works correctly.

    Args:
        user_id: The user ID from LLM tool call (may be corrupted)
        user_profile: The user's profile data (contains authentic user_id)

    Returns:
        Tuple of (error_message, corrected_user_id):
        - (None, corrected_id): Valid user_id to use (may be repaired)
        - (error_message, None): Validation failed, no recovery possible
    """
    profile_user_id = user_profile.get("user_id") or user_profile.get("id")

    # If no user_id provided, use profile_user_id directly
    if not user_id:
        if profile_user_id:
            try:
                UUID(str(profile_user_id))
                logger.info(
                    f"[NETWORKING] No user_id provided, using profile: {str(profile_user_id)[:8]}"
                )
                return (None, str(profile_user_id))
            except (ValueError, TypeError):
                pass
        return ("user_id is required and not available in user_profile", None)

    # Validate provided UUID format
    try:
        UUID(user_id)
        # UUID format is valid - check if it matches profile
        if profile_user_id and str(profile_user_id) != str(user_id):
            logger.warning(
                f"[NETWORKING] user_id mismatch: requested={user_id[:8]}, profile={str(profile_user_id)[:8]}"
            )
            return ("user_id does not match authenticated user", None)
        return (None, user_id)
    except (ValueError, TypeError):
        # UUID format is INVALID - LLM corrupted it
        # Try to recover from user_profile
        if profile_user_id:
            try:
                UUID(str(profile_user_id))
                logger.warning(
                    f"[NETWORKING] UUID repair: corrupted={user_id}, using profile={str(profile_user_id)[:8]}"
                )
                return (None, str(profile_user_id))
            except (ValueError, TypeError):
                pass

        return (f"Invalid user_id format: {user_id} (and no valid profile fallback)", None)


def _validate_user_id(user_id: str, user_profile: Dict[str, Any]) -> Optional[str]:
    """Validate user_id format and match against user_profile.

    DEPRECATED: Use _validate_and_repair_user_id() instead for auto-recovery.

    Args:
        user_id: The user ID to validate
        user_profile: The user's profile data (should contain user_id)

    Returns:
        Error message if validation fails, None if valid
    """
    error, _ = _validate_and_repair_user_id(user_id, user_profile)
    return error


async def _get_group_chat_participant_ids(
    db: DatabaseClient,
    chat_guid: str,
) -> List[str]:
    """Return user IDs for participants in a group chat."""
    try:
        participants = await db.get_group_chat_participants(chat_guid)
        ids = []
        for row in (participants or []):
            if row.get("user_id"):
                ids.append(str(row["user_id"]))
        return list(dict.fromkeys(ids))
    except Exception:
        return []


def _validate_request_id(request_id: str) -> Optional[str]:
    """Validate connection request ID format.

    Args:
        request_id: The request ID to validate

    Returns:
        Error message if validation fails, None if valid
    """
    if not request_id:
        return "request_id is required"

    try:
        UUID(request_id)
    except (ValueError, TypeError):
        return f"Invalid request_id format: {request_id}"

    return None


def _normalize_delivery_handle(raw_handle: Any) -> Optional[str]:
    """Normalize a potential iMessage delivery handle (phone or email)."""
    handle = str(raw_handle or "").strip()
    if not handle:
        return None

    # Allow handles stored as chat GUIDs like "iMessage;-;+15551234567"
    if handle.startswith("iMessage;-;"):
        handle = handle[len("iMessage;-;"):].strip()

    # Allow "mailto:user@example.com" variants
    if handle.lower().startswith("mailto:"):
        handle = handle[7:].strip()

    if "@" in handle:
        return handle.lower()

    normalized_phone = normalize_phone_number(handle)
    return normalized_phone


async def _get_preview_delivery_targets(
    db: DatabaseClient,
    user_id: str,
    preferred_handle: str,
) -> List[str]:
    """Resolve ordered delivery targets for preview link delivery."""
    raw_candidates: List[Any] = [preferred_handle]

    try:
        db_user = await db.get_user_by_id(user_id)
    except Exception as e:
        db_user = None
        logger.warning(
            "[NETWORKING] Failed to load user profile for preview delivery (%s): %s",
            user_id[:8],
            e,
        )

    if db_user:
        raw_candidates.extend(
            [
                db_user.get("phone_number"),
                db_user.get("email"),
            ]
        )

        metadata = db_user.get("metadata")
        if isinstance(metadata, dict):
            raw_candidates.extend(
                [
                    metadata.get("phone_number"),
                    metadata.get("from_number"),
                    metadata.get("imessage_handle"),
                ]
            )

    try:
        raw_candidates.extend(await db.get_linked_handles(user_id))
    except Exception as e:
        logger.warning(
            "[NETWORKING] Failed to load linked handles for preview delivery (%s): %s",
            user_id[:8],
            e,
        )

    targets: List[str] = []
    seen: set[str] = set()
    for candidate in raw_candidates:
        normalized = _normalize_delivery_handle(candidate)
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        targets.append(normalized)

    return targets


@tool(
    name="check_networking_clarity",
    description="Check if a networking request is specific enough to find a match. "
    "MUST be called BEFORE find_match to ensure we understand what the user wants. "
    "Takes the networking_request from task_instruction, NOT raw user message.",
)
async def check_networking_clarity(
    user_id: str,
    networking_request: str,
    user_profile: Dict[str, Any],
) -> ToolResult:
    """Check if networking request is clear enough to find a match.

    Args:
        user_id: The user's ID (for persisting clarity data)
        networking_request: The interpreted networking request from task_instruction
            (e.g., "User wants to connect with someone who has PM experience")
        user_profile: The user's profile data (includes demand_history, value_history, etc.)

    Returns:
        ToolResult with:
        - is_clear: bool - whether the request is specific enough
        - clarification_question: str - question to ask if not clear
        - interpreted_demand: str - what we understood if clear
    """
    # Validate and auto-repair user_id if LLM corrupted it
    validation_error, corrected_user_id = _validate_and_repair_user_id(user_id, user_profile)
    if validation_error:
        logger.warning(f"[NETWORKING] check_networking_clarity validation failed: {validation_error}")
        return ToolResult(success=False, error=validation_error)

    # Use the corrected user_id for all operations
    user_id = corrected_user_id

    try:
        openai = AzureOpenAIClient()
        db = DatabaseClient()

        # Check if user has existing demand/value context
        demand_context = user_profile.get("latest_demand") or user_profile.get("all_demand") or ""
        value_context = user_profile.get("all_value") or ""

        system_prompt = """You analyze networking requests to determine if they're specific enough to find a match.

A request is CLEAR if it specifies:
- What type of person/role/industry the user wants to connect with, OR
- What specific help/advice/opportunity the user is looking for, OR
- The user has existing demand/value context that makes the intent clear

A request is UNCLEAR if:
- It's too vague like "connect me with someone" or "find me a connection"
- It doesn't specify any criteria for matching
- AND the user has no existing demand context

IMPORTANT for clarification_question:
- Keep it SHORT and SIMPLE (under 15 words)
- Ask about WHO they want to meet, not detailed topics
- Good: "What type of person are you looking to connect with?"
- Good: "What role or industry are you interested in?"
- BAD: "What specific topics do you want to discuss with them?"
- BAD: "Can you tell me more about what you're hoping to gain from this connection?"

Output JSON only:
{
    "is_clear": true/false,
    "clarification_question": "question to ask if not clear (null if clear)",
    "interpreted_demand": "what they're looking for if clear (null if not clear)"
}"""

        user_prompt = f"""Networking request: "{networking_request}"

User's existing demand context: {demand_context if demand_context else "None"}
User's existing value context: {value_context if value_context else "None"}

Is this networking request clear enough to find a match?"""

        response = await openai.generate_response(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            model="gpt-4o-mini",
            temperature=0.1,
            trace_label="check_networking_clarity",
        )

        # Parse JSON response
        import json
        from datetime import datetime, timezone
        cleaned = response.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        result = json.loads(cleaned)

        is_clear = result.get("is_clear", False)
        clarification_question = result.get("clarification_question")
        interpreted_demand = result.get("interpreted_demand")

        # Persist data based on clarity result
        try:
            if is_clear and interpreted_demand:
                # Store interpreted_demand in demand_history and regenerate embeddings
                # IMPORTANT: Fetch FRESH demand_history from DB to avoid overwriting concurrent updates
                fresh_state = await db.get_demand_value_state(user_id)
                current_demand_history = fresh_state.get("demand_history", [])
                updated_demand_history = append_history(
                    current_demand_history,
                    interpreted_demand,
                    created_at=datetime.now(timezone.utc).isoformat()
                )

                # Update demand_history in DB
                await db.update_user_profile(user_id, {"demand_history": updated_demand_history})

                # Regenerate derived fields including embeddings for find_match
                await update_demand_value_derived_fields(
                    db=db,
                    user_id=user_id,
                    demand_history=updated_demand_history,
                )
                logger.info(f"[NETWORKING] Stored interpreted_demand in demand_history for user {user_id}")
            elif not is_clear and clarification_question:
                # Store clarification question for context on resume
                networking_clarification = {
                    "is_clear": False,
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                    "networking_request": networking_request,
                    "clarification_question": clarification_question,
                }
                await db.update_user_profile(user_id, {"networking_clarification": networking_clarification})
                logger.info(f"[NETWORKING] Stored clarification question for user {user_id}")
        except Exception as persist_error:
            logger.warning(f"[NETWORKING] Failed to persist clarity data: {persist_error}")

        return ToolResult(
            success=True,
            data={
                "is_clear": is_clear,
                "clarification_question": clarification_question,
                "interpreted_demand": interpreted_demand,
            },
        )

    except Exception as e:
        logger.error(f"[NETWORKING] check_networking_clarity failed: {e}", exc_info=True)
        # Default to clear if check fails - don't block networking
        return ToolResult(
            success=True,
            data={
                "is_clear": True,
                "clarification_question": None,
                "interpreted_demand": "Unable to analyze, proceeding with match",
            },
        )


def _is_duplicate_purpose(new_purpose: str, existing_purposes: List[str]) -> bool:
    """Check if a new purpose is too similar to existing ones.

    Uses simple keyword overlap to detect duplicates. A purpose is considered
    a duplicate if it shares significant keywords with an existing purpose.

    Args:
        new_purpose: The new purpose to check
        existing_purposes: List of existing purposes to compare against

    Returns:
        True if the new purpose is a duplicate, False otherwise
    """
    if not new_purpose or not existing_purposes:
        return False

    # Normalize and extract keywords
    def extract_keywords(text: str) -> set:
        # Lowercase and split on whitespace/punctuation
        import re
        words = set(re.findall(r'\b[a-z]{3,}\b', text.lower()))
        # Remove common stop words
        stop_words = {
            'the', 'and', 'for', 'with', 'someone', 'finding', 'looking',
            'want', 'need', 'would', 'could', 'like', 'that', 'this',
            'who', 'can', 'help', 'find', 'connect', 'meet', 'partner',
            'buddy', 'person', 'people'
        }
        return words - stop_words

    new_keywords = extract_keywords(new_purpose)
    if not new_keywords:
        return False

    for existing in existing_purposes:
        existing_keywords = extract_keywords(existing)
        if not existing_keywords:
            continue

        # Calculate overlap ratio
        overlap = len(new_keywords & existing_keywords)
        min_size = min(len(new_keywords), len(existing_keywords))

        # If more than 50% of keywords overlap, consider it a duplicate
        if min_size > 0 and overlap / min_size > 0.5:
            return True

    return False


async def _get_connection_purpose_suggestions(
    user_id: str,
    user_profile: Dict[str, Any],
    max_suggestions: int = 3,
    skip_deduplication: bool = False,
    recent_purposes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Internal helper to get connection purpose suggestions from Zep.

    This is the core logic shared by:
    - suggest_connection_purposes (the LLM tool for interactive use)
    - Proactive outreach job (automated suggestions)

    Args:
        user_id: The user's ID (must be validated before calling)
        user_profile: The user's profile data
        max_suggestions: Maximum number of suggestions to return
        skip_deduplication: If True, skip checking recent purposes (for proactive)
        recent_purposes: Optional list of recent purposes to deduplicate against

    Returns:
        Dict with:
        - has_suggestions: bool
        - suggestions: List of suggestion dicts
        - recent_facts_count: int
        - total_facts_count: int
        - fallback_question: Optional[str] (only if no suggestions)
        - skip_reason: Optional[str] (if LLM returned one)
    """
    from app.config import settings
    from app.database.client import DatabaseClient
    import json

    try:
        # Check if Zep is enabled
        if not getattr(settings, 'zep_graph_enabled', False):
            return {
                "has_suggestions": False,
                "suggestions": [],
                "fallback_question": "What type of person are you looking to connect with?",
            }

        # Get recent connection purposes for deduplication (unless skipped)
        if not skip_deduplication and recent_purposes is None:
            db = DatabaseClient()
            recent_purposes = await db.get_recent_connection_purposes(user_id, days=7)
            logger.info(
                f"[NETWORKING] Found {len(recent_purposes)} recent purposes to deduplicate against"
            )
        elif recent_purposes is None:
            recent_purposes = []

        # Get raw facts directly from Zep with metadata (valid_at, created_at)
        from app.integrations.zep_graph_client import ZepGraphClient

        zep = ZepGraphClient()

        # Check if Zep graph is available
        if not zep.is_graph_enabled():
            return {
                "has_suggestions": False,
                "suggestions": [],
                "fallback_question": "What type of person are you looking to connect with?",
            }

        # Check for unsynced highlights - if there are any, they may be processing
        # Give Zep a brief moment to catch up if user just finished onboarding
        try:
            from app.database.client import DatabaseClient

            db = DatabaseClient()
            unsynced = await db.get_unsynced_highlights_for_zep(user_id, limit=1)
            if unsynced:
                # There are unsynced highlights, which means either:
                # 1. Sync hasn't run yet, or
                # 2. Sync just ran and episodes are still processing
                # In either case, trigger a sync with processing wait
                logger.info(
                    f"[NETWORKING] Found unsynced highlights for user={user_id[:8]}..., "
                    "triggering sync before search"
                )
                from app.agents.tools.email_zep_sync import sync_unsynced_highlights_to_zep
                await sync_unsynced_highlights_to_zep(
                    user_id=user_id,
                    max_highlights=100,
                    wait_for_processing=True,
                    processing_timeout=10,  # Quick timeout for interactive flow
                )
        except Exception as e:
            # Don't fail the search if sync check fails, but log for visibility
            logger.warning(
                f"[NETWORKING] Pre-search sync check failed (continuing): {e}",
                exc_info=True,
            )

        # Search for connection-relevant facts using semantic search
        # NOTE: Zep limits query to 400 characters max
        # High-signal keywords: collaboration, events, academics, opportunities, industry
        search_query = (
            "partner teammate cofounder mentor investor advisor "
            "hackathon conference event meeting networking "
            "startup project opportunity job internship "
            "canvas assignment review due exam study course professor "
            "tech AI engineering design product business "
            "deadline upcoming goal looking for interested"
        )  # ~350 chars - under 400 limit

        search_results = await zep.search_graph(
            user_id=user_id,
            query=search_query,
            scope="edges",
            limit=50,
        )

        # Extract facts from search results
        raw_facts = []
        for result in search_results:
            if hasattr(result, "fact") and result.fact:
                raw_facts.append({
                    "fact": result.fact,
                    "created_at": getattr(result, "created_at", None),
                    "valid_from": getattr(result, "valid_from", None),
                    "score": getattr(result, "score", 0.0),
                })
            elif isinstance(result, dict) and result.get("fact"):
                raw_facts.append(result)

        if not raw_facts:
            # No Zep data available, return fallback
            return {
                "has_suggestions": False,
                "suggestions": [],
                "fallback_question": "What type of person are you looking to connect with?",
            }

        # Also get user summary for context
        zep_summary = ""
        try:
            context_result = await zep.get_user_context(user_id)
            if context_result:
                # get_user_context returns Optional[str], not a dict
                zep_summary = context_result
        except Exception:
            pass  # Summary is optional

        # Sort facts by valid_at/created_at timestamp - prioritize recent (last 3 days)
        recent_facts, older_facts = _sort_raw_facts_by_recency(raw_facts, recent_days=3)

        logger.info(
            f"[NETWORKING] suggest_connection_purposes: {len(recent_facts)} recent facts, "
            f"{len(older_facts)} older facts for user {user_id[:8]}"
        )

        # Use LLM to generate suggestions from Zep context
        openai = AzureOpenAIClient()

        # Get today's date for temporal awareness
        today = datetime.now()
        today_formatted = today.strftime("%A, %B %d, %Y")

        # Build context prioritizing recent activity WITH today's date
        context_parts = []

        # Add today's date prominently for temporal reasoning
        context_parts.append(f"## TODAY'S DATE: {today_formatted}")

        if recent_facts:
            context_parts.append(
                "## Recent Activity (Last 3 Days) - PRIORITIZE THESE:\n"
                + "\n".join(f"- {f}" for f in recent_facts[:15])
            )

        if older_facts:
            context_parts.append(
                "## Older Context (for reference only):\n"
                + "\n".join(f"- {f}" for f in older_facts[:5])
            )

        if zep_summary:
            context_parts.append(f"## User Summary:\n{zep_summary}")

        # Add minimal profile context (avoid career focus)
        hobbies = user_profile.get("hobbies", [])
        if hobbies:
            context_parts.append(f"## Hobbies: {', '.join(hobbies)}")

        user_context = "\n\n".join(context_parts)

        system_prompt = """You suggest SPECIFIC, ACTIONABLE connection purposes based on a user's recent emails.

## Your Role
You analyze a user's recent emails to identify CONCRETE opportunities where connecting with someone would help. These should be grounded in specific events, deadlines, or activities from their emails.

## CRITICAL: TIME-SENSITIVE PRIORITIZATION
The user context includes TODAY'S DATE. Use it to evaluate time-sensitivity:
1. Events in the NEXT 3 DAYS = HIGHEST PRIORITY (must suggest these!)
2. Events in the next 7 days = HIGH PRIORITY
3. Ongoing activities (gym buddy, study partner) = MEDIUM PRIORITY
4. PAST EVENTS = AUTOMATICALLY REJECT (do NOT suggest anything that already happened)

## AUTOMATIC REJECTION RULES
- If an event date is BEFORE today's date, DO NOT suggest it
- If you see "October midterm" and today is January, that's OLD - SKIP IT
- If you see "last week's hackathon", SKIP IT
- Only suggest events/deadlines that are UPCOMING or ongoing activities

## What to Look For (SPECIFIC opportunities from emails)
- Academic: "study partner for CIS 520 final next week", "someone to review my thesis draft"
- Events/Info Sessions: "someone to attend the Penn Blockchain info session with", "buddy for the startup career fair"
- Projects: "teammate for the hackathon this weekend", "co-founder for the AI project I'm working on"
- Research: "collaborator for HFT research", "someone also working on ML for finance"
- Social/Activities: "gym buddy at Pottruck", "someone to grab lunch with after class"
- Practice: "mock interview partner for quant roles", "someone to practice case studies with"

## What Makes a GOOD Suggestion
✅ Tied to a SPECIFIC email/event (mentions the actual name, date, or topic)
✅ Event is UPCOMING (in the future relative to today's date) or ongoing
✅ Includes the event date when available (e.g., "this Thursday, January 25th")
✅ Clear what kind of person they need

## What Makes a BAD Suggestion
❌ Vague/generic ("find a mentor", "connect with someone in tech")
❌ Not grounded in their emails (just guessing based on profile)
❌ Too broad ("someone interested in AI")
❌ EVENT HAS ALREADY PASSED (check dates against today!)

## Evidence Requirement
For each suggestion, you MUST:
1. Point to a SPECIFIC recent email or fact that triggered this
2. Quote or paraphrase the relevant part
3. Explain why this connection would be valuable

## Output Format
Return JSON only:
{
    "suggestions": [
        {
            "purpose": "finding someone to attend the Penn Blockchain info session with this Thursday (January 25th)",
            "group_name": "Penn Blockchain Info Session",
            "evidence": "You need to RSVP for the Penn Blockchain Education info session this Thursday",
            "reasoning": "Going to info sessions with someone else helps you stay accountable and you can compare notes afterwards.",
            "activity_type": "event",
            "event_date": "2026-01-25",
            "urgency": "high"
        },
        {
            "purpose": "finding a study partner for quantitative trading concepts",
            "group_name": "Quant Trading Study Group",
            "evidence": "You've been emailing about HFT, algorithmic trading, and ML for finance",
            "reasoning": "These are complex topics - having a study partner to discuss concepts with accelerates learning.",
            "activity_type": "academic",
            "event_date": null,
            "urgency": "medium"
        }
    ]
}

## DEDUPLICATION (CRITICAL)
The user context may include "RECENT PURPOSES TO AVOID" - these are connection purposes the user has already requested recently. DO NOT suggest any purpose that is similar to these. Look for DIFFERENT opportunities.

## Rules
1. Maximum 3 suggestions
2. Each purpose must be SPECIFIC (mention the actual event, class, project, topic)
3. Each suggestion MUST have evidence from their recent emails
4. PRIORITIZE events happening in the next 3 days - these are HIGHEST VALUE
5. NEVER suggest past events - always check dates against today's date
6. NEVER suggest purposes similar to ones in "RECENT PURPOSES TO AVOID"
7. If no actionable opportunities found, return {"suggestions": [], "skip_reason": "No specific opportunities detected in recent emails"}
8. Keep purposes conversational and under 15 words
9. Include event_date (YYYY-MM-DD or null) and urgency (high/medium/low) for each suggestion
10. group_name: A short, catchy name for the iMessage group chat (max 30 chars). Focus on the event/topic name itself, NOT phrases like "finding someone to..."
   - Good: "Penn Blockchain Info Session", "Quant Trading Study Group", "Hackathon Team"
   - Bad: "Finding someone to attend...", "Looking for a study partner..."
11. activity_type MUST be one of: academic, event, project, research, social, hobby, activity, practice, career, collaboration, mentorship, networking, interview, meeting, workshop, competition, general"""

        # Build deduplication context
        dedup_section = ""
        if recent_purposes:
            purposes_list = "\n".join(f"- {p}" for p in recent_purposes)
            dedup_section = f"""

## RECENT PURPOSES TO AVOID (already requested):
{purposes_list}

Do NOT suggest any purpose similar to the above. Find DIFFERENT opportunities."""

        user_prompt = f"""Based on this user's recent emails, suggest specific connection opportunities.

{user_context}{dedup_section}

What SPECIFIC, ACTIONABLE connections could help this user based on their recent emails?
Look for events, deadlines, projects, or activities where having a partner/buddy would help."""

        response = await openai.generate_response(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            model="gpt-4o-mini",
            temperature=0.7,
            max_tokens=600,
            trace_label="suggest_connection_purposes",
        )

        # Parse JSON response
        cleaned = response.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        result = json.loads(cleaned)
        raw_suggestions = result.get("suggestions", [])[:max_suggestions]

        # Enrich suggestions with standardized structure and filter duplicates
        suggestions = []
        for s in raw_suggestions:
            purpose = s.get("purpose", "")

            # Post-processing deduplication: check if purpose is too similar to recent ones
            if recent_purposes and _is_duplicate_purpose(purpose, recent_purposes):
                logger.info(
                    f"[NETWORKING] Filtered duplicate purpose: {purpose[:50]}..."
                )
                continue

            suggestions.append({
                "purpose": purpose,
                "group_name": s.get("group_name", ""),  # Short name for iMessage group chat
                "rationale": s.get("reasoning", ""),  # Map reasoning to rationale for backward compat
                "evidence": s.get("evidence", ""),  # Explicit source from email
                "activity_type": s.get("activity_type", "general"),
                "event_date": s.get("event_date"),  # YYYY-MM-DD or null
                "urgency": s.get("urgency", "medium"),  # high/medium/low
            })

        if suggestions:
            logger.info(
                f"[NETWORKING] Generated {len(suggestions)} life-oriented suggestions "
                f"for user {user_id[:8]} from Zep context (recent facts: {len(recent_facts)})"
            )

        return {
            "has_suggestions": len(suggestions) > 0,
            "suggestions": suggestions,
            "context_source": "zep_graph",
            "recent_facts_count": len(recent_facts),
            "total_facts_count": len(raw_facts),
            "skip_reason": result.get("skip_reason"),
        }

    except Exception as e:
        logger.error(f"[NETWORKING] _get_connection_purpose_suggestions failed: {e}", exc_info=True)
        return {
            "has_suggestions": False,
            "suggestions": [],
            "fallback_question": "What type of person are you looking to connect with?",
        }


@tool(
    name="suggest_connection_purposes",
    description="Use Zep knowledge graph to suggest specific, life-oriented connection purposes for a user. "
    "Call this when a user wants to network but hasn't specified what type of person they want. "
    "Focuses on NICHE activities (study buddies, event companions, gym partners) NOT career goals. "
    "Prioritizes recent emails and shows evidence of where each suggestion came from. "
    "Returns ranked suggestions with match_type (single/multi) indicating whether each purpose "
    "is best served by finding one person or multiple people.",
)
async def suggest_connection_purposes(
    user_id: str,
    user_profile: Dict[str, Any],
    max_suggestions: int = 3,
) -> ToolResult:
    """Suggest life-oriented connection purposes based on user's recent email activity.

    This is the LLM tool wrapper around _get_connection_purpose_suggestions().
    Includes ranking step to determine match_type for each suggestion.

    Args:
        user_id: The user's ID
        user_profile: The user's profile data
        max_suggestions: Maximum number of suggestions to return (default 3)

    Returns:
        ToolResult with ranked suggestions data including match_type
    """
    # Validate and auto-repair user_id if LLM corrupted it
    validation_error, corrected_user_id = _validate_and_repair_user_id(user_id, user_profile)
    if validation_error:
        logger.warning(f"[NETWORKING] suggest_connection_purposes validation failed: {validation_error}")
        return ToolResult(success=False, error=validation_error)

    # Use the corrected user_id
    result = await _get_connection_purpose_suggestions(
        user_id=corrected_user_id,
        user_profile=user_profile,
        max_suggestions=max_suggestions,
        skip_deduplication=False,
    )

    suggestions = result.get("suggestions", [])
    if not suggestions:
        return ToolResult(success=True, data=result)

    # Rank suggestions and classify match_type (single vs multi)
    # Get recent purposes for deduplication context
    from app.database.client import DatabaseClient
    db = DatabaseClient()

    try:
        recent_purposes = await db.get_recent_connection_purposes(corrected_user_id, days=7)
    except Exception:
        recent_purposes = []

    ranked_suggestions = await rank_purposes_for_proactive(
        suggestions=suggestions,
        user_profile=user_profile,
        recent_outreach_purposes=recent_purposes,
    )

    if not ranked_suggestions:
        # Fallback: return original suggestions without ranking
        return ToolResult(success=True, data=result)

    # Save opportunities to database for tracking/reuse
    try:
        batch_id = await db.insert_networking_opportunities_batch(
            user_id=corrected_user_id,
            source="user_requested",
            opportunities=ranked_suggestions,
        )
        if batch_id:
            logger.info(
                f"[NETWORKING] saved_opportunities user_id={corrected_user_id[:8]} "
                f"batch_id={batch_id[:8]} count={len(ranked_suggestions)} source=user_requested"
            )
    except Exception as e:
        logger.warning(
            f"[NETWORKING] save_opportunities_failed user_id={corrected_user_id[:8]} error={e}"
        )
        # Don't fail tool call - this is tracking only

    # Update result with ranked suggestions
    result["suggestions"] = ranked_suggestions

    return ToolResult(success=True, data=result)


async def rank_purposes_for_proactive(
    suggestions: List[Dict[str, Any]],
    user_profile: Dict[str, Any],
    recent_outreach_purposes: List[str],
) -> List[Dict[str, Any]]:
    """Rank all purposes for proactive outreach by priority.

    Uses LLM to rank all suggestions and classify each with match type.
    Returns all purposes in ranked order so caller can try each until a match is found.

    This is used by the proactive outreach job after getting suggestions from
    _get_connection_purpose_suggestions().

    Args:
        suggestions: List of purpose suggestions from _get_connection_purpose_suggestions
        user_profile: User's profile data
        recent_outreach_purposes: Recent outreach purposes to avoid duplicates

    Returns:
        List of suggestion dicts in ranked order (best first), each with added fields:
        - match_type: "single" or "multi"
        - max_matches: int (1 for single, 2-5 for multi)
        - rank: int (1 = best)
        - signal_text: str (copy of purpose for compatibility)
        Returns empty list if no suitable purposes or on error.
    """
    import json

    if not suggestions:
        return []

    openai = AzureOpenAIClient()

    # Build context about recent outreach to avoid
    recent_section = ""
    if recent_outreach_purposes:
        recent_list = "\n".join(f"- {p}" for p in recent_outreach_purposes[:5])
        recent_section = f"""
## RECENT OUTREACH TO AVOID (already sent recently):
{recent_list}

Deprioritize (rank lower) any purpose similar to the above - we already reached out about these."""

    # Build suggestions list for LLM
    suggestions_text = ""
    for i, s in enumerate(suggestions):
        suggestions_text += f"""
{i+1}. Purpose: {s.get('purpose', '')}
   Group Name: {s.get('group_name', '')}
   Evidence: {s.get('evidence', '')}
   Urgency: {s.get('urgency', 'medium')}
   Activity Type: {s.get('activity_type', 'general')}
"""

    system_prompt = """You are ranking networking opportunities for proactive outreach.

## Your Role
Rank ALL the suggestions from best to worst, and classify each with a match type.

## Ranking Criteria (in order of importance)
1. **Time Sensitivity**: Events happening in the next 3 days are HIGHEST priority
2. **Actionability**: Can we realistically find someone to help with this?
3. **Uniqueness**: Purposes similar to recent outreach should be ranked lower
4. **Value**: How much would this connection help the user?

## Match Type Classification
For each suggestion, classify the match type:
- **"single"**: Best for mentor/advisor, coffee chat, expert advice, job referral (1 ideal connection)
  - Examples: "mock interview partner", "advice on breaking into VC", "referral at Google"
- **"multi"**: Best for study groups, cofounder search, project collaboration, event buddies (2-5 people)
  - Examples: "study group for finals", "hackathon teammates", "people to attend career fair with"

## Output Format
Return JSON only - an array of ALL suggestions ranked from best to worst:
{
    "ranked": [
        {"index": 1, "match_type": "single", "max_matches": 1},
        {"index": 3, "match_type": "multi", "max_matches": 3},
        {"index": 2, "match_type": "single", "max_matches": 1}
    ]
}

The "index" is the original 1-indexed position of the suggestion.
Order the array from BEST (first) to WORST (last).
Include ALL suggestions in the ranking."""

    user_prompt = f"""Rank the networking opportunities for proactive outreach.

## User Profile
Name: {user_profile.get('name', 'Unknown')}
University: {user_profile.get('university', 'Unknown')}

## Available Suggestions
{suggestions_text}
{recent_section}

Rank all suggestions from best to worst."""

    try:
        response = await openai.generate_response(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            model="gpt-4o-mini",
            temperature=0.3,
            max_tokens=400,
            trace_label="rank_purposes_for_proactive",
        )

        # Parse JSON response
        cleaned = response.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()

        result = json.loads(cleaned)
        ranked_list = result.get("ranked", [])

        if not ranked_list:
            logger.info("[NETWORKING] No ranked purposes returned from LLM")
            return []

        # Build enriched list in ranked order
        ranked_suggestions = []
        for rank, item in enumerate(ranked_list, start=1):
            idx = item.get("index", 0)
            if idx < 1 or idx > len(suggestions):
                continue

            enriched = suggestions[idx - 1].copy()
            enriched["match_type"] = item.get("match_type", "single")
            enriched["max_matches"] = item.get("max_matches", 1)
            enriched["rank"] = rank
            enriched["signal_text"] = enriched.get("purpose", "")

            # Ensure match_type is valid
            if enriched["match_type"] not in ("single", "multi"):
                enriched["match_type"] = "single"
                enriched["max_matches"] = 1

            ranked_suggestions.append(enriched)

        logger.info(
            f"[NETWORKING] Ranked {len(ranked_suggestions)} purposes for proactive outreach"
        )

        return ranked_suggestions

    except Exception as e:
        logger.error(f"[NETWORKING] rank_purposes_for_proactive failed: {e}", exc_info=True)
        # Fallback: return suggestions in original order with default match type
        fallback_list = []
        for rank, s in enumerate(suggestions, start=1):
            enriched = s.copy()
            enriched["match_type"] = "single"
            enriched["max_matches"] = 1
            enriched["rank"] = rank
            enriched["signal_text"] = enriched.get("purpose", "")
            fallback_list.append(enriched)
        return fallback_list


async def _interpret_demand(
    openai: AzureOpenAIClient,
    demand_text: str,
    user_profile: Dict[str, Any],
) -> Optional[str]:
    """Interpret a networking request into a clean, searchable statement.

    The ExecutionAgent has already determined this is a specific demand before
    calling find_match(). This function just cleans up the user's messy text
    into a clear statement for storage and matching.

    Args:
        openai: OpenAI client for interpretation
        demand_text: The raw demand text from user
        user_profile: User's profile for context

    Returns:
        Clean interpreted demand string, or None if interpretation fails
    """
    # Use LLM to interpret the demand into a clean statement
    try:
        system_prompt = """Interpret this networking request into a clean, searchable statement.

The user wants to connect with someone. Convert their request into a clear statement like:
- "Looking for someone with product management experience at a tech startup"
- "Seeking advice on breaking into venture capital"
- "Want to connect with engineers who have experience with Python and ML"
- "Looking for a mentor in machine learning"

Keep it concise (1-2 sentences max). Focus on what kind of person they're looking for.

Return JSON only:
{
    "interpreted_demand": "clean statement of what they want"
}"""

        response = await openai.generate_response(
            system_prompt=system_prompt,
            user_prompt=f'Request: "{demand_text}"',
            model="gpt-4o-mini",
            temperature=0.1,
            max_tokens=150,
            trace_label="interpret_demand",
        )

        import json
        cleaned = response.strip()
        if cleaned.startswith("```json"):
            cleaned = cleaned[7:]
        if cleaned.startswith("```"):
            cleaned = cleaned[3:]
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        cleaned = cleaned.strip()
        result = json.loads(cleaned)

        return result.get("interpreted_demand")

    except Exception as e:
        logger.warning(f"[NETWORKING] Failed to interpret demand: {e}")
        # Return original text if interpretation fails
        return demand_text


async def _get_users_to_exclude(db: DatabaseClient, user_id: str) -> List[str]:
    """Get list of user IDs to exclude from matching.

    Excludes users who already have:
    - An active group chat with the initiator
    - A pending connection request (in any direction)

    Args:
        db: Database client
        user_id: The initiator's user ID

    Returns:
        List of user IDs to exclude from matching
    """
    excluded = set()

    try:
        # Get all connection requests involving this user that are active/pending
        # This includes: pending initiator approval, pending target approval, accepted, group created
        from app.database.models import ConnectionRequestStatus

        active_statuses = [
            ConnectionRequestStatus.PENDING_INITIATOR_APPROVAL.value,
            ConnectionRequestStatus.PENDING_TARGET_APPROVAL.value,
            ConnectionRequestStatus.TARGET_ACCEPTED.value,
            ConnectionRequestStatus.GROUP_CREATED.value,
        ]

        # Get requests where user is initiator
        initiator_requests = db.client.table("connection_requests").select(
            "target_user_id"
        ).eq(
            "initiator_user_id", user_id
        ).in_(
            "status", active_statuses
        ).execute()

        for req in (initiator_requests.data or []):
            if req.get("target_user_id"):
                excluded.add(req["target_user_id"])

        # Get requests where user is target
        target_requests = db.client.table("connection_requests").select(
            "initiator_user_id"
        ).eq(
            "target_user_id", user_id
        ).in_(
            "status", active_statuses
        ).execute()

        for req in (target_requests.data or []):
            if req.get("initiator_user_id"):
                excluded.add(req["initiator_user_id"])

        logger.info(f"[NETWORKING] Excluding {len(excluded)} users with active/pending connections")

    except Exception as e:
        logger.warning(f"[NETWORKING] Failed to get users to exclude: {e}")

    return list(excluded)


async def _generate_and_send_conversation_preview(
    user_id: str,
    user_name: str,
    phone_number: str,
    match_result: Dict[str, Any],
    flow_type: str = "reactive",
    connection_request_id: Optional[str] = None,
) -> None:
    """Background task: generate a discovery conversation and send the rich link.

    Runs as a fire-and-forget asyncio task so the match result returns instantly.
    On completion, sends the conversation URL as a separate iMessage bubble with
    rich link preview via Photon.
    """
    try:
        from app.agents.interaction.conversation_orchestrator import (
            create_conversation_preview,
        )
        from app.integrations.photon_client import PhotonClient

        # Wait for Frank's match presentation message to finish sending.
        # The orchestrator splits long messages into bubbles with ~0.3s delays,
        # so we wait to ensure the link arrives AFTER all message bubbles.
        await asyncio.sleep(2)

        db = DatabaseClient()
        openai = AzureOpenAIClient()
        photon = PhotonClient()

        # Ack is already sent by InteractionAgent before execution starts.
        # This background task only handles generation + delivery.

        preview = await create_conversation_preview(
            db=db,
            openai=openai,
            initiator_user_id=user_id,
            initiator_name=user_name,
            match_result=match_result,
            flow_type=flow_type,
            connection_request_id=connection_request_id,
        )

        if not preview:
            logger.info(
                "[NETWORKING] No conversation preview generated for %s "
                "(orchestrator returned None — likely insufficient data)",
                user_id[:8],
            )
        elif not preview.conversation_url:
            logger.warning(
                "[NETWORKING] Conversation preview result missing URL for %s "
                "(slug=%s); cannot deliver",
                user_id[:8],
                preview.slug,
            )
        else:
            delivery_targets = await _get_preview_delivery_targets(
                db=db,
                user_id=user_id,
                preferred_handle=phone_number,
            )

            if not delivery_targets:
                logger.warning(
                    "[NETWORKING] Conversation preview created for %s but no deliverable "
                    "phone/email handle found (profile + linked handles empty/invalid). URL: %s",
                    user_id[:8],
                    preview.conversation_url,
                )
                return

            delivered_to: Optional[str] = None
            send_errors: List[str] = []
            for target in delivery_targets:
                try:
                    await photon.send_message(
                        to_number=target,
                        content=preview.conversation_url,
                        rich_link=True,
                    )
                    delivered_to = target
                    break
                except Exception as send_error:
                    send_errors.append(str(send_error))
                    logger.warning(
                        "[NETWORKING] Failed preview delivery attempt for %s to %s: %s",
                        user_id[:8],
                        target,
                        send_error,
                    )

            if not delivered_to:
                logger.warning(
                    "[NETWORKING] Conversation preview created for %s but delivery failed "
                    "for all %d resolved handles. URL: %s last_error=%s",
                    user_id[:8],
                    len(delivery_targets),
                    preview.conversation_url,
                    send_errors[-1] if send_errors else "unknown",
                )
                return

            # Store in conversation history only after successful delivery
            await db.store_message(
                user_id=user_id,
                content=preview.conversation_url,
                message_type="bot",
                metadata={
                    "intent": "networking",
                    "task": "networking",
                    "message_part": "conversation_preview",
                    "slug": preview.slug,
                },
            )
            logger.info(
                "[NETWORKING] Sent conversation preview rich link to %s: %s",
                delivered_to,
                preview.conversation_url,
            )

    except Exception as e:
        logger.warning(
            "[NETWORKING] Background conversation preview failed for %s: %s",
            user_id[:8],
            e,
        )


def _conversation_url_from_row(
    conversation_row: Optional[Dict[str, Any]],
    base_url: str,
) -> Optional[str]:
    """Build preview URL from a discovery_conversations row if slug exists."""
    if not conversation_row:
        return None
    slug = str(conversation_row.get("slug") or "").strip()
    if not slug:
        return None
    return f"{base_url}/c/{slug}"


async def _lookup_preview_url_for_request(
    db: DatabaseClient,
    request_id: str,
    signal_group_id: Optional[str],
    base_url: str,
) -> Optional[str]:
    """Resolve preview URL by request ID, then by sibling requests in a signal group."""
    # First try the exact request ID.
    convo = await db.get_discovery_conversation_by_connection_request_id(request_id)
    convo_url = _conversation_url_from_row(convo, base_url)
    if convo_url:
        return convo_url

    # For multi-match, preview may be attached to a sibling request ID.
    if not signal_group_id:
        return None

    try:
        sibling_result = (
            db.client.table("connection_requests")
            .select("id")
            .eq("signal_group_id", signal_group_id)
            .order("created_at", desc=False)
            .execute()
        )
        sibling_ids = [
            str(r.get("id"))
            for r in (sibling_result.data or [])
            if r.get("id")
        ]
    except Exception as e:
        logger.warning(
            "[NETWORKING] Failed loading sibling requests for signal_group_id=%s: %s",
            str(signal_group_id)[:8],
            e,
        )
        return None

    for sibling_id in sibling_ids:
        if sibling_id == request_id:
            continue
        convo = await db.get_discovery_conversation_by_connection_request_id(sibling_id)
        convo_url = _conversation_url_from_row(convo, base_url)
        if convo_url:
            logger.info(
                "[NETWORKING] Reused sibling conversation preview for request %s via sibling %s",
                request_id,
                sibling_id,
            )
            return convo_url

    return None


async def _get_or_create_preview_url_for_target_invite(
    db: DatabaseClient,
    request_data: Dict[str, Any],
    request_id: str,
    initiator_name: str,
    target_user: Dict[str, Any],
) -> Optional[str]:
    """Get an existing preview URL for a request, or create one on-demand."""
    from app.config import settings as _settings

    if not getattr(_settings, "conversation_preview_enabled", False):
        return None

    base_url = _settings.conversation_preview_base_url.rstrip("/")
    signal_group_id = request_data.get("signal_group_id")

    # 1) Fast path: existing preview for this request (or sibling in same group).
    convo_url = await _lookup_preview_url_for_request(
        db=db,
        request_id=request_id,
        signal_group_id=signal_group_id,
        base_url=base_url,
    )
    if convo_url:
        return convo_url

    # 2) Brief retry window for async background generation from find_match/find_multi_matches.
    for _ in range(3):
        await asyncio.sleep(1)
        convo_url = await _lookup_preview_url_for_request(
            db=db,
            request_id=request_id,
            signal_group_id=signal_group_id,
            base_url=base_url,
        )
        if convo_url:
            return convo_url

    # 3) Fallback: create preview synchronously so target still gets a link.
    try:
        from app.agents.interaction.conversation_orchestrator import (
            create_conversation_preview,
        )

        initiator_user_id = request_data.get("initiator_user_id")
        target_user_id = request_data.get("target_user_id")
        if not initiator_user_id or not target_user_id:
            return None

        initiator_user = await db.get_user_by_id(initiator_user_id)
        if not initiator_user:
            return None

        match_result: Dict[str, Any] = {
            "target_user_id": target_user_id,
            "target_name": target_user.get("name", "there"),
            "matching_reasons": request_data.get("matching_reasons", []) or [],
            "connection_purpose": request_data.get("connection_purpose") or "",
        }

        # Include additional participants for multi-match requests.
        if signal_group_id:
            try:
                siblings = (
                    db.client.table("connection_requests")
                    .select("id,target_user_id,matching_reasons")
                    .eq("signal_group_id", signal_group_id)
                    .order("created_at", desc=False)
                    .execute()
                ).data or []
            except Exception:
                siblings = []

            additional = []
            seen_user_ids = {str(target_user_id)}
            for sibling in siblings:
                sid = sibling.get("target_user_id")
                if not sid:
                    continue
                sid = str(sid)
                if sid in seen_user_ids:
                    continue
                seen_user_ids.add(sid)

                s_user = await db.get_user_by_id(sid)
                additional.append({
                    "target_user_id": sid,
                    "target_name": (s_user or {}).get("name", "there"),
                    "matching_reasons": sibling.get("matching_reasons", []) or [],
                })

            if additional:
                match_result["all_matches"] = additional

        preview = await create_conversation_preview(
            db=db,
            openai=AzureOpenAIClient(),
            initiator_user_id=initiator_user_id,
            initiator_name=initiator_name or initiator_user.get("name") or f"User {initiator_user_id[:8]}",
            match_result=match_result,
            flow_type="reactive",
            connection_request_id=request_id,
        )
        if preview and preview.conversation_url:
            logger.info(
                "[NETWORKING] Created on-demand preview for target invite request %s",
                request_id,
            )
            return preview.conversation_url
    except Exception as e:
        logger.warning(
            "[NETWORKING] On-demand preview generation failed for request %s: %s",
            request_id,
            e,
        )

    return None


@tool(
    name="find_match",
    description="Find the best networking match for a user using structured complementary matching. "
    "Uses supply-demand skill intersection and LLM-based selection "
    "to find connections that satisfy the user's demand while ensuring mutual benefit. "
    "Pass the user's request as override_demand - if it contains specific criteria, it will be "
    "persisted to their demand history. If vague, their existing demand will be used.",
)
async def find_match(
    user_id: str,
    user_profile: Dict[str, Any],
    excluded_user_ids: Optional[List[str]] = None,
    override_demand: Optional[str] = None,
    override_value: Optional[str] = None,
) -> ToolResult:
    """Find the best networking match for a user.

    Uses adaptive matching with:
    1. Structured complementary matching (supply-demand skill intersection)
    2. LLM-based selection to pick the best match for mutual benefit

    Automatically excludes users with existing connections or pending requests.

    The ExecutionAgent has already determined this is a specific demand before
    calling this function. The demand will be interpreted into a clean statement
    and persisted to demand_history.

    Args:
        user_id: The initiator user's ID
        user_profile: The initiator's profile data
        excluded_user_ids: Additional users to exclude from matching
        override_demand: User's specific networking request (already validated as specific by ExecutionAgent)
        override_value: Override user's value for this search

    Returns:
        ToolResult with match details or error
    """
    # Validate and auto-repair user_id if LLM corrupted it
    validation_error, corrected_user_id = _validate_and_repair_user_id(user_id, user_profile)
    if validation_error:
        logger.warning(f"[NETWORKING] find_match validation failed: {validation_error}")
        return ToolResult(success=False, error=validation_error)

    # Use the corrected user_id for all operations
    user_id = corrected_user_id

    try:
        from app.agents.execution.networking.utils.adaptive_matcher import (
            AdaptiveMatcher,
        )
        from datetime import datetime, timezone

        db = DatabaseClient()
        openai = AzureOpenAIClient()

        # DEBUG: Log entry point for find_match
        logger.info(
            f"[NETWORKING] ========== FIND_MATCH ENTRY ==========\n"
            f"  User ID: {user_id}\n"
            f"  User Name: {user_profile.get('name', 'Unknown')}\n"
            f"  Override Demand: {override_demand or '(none)'}\n"
            f"  Excluded User IDs (input): {len(excluded_user_ids or [])} users"
        )

        # Automatically exclude users with active/pending connections
        auto_excluded = await _get_users_to_exclude(db, user_id)
        all_excluded = list(set((excluded_user_ids or []) + auto_excluded))
        logger.info(
            f"[NETWORKING] Auto-excluded {len(auto_excluded)} users with existing connections. "
            f"Total excluded: {len(all_excluded)}"
        )

        # Determine the demand to use for matching
        demand_to_use = None

        if override_demand:
            # Interpret the demand into a clean statement for storage
            # (ExecutionAgent already decided this is a specific demand before calling find_match)
            interpreted_demand = await _interpret_demand(openai, override_demand, user_profile)

            # Use interpreted demand, or fall back to raw text if interpretation fails
            demand_to_use = interpreted_demand or override_demand

            # Persist the demand to history (skip if already in history to avoid duplicates)
            try:
                # IMPORTANT: Fetch FRESH demand_history from DB to avoid overwriting concurrent updates
                fresh_state = await db.get_demand_value_state(user_id)
                current_demand_history = fresh_state.get("demand_history", [])

                # Check if this demand was recently added (avoid duplicates from multi-match loops)
                recent_demands = [
                    entry.get("text", "").lower().strip()
                    for entry in (current_demand_history[-3:] if current_demand_history else [])
                ]
                demand_already_added = demand_to_use.lower().strip() in recent_demands

                if not demand_already_added:
                    updated_demand_history = append_history(
                        current_demand_history,
                        demand_to_use,
                        created_at=datetime.now(timezone.utc).isoformat()
                    )
                    await db.update_user_profile(user_id, {"demand_history": updated_demand_history})

                    # Regenerate derived fields including embeddings
                    await update_demand_value_derived_fields(
                        db=db,
                        user_id=user_id,
                        demand_history=updated_demand_history,
                    )
                    logger.info(f"[NETWORKING] Persisted demand for user {user_id}: {demand_to_use[:50]}...")
                else:
                    logger.debug(f"[NETWORKING] Demand already in history, skipping persistence")
            except Exception as persist_error:
                logger.warning(f"[NETWORKING] Failed to persist demand: {persist_error}")

        logger.info(
            f"[NETWORKING] Calling AdaptiveMatcher.find_best_match with:\n"
            f"  - demand_to_use: {(demand_to_use or '(none)')[:80]}\n"
            f"  - override_value: {(override_value or '(none)')[:80] if override_value else '(none)'}\n"
            f"  - excluded_user_ids: {len(all_excluded)} users"
        )

        matcher = AdaptiveMatcher(db=db, openai=openai)
        result = await matcher.find_best_match(
            user_id=user_id,
            user_profile=user_profile,
            excluded_user_ids=all_excluded,
            override_demand=demand_to_use,
            override_value=override_value,
        )

        if not result.success:
            logger.warning(
                f"[NETWORKING] AdaptiveMatcher returned failure:\n"
                f"  - error_message: {result.error_message}\n"
                f"  - This typically means zero candidates were found.\n"
                f"  - Check if user has seeking_skills/offering_skills populated."
            )
            return ToolResult(
                success=False,
                error=result.error_message or "No suitable match found",
            )

        logger.info(
            f"[NETWORKING] Match found!\n"
            f"  - Target: {result.target_name} ({result.target_user_id[:8] if result.target_user_id else 'N/A'}...)\n"
            f"  - Match Score: {result.match_score}\n"
            f"  - Confidence: {result.match_confidence}"
        )

        # Location distance: look up both users in Find My and append distance
        try:
            from app.integrations.photon_client import PhotonClient as _PhotonClient
            from app.utils.location_service import get_distance_between_users

            initiator_phone = user_profile.get("phone_number", "")
            initiator_email = user_profile.get("email", "")
            target_phone = result.target_phone
            target_email = ""
            if result.target_user_id:
                target_user = await db.get_user_by_id(result.target_user_id)
                if target_user:
                    target_email = target_user.get("email", "") or ""
            if initiator_phone or initiator_email:
                _photon = _PhotonClient()
                distance_str = await get_distance_between_users(
                    _photon, initiator_phone or "", target_phone or "",
                    initiator_email=initiator_email or None,
                    target_email=target_email or None,
                    initiator_user_id=user_profile.get("id"),
                    target_user_id=result.target_user_id,
                )
                if distance_str:
                    result.matching_reasons.append(distance_str)
        except Exception as loc_err:
            logger.warning(f"[NETWORKING] Location distance lookup failed: {loc_err}")

        # CRITICAL: Automatically create connection request for the match
        # This ensures the InteractionAgent has a real request_id to work with
        # Previously, LLM was supposed to call create_connection_request but often skipped it
        from app.agents.execution.networking.utils.value_exchange_matcher import (
            MatchResult,
        )
        from app.agents.execution.networking.utils.handshake_manager import (
            HandshakeManager,
        )

        handshake = HandshakeManager(db=db)
        request_id = None

        try:
            match_result = MatchResult(
                target_user_id=result.target_user_id,
                target_name=result.target_name,
                target_phone=result.target_phone,
                match_score=result.match_score,
                matching_reasons=result.matching_reasons,
                llm_introduction=result.llm_introduction,
                llm_concern=result.llm_concern,
            )

            request = await handshake.create_request(
                initiator_id=user_id,
                match_result=match_result,
                connection_purpose=demand_to_use or override_demand,
            )

            request_id = request.get("id")
            logger.info(
                f"[NETWORKING] find_match: Created connection request {request_id} "
                f"for {result.target_name}"
            )
        except Exception as create_error:
            logger.error(
                f"[NETWORKING] find_match: Failed to create request for "
                f"{result.target_name}: {create_error}"
            )
            # Continue and return match data even if request creation failed
            # The LLM can still call create_connection_request manually if needed

        # Fire off discovery conversation generation in the background so the
        # match result returns immediately (~0s delay).  The background task
        # generates the conversation, persists it, and sends the rich link as a
        # follow-up iMessage bubble via Photon.
        conversation_pending = False
        from app.config import settings as _settings

        if getattr(_settings, "conversation_preview_enabled", False):
            conversation_pending = True
            phone_number = user_profile.get("phone_number", "")
            asyncio.create_task(
                _generate_and_send_conversation_preview(
                    user_id=user_id,
                    user_name=user_profile.get("name", ""),
                    phone_number=phone_number,
                    match_result={
                        "target_user_id": result.target_user_id,
                        "target_name": result.target_name,
                        "matching_reasons": result.matching_reasons,
                        "mutual_benefit": result.mutual_benefit,
                        "demand_satisfaction": result.demand_satisfaction,
                        "match_summary": result.match_summary,
                        "match_confidence": result.match_confidence,
                        "connection_purpose": demand_to_use or override_demand or "",
                    },
                    flow_type="reactive",
                    connection_request_id=request_id,
                )
            )

        return ToolResult(
            success=True,
            data={
                "target_user_id": result.target_user_id,
                "target_name": result.target_name,
                "target_phone": result.target_phone,
                "match_score": result.match_score,
                "match_confidence": result.match_confidence,
                "matching_reasons": result.matching_reasons,
                "llm_introduction": result.llm_introduction,
                "llm_concern": result.llm_concern,
                "demand_satisfaction": result.demand_satisfaction,
                "mutual_benefit": result.mutual_benefit,
                "match_summary": result.match_summary,
                # Include request_id so ExecutionAgent can return it in wait_for_user
                "connection_request_id": request_id,
                "request_id": request_id,  # Alias for consistency with multi-match
                # Conversation is generating in background — synthesis should ack this
                "conversation_pending": conversation_pending,
            },
        )

    except Exception as e:
        logger.error(f"[NETWORKING] find_match failed: {e}", exc_info=True)
        return ToolResult(
            success=False,
            error=f"Match search failed: {str(e)}",
        )


@tool(
    name="create_connection_request",
    description="Create a new connection request between the initiator and a matched target user. "
    "This puts the request in PENDING_INITIATOR_APPROVAL status.",
)
async def create_connection_request(
    initiator_id: str,
    target_user_id: str,
    target_name: str,
    target_phone: str,
    match_score: float,
    matching_reasons: List[str],
    llm_introduction: str,
    llm_concern: Optional[str] = None,
    excluded_candidates: Optional[List[str]] = None,
) -> ToolResult:
    """Create a connection request for a match.

    Args:
        initiator_id: The initiator user's ID
        target_user_id: The matched target user's ID
        target_name: Target user's name
        target_phone: Target user's phone
        match_score: Score from the matcher
        matching_reasons: Reasons for the match
        llm_introduction: LLM-generated introduction
        llm_concern: Optional concern about the match
        excluded_candidates: Previously rejected candidates

    Returns:
        ToolResult with connection request ID
    """
    # Validate UUIDs
    try:
        UUID(initiator_id)
    except (ValueError, TypeError):
        return ToolResult(success=False, error=f"Invalid initiator_id format: {initiator_id}")

    try:
        UUID(target_user_id)
    except (ValueError, TypeError):
        return ToolResult(success=False, error=f"Invalid target_user_id format: {target_user_id}")

    try:
        from app.agents.execution.networking.utils.value_exchange_matcher import (
            MatchResult,
        )
        from app.agents.execution.networking.utils.handshake_manager import (
            HandshakeManager,
        )

        match_result = MatchResult(
            target_user_id=target_user_id,
            target_name=target_name,
            target_phone=target_phone,
            match_score=match_score,
            matching_reasons=matching_reasons,
            llm_introduction=llm_introduction,
            llm_concern=llm_concern,
        )

        handshake = HandshakeManager()
        request = await handshake.create_request(
            initiator_id=initiator_id,
            match_result=match_result,
            excluded_candidates=excluded_candidates,
        )

        return ToolResult(
            success=True,
            data={
                "connection_request_id": request.get("id"),
                "status": request.get("status"),
            },
        )

    except Exception as e:
        logger.error(f"[NETWORKING] create_connection_request failed: {e}", exc_info=True)
        return ToolResult(
            success=False,
            error=f"Failed to create connection request: {str(e)}",
        )


@tool(
    name="get_pending_connection_request",
    description="Get any pending connection request for a user, either as initiator or target.",
)
async def get_pending_connection_request(
    user_id: str,
    as_initiator: bool = True,
    user_profile: Optional[Dict[str, Any]] = None,
) -> ToolResult:
    """Get pending connection request for a user.

    Args:
        user_id: User's ID
        as_initiator: Whether to check as initiator (True) or target (False)
        user_profile: Optional user profile for UUID auto-repair

    Returns:
        ToolResult with pending request data or None
    """
    # Validate and auto-repair user_id if LLM corrupted it
    if user_profile:
        validation_error, corrected_user_id = _validate_and_repair_user_id(user_id, user_profile)
        if validation_error:
            logger.warning(f"[NETWORKING] get_pending_connection_request validation failed: {validation_error}")
            return ToolResult(success=False, error=validation_error)
        user_id = corrected_user_id
    else:
        # No user_profile - just validate UUID format
        try:
            UUID(user_id)
        except (ValueError, TypeError):
            logger.warning(f"[NETWORKING] get_pending_connection_request invalid user_id: {user_id}")
            return ToolResult(
                success=False,
                error=f"Invalid user_id format: {user_id}. Please use the user_id from user_profile.",
            )

    try:
        from app.agents.execution.networking.utils.handshake_manager import (
            HandshakeManager,
        )

        handshake = HandshakeManager()

        if as_initiator:
            request = await handshake.get_pending_for_initiator(user_id)
        else:
            request = await handshake.get_pending_for_target(user_id)

        if not request:
            return ToolResult(
                success=True,
                data=None,
                metadata={"has_pending": False},
            )

        return ToolResult(
            success=True,
            data=request,
            metadata={"has_pending": True},
        )

    except Exception as e:
        logger.error(f"[NETWORKING] get_pending_connection_request failed: {e}", exc_info=True)
        return ToolResult(
            success=False,
            error=f"Failed to get pending request: {str(e)}",
        )


@tool(
    name="confirm_connection_request",
    description="Initiator confirms the match - moves request to PENDING_TARGET_APPROVAL.",
)
async def confirm_connection_request(request_id: str) -> ToolResult:
    """Confirm a connection request as the initiator.

    Args:
        request_id: The connection request ID

    Returns:
        ToolResult with updated request status
    """
    try:
        from app.agents.execution.networking.utils.handshake_manager import (
            HandshakeManager,
        )

        handshake = HandshakeManager()
        request = await handshake.initiator_confirms(request_id)

        return ToolResult(
            success=True,
            data={
                "request_id": request_id,
                "status": request.get("status"),
                "target_notified_at": request.get("target_notified_at"),
            },
        )

    except Exception as e:
        logger.error(f"[NETWORKING] confirm_connection_request failed: {e}", exc_info=True)
        return ToolResult(
            success=False,
            error=f"Failed to confirm request: {str(e)}",
        )


@tool(
    name="confirm_and_send_invitation",
    description="Initiator confirms the match AND sends invitation to target in one atomic operation. "
    "Automatically looks up target info and matching reasons from the connection request. "
    "Use this instead of calling confirm_connection_request and send_invitation separately. "
    "IMPORTANT: This is for CASE B (initiator confirming). Do NOT use for CASE C (target accepting).",
)
async def confirm_and_send_invitation(
    request_id: str,
    initiator_name: str,
) -> ToolResult:
    """Confirm a connection request and send invitation to target atomically.

    This is the preferred way to handle user confirmation - it ensures the invitation
    is sent immediately after confirmation without requiring another agent iteration.

    Target info and matching reasons are looked up from the connection request.

    Args:
        request_id: The connection request ID
        initiator_name: Name of the initiator

    Returns:
        ToolResult with confirmation status and invitation details
    """
    # Validate request_id
    validation_error = _validate_request_id(request_id)
    if validation_error:
        logger.warning(f"[NETWORKING] confirm_and_send_invitation validation failed: {validation_error}")
        return ToolResult(success=False, error=validation_error)

    try:
        from app.agents.execution.networking.utils.handshake_manager import (
            HandshakeManager,
        )
        from app.agents.execution.networking.utils.message_generator import (
            generate_invitation_message,
        )
        from app.integrations.photon_client import PhotonClient

        db = DatabaseClient()

        # Step 0: Get connection request data and check current status
        request_data = await db.get_connection_request(request_id)
        if not request_data:
            return ToolResult(
                success=False,
                error=f"Connection request {request_id} not found",
            )

        # Check if request is already past the initiator confirmation stage
        # This prevents wasted LLM iterations on already-processed requests
        current_status = request_data.get("status")

        # If request is at PENDING_TARGET_APPROVAL, this means the initiator already confirmed
        # and we're now waiting for the TARGET to accept. If you're processing a target's
        # acceptance (CASE C), use target_responds() instead!
        if current_status == ConnectionRequestStatus.PENDING_TARGET_APPROVAL.value:
            logger.info(
                f"[NETWORKING] confirm_and_send_invitation: request {request_id} is at pending_target_approval. "
                "If this is CASE C (target accepting), use target_responds() instead."
            )
            return ToolResult(
                success=True,
                data={
                    "request_id": request_id,
                    "status": current_status,
                    "already_confirmed": True,
                    "message": "Request already confirmed by initiator. If processing target's acceptance (CASE C), "
                    "use target_responds(request_id, accept=true) instead.",
                    "hint": "For CASE C (target accepting invitation), use target_responds() not confirm_and_send_invitation()",
                },
            )

        # If already accepted or group created, just return success
        if current_status in [
            ConnectionRequestStatus.TARGET_ACCEPTED.value,
            ConnectionRequestStatus.GROUP_CREATED.value,
        ]:
            logger.info(
                f"[NETWORKING] confirm_and_send_invitation: request {request_id} already processed "
                f"(status={current_status}), returning success without re-confirming"
            )
            return ToolResult(
                success=True,
                data={
                    "request_id": request_id,
                    "status": current_status,
                    "already_confirmed": True,
                    "message": "Request was already confirmed and processed",
                },
            )

        target_user_id = request_data.get("target_user_id")
        if not target_user_id:
            return ToolResult(
                success=False,
                error="Connection request missing target_user_id",
            )

        # Get matching reasons from the connection request
        matching_reasons = request_data.get("matching_reasons", [])

        # Look up target user to get phone number and name
        target_user = await db.get_user_by_id(target_user_id)
        if not target_user or not target_user.get("phone_number"):
            return ToolResult(
                success=False,
                error=f"Could not find phone number for target user {target_user_id}",
            )

        target_phone = target_user.get("phone_number")
        target_name = target_user.get("name", "there")
        logger.info(f"[NETWORKING] Resolved target: {target_name} ({target_phone})")

        # Step 1: Confirm the connection request
        handshake = HandshakeManager()
        request = await handshake.initiator_confirms(request_id)

        # Step 2: Generate invitation message
        message = await generate_invitation_message(
            initiator_name=initiator_name,
            target_name=target_name,
            matching_reasons=matching_reasons,
        )

        if not message:
            return ToolResult(
                success=False,
                error="Request confirmed but failed to generate invitation message",
            )

        # Step 3: Send invitation to target
        photon = PhotonClient()
        await photon.send_message(to_number=target_phone, content=message)

        # Step 3.5: Send conversation preview link to target (with robust fallback).
        convo_url = await _get_or_create_preview_url_for_target_invite(
            db=db,
            request_data=request_data,
            request_id=request_id,
            initiator_name=initiator_name,
            target_user=target_user,
        )
        preview_sent = False
        if convo_url:
            try:
                await photon.send_message(
                    to_number=target_phone,
                    content=convo_url,
                    rich_link=True,
                )
                preview_sent = True
                logger.info(
                    "[NETWORKING] Sent conversation preview to target %s: %s",
                    target_name,
                    convo_url,
                )
            except Exception as rich_err:
                # Fallback to plain URL if rich-link send fails on this handle.
                try:
                    await photon.send_message(
                        to_number=target_phone,
                        content=convo_url,
                    )
                    preview_sent = True
                    logger.info(
                        "[NETWORKING] Sent plain conversation URL to target %s after rich-link failure: %s",
                        target_name,
                        convo_url,
                    )
                except Exception as plain_err:
                    logger.warning(
                        "[NETWORKING] Could not send conversation preview to target %s: rich_err=%s plain_err=%s",
                        target_name,
                        rich_err,
                        plain_err,
                    )
        else:
            logger.info(
                "[NETWORKING] No conversation preview available for target %s request %s",
                target_name,
                request_id,
            )

        if preview_sent:
            try:
                await db.store_message(
                    user_id=target_user["id"],
                    content=convo_url,
                    message_type="bot",
                    metadata={
                        "intent": "networking_invitation_preview",
                        "connection_request_id": request_id,
                    },
                )
            except Exception as e:
                logger.warning(
                    "[NETWORKING] Failed to store preview link in target conversation: %s",
                    e,
                )

        # Step 4: Store the invitation in target's conversation history
        # This ensures the target's InteractionAgent has context when they respond
        # Note: We already have target_user from step 0
        try:
            await db.store_message(
                user_id=target_user["id"],
                content=message,
                message_type="bot",
                metadata={
                    "intent": "networking_invitation",
                    "connection_request_id": request_id,
                    "initiator_name": initiator_name,
                },
            )
            logger.info(f"[NETWORKING] Stored invitation in target's conversation: {target_user['id']}")
        except Exception as e:
            # Don't fail the whole operation if we can't store the message
            logger.warning(f"[NETWORKING] Failed to store invitation in target's conversation: {e}")

        return ToolResult(
            success=True,
            data={
                "request_id": request_id,
                "status": request.get("status"),
                "invitation_sent": True,
                "target_name": target_name,  # CRITICAL: Include target_name for response synthesis
                "target_phone": target_phone,
                "message": message,
            },
        )

    except Exception as e:
        logger.error(f"[NETWORKING] confirm_and_send_invitation failed: {e}", exc_info=True)
        return ToolResult(
            success=False,
            error=f"Failed to confirm and send invitation: {str(e)}",
        )


@tool(
    name="request_different_match",
    description="Initiator requests a different match - cancels current and excludes target. "
    "IMPORTANT: current_target_id must be a UUID (e.g., 'fa8ad95d-d21f-4b58-8ac7-807e5b8183fc'), NOT a name.",
)
async def request_different_match(
    request_id: str,
    current_target_id: str,
) -> ToolResult:
    """Request a different match, excluding the current target.

    Args:
        request_id: The connection request ID (UUID format)
        current_target_id: Current target's USER ID to exclude (UUID format, NOT a name)

    Returns:
        ToolResult indicating cancellation
    """
    # Validate request_id
    validation_error = _validate_request_id(request_id)
    if validation_error:
        logger.warning(f"[NETWORKING] request_different_match validation failed: {validation_error}")
        return ToolResult(success=False, error=validation_error)

    # Validate current_target_id is a UUID, not a name
    try:
        UUID(current_target_id)
    except (ValueError, TypeError):
        logger.warning(
            f"[NETWORKING] request_different_match received invalid current_target_id: {current_target_id}"
        )
        return ToolResult(
            success=False,
            error=f"current_target_id must be a UUID, not a name. Received: '{current_target_id}'. "
            "Use the target_user_id from the connection request data.",
        )

    try:
        from app.agents.execution.networking.utils.handshake_manager import (
            HandshakeManager,
        )

        db = DatabaseClient()

        # Check if request is already cancelled or processed
        request_data = await db.get_connection_request(request_id)
        if request_data:
            current_status = request_data.get("status")
            terminal_statuses = [
                ConnectionRequestStatus.CANCELLED.value,
                ConnectionRequestStatus.TARGET_DECLINED.value,
                ConnectionRequestStatus.EXPIRED.value,
            ]
            if current_status in terminal_statuses:
                logger.info(
                    f"[NETWORKING] request_different_match: request {request_id} already in terminal state "
                    f"(status={current_status}), returning success"
                )
                return ToolResult(
                    success=True,
                    data={
                        "request_id": request_id,
                        "status": current_status,
                        "already_cancelled": True,
                        "excluded_target": current_target_id,
                    },
                )

        handshake = HandshakeManager()
        request = await handshake.initiator_requests_different(
            request_id=request_id,
            current_target_id=current_target_id,
        )

        return ToolResult(
            success=True,
            data={
                "request_id": request_id,
                "status": "cancelled",
                "excluded_target": current_target_id,
            },
        )

    except Exception as e:
        logger.error(f"[NETWORKING] request_different_match failed: {e}", exc_info=True)
        return ToolResult(
            success=False,
            error=f"Failed to request different match: {str(e)}",
        )


@tool(
    name="cancel_connection_request",
    description="Initiator cancels networking for now.",
)
async def cancel_connection_request(request_id: str) -> ToolResult:
    """Cancel a connection request.

    Args:
        request_id: The connection request ID

    Returns:
        ToolResult indicating cancellation
    """
    try:
        from app.agents.execution.networking.utils.handshake_manager import (
            HandshakeManager,
        )

        handshake = HandshakeManager()
        await handshake.initiator_cancels(request_id)

        return ToolResult(
            success=True,
            data={"request_id": request_id, "status": "cancelled"},
        )

    except Exception as e:
        logger.error(f"[NETWORKING] cancel_connection_request failed: {e}", exc_info=True)
        return ToolResult(
            success=False,
            error=f"Failed to cancel request: {str(e)}",
        )


@tool(
    name="target_responds",
    description="Target user accepts or declines the connection request. "
    "IMPORTANT: This is for CASE C (target responding to invitation). Do NOT use for CASE B (initiator confirming). "
    "For multi-match requests, checks threshold and returns info about group creation.",
)
async def target_responds(
    request_id: str,
    accept: bool,
    decline_reason: Optional[str] = None,
) -> ToolResult:
    """Process target's response to a connection request.

    For multi-match requests (study groups, etc.), this checks the acceptance
    threshold and returns info about whether a group should be created.

    Args:
        request_id: The connection request ID
        accept: Whether to accept or decline
        decline_reason: Optional reason if declining

    Returns:
        ToolResult with updated status and multi_match_status if applicable
    """
    # Validate request_id
    validation_error = _validate_request_id(request_id)
    if validation_error:
        logger.warning(f"[NETWORKING] target_responds validation failed: {validation_error}")
        return ToolResult(success=False, error=validation_error)

    try:
        from app.agents.execution.networking.utils.handshake_manager import (
            HandshakeManager,
        )

        db = DatabaseClient()
        handshake = HandshakeManager(db=db)

        # Check current status to handle already-processed requests gracefully
        request_data = await db.get_connection_request(request_id)
        if request_data:
            current_status = request_data.get("status")

            # If already accepted or group created, return success without re-processing
            if current_status in [
                ConnectionRequestStatus.TARGET_ACCEPTED.value,
                ConnectionRequestStatus.GROUP_CREATED.value,
            ]:
                logger.info(
                    f"[NETWORKING] target_responds: request {request_id} already accepted "
                    f"(status={current_status}), returning success without re-processing"
                )
                # Fetch initiator name for response context
                initiator_user_id = request_data.get("initiator_user_id")
                initiator_name = "someone"
                if initiator_user_id:
                    initiator_user = await db.get_user_by_id(initiator_user_id)
                    if initiator_user:
                        initiator_name = initiator_user.get("name", "someone")

                return ToolResult(
                    success=True,
                    data={
                        "request_id": request_id,
                        "status": current_status,
                        "accepted": True,
                        "already_accepted": True,
                        "ready_for_group": current_status == ConnectionRequestStatus.TARGET_ACCEPTED.value,
                        "group_already_created": current_status == ConnectionRequestStatus.GROUP_CREATED.value,
                        "initiator_name": initiator_name,
                        "action_type": "target_accepted",
                    },
                )

            # If already declined, return success without re-processing
            if current_status == ConnectionRequestStatus.TARGET_DECLINED.value:
                logger.info(
                    f"[NETWORKING] target_responds: request {request_id} already declined, "
                    "returning success without re-processing"
                )
                return ToolResult(
                    success=True,
                    data={
                        "request_id": request_id,
                        "status": current_status,
                        "accepted": False,
                        "already_declined": True,
                    },
                )

        if accept:
            # Use multi-match aware acceptance that checks threshold
            request = await handshake.target_accepts_multi_match(request_id)

            multi_match_status = request.get("multi_match_status", {})
            is_multi = multi_match_status.get("is_multi_match", False)
            ready_for_group = multi_match_status.get("ready_for_group", True)

            # Fetch initiator name for response context
            # This helps InteractionAgent know who invited the target
            initiator_user_id = request.get("initiator_user_id")
            initiator_name = "someone"
            if initiator_user_id:
                initiator_user = await db.get_user_by_id(initiator_user_id)
                if initiator_user:
                    initiator_name = initiator_user.get("name", "someone")

            return ToolResult(
                success=True,
                data={
                    "request_id": request_id,
                    "status": request.get("status"),
                    "accepted": True,
                    "is_multi_match": is_multi,
                    "multi_match_status": multi_match_status,
                    # For single-match or when threshold met, group should be created
                    "ready_for_group": ready_for_group,
                    # Include initiator name for CASE C response synthesis
                    "initiator_name": initiator_name,
                    "action_type": "target_accepted",  # Explicit marker for CASE C
                },
            )
        else:
            request = await handshake.target_declines(request_id, decline_reason)
            return ToolResult(
                success=True,
                data={
                    "request_id": request_id,
                    "status": request.get("status"),
                    "accepted": False,
                },
            )

    except Exception as e:
        logger.error(f"[NETWORKING] target_responds failed: {e}", exc_info=True)
        return ToolResult(
            success=False,
            error=f"Failed to process response: {str(e)}",
        )


@tool(
    name="create_group_chat",
    description="Create a group chat for connected users. "
    "For single-match: creates 2-person chat. "
    "For multi-match: creates N-person chat if threshold met, or adds late joiner to existing group. "
    "Pass multi_match_status from target_responds to handle multi-match scenarios.",
)
async def create_group_chat(
    connection_request_id: str,
    multi_match_status: Optional[Dict[str, Any]] = None,
) -> ToolResult:
    """Create a group chat for connected users.

    For single-match requests: creates a standard 2-person group chat.
    For multi-match requests: either creates an N-person group or adds to existing.

    Args:
        connection_request_id: The connection request ID
        multi_match_status: Optional status from target_responds with threshold info

    Returns:
        ToolResult with group chat GUID
    """
    # Validate connection_request_id
    validation_error = _validate_request_id(connection_request_id)
    if validation_error:
        logger.warning(f"[NETWORKING] create_group_chat validation failed: {validation_error}")
        return ToolResult(success=False, error=validation_error)

    try:
        from app.groupchat.features.provisioning import GroupChatService
        from app.agents.execution.networking.utils.handshake_manager import (
            HandshakeManager,
        )

        db = DatabaseClient()
        handshake = HandshakeManager(db=db)

        # Get connection request data
        request_data = await db.get_connection_request(connection_request_id)
        if not request_data:
            return ToolResult(
                success=False,
                error=f"Connection request {connection_request_id} not found",
            )

        # If this request is tied to an existing group chat, add the target to that chat
        existing_chat_guid = request_data.get("group_chat_guid")
        if existing_chat_guid:
            current_status = request_data.get("status")
            if current_status == ConnectionRequestStatus.GROUP_CREATED.value:
                return ToolResult(
                    success=True,
                    data={
                        "chat_guid": str(existing_chat_guid),
                        "already_added": True,
                        "action_type": "participant_added",
                    },
                )

            target_user_id = request_data.get("target_user_id")
            if not target_user_id:
                return ToolResult(
                    success=False,
                    error="Connection request missing target_user_id",
                )

            target_user = await db.get_user_by_id(target_user_id)
            if not target_user or not target_user.get("phone_number"):
                return ToolResult(
                    success=False,
                    error=f"Could not find phone number for target user {target_user_id}",
                )

            existing_member_names = []
            try:
                participant_ids = await _get_group_chat_participant_ids(db, str(existing_chat_guid))
                if participant_ids:
                    members = await asyncio.gather(
                        *[db.get_user_by_id(pid) for pid in participant_ids],
                        return_exceptions=True,
                    )
                    for member in members:
                        if isinstance(member, dict):
                            name = member.get("name")
                            if name and member.get("id") != target_user_id:
                                existing_member_names.append(name)
            except Exception:
                existing_member_names = []

            connection_purpose = request_data.get("connection_purpose")
            matching_reasons = request_data.get("matching_reasons", [])
            llm_introduction = request_data.get("llm_introduction")

            service = GroupChatService()
            add_result = await service.add_participant_to_group(
                chat_guid=str(existing_chat_guid),
                user_id=target_user_id,
                phone=target_user.get("phone_number"),
                name=target_user.get("name", "friend"),
                connection_request_id=connection_request_id,
                existing_member_names=existing_member_names,
                connection_purpose=connection_purpose,
                matching_reasons=matching_reasons,
                llm_introduction=llm_introduction,
            )

            try:
                await db.update_connection_request_status(
                    request_id=connection_request_id,
                    status=ConnectionRequestStatus.GROUP_CREATED,
                    additional_updates={
                        "group_chat_guid": str(existing_chat_guid),
                        "group_created_at": datetime.utcnow().isoformat(),
                    },
                    expected_current_status=ConnectionRequestStatus.TARGET_ACCEPTED,
                )
            except Exception as e:
                logger.warning(
                    "[NETWORKING] Failed to update request status for %s: %s",
                    str(connection_request_id),
                    e,
                )

            return ToolResult(
                success=True,
                data={
                    "chat_guid": str(existing_chat_guid),
                    "added_user_id": add_result.get("added_user_id"),
                    "added_user_name": add_result.get("added_user_name"),
                    "action_type": "participant_added",
                },
            )

        # Check if this is a multi-match request
        is_multi_match = request_data.get("is_multi_match", False)
        signal_group_id = request_data.get("signal_group_id")

        # Use multi_match_status if provided, otherwise fetch fresh
        if multi_match_status is None and is_multi_match and signal_group_id:
            check_result = await db.check_multi_match_ready_v1(signal_group_id)
            multi_match_status = {
                "is_multi_match": True,
                "signal_group_id": signal_group_id,
                "ready_for_group": check_result.get("ready", False),
                "existing_chat_guid": check_result.get("chat_guid"),
                "accepted_request_ids": check_result.get("accepted_request_ids", []),
            }

        # Handle multi-match group creation
        if is_multi_match and multi_match_status:
            existing_chat = multi_match_status.get("existing_chat_guid")

            if existing_chat:
                # Late joiner - add to existing group
                result = await handshake.add_late_joiner_to_group(
                    request_id=connection_request_id,
                    existing_chat_guid=existing_chat,
                )
                return ToolResult(
                    success=True,
                    data={
                        "chat_guid": existing_chat,
                        "added_user_name": result.get("added_user_name"),
                        "is_late_joiner": True,
                    },
                )

            # Create new multi-person group
            accepted_ids = multi_match_status.get("accepted_request_ids", [])
            if connection_request_id not in accepted_ids:
                accepted_ids.append(connection_request_id)

            result = await handshake.create_multi_person_group(
                signal_group_id=signal_group_id,
                accepted_request_ids=accepted_ids,
            )

            return ToolResult(
                success=True,
                data={
                    "chat_guid": result.get("chat_guid"),
                    "participant_count": len(result.get("participants", [])),
                    "is_multi_person": True,
                },
            )

        # Standard single-match flow
        initiator_user_id = request_data.get("initiator_user_id")
        target_user_id = request_data.get("target_user_id")
        matching_reasons = request_data.get("matching_reasons", [])

        # Safety check: before creating a new 2-person chat, verify these users
        # don't already share a group chat. This catches edge cases where
        # group_chat_guid wasn't properly set on the connection request.
        try:
            existing_shared_chat = await db.get_group_chat_for_users(
                initiator_user_id, target_user_id
            )
            if existing_shared_chat:
                existing_guid = existing_shared_chat.get("chat_guid")
                logger.warning(
                    f"[NETWORKING] create_group_chat: Safety check caught existing group chat "
                    f"{existing_guid} between {initiator_user_id} and {target_user_id}. "
                    f"Adding target to existing chat instead of creating new one."
                )

                target_user = await db.get_user_by_id(target_user_id)
                if target_user and target_user.get("phone_number"):
                    service = GroupChatService()
                    connection_purpose = request_data.get("connection_purpose")
                    llm_introduction = request_data.get("llm_introduction")

                    existing_member_names = []
                    try:
                        participant_ids = await _get_group_chat_participant_ids(db, str(existing_guid))
                        if participant_ids:
                            members = await asyncio.gather(
                                *[db.get_user_by_id(pid) for pid in participant_ids],
                                return_exceptions=True,
                            )
                            for member in members:
                                if isinstance(member, dict):
                                    name = member.get("name")
                                    if name and member.get("id") != target_user_id:
                                        existing_member_names.append(name)
                    except Exception:
                        existing_member_names = []

                    await service.add_participant_to_group(
                        chat_guid=str(existing_guid),
                        user_id=target_user_id,
                        phone=target_user.get("phone_number"),
                        name=target_user.get("name", "friend"),
                        connection_request_id=connection_request_id,
                        existing_member_names=existing_member_names,
                        connection_purpose=connection_purpose,
                        matching_reasons=matching_reasons,
                        llm_introduction=llm_introduction,
                    )

                    try:
                        await db.update_connection_request_status(
                            request_id=connection_request_id,
                            status=ConnectionRequestStatus.GROUP_CREATED,
                            additional_updates={
                                "group_chat_guid": str(existing_guid),
                                "group_created_at": datetime.utcnow().isoformat(),
                            },
                            expected_current_status=ConnectionRequestStatus.TARGET_ACCEPTED,
                        )
                    except Exception as status_err:
                        logger.warning(
                            f"[NETWORKING] Failed to update request status after safety add: {status_err}"
                        )

                    return ToolResult(
                        success=True,
                        data={
                            "chat_guid": str(existing_guid),
                            "added_user_name": target_user.get("name", "friend"),
                            "action_type": "participant_added",
                            "safety_check": True,
                        },
                    )
        except Exception as safety_err:
            logger.error(
                f"[NETWORKING] Safety check for existing group chat failed: {safety_err}",
                exc_info=True,
            )
            return ToolResult(
                success=False,
                error=f"Failed to check for existing group chat: {safety_err}",
            )

        # Look up both users
        initiator_user = await db.get_user_by_id(initiator_user_id)
        if not initiator_user or not initiator_user.get("phone_number"):
            return ToolResult(
                success=False,
                error=f"Could not find phone number for initiator user {initiator_user_id}",
            )
        initiator_phone = initiator_user.get("phone_number")
        initiator_name = initiator_user.get("name", "friend")

        target_user = await db.get_user_by_id(target_user_id)
        if not target_user or not target_user.get("phone_number"):
            return ToolResult(
                success=False,
                error=f"Could not find phone number for target user {target_user_id}",
            )
        target_phone = target_user.get("phone_number")
        target_name = target_user.get("name", "friend")

        # Get shared university if any
        university = None
        if initiator_user.get("university") and initiator_user.get("university") == target_user.get("university"):
            university = initiator_user.get("university")

        logger.info(f"[NETWORKING] create_group_chat: {initiator_name} <-> {target_name}")

        service = GroupChatService()
        result = await service.create_group(
            user_a_phone=initiator_phone,
            user_b_phone=target_phone,
            user_a_name=initiator_name,
            user_b_name=target_name,
            connection_request_id=connection_request_id,
            user_a_id=initiator_user_id,
            user_b_id=target_user_id,
            university=university,
            matching_reasons=matching_reasons,
        )

        chat_guid = result.get("chat_guid")

        # Mark the connection request as having group created
        await handshake.mark_group_created(connection_request_id, chat_guid)

        # Track first networking completion for initiator and send location prompt
        initiator_facts = initiator_user.get("personal_facts", {}) or {}
        is_first_networking = not initiator_facts.get("first_networking_completed")

        if is_first_networking:
            from datetime import datetime
            await db.update_user_profile(
                user_id=initiator_user_id,
                personal_facts={
                    **initiator_facts,
                    "first_networking_completed": datetime.utcnow().isoformat(),
                },
            )
            logger.info(f"[NETWORKING] Marked first networking completed for {initiator_user_id}")

            # Send location sharing info if not already prompted and no location set
            location_prompted = initiator_facts.get("location_sharing_prompted")
            has_location = initiator_user.get("location")

            if not location_prompted and not has_location:
                try:
                    import os
                    from app.integrations.photon_client import PhotonClient

                    photon = PhotonClient()
                    location_prompt = (
                        f"hey {initiator_name.lower() if initiator_name else 'quick thing'} - "
                        "if you share your location with me, i can connect you with people "
                        "nearby. think study partners at your campus library, coffee chats with someone "
                        "in your city working on similar stuff, or grabbing lunch with a founder down the "
                        "street. in-person connections hit different. just tap the + on the left of the "
                        "typing box and send your location"
                    )
                    await photon.send_message(to_number=initiator_phone, content=location_prompt)

                    # Send location instruction image
                    # Path: go up 5 levels from networking.py to reach /app, then join with scripts
                    # /app/app/agents/tools/networking.py -> /app/scripts
                    script_dir = os.path.join(
                        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))),
                        "scripts",
                    )
                    image_path = os.path.join(script_dir, "find_my.jpg")
                    if os.path.exists(image_path):
                        try:
                            await photon.send_attachment(
                                to_number=initiator_phone,
                                file_path=image_path,
                                file_name="location-instructions.jpg",
                            )
                            logger.info(f"[NETWORKING] Sent location instruction image to {initiator_user_id}")
                        except Exception as img_err:
                            logger.warning(f"[NETWORKING] Failed to send location image: {img_err}")
                    else:
                        logger.warning(f"[NETWORKING] Location image not found: {image_path}")

                    # Mark as prompted so we don't send again
                    await db.update_user_profile(
                        user_id=initiator_user_id,
                        personal_facts={
                            **initiator_facts,
                            "first_networking_completed": datetime.utcnow().isoformat(),
                            "location_sharing_prompted": True,
                        },
                    )
                    logger.info(f"[NETWORKING] Sent location sharing info to {initiator_user_id}")
                except Exception as e:
                    logger.warning(f"[NETWORKING] Failed to send location info: {e}")

        return ToolResult(
            success=True,
            data={
                "chat_guid": chat_guid,
                "initiator_name": initiator_name,
                "target_name": target_name,
                # Group chat created - both users will be notified
                "action_type": "group_chat_created",
            },
        )

    except Exception as e:
        logger.error(f"[NETWORKING] create_group_chat failed: {e}", exc_info=True)
        return ToolResult(
            success=False,
            error=f"Failed to create group chat: {str(e)}",
        )


@tool(
    name="find_multi_matches",
    description="Find multiple networking matches for a user. Use this for signals that benefit from "
    "connecting with multiple people (study groups, cofounder search, peer networking). "
    "Returns up to 3 matches maximum, excluding previously found targets.",
)
async def find_multi_matches(
    user_id: str,
    user_profile: Dict[str, Any],
    signal_text: str,
    max_matches: int = 3,
    excluded_user_ids: Optional[List[str]] = None,
    group_name: Optional[str] = None,
) -> ToolResult:
    """Find multiple networking matches for a user.

    This is used for multi-match signals like:
    - Study group formation
    - Cofounder search
    - Peer networking
    - Project collaboration

    Args:
        user_id: The initiator user's ID
        user_profile: The initiator's profile data
        signal_text: The signal/demand text to match against
        max_matches: Maximum number of matches to return (default 3, max 3)
        excluded_user_ids: User IDs to exclude from matching
        group_name: Short name for the group chat (max 30 chars). If not provided,
                    signal_text will be used for group naming.

    Returns:
        ToolResult with list of matches
    """
    # Enforce maximum of 3 matches
    max_matches = min(max_matches, 3)
    # Validate and auto-repair user_id if LLM corrupted it
    validation_error, corrected_user_id = _validate_and_repair_user_id(user_id, user_profile)
    if validation_error:
        logger.warning(f"[NETWORKING] find_multi_matches validation failed: {validation_error}")
        return ToolResult(success=False, error=validation_error)

    # Use the corrected user_id for all operations
    user_id = corrected_user_id

    try:
        from app.agents.execution.networking.utils.adaptive_matcher import (
            AdaptiveMatcher,
        )
        from datetime import datetime, timezone

        db = DatabaseClient()
        openai = AzureOpenAIClient()

        # Automatically exclude users with active/pending connections
        auto_excluded = await _get_users_to_exclude(db, user_id)
        all_excluded = list(set((excluded_user_ids or []) + auto_excluded))

        # Persist the demand ONCE before finding matches (skip if already in history)
        try:
            # Interpret the demand into a clean statement
            interpreted_demand = await _interpret_demand(openai, signal_text, user_profile)
            demand_to_persist = interpreted_demand or signal_text

            # IMPORTANT: Fetch FRESH demand_history from DB to avoid overwriting concurrent updates
            fresh_state = await db.get_demand_value_state(user_id)
            current_demand_history = fresh_state.get("demand_history", [])
            recent_demands = [
                entry.get("text", "").lower().strip()
                for entry in (current_demand_history[-3:] if current_demand_history else [])
            ]
            demand_already_added = demand_to_persist.lower().strip() in recent_demands

            if not demand_already_added:
                updated_demand_history = append_history(
                    current_demand_history,
                    demand_to_persist,
                    created_at=datetime.now(timezone.utc).isoformat()
                )
                await db.update_user_profile(user_id, {"demand_history": updated_demand_history})
                await update_demand_value_derived_fields(
                    db=db,
                    user_id=user_id,
                    demand_history=updated_demand_history,
                )
                logger.info(f"[NETWORKING] find_multi_matches: Persisted demand for user {user_id}: {demand_to_persist[:50]}...")
        except Exception as persist_error:
            logger.warning(f"[NETWORKING] find_multi_matches: Failed to persist demand: {persist_error}")

        # DEBUG: Log what user_profile contains when tool is called
        logger.info(
            f"[NETWORKING] find_multi_matches received user_profile with:\n"
            f"  - seeking_skills: {user_profile.get('seeking_skills')}\n"
            f"  - offering_skills: {user_profile.get('offering_skills')}"
        )

        matcher = AdaptiveMatcher(db=db, openai=openai)
        batch_results = await matcher.find_best_matches(
            user_id=user_id,
            user_profile=user_profile,
            excluded_user_ids=all_excluded,
            override_demand=signal_text,
            select_count=max_matches,
        )

        matches = []
        for result in batch_results:
            if not result.success:
                continue
            match_data = {
                "target_user_id": result.target_user_id,
                "target_name": result.target_name,
                "target_phone": result.target_phone,
                "match_score": result.match_score,
                "match_confidence": result.match_confidence,
                "matching_reasons": result.matching_reasons,
                "llm_introduction": result.llm_introduction,
                "llm_concern": result.llm_concern,
            }
            matches.append(match_data)

        # Location distance: look up all users in Find My and append distances
        try:
            from app.integrations.photon_client import PhotonClient as _PhotonClient
            from app.utils.location_service import (
                get_friend_locations,
                get_distance_between_users,
            )

            initiator_phone = user_profile.get("phone_number", "")
            initiator_email = user_profile.get("email", "")
            if (initiator_phone or initiator_email) and matches:
                _photon = _PhotonClient()
                cached_locations = await get_friend_locations(_photon)
                for match in matches:
                    target_phone = match.get("target_phone", "")
                    target_email = ""
                    target_uid = match.get("target_user_id", "")
                    if target_uid:
                        target_user = await db.get_user_by_id(target_uid)
                        if target_user:
                            target_email = target_user.get("email", "") or ""
                    distance_str = await get_distance_between_users(
                        _photon, initiator_phone or "", target_phone or "",
                        cached_locations=cached_locations,
                        initiator_email=initiator_email or None,
                        target_email=target_email or None,
                        initiator_user_id=user_profile.get("id"),
                        target_user_id=target_uid or None,
                    )
                    if distance_str:
                        match["matching_reasons"].append(distance_str)
        except Exception as loc_err:
            logger.warning(f"[NETWORKING] Location distance lookup failed: {loc_err}")

        if not matches:
            return ToolResult(
                success=False,
                error="No suitable matches found",
            )

        # CRITICAL: Automatically create connection requests for ALL matches
        # This ensures the InteractionAgent has real request_ids to work with
        # Previously, LLM was supposed to call create_connection_request but often skipped it
        from app.agents.execution.networking.utils.value_exchange_matcher import (
            MatchResult,
        )
        from app.agents.execution.networking.utils.handshake_manager import (
            HandshakeManager,
        )
        import uuid

        handshake = HandshakeManager(db=db)
        created_requests = []

        # Generate a single signal_group_id for ALL matches in this multi-match request
        # This links all requests together so the system knows they belong to the same group
        signal_group_id = str(uuid.uuid4())
        multi_match_threshold = 1  # Create group as soon as first target accepts

        logger.info(
            f"[NETWORKING] find_multi_matches: Creating {len(matches)} connection requests "
            f"with signal_group_id={signal_group_id[:8]}..."
        )

        for match in matches:
            try:
                match_result = MatchResult(
                    target_user_id=match["target_user_id"],
                    target_name=match["target_name"],
                    target_phone=match["target_phone"],
                    match_score=match["match_score"],
                    matching_reasons=match["matching_reasons"],
                    llm_introduction=match["llm_introduction"],
                    llm_concern=match.get("llm_concern"),
                )

                request = await handshake.create_request(
                    initiator_id=user_id,
                    match_result=match_result,
                    signal_group_id=signal_group_id,
                    is_multi_match=True,
                    multi_match_threshold=multi_match_threshold,
                    connection_purpose=group_name or signal_text,  # Prefer short group_name for group naming
                )

                request_id = request.get("id")
                match["connection_request_id"] = request_id
                # CRITICAL: Only include request_id and target_name, NOT target_user_id
                # The LLM has confused target_user_id with request_id in the past,
                # causing confirm_and_send_invitation to fail with wrong UUID
                created_requests.append({
                    "request_id": request_id,
                    "target_name": match["target_name"],
                })
                logger.info(
                    f"[NETWORKING] find_multi_matches: Created connection request {request_id} "
                    f"for {match['target_name']} (group={signal_group_id[:8]})"
                )
            except Exception as create_error:
                logger.error(
                    f"[NETWORKING] find_multi_matches: Failed to create request for "
                    f"{match['target_name']}: {create_error}"
                )
                # Continue with other matches even if one fails

        if not created_requests:
            return ToolResult(
                success=False,
                error="Found matches but failed to create connection requests",
            )

        # Fire off discovery conversation generation in the background
        conversation_pending = False
        from app.config import settings as _settings

        if getattr(_settings, "conversation_preview_enabled", False) and matches:
            conversation_pending = True
            phone_number = user_profile.get("phone_number", "")
            # Build match_result with all_matches for multi-participant conversation
            multi_match_result = {
                "target_user_id": matches[0].get("target_user_id", ""),
                "target_name": matches[0].get("target_name", ""),
                "matching_reasons": matches[0].get("matching_reasons", []),
                "connection_purpose": group_name or signal_text or "",
                "all_matches": [
                    {
                        "target_user_id": m.get("target_user_id", ""),
                        "target_name": m.get("target_name", ""),
                        "matching_reasons": m.get("matching_reasons", []),
                    }
                    for m in matches[1:]
                ],
            }
            asyncio.create_task(
                _generate_and_send_conversation_preview(
                    user_id=user_id,
                    user_name=user_profile.get("name", ""),
                    phone_number=phone_number,
                    match_result=multi_match_result,
                    flow_type="reactive",
                    connection_request_id=created_requests[0]["request_id"] if created_requests else None,
                )
            )

        # CRITICAL: Sanitize matches to remove target_user_id before returning
        # The LLM has confused target_user_id with request_id, causing failures.
        # We keep only the fields needed for response synthesis.
        sanitized_matches = []
        for m in matches:
            sanitized_matches.append({
                "target_name": m.get("target_name"),
                "matching_reasons": m.get("matching_reasons", []),
                "llm_introduction": m.get("llm_introduction"),
                "connection_request_id": m.get("connection_request_id"),  # This IS the request_id
                # REMOVED: target_user_id, target_phone - not needed, causes LLM confusion
            })

        return ToolResult(
            success=True,
            data={
                "matches": sanitized_matches,
                "count": len(sanitized_matches),
                "signal_text": signal_text,
                "connection_requests": created_requests,
                "request_ids": [r["request_id"] for r in created_requests],
                "is_multi_match": True,
                "signal_group_id": signal_group_id,
                "conversation_pending": conversation_pending,
            },
        )

    except Exception as e:
        logger.error(f"[NETWORKING] find_multi_matches failed: {e}", exc_info=True)
        return ToolResult(
            success=False,
            error=f"Multi-match search failed: {str(e)}",
        )


@tool(
    name="get_user_connections",
    description="Get the list of past connections for a user. Use this when user asks about their connection history or who they've connected with.",
)
async def get_user_connections(
    user_id: str,
    user_profile: Optional[Dict[str, Any]] = None,
) -> ToolResult:
    """Get list of past connections for a user.

    Args:
        user_id: User's ID
        user_profile: Optional user profile for UUID auto-repair

    Returns:
        ToolResult with list of past connections
    """
    # Validate and auto-repair user_id if LLM corrupted it
    if user_profile:
        validation_error, corrected_user_id = _validate_and_repair_user_id(user_id, user_profile)
        if validation_error:
            logger.warning(f"[NETWORKING] get_user_connections validation failed: {validation_error}")
            return ToolResult(success=False, error=validation_error)
        user_id = corrected_user_id
    else:
        # No user_profile - just validate UUID format
        try:
            UUID(user_id)
        except (ValueError, TypeError):
            logger.warning(f"[NETWORKING] get_user_connections invalid user_id: {user_id}")
            return ToolResult(
                success=False,
                error=f"Invalid user_id format: {user_id}. Please use the user_id from user_profile.",
            )

    try:
        db = DatabaseClient()
        # Get completed connection requests (GROUP_CREATED status) for this user
        connections = await db.get_user_connections(user_id)

        if not connections:
            return ToolResult(
                success=True,
                data={
                    "connections": [],
                    "count": 0,
                },
            )

        return ToolResult(
            success=True,
            data={
                "connections": connections,
                "count": len(connections),
            },
        )

    except Exception as e:
        logger.error(f"[NETWORKING] get_user_connections failed: {e}", exc_info=True)
        return ToolResult(
            success=False,
            error=f"Failed to get connections: {str(e)}",
        )


@tool(
    name="get_connection_info",
    description="Get detailed info about a connection or person. Use when user asks about a specific person, connection status, or pending connections. Returns person info (name, university, major, interests) and connection details (status, matching reasons, purpose, is_multi_match).",
)
async def get_connection_info(
    user_id: str,
    target_name: Optional[str] = None,
    include_pending: bool = True,
) -> ToolResult:
    """Get connection info for a user, optionally filtered by target name.

    Searches across ALL connection statuses (not just completed) to find:
    - Completed connections (GROUP_CREATED)
    - Pending connections where user is initiator (PENDING_INITIATOR_APPROVAL)
    - Pending invitations where user is target (PENDING_TARGET_APPROVAL)
    - Other statuses (TARGET_ACCEPTED, TARGET_DECLINED, CANCELLED, EXPIRED)

    Args:
        user_id: User's ID
        target_name: Optional name to filter by (case-insensitive partial match)
        include_pending: Whether to include pending connections (default True)

    Returns:
        ToolResult with:
        - connections: List of matching connections with full details
        - person_info: Profile info if single person match found
        - query_type: "all" | "specific_person" | "pending_list"
    """
    # Validate user_id format
    try:
        UUID(user_id)
    except (ValueError, TypeError):
        logger.warning(f"[NETWORKING] get_connection_info invalid user_id: {user_id}")
        return ToolResult(success=False, error=f"Invalid user_id format: {user_id}")

    try:
        db = DatabaseClient()
        connections = []
        person_info = None

        logger.info(f"[NETWORKING] get_connection_info: user_id={user_id}, target_name={target_name}, include_pending={include_pending}")

        # Get completed connections
        completed = await db.get_user_connections(user_id)
        logger.info(f"[NETWORKING] get_connection_info: found {len(completed)} completed connections")
        for conn in completed:
            conn["status"] = ConnectionRequestStatus.GROUP_CREATED.value
            connections.append(conn)

        # Get pending connections if requested
        if include_pending:
            # Pending as initiator (waiting for user to confirm match)
            pending_initiator = await db.list_pending_requests_for_initiator(user_id, limit=10)
            logger.info(f"[NETWORKING] get_connection_info: found {len(pending_initiator)} pending_initiator requests")
            for req in pending_initiator:
                logger.info(f"[NETWORKING] get_connection_info: pending_initiator req target_user_id={req.get('target_user_id')}, status={req.get('status')}")
                target_user = await db.get_user_by_id(req.get("target_user_id"))
                target_name_val = target_user.get("name", "Unknown") if target_user else "Unknown"
                connections.append({
                    "connection_id": req.get("id"),
                    "connected_with_id": req.get("target_user_id"),
                    "connected_with_name": target_name_val,
                    "user_role": "initiator",
                    "match_score": req.get("match_score"),
                    "matching_reasons": req.get("matching_reasons", []),
                    "connection_purpose": req.get("connection_purpose"),
                    "is_multi_match": req.get("is_multi_match", False),
                    "status": req.get("status"),
                    "created_at": req.get("created_at"),
                    "updated_at": req.get("updated_at"),
                })

            # Connections where user is initiator and waiting for target to respond
            awaiting_target = await db.list_requests_awaiting_target_response(user_id, limit=10)
            for req in awaiting_target:
                target_user = await db.get_user_by_id(req.get("target_user_id"))
                target_name_val = target_user.get("name", "Unknown") if target_user else "Unknown"
                connections.append({
                    "connection_id": req.get("id"),
                    "connected_with_id": req.get("target_user_id"),
                    "connected_with_name": target_name_val,
                    "user_role": "initiator",
                    "match_score": req.get("match_score"),
                    "matching_reasons": req.get("matching_reasons", []),
                    "connection_purpose": req.get("connection_purpose"),
                    "is_multi_match": req.get("is_multi_match", False),
                    "status": req.get("status"),
                    "created_at": req.get("created_at"),
                    "updated_at": req.get("updated_at"),
                })

            # Pending as target (waiting for user to accept/decline invitation)
            pending_target = await db.list_pending_requests_for_target(user_id, limit=10)
            for req in pending_target:
                initiator_user = await db.get_user_by_id(req.get("initiator_user_id"))
                initiator_name = initiator_user.get("name", "Unknown") if initiator_user else "Unknown"
                connections.append({
                    "connection_id": req.get("id"),
                    "connected_with_id": req.get("initiator_user_id"),
                    "connected_with_name": initiator_name,
                    "user_role": "target",
                    "match_score": req.get("match_score"),
                    "matching_reasons": req.get("matching_reasons", []),
                    "connection_purpose": req.get("connection_purpose"),
                    "is_multi_match": req.get("is_multi_match", False),
                    "status": req.get("status"),
                    "created_at": req.get("created_at"),
                    "updated_at": req.get("updated_at"),
                })

        # Filter by target_name if provided
        query_type = "all"
        logger.info(f"[NETWORKING] get_connection_info: total connections before filter = {len(connections)}")
        for c in connections:
            logger.info(f"[NETWORKING] get_connection_info: connection name='{c.get('connected_with_name')}', status={c.get('status')}")
        if target_name:
            query_type = "specific_person"
            target_name_lower = target_name.lower()
            filtered = [
                c for c in connections
                if target_name_lower in c.get("connected_with_name", "").lower()
            ]
            logger.info(f"[NETWORKING] get_connection_info: filtered for '{target_name}' -> {len(filtered)} results")
            connections = filtered

            # If we found exactly one person, get their profile info
            if len(connections) == 1:
                other_user_id = connections[0].get("connected_with_id")
                if other_user_id:
                    other_user = await db.get_user_by_id(other_user_id)
                    if other_user:
                        # Return only disclosable info
                        person_info = {
                            "name": other_user.get("name"),
                            "university": other_user.get("university"),
                            "major": other_user.get("major"),
                            "career_interests": other_user.get("career_interests", []),
                        }
            elif len(connections) > 1:
                # Multiple matches - get profile info for each
                for conn in connections:
                    other_user_id = conn.get("connected_with_id")
                    if other_user_id:
                        other_user = await db.get_user_by_id(other_user_id)
                        if other_user:
                            conn["person_info"] = {
                                "name": other_user.get("name"),
                                "university": other_user.get("university"),
                                "major": other_user.get("major"),
                                "career_interests": other_user.get("career_interests", []),
                            }

        if not connections:
            message = "No connections found"
            if target_name:
                message = f"No connections found with name matching '{target_name}'"
            return ToolResult(
                success=True,
                data={
                    "query_type": query_type,
                    "connections": [],
                    "count": 0,
                    "message": message,
                },
            )

        return ToolResult(
            success=True,
            data={
                "query_type": query_type,
                "connections": connections,
                "count": len(connections),
                "person_info": person_info,
            },
        )

    except Exception as e:
        logger.error(f"[NETWORKING] get_connection_info failed: {e}", exc_info=True)
        return ToolResult(
            success=False,
            error=f"Failed to get connection info: {str(e)}",
        )
