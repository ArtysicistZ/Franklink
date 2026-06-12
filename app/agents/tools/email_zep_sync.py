"""Email to Zep graph synchronization utilities.

Provides functions to sync emails from Supabase to Zep's knowledge graph
for semantic search and context retrieval.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.config import settings

logger = logging.getLogger(__name__)

# Patterns indicating highly sensitive content that should NOT be synced to Zep
# Only filter out emails with PII, financial account details, or medical records
# REDUCED SCOPE: Previously was too aggressive and blocked useful professional content
SENSITIVE_SENDER_PATTERNS = [
    # Financial institutions with account details (keep minimal)
    "noreply@chase", "alerts@bankofamerica", "noreply@wellsfargo",
    "noreply@capitalone", "alerts@citi",
    # Medical portals with health records
    "mychart", "myhealth", "labcorp", "questdiagnostics",
    # Tax/Government with PII
    "irs.gov", "ssa.gov", "turbotax", "hrblock",
    # Credit bureaus
    "equifax", "experian", "transunion",
]

SENSITIVE_SUBJECT_KEYWORDS = [
    # Financial statements with account numbers
    "account statement", "bank statement", "credit score", "credit report",
    "wire transfer", "tax return", "w-2", "1099",
    # Medical records
    "test results", "lab results", "medical record", "health record",
    "diagnosis", "treatment plan", "explanation of benefits",
    # Security/Authentication (these are useless for context anyway)
    "password reset", "verify your identity", "security alert",
    "verification code", "2fa code", "one-time password",
]

SENSITIVE_BODY_KEYWORDS = [
    # Financial identifiers (actual account numbers)
    "account number", "routing number", "ssn", "social security number",
    "card ending in", "last 4 digits",
    # Medical specifics
    "blood test result", "biopsy result", "medical diagnosis",
    # Personal identifiers
    "driver's license number", "passport number",
]


def _contains_sensitive_content(subject: str, body: str) -> bool:
    """
    Check if email contains sensitive content that should not be synced to Zep.

    This filters out:
    - Financial/banking emails (statements, transactions, tax docs)
    - Medical/healthcare emails (test results, prescriptions, appointments)
    - Security-related emails (password resets, verification codes)

    Args:
        subject: Email subject line
        body: Email body content

    Returns:
        True if email contains sensitive content and should be skipped
    """
    subject_lower = (subject or "").lower()
    body_lower = (body or "").lower()
    combined = f"{subject_lower} {body_lower}"

    # Check subject for sensitive keywords
    for keyword in SENSITIVE_SUBJECT_KEYWORDS:
        if keyword in subject_lower:
            return True

    # Check body for sensitive keywords
    for keyword in SENSITIVE_BODY_KEYWORDS:
        if keyword in body_lower:
            return True

    return False


def chunk_emails_for_zep(
    emails: List[Dict[str, Any]],
    max_chars: Optional[int] = None,
) -> List[List[Dict[str, Any]]]:
    """
    Chunk emails to respect Zep's 10,000 character limit per graph.add call.

    Args:
        emails: List of email dictionaries
        max_chars: Maximum characters per chunk (default from settings)

    Returns:
        List of email chunks, each within the character limit
    """
    if not emails:
        return []

    max_chars = max_chars or settings.zep_graph_chunk_size

    chunks: List[List[Dict[str, Any]]] = []
    current_chunk: List[Dict[str, Any]] = []
    current_size = 0

    for email in emails:
        email_text = format_single_email(email)
        email_size = len(email_text)

        if email_size > max_chars:
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = []
                current_size = 0

            truncated = _truncate_email_for_size(email, max_chars - 100)
            chunks.append([truncated])
            continue

        if current_size + email_size > max_chars:
            if current_chunk:
                chunks.append(current_chunk)
            current_chunk = [email]
            current_size = email_size
        else:
            current_chunk.append(email)
            current_size += email_size

    if current_chunk:
        chunks.append(current_chunk)

    return chunks


def format_single_email(email: Dict[str, Any]) -> str:
    """
    Format a single email for Zep graph ingestion.

    Optimized for Zep's fact extraction:
    - Clear structure with labeled fields
    - PII already scrubbed (from build_email_signals)
    - Temporal markers for recency
    - Event dates extracted from content for time-sensitive retrieval

    Args:
        email: Email dictionary with sender, subject, body, etc.

    Returns:
        Formatted email string
    """
    from app.utils.event_date_extractor import extract_event_dates, format_event_dates_for_zep

    sender = email.get("sender") or email.get("sender_domain") or "unknown"
    subject = email.get("subject") or "(no subject)"
    body = email.get("body") or email.get("body_excerpt") or email.get("snippet") or ""
    received_at = email.get("received_at") or ""
    # Support both is_sent (user_emails) and is_from_me (user_email_highlights)
    is_sent = email.get("is_sent", False) or email.get("is_from_me", False)

    # Filter out sensitive content (medical/financial info) before syncing
    if _contains_sensitive_content(subject, body):
        return ""  # Skip this email entirely

    # Reduced from 500 to 250 chars for faster Zep processing
    # Most networking-relevant info is in subject + first 250 chars
    if len(body) > 250:
        body = body[:247] + "..."

    direction = "Sent" if is_sent else "Received"

    # Parse received_at for both display and as reference date for relative dates
    date_str = ""
    reference_date = None
    if received_at:
        try:
            if isinstance(received_at, str):
                dt = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
                reference_date = dt.replace(tzinfo=None)
            elif isinstance(received_at, datetime):
                date_str = received_at.strftime("%Y-%m-%d")
                reference_date = received_at
        except (ValueError, TypeError):
            date_str = str(received_at)[:10] if received_at else ""

    # Extract event dates from subject + body for time-sensitive context
    combined_text = f"{subject} {body}"
    event_dates = []
    try:
        event_dates = extract_event_dates(combined_text, reference_date=reference_date)
    except Exception as e:
        logger.debug(f"Event date extraction failed: {e}")

    event_annotation = format_event_dates_for_zep(event_dates) if event_dates else ""

    lines = [
        f"Email ({direction}) from {sender}" + (f" on {date_str}" if date_str else "") + ":",
        f"Subject: {subject}",
    ]
    if body.strip():
        lines.append(f"Content: {body}")

    # Add event dates as structured annotation for Zep
    if event_annotation:
        lines.append(f"Event Dates Mentioned: {event_annotation}")

    lines.append("---")

    return "\n".join(lines)


def format_emails_for_graph(emails: List[Dict[str, Any]]) -> str:
    """
    Format multiple emails as text for Zep graph ingestion.

    Args:
        emails: List of email dictionaries

    Returns:
        Combined formatted email text (sensitive emails are filtered out)
    """
    if not emails:
        return ""

    # Filter out empty strings (sensitive emails that were skipped)
    formatted = [format_single_email(email) for email in emails]
    formatted = [f for f in formatted if f]  # Remove empty strings
    return "\n".join(formatted)


def _truncate_email_for_size(
    email: Dict[str, Any],
    max_chars: int,
) -> Dict[str, Any]:
    """
    Create a truncated copy of an email to fit within size limit.

    Args:
        email: Original email dictionary
        max_chars: Maximum characters for the formatted output

    Returns:
        Truncated email dictionary
    """
    truncated = {
        "sender": email.get("sender") or email.get("sender_domain") or "unknown",
        "subject": email.get("subject") or "(no subject)",
        "received_at": email.get("received_at"),
        "is_sent": email.get("is_sent", False),
    }

    header_size = len(format_single_email({**truncated, "body": ""}))
    available_for_body = max_chars - header_size - 50

    body = email.get("body") or email.get("snippet") or ""
    if len(body) > available_for_body:
        truncated["body"] = body[:available_for_body - 3] + "..."
    else:
        truncated["body"] = body

    return truncated


async def _sync_email_data_to_zep(
    user_id: str,
    emails: List[Dict[str, Any]],
    max_concurrent: int = 3,
    wait_for_processing: bool = False,
    processing_timeout: int = 60,
) -> Dict[str, Any]:
    """
    Internal helper to sync email data to a user's Zep knowledge graph.

    This is used by sync_unsynced_highlights_to_zep to sync highlight emails.
    Only highlight emails (curated, important emails) should be synced to Zep.

    Args:
        user_id: User identifier
        emails: List of email/highlight dictionaries to sync (must have 'id' field)
        max_concurrent: Maximum concurrent API calls
        wait_for_processing: If True, wait for Zep to finish processing episodes
        processing_timeout: Max seconds to wait for processing (if wait_for_processing=True)

    Returns:
        Dict with sync results:
        - success: bool
        - emails_synced: int
        - chunks_sent: int
        - errors: List[str]
        - synced_email_ids: List[str]
        - episode_ids: List[str] (episode UUIDs for tracking processing status)
    """
    from app.integrations.zep_graph_client import get_zep_graph_client

    result = {
        "success": False,
        "emails_synced": 0,
        "chunks_sent": 0,
        "errors": [],
        "synced_email_ids": [],
        "episode_ids": [],
    }

    if not emails:
        result["success"] = True
        return result

    if not settings.zep_graph_enabled or not settings.zep_graph_sync_emails:
        logger.debug(f"Zep graph sync disabled, skipping {len(emails)} emails")
        result["success"] = True
        return result

    zep = get_zep_graph_client()
    if not zep.is_graph_enabled():
        logger.debug("Zep graph client not available, skipping sync")
        result["success"] = True
        return result

    chunks = chunk_emails_for_zep(emails)
    if not chunks:
        result["success"] = True
        return result

    logger.info(
        f"[ZEP_SYNC] Starting sync user={user_id[:8]}... "
        f"emails={len(emails)} chunks={len(chunks)}"
    )

    semaphore = asyncio.Semaphore(max_concurrent)
    synced_email_ids: List[str] = []
    episode_ids: List[str] = []

    async def sync_chunk(chunk: List[Dict[str, Any]], chunk_idx: int) -> Optional[str]:
        """Sync a chunk and return episode_id if successful."""
        async with semaphore:
            try:
                text = format_emails_for_graph(chunk)
                add_result = await zep.add_to_graph(
                    user_id=user_id,
                    data=text,
                    data_type="text",
                )
                if add_result.success:
                    # Collect email IDs from this successful chunk
                    for email in chunk:
                        email_id = email.get("id")
                        if email_id:
                            synced_email_ids.append(email_id)
                    return add_result.episode_id
                else:
                    result["errors"].append(
                        f"Chunk {chunk_idx}: {add_result.error}"
                    )
                    return None
            except Exception as e:
                result["errors"].append(f"Chunk {chunk_idx}: {str(e)}")
                return None

    tasks = [sync_chunk(chunk, i) for i, chunk in enumerate(chunks)]
    chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

    # Collect successful episode IDs
    for res in chunk_results:
        if isinstance(res, str) and res:
            episode_ids.append(res)

    # Count successful chunks - sync_chunk returns episode_id (str) on success, None on failure
    successful_chunks = sum(
        1 for r in chunk_results
        if isinstance(r, str)
    )
    result["chunks_sent"] = successful_chunks

    emails_in_successful = sum(
        len(chunks[i])
        for i, r in enumerate(chunk_results)
        if isinstance(r, str)
    )
    result["emails_synced"] = emails_in_successful
    result["synced_email_ids"] = synced_email_ids
    result["episode_ids"] = episode_ids
    result["success"] = successful_chunks > 0

    logger.info(
        f"[ZEP_SYNC] Completed user={user_id[:8]}... "
        f"synced={result['emails_synced']}/{len(emails)} "
        f"chunks={successful_chunks}/{len(chunks)} "
        f"episodes={len(episode_ids)} "
        f"errors={len(result['errors'])}"
    )

    # Optionally wait for episode processing
    if wait_for_processing and episode_ids:
        processed = await wait_for_episodes_processed(
            episode_ids=episode_ids,
            timeout_seconds=processing_timeout,
        )
        result["episodes_processed"] = processed
        if not processed:
            logger.warning(
                f"[ZEP_SYNC] Some episodes still processing for user={user_id[:8]}..."
            )

    return result


async def wait_for_episodes_processed(
    episode_ids: List[str],
    timeout_seconds: int = 60,
    poll_interval: float = 2.0,
) -> bool:
    """
    Wait for Zep episodes to finish processing.

    Zep processes data asynchronously. This function polls episode status
    until all episodes are processed or timeout is reached.

    Args:
        episode_ids: List of episode UUIDs to check
        timeout_seconds: Maximum seconds to wait
        poll_interval: Seconds between polls

    Returns:
        True if all episodes processed, False if timeout reached
    """
    import time

    if not episode_ids:
        return True

    from app.integrations.zep_graph_client import get_zep_graph_client

    zep = get_zep_graph_client()
    if not zep.is_graph_enabled():
        return True

    pending = set(episode_ids)
    start_time = time.monotonic()

    async def check_episode(episode_id: str) -> tuple:
        """Check if an episode is processed. Returns (episode_id, is_processed, is_not_found)."""
        try:
            episode = await zep.get_episode(episode_id)
            if episode is None:
                # Episode not found - don't keep polling for it
                logger.debug(f"[ZEP_SYNC] Episode {episode_id[:8]}... not found, skipping")
                return (episode_id, False, True)
            is_processed = episode.get("processed", False)
            if is_processed:
                logger.debug(f"[ZEP_SYNC] Episode {episode_id[:8]}... processed")
            return (episode_id, is_processed, False)
        except Exception as e:
            logger.debug(f"[ZEP_SYNC] Error checking episode {episode_id[:8]}...: {e}")
            return (episode_id, False, False)

    while pending:
        elapsed = time.monotonic() - start_time
        if elapsed >= timeout_seconds:
            logger.warning(
                f"[ZEP_SYNC] Timeout waiting for {len(pending)} episodes to process"
            )
            return False

        # Check all pending episodes in parallel
        results = await asyncio.gather(*[check_episode(ep_id) for ep_id in pending])

        # Update pending set - remove processed and not-found episodes
        pending = {
            ep_id for ep_id, is_processed, is_not_found in results
            if not is_processed and not is_not_found
        }

        if pending:
            await asyncio.sleep(poll_interval)

    logger.info(f"[ZEP_SYNC] All {len(episode_ids)} episodes processed")
    return True


async def sync_unsynced_highlights_to_zep(
    user_id: str,
    max_highlights: int = 500,
    max_concurrent: int = 3,
    wait_for_processing: bool = False,
    processing_timeout: int = 60,
) -> Dict[str, Any]:
    """
    Sync only unsynced email highlights to Zep for a user (incremental sync).

    This queries the user_email_highlights table for entries where zep_synced_at IS NULL,
    syncs them to Zep, and marks them as synced.

    Highlights are pre-filtered important emails (no ads/promotions) so this
    produces higher quality context for Zep's knowledge graph.

    Args:
        user_id: User identifier
        max_highlights: Maximum highlights to sync in one call
        max_concurrent: Maximum concurrent API calls
        wait_for_processing: If True, wait for Zep to finish processing episodes
        processing_timeout: Max seconds to wait for processing (if wait_for_processing=True)

    Returns:
        Dict with sync results including highlights_found and highlights_synced
    """
    from app.database.client import DatabaseClient

    result = {
        "success": False,
        "highlights_found": 0,
        "highlights_synced": 0,
        "chunks_sent": 0,
        "errors": [],
        "profile_refresh_scheduled": False,
    }

    if not settings.zep_graph_enabled or not settings.zep_graph_sync_emails:
        logger.debug(f"[ZEP_SYNC] Zep graph sync disabled for user={user_id[:8]}...")
        result["success"] = True
        return result

    try:
        db = DatabaseClient()
        unsynced_highlights = await db.get_unsynced_highlights_for_zep(
            user_id=user_id,
            limit=max_highlights,
        )

        result["highlights_found"] = len(unsynced_highlights)

        if not unsynced_highlights:
            logger.debug(f"[ZEP_SYNC] No unsynced highlights for user={user_id[:8]}...")
            result["success"] = True
            return result

        logger.info(
            f"[ZEP_SYNC] Found {len(unsynced_highlights)} unsynced highlights for user={user_id[:8]}..."
        )

        # Sync highlights to Zep (format_single_email handles both emails and highlights)
        sync_result = await _sync_email_data_to_zep(
            user_id=user_id,
            emails=unsynced_highlights,
            max_concurrent=max_concurrent,
            wait_for_processing=wait_for_processing,
            processing_timeout=processing_timeout,
        )

        # Mark successfully synced highlights
        if sync_result.get("synced_email_ids"):
            marked = await db.mark_highlights_zep_synced(sync_result["synced_email_ids"])
            logger.info(
                f"[ZEP_SYNC] Marked {marked} highlights as synced for user={user_id[:8]}..."
            )

        result["success"] = sync_result["success"]
        result["highlights_synced"] = sync_result["emails_synced"]
        result["chunks_sent"] = sync_result["chunks_sent"]
        result["errors"] = sync_result["errors"]

        # Trigger non-blocking profile+graph refresh after new Zep ingestion.
        # This keeps graph-first matching fresher without adding request latency.
        if result["highlights_synced"] > 0:
            try:
                from app.jobs.user_profile_synthesis import schedule_profile_refresh_after_zep_sync

                # If we already waited for Zep processing, refresh immediately.
                # Otherwise use the configured delay to allow async processing.
                should_refresh_now = bool(sync_result.get("episodes_processed", False))
                refresh_delay = 0.0 if should_refresh_now else float(
                    getattr(settings, "profile_synthesis_refresh_delay_seconds", 20.0)
                )

                scheduled = schedule_profile_refresh_after_zep_sync(
                    user_id=user_id,
                    delay_seconds=refresh_delay,
                    force=True,
                )
                result["profile_refresh_scheduled"] = scheduled
            except Exception as refresh_error:
                logger.warning(
                    "[ZEP_SYNC] Failed to schedule profile refresh user=%s err=%s",
                    user_id[:8] if user_id else "unknown",
                    str(refresh_error),
                )

        return result

    except Exception as e:
        logger.error(
            f"[ZEP_SYNC] Error in highlight sync for user={user_id[:8]}...: {e}",
            exc_info=True,
        )
        result["errors"].append(str(e))
        return result


async def sync_profile_to_zep(
    user_id: str,
    profile: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Sync user profile data to their Zep knowledge graph.

    This should be called when user profile is updated to keep
    the graph in sync with the latest user information.

    Args:
        user_id: User identifier
        profile: User profile dictionary

    Returns:
        Dict with sync results
    """
    from app.integrations.zep_graph_client import get_zep_graph_client

    result = {
        "success": False,
        "error": None,
    }

    if not settings.zep_graph_enabled:
        result["success"] = True
        return result

    zep = get_zep_graph_client()
    if not zep.is_graph_enabled():
        result["success"] = True
        return result

    profile_text = format_profile_for_graph(profile)
    if not profile_text:
        result["success"] = True
        return result

    try:
        add_result = await zep.add_to_graph(
            user_id=user_id,
            data=profile_text,
            data_type="text",
        )
        result["success"] = add_result.success
        if not add_result.success:
            result["error"] = add_result.error

    except Exception as e:
        result["error"] = str(e)

    return result


def format_profile_for_graph(profile: Dict[str, Any]) -> str:
    """
    Format user profile for Zep graph ingestion.

    Args:
        profile: User profile dictionary

    Returns:
        Formatted profile text
    """
    lines = ["User Profile:"]

    name = profile.get("name")
    if name:
        lines.append(f"Name: {name}")

    university = profile.get("university")
    if university:
        lines.append(f"University: {university}")

    major = profile.get("major")
    if major:
        lines.append(f"Major: {major}")

    year = profile.get("year")
    if year:
        lines.append(f"Year: {year}")

    location = profile.get("location")
    if location:
        lines.append(f"Location: {location}")

    career_interests = profile.get("career_interests") or []
    if career_interests:
        lines.append(f"Career interests: {', '.join(career_interests)}")

    all_demand = profile.get("all_demand")
    if all_demand:
        lines.append(f"What they're seeking: {all_demand}")

    all_value = profile.get("all_value")
    if all_value:
        lines.append(f"What they offer: {all_value}")

    if len(lines) <= 1:
        return ""

    lines.append("---")
    return "\n".join(lines)
