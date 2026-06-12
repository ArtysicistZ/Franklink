"""Proactive outreach service.

Analyzes email-derived signals and suggests networking matches.
Runs daily at 6 PM UTC.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.config import settings
from app.context import set_llm_context, clear_llm_context
from app.database.client import DatabaseClient
from app.integrations.azure_openai_client import AzureOpenAIClient
from app.proactive.config import (
    PROACTIVE_OUTREACH_COOLDOWN_DAYS,
    PROACTIVE_OUTREACH_MAX_SIGNALS,
    PROACTIVE_OUTREACH_WORKER_MAX_ATTEMPTS,
    PROACTIVE_OUTREACH_WORKER_STALE_MINUTES,
    PROACTIVE_MULTI_MATCH_THRESHOLD,
    PROACTIVE_MULTI_MATCH_MAX_TARGETS,
    compute_backoff_seconds,
)
from app.proactive.outreach.duplicate_checker import check_duplicate_outreach
from app.proactive.outreach.message_generator import (
    build_email_context_summary,
    generate_proactive_suggestion_message,
)

logger = logging.getLogger(__name__)


def _clip(s: str, max_len: int = 40) -> str:
    """Truncate string for logging."""
    return s[:max_len] if len(s) > max_len else s


@dataclass
class ProactiveOutreachService:
    """Service for proactive outreach jobs."""

    db: DatabaseClient
    worker_id: str
    openai: Optional[AzureOpenAIClient] = None

    async def run_once(self, *, max_jobs: int) -> int:
        """
        Process a batch of proactive outreach jobs.

        Returns:
            Number of jobs processed
        """
        if not getattr(settings, "proactive_outreach_worker_enabled", False):
            logger.info("[PROACTIVE_OUTREACH] disabled")
            return 0

        # Claim available jobs
        jobs = await self.db.claim_proactive_outreach_jobs_v1(
            worker_id=self.worker_id,
            max_jobs=max_jobs,
            stale_minutes=PROACTIVE_OUTREACH_WORKER_STALE_MINUTES,
        )

        if not jobs:
            logger.debug(
                "[PROACTIVE_OUTREACH] idle worker=%s max_jobs=%d",
                self.worker_id,
                max_jobs,
            )
            return 0

        logger.info(
            "[PROACTIVE_OUTREACH] claimed worker=%s count=%d",
            self.worker_id,
            len(jobs),
        )

        processed = 0
        for job in jobs:
            processed += 1
            user_id = str((job or {}).get("user_id") or "").strip()
            attempts = int((job or {}).get("attempts") or 0)

            try:
                # Set LLM context for usage tracking
                set_llm_context(user_id=user_id, job_type="proactive_outreach")
                await self._process_job(job)
            except Exception as e:
                logger.error(
                    "[PROACTIVE_OUTREACH] job_crash user_id=%s err=%s",
                    _clip(user_id),
                    str(e),
                    exc_info=True,
                )
                if user_id:
                    try:
                        await self._fail_job(
                            user_id=user_id,
                            attempts=attempts,
                            error=f"job_crash:{type(e).__name__}:{e}",
                        )
                    except Exception:
                        await self.db.release_proactive_outreach_job_v1(
                            user_id=user_id,
                            worker_id=self.worker_id,
                        )
            finally:
                clear_llm_context()

        return processed

    async def _process_job(self, job: Dict[str, Any]) -> None:
        """Process a single proactive outreach job."""
        user_id = str(job.get("user_id") or "").strip()
        if not user_id:
            return

        attempts = int(job.get("attempts") or 0)

        logger.info(
            "[PROACTIVE_OUTREACH] job_start user_id=%s attempts=%d",
            _clip(user_id),
            attempts,
        )

        # Get user and check preconditions
        user = await self.db.get_user_by_id(user_id)
        if not user:
            logger.warning("[PROACTIVE_OUTREACH] user_not_found user_id=%s", _clip(user_id))
            await self._skip_job(user_id=user_id, reason="user_not_found")
            return

        # Check if user is onboarded
        if not user.get("is_onboarded"):
            logger.info("[PROACTIVE_OUTREACH] user_not_onboarded user_id=%s", _clip(user_id))
            await self._skip_job(user_id=user_id, reason="not_onboarded")
            return

        # Check email connection status
        personal_facts = user.get("personal_facts") or {}
        email_connect = personal_facts.get("email_connect") or {}
        if email_connect.get("status") != "connected":
            logger.info("[PROACTIVE_OUTREACH] email_not_connected user_id=%s", _clip(user_id))
            await self._skip_job(user_id=user_id, reason="email_not_connected")
            return

        # Check proactive preference (opt-out)
        if not user.get("proactive_preference", True):
            logger.info("[PROACTIVE_OUTREACH] user_opted_out user_id=%s", _clip(user_id))
            await self._skip_job(user_id=user_id, reason="user_opted_out")
            return

        # Step 1: Get connection purpose suggestions from Zep (shared with networking task)
        # This uses the same logic as suggest_connection_purposes tool
        from app.agents.tools.networking import (
            _get_connection_purpose_suggestions,
            rank_purposes_for_proactive,
            find_match,
        )

        suggestions_result = await _get_connection_purpose_suggestions(
            user_id=user_id,
            user_profile=user,
            max_suggestions=PROACTIVE_OUTREACH_MAX_SIGNALS,
            skip_deduplication=True,  # We handle deduplication separately for proactive
        )

        suggestions = suggestions_result.get("suggestions", [])
        if not suggestions:
            logger.info(
                "[PROACTIVE_OUTREACH] no_suggestions user_id=%s reason=%s",
                _clip(user_id),
                suggestions_result.get("skip_reason", "no_zep_data"),
            )
            await self._skip_job(user_id=user_id, reason="no_suggestions")
            return

        logger.info(
            "[PROACTIVE_OUTREACH] got_suggestions user_id=%s count=%d",
            _clip(user_id),
            len(suggestions),
        )

        # Step 2: Get recent outreach purposes for deduplication
        recent_outreach = await self.db.get_recent_proactive_outreach_purposes(
            user_id=user_id,
            days=PROACTIVE_OUTREACH_COOLDOWN_DAYS,
        )
        recent_outreach_purposes = [r.get("signal_text", "") for r in recent_outreach if r.get("signal_text")]

        # Step 3: Rank all purposes by priority (single LLM call)
        ranked_purposes = await rank_purposes_for_proactive(
            suggestions=suggestions,
            user_profile=user,
            recent_outreach_purposes=recent_outreach_purposes,
        )

        if not ranked_purposes:
            logger.info("[PROACTIVE_OUTREACH] no_ranked_purposes user_id=%s", _clip(user_id))
            await self._skip_job(user_id=user_id, reason="no_suitable_purpose")
            return

        logger.info(
            "[PROACTIVE_OUTREACH] ranked_purposes user_id=%s count=%d",
            _clip(user_id),
            len(ranked_purposes),
        )

        # Step 3.5: Save ranked opportunities to database for tracking/reuse
        try:
            batch_id = await self.db.insert_networking_opportunities_batch(
                user_id=user_id,
                source="proactive",
                opportunities=ranked_purposes,
            )
            if batch_id:
                logger.info(
                    "[PROACTIVE_OUTREACH] saved_opportunities user_id=%s batch_id=%s count=%d",
                    _clip(user_id),
                    _clip(batch_id),
                    len(ranked_purposes),
                )
        except Exception as e:
            logger.warning(
                "[PROACTIVE_OUTREACH] save_opportunities_failed user_id=%s error=%s",
                _clip(user_id),
                str(e),
            )
            # Don't fail job - this is tracking only

        # Step 4-5: Try each ranked purpose until we find a match
        match_found = False
        used_signal = None
        match_result = None
        tried_count = 0

        for ranked_signal in ranked_purposes:
            tried_count += 1
            signal_text = ranked_signal.get("signal_text") or ranked_signal.get("purpose", "")
            match_type = ranked_signal.get("match_type", "single")

            logger.info(
                "[PROACTIVE_OUTREACH] trying_purpose user_id=%s rank=%d match_type=%s purpose=%s",
                _clip(user_id),
                ranked_signal.get("rank", tried_count),
                match_type,
                _clip(signal_text, 50),
            )

            # Check for duplicate outreach (signal-level)
            is_duplicate = await check_duplicate_outreach(
                self.db,
                user_id=user_id,
                signal_text=signal_text,
                cooldown_days=PROACTIVE_OUTREACH_COOLDOWN_DAYS,
            )
            if is_duplicate:
                logger.info(
                    "[PROACTIVE_OUTREACH] duplicate_signal user_id=%s signal=%s, trying next",
                    _clip(user_id),
                    _clip(signal_text, 50),
                )
                continue  # Try next ranked purpose

            # Try to find match(es) based on match_type
            if match_type == "multi":
                # Multi-match: find multiple people
                matches = await self._find_multi_matches(
                    user_id=user_id,
                    user_profile=user,
                    signal_text=signal_text,
                    max_matches=ranked_signal.get("max_matches", PROACTIVE_MULTI_MATCH_MAX_TARGETS),
                )
                if matches:
                    match_found = True
                    match_result = matches[0]  # Primary match for message generation
                    used_signal = ranked_signal
                    used_signal["all_matches"] = matches
                    break
                else:
                    logger.info(
                        "[PROACTIVE_OUTREACH] no_multi_match user_id=%s signal=%s, trying next",
                        _clip(user_id),
                        _clip(signal_text, 50),
                    )
            else:
                # Single match: find one best person
                result = await find_match(
                    user_id=user_id,
                    user_profile=user,
                    override_demand=signal_text,
                )

                if result.success:
                    # Check if target was recently suggested
                    target_user_id = result.data.get("target_user_id")
                    if target_user_id:
                        is_target_duplicate = await check_duplicate_outreach(
                            self.db,
                            user_id=user_id,
                            signal_text=signal_text,
                            target_user_id=target_user_id,
                            cooldown_days=PROACTIVE_OUTREACH_COOLDOWN_DAYS,
                        )
                        if is_target_duplicate:
                            logger.info(
                                "[PROACTIVE_OUTREACH] duplicate_target user_id=%s target=%s, trying next",
                                _clip(user_id),
                                _clip(target_user_id),
                            )
                        else:
                            match_found = True
                            match_result = result.data
                            used_signal = ranked_signal
                            break
                else:
                    logger.info(
                        "[PROACTIVE_OUTREACH] no_match user_id=%s signal=%s, trying next",
                        _clip(user_id),
                        _clip(signal_text, 50),
                    )

        if not match_found:
            logger.info(
                "[PROACTIVE_OUTREACH] no_match_found user_id=%s tried=%d purposes",
                _clip(user_id),
                tried_count,
            )
            await self._skip_job(user_id=user_id, reason="no_match")
            return

        logger.info(
            "[PROACTIVE_OUTREACH] match_found user_id=%s rank=%d purpose=%s",
            _clip(user_id),
            used_signal.get("rank", 0) if used_signal else 0,
            _clip(used_signal.get("signal_text", "")[:50] if used_signal else "?"),
        )

        # Step 6: Create connection request and send message
        await self._create_outreach(
            user_id=user_id,
            user=user,
            signal=used_signal,
            match_result=match_result,
        )

    async def _find_multi_matches(
        self,
        *,
        user_id: str,
        user_profile: Dict[str, Any],
        signal_text: str,
        max_matches: int = 5,
    ) -> List[Dict[str, Any]]:
        """Find multiple matches for a multi-person signal."""
        from app.agents.tools.networking import find_match

        matches = []
        excluded_ids = []

        for _ in range(min(max_matches, PROACTIVE_MULTI_MATCH_MAX_TARGETS)):
            result = await find_match(
                user_id=user_id,
                user_profile=user_profile,
                override_demand=signal_text,
                excluded_user_ids=excluded_ids,
            )

            if not result.success:
                break

            target_id = result.data.get("target_user_id")
            if target_id:
                # Check if target was recently suggested
                is_dup = await check_duplicate_outreach(
                    self.db,
                    user_id=user_id,
                    signal_text=signal_text,
                    target_user_id=target_id,
                    cooldown_days=PROACTIVE_OUTREACH_COOLDOWN_DAYS,
                )
                if is_dup:
                    excluded_ids.append(target_id)
                    continue

                matches.append(result.data)
                excluded_ids.append(target_id)

        logger.info(
            "[PROACTIVE_OUTREACH] multi_match_found user_id=%s count=%d",
            _clip(user_id),
            len(matches),
        )

        return matches

    async def _create_outreach(
        self,
        *,
        user_id: str,
        user: Dict[str, Any],
        signal: Dict[str, Any],
        match_result: Dict[str, Any],
    ) -> None:
        """Create connection request and send proactive message."""
        from app.agents.tools.networking import create_connection_request
        from app.integrations.photon_client import PhotonClient
        import uuid

        target_user_id = match_result.get("target_user_id")
        target_name = match_result.get("target_name")
        target_phone = match_result.get("target_phone")
        match_type = signal.get("match_type", "single")
        all_matches = signal.get("all_matches", [match_result])

        logger.info(
            "[PROACTIVE_OUTREACH] creating_outreach user_id=%s target=%s match_type=%s",
            _clip(user_id),
            _clip(target_name or target_user_id or "?"),
            match_type,
        )

        # Generate signal_group_id for multi-match
        signal_group_id = str(uuid.uuid4()) if match_type == "multi" and len(all_matches) > 1 else None

        # Create connection request(s)
        connection_request_ids = []

        for i, match in enumerate(all_matches):
            conn_result = await create_connection_request(
                initiator_id=user_id,
                target_user_id=match.get("target_user_id"),
                target_name=match.get("target_name"),
                target_phone=match.get("target_phone"),
                match_score=match.get("match_score"),
                matching_reasons=match.get("matching_reasons", []),
                llm_introduction=match.get("llm_introduction"),
                llm_concern=match.get("llm_concern"),
            )

            if not conn_result.success:
                logger.error(
                    "[PROACTIVE_OUTREACH] connection_request_failed user_id=%s target=%s error=%s",
                    _clip(user_id),
                    _clip(match.get("target_name", "?")),
                    conn_result.error,
                )
                continue

            request_id = conn_result.data.get("connection_request_id")
            connection_request_ids.append(request_id)

            # Update with multi-match tracking if applicable
            if signal_group_id:
                try:
                    update_data = {
                        "signal_group_id": signal_group_id,
                        "signal_id": signal.get("id"),
                        "is_multi_match": True,
                        "multi_match_threshold": PROACTIVE_MULTI_MATCH_THRESHOLD,
                    }
                    # Store group_name for iMessage group naming (fallback to signal_text)
                    group_name = signal.get("group_name") or signal.get("signal_text")
                    if group_name:
                        update_data["connection_purpose"] = group_name
                    await self.db.client.table("connection_requests").update(
                        update_data
                    ).eq("id", request_id).execute()
                except Exception as e:
                    logger.warning(
                        "[PROACTIVE_OUTREACH] multi_match_update_failed request_id=%s error=%s",
                        request_id,
                        str(e),
                    )

        if not connection_request_ids:
            logger.error(
                "[PROACTIVE_OUTREACH] all_connection_requests_failed user_id=%s",
                _clip(user_id),
            )
            await self._fail_job(
                user_id=user_id,
                attempts=0,
                error="all_connection_requests_failed",
            )
            return

        # Use first connection request ID for tracking
        connection_request_id = connection_request_ids[0]

        # Generate discovery conversation preview (best-effort)
        conversation_preview = None
        from app.config import settings as _settings

        if getattr(_settings, "conversation_preview_enabled", False):
            try:
                from app.agents.interaction.conversation_orchestrator import (
                    create_conversation_preview,
                )
                from app.integrations.azure_openai_client import AzureOpenAIClient

                conversation_preview = await create_conversation_preview(
                    db=self.db,
                    openai=AzureOpenAIClient(),
                    initiator_user_id=user_id,
                    initiator_name=user.get("name") or user.get("first_name") or f"User {user_id[:8]}",
                    match_result={
                        **(match_result or {}),
                        "all_matches": all_matches or [],
                    },
                    flow_type="proactive",
                    connection_request_id=connection_request_id,
                )
            except Exception as conv_err:
                logger.warning(
                    "[PROACTIVE_OUTREACH] conversation_preview_failed user_id=%s error=%s",
                    _clip(user_id),
                    str(conv_err),
                )

        # Generate proactive message
        # Build context from signal's extraction_reasoning (no longer needs highlights)
        email_context = build_email_context_summary([], signal)
        message = await generate_proactive_suggestion_message(
            user_profile=user,
            signal=signal,
            match_result=match_result,
            email_context=email_context,
            is_multi_match=(match_type == "multi" and len(all_matches) > 1),
            all_matches=all_matches,
            conversation_url=conversation_preview.conversation_url if conversation_preview else None,
            conversation_teaser=conversation_preview.teaser_summary if conversation_preview else None,
        )

        if not message:
            logger.error(
                "[PROACTIVE_OUTREACH] message_generation_failed user_id=%s",
                _clip(user_id),
            )
            # Still complete - connection request was created
            message = self._fallback_message(user, signal, match_result, all_matches)

        # Send message
        user_phone = user.get("phone_number")
        if not user_phone:
            logger.error(
                "[PROACTIVE_OUTREACH] no_phone user_id=%s",
                _clip(user_id),
            )
            await self._fail_job(
                user_id=user_id,
                attempts=0,
                error="no_user_phone",
            )
            return

        try:
            photon = PhotonClient()
            await photon.send_message(to_number=user_phone, content=message)
            logger.info(
                "[PROACTIVE_OUTREACH] message_sent user_id=%s",
                _clip(user_id),
            )
        except Exception as e:
            logger.error(
                "[PROACTIVE_OUTREACH] send_failed user_id=%s error=%s",
                _clip(user_id),
                str(e),
            )
            await self._fail_job(
                user_id=user_id,
                attempts=0,
                error=f"send_failed:{e}",
            )
            return

        # Store message in conversation history
        try:
            await self.db.store_message(
                user_id=user_id,
                content=message,
                message_type="bot",
                metadata={
                    "intent": "proactive_networking_suggestion",
                    "connection_request_id": connection_request_id,
                    "connection_request_ids": connection_request_ids,
                    "proactive": True,
                    "signal_text": signal.get("signal_text"),
                    "match_type": match_type,
                    "target_name": target_name,
                    "target_names": [m.get("target_name") for m in all_matches],
                    "signal_group_id": signal_group_id,
                },
            )
        except Exception as e:
            logger.warning(
                "[PROACTIVE_OUTREACH] store_message_failed user_id=%s error=%s",
                _clip(user_id),
                str(e),
            )
            # Don't fail job - message was sent

        # Save task state so the routing system knows we're waiting for confirmation
        # This enables the user's response to be routed correctly (CASE B for initiator confirmation)
        try:
            is_multi = match_type == "multi" and len(all_matches) > 1
            waiting_for = "multi_match_confirmation" if is_multi else "match_confirmation"

            # Build key_data with request IDs and match details (same format as networking task)
            key_data = {
                "waiting_for": waiting_for,
                "proactive": True,
            }

            if is_multi:
                key_data["request_ids"] = connection_request_ids
                key_data["match_names"] = [m.get("target_name") for m in all_matches]
                key_data["is_multi_match"] = True
            else:
                key_data["request_id"] = connection_request_id
                key_data["match_name"] = target_name

            self.db.client.table("task_state").insert({
                "user_id": user_id,
                "task_name": "networking",
                "instruction": f"proactive suggestion: {signal.get('signal_text', '')[:200]}",
                "outcome": f"Found match: {target_name}" if not is_multi else f"Found {len(all_matches)} matches",
                "status": "waiting",
                "key_data": key_data,
            }).execute()

            logger.info(
                "[PROACTIVE_OUTREACH] task_state_saved user_id=%s waiting_for=%s",
                _clip(user_id),
                waiting_for,
            )
        except Exception as e:
            logger.warning(
                "[PROACTIVE_OUTREACH] task_state_save_failed user_id=%s error=%s",
                _clip(user_id),
                str(e),
            )
            # Don't fail job - message was sent, routing may work from conversation context

        # Track the outreach
        signal_text = signal.get("signal_text") or ""
        try:
            await self.db.create_proactive_outreach_tracking_v1(
                user_id=user_id,
                signal_id=signal.get("id"),
                signal_text=signal_text,
                target_user_id=target_user_id,
                connection_request_id=connection_request_id,
                outreach_type="email_derived",
                message_sent=message,
            )
        except Exception as e:
            logger.warning(
                "[PROACTIVE_OUTREACH] tracking_failed user_id=%s error=%s",
                _clip(user_id),
                str(e),
            )
            # Don't fail job - outreach was sent

        # Complete job
        await self._complete_job(
            user_id=user_id,
            signal_id=signal.get("id"),
            connection_request_id=connection_request_id,
        )

    def _fallback_message(
        self,
        user: Dict[str, Any],
        signal: Dict[str, Any],
        match_result: Dict[str, Any],
        all_matches: List[Dict[str, Any]],
    ) -> str:
        """Generate a fallback message if LLM fails."""
        name = (user.get("name") or "there").split()[0].lower()
        match_type = signal.get("match_type", "single")

        if match_type == "multi" and len(all_matches) > 1:
            # Multi-match fallback
            target_names = [m.get("target_name", "someone").split()[0] for m in all_matches[:3]]
            names_str = ", ".join(target_names[:-1]) + f" and {target_names[-1]}" if len(target_names) > 1 else target_names[0]
            return f"hey {name}, found some people who might be helpful for what you're working on. {names_str} could all be good connections. want me to send intros to all of them"
        else:
            # Single match fallback
            target = (match_result.get("target_name") or "someone").split()[0]
            reasons = match_result.get("matching_reasons") or []
            reason = reasons[0] if reasons else "they might be a good connection"
            return f"hey {name}, found someone who might be helpful for what you're working on. {target} could be a good match because {reason}. want me to send an intro"

    async def _complete_job(
        self,
        *,
        user_id: str,
        signal_id: Optional[str],
        connection_request_id: str,
    ) -> None:
        """Mark job as complete (outreach sent)."""
        result = await self.db.complete_proactive_outreach_job_v1(
            user_id=user_id,
            worker_id=self.worker_id,
            signal_id=signal_id,
            connection_request_id=connection_request_id,
        )
        if result:
            logger.info(
                "[PROACTIVE_OUTREACH] job_complete user_id=%s",
                _clip(user_id),
            )
        else:
            logger.warning(
                "[PROACTIVE_OUTREACH] job_complete_failed user_id=%s",
                _clip(user_id),
            )

    async def _skip_job(
        self,
        *,
        user_id: str,
        reason: str,
    ) -> None:
        """Mark job as skipped (no outreach sent)."""
        result = await self.db.skip_proactive_outreach_job_v1(
            user_id=user_id,
            worker_id=self.worker_id,
            skip_reason=reason,
        )
        if result:
            logger.info(
                "[PROACTIVE_OUTREACH] job_skipped user_id=%s reason=%s",
                _clip(user_id),
                reason,
            )
        else:
            logger.warning(
                "[PROACTIVE_OUTREACH] job_skip_failed user_id=%s",
                _clip(user_id),
            )

    async def _fail_job(
        self,
        *,
        user_id: str,
        attempts: int,
        error: str,
    ) -> None:
        """Mark job as failed with backoff."""
        backoff = compute_backoff_seconds(attempts)
        logger.warning(
            "[PROACTIVE_OUTREACH] job_fail user_id=%s attempts=%d backoff_sec=%d err=%s",
            _clip(user_id),
            attempts,
            backoff,
            _clip(str(error), 160),
        )
        result = await self.db.fail_proactive_outreach_job_v1(
            user_id=user_id,
            worker_id=self.worker_id,
            error=error,
            backoff_seconds=backoff,
            max_attempts=PROACTIVE_OUTREACH_WORKER_MAX_ATTEMPTS,
        )
        if result is None:
            await self.db.release_proactive_outreach_job_v1(
                user_id=user_id,
                worker_id=self.worker_id,
            )
