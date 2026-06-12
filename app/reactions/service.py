from __future__ import annotations

import json
import logging
import random
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from app.config import settings
from app.integrations.azure_openai_client import AzureOpenAIClient
from app.integrations.photon_client import PhotonClient
from app.utils.redis_client import redis_client

logger = logging.getLogger(__name__)

_VALID_REACTIONS = {"love", "like", "dislike", "laugh", "emphasize", "question"}
_LOCK_TTL_SEC = 30
_SENT_TTL_SEC = 30 * 24 * 60 * 60  # 30 days

# Probability of considering a reaction (50% of messages)
_REACTION_PROBABILITY = 0.50

_LIST_LINE_RE = re.compile(r"^\s*(?:[-*•]|\d+\s*[\)\.\-:])\s+")


def _strip_code_fences(text: str) -> str:
    cleaned = (text or "").strip()
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    if cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    return cleaned.strip()


def _safe_json_loads(raw: str) -> Optional[Dict[str, Any]]:
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _has_list_formatting(text: str) -> bool:
    for line in (text or "").splitlines():
        if _LIST_LINE_RE.match(line.strip()):
            return True
    return False


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip().lower())

@dataclass
class ReactionService:
    """
    Decides whether to send a Tapback reaction to an inbound user message.

    - Uses strict idempotency on message GUID (never react twice).
    - Uses LLM for general messages, but is conservative by default.
    - Allows deterministic overrides (e.g., onboarding name/career interest).
    """

    photon: PhotonClient
    openai: Optional[AzureOpenAIClient] = None

    async def maybe_send_reaction(
        self,
        *,
        to_number: str,
        message_guid: str | None,
        message_content: str | None,
        chat_guid: str | None = None,
        forced_reaction: str | None = None,
        context: Optional[Dict[str, Any]] = None,
        part_index: int = 0,
    ) -> None:
        if not getattr(settings, "reactions_enabled", True):
            return

        msg_guid = str(message_guid or "").strip()
        if not msg_guid:
            return

        # Do not LLM-react during onboarding (node-level forced reactions handle the UX).
        task = str((context or {}).get("task") or "").strip().lower()
        if task == "onboarding" and not forced_reaction:
            return

        content = str(message_content or "").strip()
        if not forced_reaction:
            # Skip empty/attachments
            if not content or content in {"[attachment]", "[empty]"}:
                return
            # Skip very long messages (avoid over-reacting to essays)
            if len(content) > 300:
                return
            # Skip messages with list formatting (likely structured content)
            if _has_list_formatting(content):
                return

            # Probability gate: only consider reacting to ~50% of messages
            # This makes reactions feel occasional and natural, not spammy
            if random.random() > _REACTION_PROBABILITY:
                logger.debug("[REACTION] Skipped by probability gate (50%%)")
                return

        # Idempotency + concurrency guard
        lock_key = f"reaction:v1:lock:{msg_guid}"
        sent_key = f"reaction:v1:sent:{msg_guid}"

        redis_available = True
        try:
            if redis_client.client.get(sent_key):
                return
        except Exception:
            # If Redis is down, only allow forced reactions (onboarding UX).
            if not forced_reaction:
                return
            redis_available = False

        if redis_available:
            try:
                got_lock = redis_client.client.set(lock_key, "1", nx=True, ex=_LOCK_TTL_SEC)
            except Exception:
                if not forced_reaction:
                    return
                redis_available = False
                got_lock = True
            if got_lock is not True:
                return

        try:
            reaction = (forced_reaction or "").strip().lower() or None
            if reaction is None:
                reaction = await self._decide_reaction_llm(content, context=context)

            if not reaction or reaction not in _VALID_REACTIONS:
                return

            result = await self.photon.send_reaction(
                to_number=to_number,
                message_guid=msg_guid,
                reaction=reaction,
                chat_guid=chat_guid,
                part_index=int(part_index or 0),
            )

            if isinstance(result, dict) and result.get("success") is True:
                try:
                    if redis_available:
                        redis_client.client.setex(sent_key, _SENT_TTL_SEC, reaction)
                except Exception:
                    pass
        except Exception as e:
            logger.debug("[REACTION] Failed to send reaction: %s", e)
        finally:
            try:
                if redis_available:
                    redis_client.client.delete(lock_key)
            except Exception:
                pass

    async def _decide_reaction_llm(self, message_text: str, *, context: Optional[Dict[str, Any]] = None) -> Optional[str]:
        if not getattr(settings, "reactions_llm_enabled", True):
            return None
        if self.openai is None:
            return None

        msg = str(message_text or "").strip()
        if not msg:
            return None

        # Guide LLM to make natural, human-like reaction decisions
        system_prompt = (
            "you are frank, deciding whether to send an iMessage tapback reaction to a user's message.\n"
            "available reactions: love (❤️), like (👍), laugh (😂), emphasize (!!), question (??), dislike (👎)\n"
            "\n"
            "react like a real friend would - naturally and occasionally, not to every message.\n"
            "\n"
            "GOOD times to react:\n"
            "- user shares good news or excitement → love or like\n"
            "- user says something genuinely funny → laugh\n"
            "- user shares an accomplishment → love or emphasize\n"
            "- user expresses gratitude → love or like\n"
            "- user says something surprising/impressive → emphasize\n"
            "- casual affirmations like 'sounds good', 'cool', 'nice' → like\n"
            "\n"
            "DO NOT react when:\n"
            "- user is asking a question or making a request\n"
            "- user is sharing something sad, sensitive, or serious\n"
            "- message is neutral/informational with no emotional content\n"
            "- you're unsure - when in doubt, don't react\n"
            "\n"
            "NEVER use dislike or question reactions - they come across as dismissive.\n"
            "\n"
            "output JSON only: {\"react\": true|false, \"reaction\": \"love|like|laugh|emphasize\"}\n"
            "if react=false, set reaction to empty string."
        )

        intent = str((context or {}).get("intent") or "").strip()
        task = str((context or {}).get("task") or "").strip()
        stage = str((context or {}).get("onboarding_stage") or "").strip()

        user_prompt = (
            f"context:\n"
            f"- intent: {intent or 'unknown'}\n"
            f"- task: {task or 'unknown'}\n"
            f"- onboarding_stage: {stage or 'n/a'}\n"
            f"\n"
            f"user_message:\n{msg}\n"
        )

        try:
            raw = await self.openai.generate_response(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                model=str(getattr(settings, "reactions_model", "gpt-4o-mini") or "gpt-4o-mini"),
                temperature=0.2,
                trace_label="tapback_reaction_decide",
            )
        except Exception:
            return None

        data = _safe_json_loads(_strip_code_fences(str(raw or "")))
        if not data:
            return None

        should_react = bool(data.get("react") is True or str(data.get("react") or "").strip().lower() == "true")
        if not should_react:
            return None

        reaction = str(data.get("reaction") or "").strip().lower()
        if reaction not in _VALID_REACTIONS:
            return None

        # Never use negative reactions from LLM - they feel dismissive
        if reaction in ("dislike", "question"):
            return None

        # Final hard guardrails against reacting to requests
        lowered = _normalize_text(msg)
        if any(token in lowered for token in ("help", "can you", "could you", "please", "connect me", "introduce", "schedule")):
            return None

        return reaction
