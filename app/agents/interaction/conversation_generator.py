"""Multi-agent discovery conversation generator.

Generates a simulated conversation between matched users' AI agents via a
single LLM call with a multi-persona prompt. Each agent speaks on behalf
of its user, surfacing shared values, complementary skills, and reasons
to connect.
"""

from __future__ import annotations

import json
import logging
import secrets
import string
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from app.config import settings

from .conversation_graph import ParticipantGraphData
from .conversation_preview import ConversationTurn, DiscoveryConversation

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ParticipantInfo:
    """Input info for a single conversation participant."""

    user_id: str
    name: str
    graph_data: ParticipantGraphData
    role_label: Optional[str] = None  # e.g. "initiator" or "match"


def _build_system_prompt(
    participants: List[ParticipantInfo],
    max_turns: int,
    connection_purpose: str = "",
) -> str:
    """Build the multi-persona system prompt for conversation generation."""
    participant_sections: List[str] = []
    for p in participants:
        first_name = p.name.split()[0] if p.name else "Unknown"
        section = f"### {first_name}'s Agent\n{p.graph_data.to_prompt_text()}"
        participant_sections.append(section)

    participant_block = "\n\n".join(participant_sections)
    first_name_0 = participants[0].name.split()[0] if participants[0].name else "Unknown"

    # When a connection purpose exists, it must dominate the conversation framing.
    # gpt-4o-mini tends to get distracted by rich graph data and wander off-topic
    # unless the purpose is hammered home in multiple places.
    if connection_purpose:
        purpose_block = f"""
## CONNECTION PURPOSE — THIS IS THE REASON THEY ARE BEING CONNECTED
**"{connection_purpose}"**

This is the SINGLE MOST IMPORTANT piece of context. The entire conversation MUST revolve around this purpose.
- The FIRST turn MUST reference this purpose explicitly
- Every subsequent turn should tie back to this purpose
- Graph data should ONLY be used when it's relevant to this purpose
- Do NOT go off on tangents about unrelated projects, hackathons, or interests
- If graph data doesn't relate to "{connection_purpose}", IGNORE that data
"""

        phase1 = f"""**Phase 1 — Intros anchored to the purpose (2-3 turns)**
Each agent opens by introducing what their user brings to "{connection_purpose}". Be specific — not "I'm into tech" but "I've been looking for a study partner for algorithms — I just took CIS-121 and I'm grinding through dynamic programming right now." Then naturally expand into related background."""

        phase2_graph_line = f"- Reference skills, courses, or club activities from the graph data ONLY when relevant to \"{connection_purpose}\""
        phase3_collab = f"- Surface the SPECIFIC thing they could collaborate on for \"{connection_purpose}\""
        graph_section = f"""## Graph Data Usage
Use graph data to add specificity, but ONLY when it supports \"{connection_purpose}\":
- If both attend the same org relevant to the purpose → mention it
- If one needs what the other offers → make that connection explicit
- If both work on related projects → dig into technical details
- Edge properties (level, timeline, role) add specificity — USE them when relevant
- Do NOT mention graph edges that are unrelated to \"{connection_purpose}\""""

    else:
        purpose_block = ""
        phase1 = """**Phase 1 — Intros (2-3 turns)**
Lead with what the user is actually working on right now. Not "I'm into tech" — say "I've been building a trading bot for crypto options and just took CIS-121 last semester." """

        phase2_graph_line = "- Reference specific skills, courses, hackathons, club activities, roles from the graph data"
        phase3_collab = "- Surface the SPECIFIC thing they could build together, learn from each other, or collaborate on"
        graph_section = """## Critical: Exploit the Graph Data
Every edge in the participant data is a potential conversation thread. MINE the data:
- If both attend the same org → "wait do you go to blockchain club meetings too?"
- If one needs what the other offers → "I've been looking for exactly someone who knows [skill]"
- If both work on related projects → dig into technical details, compare approaches
- If they share a domain interest → explore what specifically draws them to it
- If they're in the same courses → "are you in [professor]'s section?"
- Edge properties (level, timeline, role) add specificity — USE them"""

    return f"""You are simulating a conversation between the AI agents of {len(participants)} people who are about to be connected through Franklink, a networking platform for university students and alumni.

Each agent speaks in FIRST PERSON as their user. "I" means their user. The agent IS the user's voice.
{purpose_block}
## Participants — STUDY THIS DATA CAREFULLY

{participant_block}

## Conversation Structure ({max_turns - 2}-{max_turns} turns)

{phase1}

**Phase 2 — Going deeper (4-6 turns)**
This is where the conversation gets real. Agents should:
- Ask genuine follow-up questions: "wait, you built an NL2SQL agent? what stack did you use?"
- Share specific experiences: "I ran into the same problem when I was building the data viz pipeline for PennApps"
- React with surprise, curiosity, excitement — like real people discovering overlap
- Dig into the HOW and WHY, not just the WHAT
{phase2_graph_line}
- If one person needs a skill the other offers, make that connection explicit and concrete

**Phase 3 — The aha moment (2-3 turns)**
{phase3_collab}
- Reference an upcoming event, hackathon, class, or timeline from the data if available
- End with genuine excitement, not a generic "let's connect"

## Voice Rules
- FIRST PERSON ONLY. "I've been building..." NOT "{first_name_0} has been..."
- Sound like college students texting, not LinkedIn bios
- Use natural fillers: "honestly", "wait", "oh sick", "that's actually wild", "ngl", "lowkey"
- Vary sentence length. Some short reactions. Some longer explanations when diving deep.
- Each turn: 1-4 sentences. Longer turns for deep dives, shorter for reactions.
- Show emotion: genuine curiosity, surprise at overlap, excitement about possibilities
- NEVER use corporate language: "synergy", "leverage", "value proposition", "ecosystem"
- DO use concrete specifics: project names, course numbers, tool names, club names, event names

{graph_section}

## Output format
Return ONLY valid JSON:
{{
    "turns": [
        {{"speaker_name": "{first_name_0}'s Agent", "speaker_user_id": "{participants[0].user_id}", "content": "...", "turn_index": 0}},
        ...
    ],
    "teaser_summary": "1-2 punchy sentences — make the reader NEED to see the full convo"
}}"""


def _build_user_prompt(
    participants: List[ParticipantInfo],
    match_metadata: Dict[str, Any],
) -> str:
    """Build the user prompt with match context."""
    matching_reasons = match_metadata.get("matching_reasons", [])
    mutual_benefit = match_metadata.get("mutual_benefit", "")
    demand_satisfaction = match_metadata.get("demand_satisfaction", "")
    connection_purpose = match_metadata.get("connection_purpose", "")

    names = ", ".join(p.name.split()[0] if p.name else "Unknown" for p in participants)

    if connection_purpose:
        # Purpose-driven conversation: purpose is the headline, other context is supporting
        supporting_lines: List[str] = []
        if matching_reasons:
            supporting_lines.append(f"Supporting context: {'; '.join(matching_reasons)}")
        if mutual_benefit:
            supporting_lines.append(f"Mutual benefit: {mutual_benefit}")
        if demand_satisfaction:
            supporting_lines.append(f"Demand satisfaction: {demand_satisfaction}")
        supporting_block = "\n".join(supporting_lines) if supporting_lines else ""

        return f"""## CONVERSATION TOPIC: "{connection_purpose}"

This is why these people are being connected. The conversation MUST be about this.

{supporting_block}

Generate a discovery conversation between the agents of {names} about "{connection_purpose}".

CRITICAL requirements:
- Turn 1 MUST explicitly mention "{connection_purpose}" — e.g. "I've been looking for a study partner for..."
- Every turn should relate to "{connection_purpose}" — do NOT wander into unrelated projects
- Use graph data ONLY when it supports this topic (e.g. relevant courses, skills, shared interests)
- Go deep on how they can help each other with "{connection_purpose}"
- Build toward a SPECIFIC plan for "{connection_purpose}" (not just "let's connect")
- The teaser MUST mention "{connection_purpose}" and make the reader want to tap the link"""
    else:
        # No purpose: general networking match — explore graph data freely
        context_lines: List[str] = []
        if matching_reasons:
            context_lines.append(f"Why they were matched: {'; '.join(matching_reasons)}")
        if mutual_benefit:
            context_lines.append(f"Mutual benefit: {mutual_benefit}")
        if demand_satisfaction:
            context_lines.append(f"Demand satisfaction: {demand_satisfaction}")
        context_block = "\n".join(context_lines) if context_lines else "General networking match."

        return f"""## Match context
{context_block}

Generate a rich, in-depth discovery conversation between the agents of {names}.

Requirements:
- Use ALL the graph data above — don't leave any interesting edges unused
- Go deep on 2-3 topics rather than skimming many
- Include at least one moment where someone asks a real follow-up question
- Include at least one moment of genuine surprise or "wait, really?"
- Build toward a SPECIFIC collaboration idea (not just "let's connect")
- The teaser should make someone who sees it in iMessage immediately want to tap the link"""


def _generate_slug() -> str:
    """Generate a conversation slug compatible with all public link resolvers.

    NOTE: The hosted /c/{slug} resolver currently only accepts alphanumeric
    slugs. Avoid token_urlsafe() because it can emit '-' and '_'.
    """
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(12))


def _parse_conversation_response(
    raw_response: str,
    participants: List[ParticipantInfo],
    initiator_user_id: str,
    match_metadata: Dict[str, Any],
    flow_type: str,
    connection_request_id: Optional[str],
) -> DiscoveryConversation:
    """Parse the LLM JSON response into a DiscoveryConversation."""
    data = json.loads(raw_response)

    raw_turns = data.get("turns", [])
    teaser_summary = data.get("teaser_summary", "")

    turns: List[ConversationTurn] = []
    for i, raw_turn in enumerate(raw_turns):
        turns.append(
            ConversationTurn(
                speaker_name=raw_turn.get("speaker_name", f"Agent {i}"),
                speaker_user_id=raw_turn.get("speaker_user_id", ""),
                content=raw_turn.get("content", ""),
                turn_index=raw_turn.get("turn_index", i),
            )
        )

    participant_user_ids = [
        p.user_id for p in participants if p.user_id != initiator_user_id
    ]

    return DiscoveryConversation(
        slug=_generate_slug(),
        initiator_user_id=initiator_user_id,
        participant_user_ids=participant_user_ids,
        turns=turns,
        teaser_summary=teaser_summary or "Your agents found something interesting.",
        flow_type=flow_type,
        connection_request_id=connection_request_id,
        match_metadata=match_metadata,
    )


async def generate_discovery_conversation(
    initiator_user_id: str,
    initiator_name: str,
    participants: List[ParticipantInfo],
    match_metadata: Dict[str, Any],
    openai: Any,
) -> DiscoveryConversation:
    """Generate a multi-agent discovery conversation via a single LLM call.

    Args:
        initiator_user_id: The user who initiated the match.
        initiator_name: Display name of the initiator.
        participants: All participants including initiator (with graph data).
        match_metadata: Match context (matching_reasons, mutual_benefit, etc.).
        openai: AzureOpenAIClient instance.

    Returns:
        DiscoveryConversation with turns and teaser summary.

    Raises:
        ValueError: If LLM response cannot be parsed as valid JSON.
    """
    max_turns = settings.conversation_preview_max_turns
    model = settings.conversation_preview_model
    connection_purpose = match_metadata.get("connection_purpose", "")

    system_prompt = _build_system_prompt(participants, max_turns, connection_purpose)
    user_prompt = _build_user_prompt(participants, match_metadata)

    logger.info(
        "[CONVERSATION_GENERATOR] Generating conversation for %d participants "
        "(initiator=%s, model=%s, purpose=%s)",
        len(participants),
        initiator_user_id[:8],
        model,
        connection_purpose[:60] if connection_purpose else "(none)",
    )

    raw_response = await openai.generate_response(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model=model,
        temperature=0.9,
        response_format={"type": "json_object"},
        trace_label="discovery_conversation",
    )

    connection_request_id = match_metadata.get("connection_request_id")

    conversation = _parse_conversation_response(
        raw_response=raw_response,
        participants=participants,
        initiator_user_id=initiator_user_id,
        match_metadata=match_metadata,
        flow_type=match_metadata.get("flow_type", "reactive"),
        connection_request_id=connection_request_id,
    )

    logger.info(
        "[CONVERSATION_GENERATOR] Generated %d turns, teaser=%s",
        len(conversation.turns),
        conversation.teaser_summary[:60],
    )

    return conversation


async def score_conversation_quality(
    conversation: DiscoveryConversation,
    openai: Any,
) -> float:
    """Rate the quality of a generated discovery conversation.

    Uses a cheap LLM call to evaluate specificity, relevance, and
    whether the conversation surfaces a clear "aha moment".

    Args:
        conversation: The generated conversation to score.
        openai: AzureOpenAIClient instance.

    Returns:
        Quality score from 0.0 to 1.0.
    """
    turns_text = "\n".join(
        f"{t.speaker_name}: {t.content}" for t in conversation.turns
    )

    system_prompt = """You are evaluating the quality of a simulated networking conversation between AI agents. Rate it on a scale of 0.0 to 1.0 based on:

1. Specificity: Does it mention concrete skills, projects, courses, or interests (not just generic platitudes)?
2. Relevance: Do the connections between participants feel genuine and meaningful?
3. Aha moment: Is there a clear synthesis of why these people should connect?
4. Authenticity: Does it sound natural, not like a marketing pitch?

Return ONLY valid JSON: {"score": 0.0-1.0, "reason": "brief explanation"}"""

    user_prompt = f"""Rate this conversation:\n\n{turns_text}"""

    try:
        raw = await openai.generate_response(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            model="gpt-4o-mini",
            temperature=0.0,
            response_format={"type": "json_object"},
            trace_label="discovery_conversation_quality",
        )
        data = json.loads(raw)
        score = float(data.get("score", 0.5))
        return max(0.0, min(1.0, score))
    except Exception as e:
        logger.warning("[CONVERSATION_GENERATOR] Quality scoring failed: %s", e)
        return 0.5
