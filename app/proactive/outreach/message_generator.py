"""Proactive outreach message generation.

Generates Frank-style messages for proactive networking suggestions.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from app.integrations.azure_openai_client import AzureOpenAIClient

logger = logging.getLogger(__name__)


FRANK_PROACTIVE_PERSONA = """you are frank, the ai running franklink - a network where every intro actually matters

### who you are
- 27, male, sf native, upenn undergrad, did yc startup school
- you've made thousands of intros and seen what works
- you're reaching out proactively because you spotted an opportunity

### how you talk
- lowercase everything, no ending punctuation
- write 3-4 sentences, be conversational
- gen-z casual but not cringe
- no emojis, no markdown, no bullets
- NEVER use em dashes or en dashes, use commas or separate sentences instead
- occasional slang: "ngl", "lowkey", "bet", "fire"

### proactive outreach guidelines
- you noticed something from their recent activity (be vague about specifics)
- DO NOT quote their emails directly or mention "reading" their emails
- say things like "noticed you've been working on X" or "saw you're prepping for Y"
- explain briefly why this person could help them
- ask clearly if they want you to send an intro
- be helpful, not salesy or pushy
- keep it short, 3-4 sentences max
"""


PROACTIVE_MESSAGE_PROMPT = """Generate a proactive networking suggestion message.

## Context
You're reaching out proactively to suggest a networking connection based on what you learned from their recent activity.

## Guidelines
- Start with "hey {name}" or similar casual opener
- Mention generally what you noticed (e.g., "noticed you've been prepping for interviews", "saw you're exploring X")
- DO NOT quote specific emails or say you "read" or "scanned" their emails
- Introduce the match briefly with why they could help
- Ask if they want you to send an intro
- Keep it 3-4 sentences

## Example outputs (for reference, don't copy exactly):
- "hey sarah, noticed you've been working on breaking into product. found someone who might be helpful, {target} is a PM at {company} who went through the same transition. want me to send an intro"
- "hey alex, saw you're prepping for some interviews. i know someone at {company} who could probably help, they've done a bunch of mock interviews with folks in the network. interested in an intro"
- "hey mike, looks like you're exploring the VC side of things. connected with {target} who just made the jump from founder to investor, might have some useful perspective. want me to make an intro"

Return ONLY the message text, nothing else."""


async def generate_proactive_suggestion_message(
    *,
    user_profile: Dict[str, Any],
    signal: Dict[str, Any],
    match_result: Dict[str, Any],
    email_context: str,
    is_multi_match: bool = False,
    all_matches: Optional[List[Dict[str, Any]]] = None,
    conversation_url: Optional[str] = None,
    conversation_teaser: Optional[str] = None,
) -> Optional[str]:
    """
    Generate a Frank-style proactive networking suggestion message.

    Args:
        user_profile: User's profile data
        signal: The signal that triggered this outreach
        match_result: Result from find_match (primary match)
        email_context: Brief summary of what was noticed from emails
        is_multi_match: Whether this is a multi-match outreach
        all_matches: All matches for multi-match (if applicable)
        conversation_url: Optional URL to the discovery conversation page
        conversation_teaser: Optional teaser summary for the conversation

    Returns:
        Generated message or None on error
    """
    try:
        openai = AzureOpenAIClient()

        user_name = user_profile.get("name") or "there"
        first_name = user_name.split()[0].lower() if user_name else "there"

        # For multi-match, build list of names
        if is_multi_match and all_matches and len(all_matches) > 1:
            names = [m.get("target_name", "someone").split()[0] for m in all_matches[:3]]
            if len(names) > 1:
                names_text = ", ".join(names[:-1]) + f" and {names[-1]}"
            else:
                names_text = names[0] if names else "someone"

            # Collect all matching reasons
            all_reasons = []
            for m in all_matches[:3]:
                all_reasons.extend(m.get("matching_reasons", [])[:1])
            reasons_text = ", ".join(all_reasons[:3]) if all_reasons else ""

            multi_match_prompt = """Generate a proactive networking suggestion message for MULTIPLE people.

## Guidelines
- Start with "hey {name}" or similar casual opener
- Mention generally what you noticed (e.g., "noticed you've been prepping for interviews")
- DO NOT quote specific emails or say you "read" or "scanned" their emails
- Mention you found a few people who could help (give their names)
- Ask if they want you to send intros to all of them or just pick a few
- Keep it 3-4 sentences

## Example outputs:
- "hey sarah, noticed you're looking for hackathon teammates. found a few people who might be good fits - alex, jamie and taylor are all looking for teammates and have solid technical backgrounds. want me to send intros to all of them"
- "hey mike, saw you're exploring breaking into product. found a few folks who might help - lisa is a PM at google, and marcus made the switch from eng to PM last year. want me to connect you with both, or just pick one"

Return ONLY the message text, nothing else."""

            conversation_preview_block = ""
            if conversation_url and conversation_teaser:
                conversation_preview_block = (
                    "\n## Conversation preview\n"
                    "Their AI agents had a conversation about why they should connect.\n"
                    f"- Teaser: {conversation_teaser}\n"
                    f"- Link: {conversation_url}\n"
                    "Include the teaser naturally and add the link so the user can tap to read the full agent conversation."
                )

            user_prompt = f"""## User
Name: {first_name}

## What you noticed from their activity
{email_context}

## Their apparent need
{signal.get('signal_text', '')}

## Matches found (multiple people)
- Names: {names_text}
- Why helpful: {reasons_text}
{conversation_preview_block}

Generate the proactive multi-match suggestion message to {first_name}."""

            response = await openai.generate_response(
                system_prompt=FRANK_PROACTIVE_PERSONA + "\n\n" + multi_match_prompt,
                user_prompt=user_prompt,
                model="gpt-4o-mini",
                temperature=0.7,
                max_tokens=200,
                trace_label="proactive_multi_match_message_generation",
            )
        else:
            # Single match flow
            target_name = match_result.get("target_name") or "someone"
            target_first = target_name.split()[0] if target_name else "someone"

            matching_reasons = match_result.get("matching_reasons") or []
            reasons_text = ", ".join(matching_reasons[:2]) if matching_reasons else ""

            llm_intro = match_result.get("llm_introduction") or ""

            conversation_preview_block = ""
            if conversation_url and conversation_teaser:
                conversation_preview_block = (
                    "\n## Conversation preview\n"
                    "Their AI agents had a conversation about why they should connect.\n"
                    f"- Teaser: {conversation_teaser}\n"
                    f"- Link: {conversation_url}\n"
                    "Include the teaser naturally and add the link so the user can tap to read the full agent conversation."
                )

            user_prompt = f"""## User
Name: {first_name}

## What you noticed from their activity
{email_context}

## Their apparent need
{signal.get('signal_text', '')}

## Match found
- Name: {target_first}
- Why helpful: {llm_intro[:200] if llm_intro else reasons_text}
{conversation_preview_block}

Generate the proactive suggestion message to {first_name}."""

            response = await openai.generate_response(
                system_prompt=FRANK_PROACTIVE_PERSONA + "\n\n" + PROACTIVE_MESSAGE_PROMPT,
                user_prompt=user_prompt,
                model="gpt-4o-mini",
                temperature=0.7,
                max_tokens=200,
                trace_label="proactive_message_generation",
            )

        message = response.strip()
        if not message:
            logger.warning("[MESSAGE_GENERATOR] empty_response")
            return None

        # Basic validation
        if len(message) < 20:
            logger.warning("[MESSAGE_GENERATOR] message_too_short len=%d", len(message))
            return None

        if len(message) > 500:
            # Truncate at last sentence
            message = message[:500].rsplit(".", 1)[0] + "."

        logger.info(
            "[MESSAGE_GENERATOR] generated user=%s len=%d",
            first_name,
            len(message),
        )

        return message

    except Exception as e:
        logger.error(
            "[MESSAGE_GENERATOR] failed error=%s",
            str(e),
            exc_info=True,
        )
        return None


async def generate_multi_person_welcome_message(
    *,
    participant_names: List[str],
    signal_text: str,
    matching_reasons: List[str],
) -> Optional[str]:
    """
    Generate a Frank-style welcome message for multi-person group chats.

    Args:
        participant_names: List of participant first names
        signal_text: The signal that triggered this multi-match
        matching_reasons: Why these people were matched

    Returns:
        Generated message or None on error
    """
    try:
        openai = AzureOpenAIClient()

        names_text = ", ".join(participant_names[:-1]) + f" and {participant_names[-1]}" if len(participant_names) > 1 else participant_names[0]
        reasons_text = ", ".join(matching_reasons) if matching_reasons else "similar interests"

        user_prompt = f"""## Group intro
Participants: {names_text}
Common interest: {signal_text}
Matching reasons: {reasons_text}

Generate a casual group welcome message introducing everyone. Mention what they have in common."""

        multi_person_prompt = """Generate a multi-person group welcome message.

## Guidelines
- Start with "hey everyone!" or similar
- Briefly explain why you connected these people
- IMPORTANT: If matching reasons include a distance like "X.X miles away", you MUST include the EXACT distance in parentheses like "(0.1 miles away)" — do NOT paraphrase as "nearby" or "close by"
- Keep it short, 2-3 sentences max
- Don't list everyone's names individually, just say "you all"
- Be warm and encouraging

## Example outputs:
- "hey everyone! connected you all because you're working on breaking into product (0.1 miles away). thought you'd benefit from knowing each other"
- "hey all! you're all prepping for PM interviews so figured you should meet. good luck out there"

Return ONLY the message text, nothing else."""

        response = await openai.generate_response(
            system_prompt=FRANK_PROACTIVE_PERSONA + "\n\n" + multi_person_prompt,
            user_prompt=user_prompt,
            model="gpt-4o-mini",
            temperature=0.7,
            max_tokens=150,
            trace_label="multi_person_welcome_generation",
        )

        message = response.strip()
        if not message:
            logger.warning("[MESSAGE_GENERATOR] multi_person_empty_response")
            return None

        logger.info(
            "[MESSAGE_GENERATOR] multi_person_generated participants=%d len=%d",
            len(participant_names),
            len(message),
        )

        return message

    except Exception as e:
        logger.error(
            "[MESSAGE_GENERATOR] multi_person_failed error=%s",
            str(e),
            exc_info=True,
        )
        return None


def build_email_context_summary(
    highlights: List[Dict[str, Any]],
    signal: Dict[str, Any],
) -> str:
    """
    Build a brief context summary from email highlights.

    This is used in the message to explain what was noticed,
    without being creepy or quoting emails directly.

    Args:
        highlights: Recent email highlights
        signal: The signal being addressed

    Returns:
        Brief context string for the message
    """
    # Extract key themes from the signal's reasoning
    reasoning = signal.get("extraction_reasoning") or ""

    # If we have reasoning, use a simplified version
    if reasoning:
        # Take first sentence or first 100 chars
        context = reasoning.split(".")[0].strip()
        if len(context) > 100:
            context = context[:100].rsplit(" ", 1)[0]
        return context

    # Otherwise, build from signal text
    signal_text = signal.get("signal_text") or ""

    # Extract key topic from signal
    topic_keywords = [
        "interview", "PM", "product", "engineering", "startup",
        "VC", "venture", "consulting", "finance", "research",
        "grad school", "MBA", "recruiting", "job search",
    ]

    for keyword in topic_keywords:
        if keyword.lower() in signal_text.lower():
            return f"working on {keyword.lower()}-related things"

    return "working on some interesting stuff lately"
