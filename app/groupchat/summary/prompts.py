from __future__ import annotations

from typing import Dict, List, Optional


def build_groupchat_summary_messages(
    *,
    chat_guid: str,
    participant_names: List[str],
    segment_start_at: Optional[str],
    segment_end_at: str,
    transcript_lines: List[str],
) -> List[Dict[str, str]]:
    # Build dynamic "Each Person" section for all participants
    person_sections = "\n".join(
        f"### {name}\n- ..."
        for name in participant_names
    ) if participant_names else "### (participants)\n- ..."

    system = (
        "You are a careful group chat summarizer for Franklink.\n"
        "You must only use information present in the transcript lines.\n"
        "Do not invent facts. If uncertain, omit.\n"
        "Output Markdown only (no JSON, no code fences).\n"
        "\n"
        "If the transcript looks truncated (starts mid-topic or missing context), add this as the first line:\n"
        "NOTE: Transcript window may be incomplete.\n"
        "\n"
        "Use exactly this template:\n"
        "## Topics\n"
        "- ...\n"
        "\n"
        "## Each Person\n"
        f"{person_sections}\n"
        "\n"
        "## Agreements\n"
        "- ...\n"
        "\n"
        "## Disagreements\n"
        "- ...\n"
        "\n"
        "## Decisions\n"
        "- ...\n"
        "\n"
        "## Action Items\n"
        "- ...\n"
        "\n"
        "## Open Questions\n"
        "- ...\n"
        "\n"
        "## One-line Summary\n"
        "...\n"
    )

    start_line = segment_start_at or "(first segment)"
    participants_str = ", ".join(participant_names) if participant_names else "unknown"
    user = (
        f"Chat: {chat_guid}\n"
        f"Segment: {start_line} -> {segment_end_at}\n"
        f"Participants: {participants_str}\n"
        "\n"
        "Transcript:\n"
        + "\n".join(transcript_lines)
    )

    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]

