"""Web endpoint for rendering discovery conversations as mobile-friendly HTML.

Serves a single page at /c/{slug} that displays the multi-agent dialogue
in a chat bubble UI. Designed for iPhone viewport (users tap from iMessage).
"""

from __future__ import annotations

import html
import logging
import re
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

from app.database.client.client import DatabaseClient

logger = logging.getLogger(__name__)

# Regex to validate hex colors (prevents XSS via style attributes)
_HEX_COLOR_PATTERN = re.compile(r"^#[0-9a-fA-F]{6}$")


def _validate_hex_color(color: str, default: str = "#1A73E8") -> str:
    """Ensure color is a valid hex color, fallback to default."""
    if color and _HEX_COLOR_PATTERN.fullmatch(color):
        return color
    return default


def _render_purpose_section(
    initiator_name: str,
    connection_purpose: str,
    accent_color: str = "#1A73E8",
) -> str:
    """Render the connection purpose card with initiator avatar."""
    if not connection_purpose:
        return ""

    # Validate accent_color to prevent XSS via style attributes
    safe_color = _validate_hex_color(accent_color)
    name_display = html.escape(initiator_name or "Someone")
    purpose_escaped = html.escape(connection_purpose)
    initial = name_display[0].upper() if name_display else "?"

    return f'''
        <section class="purpose-card">
            <div class="purpose-header">
                <div class="purpose-avatar" style="background:{safe_color}1A;color:{safe_color};border-color:{safe_color}66">{initial}</div>
                <div class="purpose-label">{name_display} is looking for:</div>
            </div>
            <blockquote class="purpose-text">"{purpose_escaped}"</blockquote>
        </section>'''


def _render_matching_reasons(matching_reasons: List[str]) -> str:
    """Render the matching reasons pills."""
    if not matching_reasons:
        return ""

    # Cap at 3 reasons, truncate long ones
    pills = []
    for reason in matching_reasons[:3]:
        truncated = reason[:45] + "..." if len(reason) > 45 else reason
        pills.append(f'<span class="reason-pill">{html.escape(truncated)}</span>')

    return f'''
        <section class="match-reasons">
            <div class="reasons-label">Why they match:</div>
            <div class="reasons-pills">{" ".join(pills)}</div>
        </section>'''

router = APIRouter()

# Color palette for speaker bubbles (supports up to 6 speakers)
_SPEAKER_COLORS = [
    ("#E8F0FE", "#1A73E8"),  # Blue (bg, text accent)
    ("#FEF3E8", "#E87A1A"),  # Orange
    ("#E8FEF0", "#1AE87A"),  # Green
    ("#F3E8FE", "#7A1AE8"),  # Purple
    ("#FEE8E8", "#E81A1A"),  # Red
    ("#E8FEFE", "#1AE8E8"),  # Teal
]


def _get_speaker_color(index: int) -> tuple:
    """Get background and accent color for a speaker by index."""
    return _SPEAKER_COLORS[index % len(_SPEAKER_COLORS)]


def _render_turn_html(
    turn: Dict[str, Any],
    speaker_index: int,
    is_right: bool,
    turn_index: int,
    is_continuation: bool,
) -> str:
    """Render a single conversation turn as an HTML chat bubble."""
    bg_color, accent_color = _get_speaker_color(speaker_index)
    raw_speaker_name = str(turn.get("speaker_name", "Agent"))
    speaker_name = html.escape(raw_speaker_name)
    content = html.escape(str(turn.get("content", "")))
    speaker_initial = html.escape(raw_speaker_name[:1].upper()) if raw_speaker_name else "A"
    side_class = "is-right" if is_right else "is-left"
    continuation_class = "is-continuation" if is_continuation else "is-first"
    # Tighter spacing for consecutive messages from same speaker.
    bottom_margin = "6px" if is_continuation else "14px"
    # Staggered fade-in animation
    delay = f"{min(turn_index * 0.06, 1.0):.2f}s"
    # Hide name label for continuation messages
    name_html = (
        f'<div class="turn-speaker" style="color:{accent_color}">{speaker_name}</div>'
        if not is_continuation
        else ""
    )
    avatar_html = (
        '<div class="turn-avatar is-spacer" aria-hidden="true"></div>'
        if is_continuation
        else (
            f'<div class="turn-avatar" style="color:{accent_color};border-color:{accent_color}66;'
            f'background:{accent_color}1A">{speaker_initial}</div>'
        )
    )

    return (
        f'<article class="turn {side_class} {continuation_class}" '
        f'style="margin-bottom:{bottom_margin};animation-delay:{delay};'
        f'--bubble-bg:{bg_color};--bubble-accent:{accent_color}">\n'
        f"  {avatar_html}\n"
        f'  <div class="turn-bubble">\n'
        f"    {name_html}\n"
        f'    <p class="turn-content">{content}</p>\n'
        f"  </div>\n"
        f"</article>"
    )


def render_conversation_html(
    conversation: Dict[str, Any],
    initiator_name: Optional[str] = None,
) -> str:
    """Render a discovery conversation as a self-contained mobile-friendly HTML page."""
    turns: List[Dict[str, Any]] = conversation.get("turns", [])
    teaser_raw = str(conversation.get("teaser_summary", "")).strip()
    teaser = html.escape(teaser_raw or "Your agents already handled the warm intro details.")

    # Extract match metadata for purpose and reasons display
    match_metadata = conversation.get("match_metadata", {})
    connection_purpose = match_metadata.get("connection_purpose", "")
    matching_reasons = match_metadata.get("matching_reasons", [])

    # Build speaker index map (for consistent coloring)
    speaker_order: Dict[str, int] = {}
    for turn in turns:
        uid = turn.get("speaker_user_id", "")
        if uid not in speaker_order:
            speaker_order[uid] = len(speaker_order)

    # Render turns with continuation detection
    turns_html_parts: List[str] = []
    prev_uid = None
    for i, turn in enumerate(turns):
        uid = turn.get("speaker_user_id", "")
        speaker_idx = speaker_order.get(uid, 0)
        is_right = speaker_idx % 2 == 1
        is_continuation = uid == prev_uid
        turns_html_parts.append(
            _render_turn_html(turn, speaker_idx, is_right, i, is_continuation)
        )
        prev_uid = uid

    turns_html = "\n".join(turns_html_parts)

    # Collect speaker names for the header
    speaker_names: List[str] = []
    seen_uids: set = set()
    first_speaker_name = None  # Track the first speaker (initiator) name
    for i, turn in enumerate(turns):
        uid = turn.get("speaker_user_id", "")
        if uid not in seen_uids:
            seen_uids.add(uid)
            name = turn.get("speaker_name", "Agent")
            # Strip "'s Agent" suffix for the header display
            clean_name = name.replace("'s Agent", "").strip()
            # For first speaker (initiator), use initiator_name if provided
            if i == 0 and initiator_name:
                clean_name = initiator_name
            if i == 0:
                first_speaker_name = clean_name
            speaker_names.append(html.escape(clean_name))

    if len(speaker_names) > 2:
        names_display = ", ".join(speaker_names[:-1]) + f" &amp; {speaker_names[-1]}"
    elif len(speaker_names) == 2:
        names_display = f"{speaker_names[0]} &amp; {speaker_names[1]}"
    else:
        names_display = speaker_names[0] if speaker_names else "Agents"

    # Render new UI sections
    purpose_html = _render_purpose_section(
        initiator_name or first_speaker_name or "Someone",
        connection_purpose,
    )
    reasons_html = _render_matching_reasons(matching_reasons)

    agentic_title = f"What {names_display}'s agents discovered"
    participant_label = (
        f"{len(speaker_names)} participants"
        if len(speaker_names) != 1
        else "1 participant"
    )
    turn_label = f"{len(turns)} turns" if len(turns) != 1 else "1 turn"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>{agentic_title} | Franklink</title>
    <meta property="og:title" content="{agentic_title}">
    <meta property="og:description" content="{teaser}">
    <meta property="og:type" content="article">
    <meta property="og:site_name" content="Franklink">
    <meta name="description" content="{teaser}">
    <style>
        :root {{
            --ink: #172238;
            --ink-soft: #4d5c74;
            --panel: #ffffff;
            --line: #d8e1ef;
            --card-shadow: 0 24px 58px rgba(19, 37, 68, 0.12);
            --hero-start: #07142c;
            --hero-mid: #123768;
            --hero-end: #1895c7;
            --accent: #18a4de;
            --teaser-bg: #f5f9ff;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: "Avenir Next", "SF Pro Display", "Segoe UI Variable", "Trebuchet MS", sans-serif;
            background:
                radial-gradient(circle at 0% 0%, rgba(24, 164, 222, 0.22), transparent 46%),
                radial-gradient(circle at 100% 10%, rgba(255, 166, 77, 0.2), transparent 40%),
                linear-gradient(180deg, #eef3fb 0%, #e8edf6 100%);
            color: var(--ink);
            min-height: 100vh;
            -webkit-font-smoothing: antialiased;
            padding: 18px 14px 24px;
        }}
        .shell {{
            width: min(760px, 100%);
            margin: 0 auto;
            border-radius: 24px;
            overflow: hidden;
            background: var(--panel);
            border: 1px solid #dce5f3;
            box-shadow: var(--card-shadow);
        }}
        .hero {{
            padding: 24px 18px 18px;
            background: linear-gradient(132deg, var(--hero-start) 0%, var(--hero-mid) 53%, var(--hero-end) 100%);
            color: #ffffff;
        }}
        .hero-kicker {{
            display: inline-flex;
            align-items: center;
            gap: 7px;
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 1px;
            text-transform: uppercase;
            color: rgba(255, 255, 255, 0.76);
            background: rgba(255, 255, 255, 0.13);
            border: 1px solid rgba(255, 255, 255, 0.24);
            border-radius: 999px;
            padding: 6px 10px;
            margin-bottom: 14px;
        }}
        .hero-dot {{
            width: 8px;
            height: 8px;
            border-radius: 999px;
            background: #7ef9ac;
            box-shadow: 0 0 0 7px rgba(126, 249, 172, 0.23);
        }}
        .hero-title {{
            font-size: 24px;
            font-weight: 700;
            line-height: 1.28;
            text-wrap: balance;
            margin-bottom: 14px;
        }}
        .hero-meta {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
        }}
        .meta-chip {{
            font-size: 12px;
            line-height: 1;
            color: rgba(255, 255, 255, 0.88);
            border: 1px solid rgba(255, 255, 255, 0.3);
            background: rgba(255, 255, 255, 0.12);
            border-radius: 999px;
            padding: 7px 10px;
        }}
        .teaser {{
            margin: 14px 14px 0;
            background: var(--teaser-bg);
            border: 1px solid #cfe0fb;
            border-radius: 14px;
            padding: 14px 14px;
            font-size: 14px;
            line-height: 1.5;
            color: var(--ink-soft);
        }}
        .conversation {{
            margin: 12px;
            background: #fbfdff;
            border: 1px solid var(--line);
            border-radius: 16px;
            padding: 14px 11px 10px;
        }}
        .turn {{
            display: flex;
            align-items: flex-end;
            gap: 10px;
            opacity: 0;
            animation: fadeUp 0.36s ease-out forwards;
        }}
        .turn.is-right {{
            flex-direction: row-reverse;
        }}
        .turn-avatar {{
            width: 28px;
            height: 28px;
            flex: 0 0 28px;
            border-radius: 999px;
            border: 1px solid;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
            font-size: 12px;
            line-height: 1;
            text-transform: uppercase;
        }}
        .turn-avatar.is-spacer {{
            border: 0;
            background: transparent;
        }}
        .turn-bubble {{
            max-width: min(82%, 520px);
            border-radius: 16px 16px 16px 8px;
            border: 1px solid rgba(29, 55, 95, 0.16);
            background: var(--bubble-bg);
            padding: 10px 12px 11px;
            box-shadow: 0 5px 12px rgba(25, 39, 66, 0.08);
        }}
        .turn.is-right .turn-bubble {{
            border-radius: 16px 16px 8px 16px;
            text-align: left;
        }}
        .turn-speaker {{
            font-size: 11px;
            line-height: 1.25;
            letter-spacing: 0.2px;
            font-weight: 700;
            margin-bottom: 4px;
            text-transform: uppercase;
        }}
        .turn-content {{
            font-size: 15px;
            line-height: 1.45;
            color: var(--ink);
            white-space: pre-wrap;
            word-break: break-word;
        }}
        .turn.is-continuation .turn-bubble {{
            padding-top: 9px;
        }}
        @keyframes fadeUp {{
            from {{ opacity: 0; transform: translateY(8px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}
        .footer {{
            text-align: center;
            padding: 6px 16px 20px;
            font-size: 12px;
            color: #63708a;
            letter-spacing: 0.2px;
        }}
        .footer a {{
            color: #0f6edb;
            font-weight: 600;
            text-decoration: none;
        }}
        /* Connection Purpose Card - Blue theme (consistent) */
        .purpose-card {{
            margin: 14px 14px 0;
            padding: 16px;
            background: var(--teaser-bg);
            border: 1px solid #cfe0fb;
            border-radius: 14px;
        }}
        .purpose-header {{
            display: flex;
            align-items: center;
            gap: 10px;
            margin-bottom: 10px;
        }}
        .purpose-avatar {{
            width: 32px;
            height: 32px;
            border-radius: 999px;
            border: 1px solid;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
            font-size: 14px;
            flex-shrink: 0;
        }}
        .purpose-label {{
            font-size: 13px;
            font-weight: 600;
            color: var(--ink-soft);
        }}
        .purpose-text {{
            font-size: 16px;
            font-weight: 600;
            color: var(--ink);
            line-height: 1.4;
            margin: 0;
            padding-left: 42px;
            font-style: italic;
        }}
        /* Matching Reasons Pills */
        .match-reasons {{
            margin: 14px 14px 0;
        }}
        .reasons-label {{
            font-size: 11px;
            font-weight: 700;
            color: var(--ink-soft);
            text-transform: uppercase;
            letter-spacing: 0.8px;
            margin-bottom: 10px;
        }}
        .reasons-pills {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
        }}
        .reason-pill {{
            display: inline-block;
            font-size: 13px;
            line-height: 1;
            color: #1E40AF;
            background: #EFF6FF;
            border: 1px solid #BFDBFE;
            border-radius: 999px;
            padding: 8px 14px;
        }}
        @media (max-width: 640px) {{
            body {{ padding: 0; background: #edf2f9; }}
            .shell {{
                border-radius: 0;
                border-left: 0;
                border-right: 0;
                min-height: 100vh;
            }}
            .hero-title {{ font-size: 22px; }}
            .turn-bubble {{ max-width: calc(100% - 2px); }}
        }}
        @media (prefers-reduced-motion: reduce) {{
            .turn {{ animation: none; opacity: 1; }}
        }}
    </style>
</head>
<body>
    <main class="shell">
        <header class="hero">
            <div class="hero-kicker"><span class="hero-dot"></span>Live Agent Network</div>
            <h1 class="hero-title">{agentic_title}</h1>
            <div class="hero-meta">
                <span class="meta-chip">{participant_label}</span>
                <span class="meta-chip">{turn_label}</span>
            </div>
        </header>{purpose_html}{reasons_html}
        <aside class="teaser">{teaser}</aside>
        <section class="conversation" aria-label="Conversation transcript">
            {turns_html}
        </section>
        <footer class="footer">
            Powered by <a href="https://franklink.ai">Franklink</a> agents
        </footer>
    </main>
</body>
</html>"""


@router.get("/c/{slug}", response_class=HTMLResponse)
async def view_conversation(slug: str) -> HTMLResponse:
    """Render a discovery conversation as a mobile-friendly HTML page."""
    db = DatabaseClient()
    conversation = await db.get_discovery_conversation_by_slug(slug)
    if not conversation:
        raise HTTPException(status_code=404, detail="Conversation not found")

    # Fetch initiator name from DB for reliable display (avoids "Unknown")
    initiator_name = None
    initiator_user_id = conversation.get("initiator_user_id")
    if initiator_user_id:
        try:
            initiator = await db.get_user_by_id(initiator_user_id)
            if initiator:
                initiator_name = initiator.get("name") or initiator.get("first_name")
        except Exception as e:
            logger.warning(
                "[CONVERSATION_PAGE] Failed to fetch initiator user %s: %s",
                initiator_user_id[:8] if initiator_user_id else "?",
                e,
            )

    page_html = render_conversation_html(conversation, initiator_name=initiator_name)
    return HTMLResponse(content=page_html)
