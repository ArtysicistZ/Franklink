"""Capability definitions for Frank's tasks.

This module defines what Frank CAN do (finite set) rather than trying to
enumerate what it cannot do (infinite set). When a user requests something
outside these boundaries, Frank should gracefully decline with a helpful response.

Each task has:
- can_do: Exhaustive list of what the task CAN accomplish
- common_misconceptions: Things users often ask for that are outside scope
"""

from typing import Dict, List, Any


NETWORKING_CAPABILITIES = {
    "can_do": [
        "Find and suggest matches in the Franklink network based on user's networking needs",
        "Suggest connection purposes based on user's email activity (via Zep)",
        "Create and manage connection requests between Franklink users",
        "Send invitation notifications to matched users (after user confirms the match)",
        "Create iMessage group chats when both parties accept a connection",
        "Retrieve information about user's existing connections and connection history",
        "Cancel or modify pending connection requests",
        "Handle multi-match requests (e.g., finding multiple study partners)",
    ],
    "common_misconceptions": [
        {
            "patterns": [
                "send my resume",
                "share my resume",
                "forward my resume",
                "send my portfolio",
                "share my cv",
                "attach my",
                "send my file",
                "share my document",
                "forward my document",
                "send them my",
            ],
            "category": "document_sharing",
            "graceful_response": "i can connect you with them directly, once you're in a group chat you can share your resume yourself. want me to make the intro?",
        },
        {
            "patterns": [
                "email them",
                "send them an email",
                "contact them on linkedin",
                "message them on",
                "reach out via email",
                "send an email to",
                "text them outside",
            ],
            "category": "external_messaging",
            "graceful_response": "i can only make intros within franklink via imessage. want me to find a match and set up a group chat?",
        },
        {
            "patterns": [
                "apply for me",
                "submit application",
                "send my application",
                "fill out the form",
                "apply to this job",
                "submit my resume to",
            ],
            "category": "job_application",
            "graceful_response": "can't apply for you, but i can connect you with someone at that company who might be able to refer you. want me to look?",
        },
        {
            "patterns": [
                "find their phone number",
                "get their email",
                "share their contact",
                "give me their linkedin",
                "what's their email",
                "what's their phone",
                "tell me their contact",
            ],
            "category": "contact_disclosure",
            "graceful_response": "i protect everyone's contact info, that's shared naturally in the group chat once you're both connected. want me to make an intro first?",
        },
        {
            "patterns": [
                "schedule a meeting with them directly",
                "book time on their calendar",
                "set up a call with them",
                "arrange a meeting directly",
            ],
            "category": "direct_scheduling",
            "graceful_response": "i set up group chats where you two can coordinate directly. want me to find a match first?",
        },
    ],
}

UPDATE_CAPABILITIES = {
    "can_do": [
        "Update user's own profile fields (name, university, year, major, career_interests)",
        "Add new items to user's demand history (what they're looking for)",
        "Modify or delete existing items in user's demand history",
        "Add new items to user's value history (what they can offer)",
        "Modify or delete existing items in user's value history",
        "Refresh embeddings after demand/value updates",
    ],
    "common_misconceptions": [
        {
            "patterns": [
                "update someone else's profile",
                "change their info",
                "modify their profile",
                "update their school",
                "change their name",
                "edit their profile",
                "change his school",
                "change her school",
                "update his profile",
                "update her profile",
                "change [name]'s school",
                "update [name]'s profile",
                "change [name]'s info",
            ],
            "category": "modify_others",
            "graceful_response": "i can only update your own profile. what would you like to change about yours?",
            "detection_rule": "Any request to modify profile info (school, name, year, major, etc.) for ANYONE other than the user making the request. Look for: possessives with names (e.g., 'jimmy's school'), pronouns ('his/her/their school'), or explicit mentions ('change someone else's').",
        },
        {
            "patterns": [
                "delete my account",
                "remove my profile",
                "deactivate my account",
            ],
            "category": "account_deletion",
            "graceful_response": "i can't delete accounts, but if you want to stop using franklink just let me know and i won't reach out anymore",
        },
    ],
}

GROUPCHAT_MAINTENANCE_CAPABILITIES = {
    "can_do": [
        "Generate and share relevant news articles with discussion polls in group chats",
        "Schedule meetings for group chat participants",
        "Retrieve information about group chat participants",
        "Send messages to existing group chats on user's behalf",
    ],
    "common_misconceptions": [
        {
            "patterns": [
                "create a new group",
                "start a chat with",
                "add someone to group",
                "add them to the group",
                "invite someone to the group",
                "make a new group chat",
            ],
            "category": "group_creation",
            "graceful_response": "group chats get created automatically when both people accept a connection. want me to help find someone to connect with first?",
        },
        {
            "patterns": [
                "remove someone from group",
                "kick them from the group",
                "delete this group",
            ],
            "category": "group_modification",
            "graceful_response": "i can't remove people from groups or delete them, but you can leave the group yourself if you want",
        },
    ],
}


def get_capability_boundaries_for_prompt() -> str:
    """Format capability boundaries for injection into the routing decision prompt.

    Returns:
        Formatted string describing what Frank can and cannot do.
    """
    sections = []

    # Networking capabilities
    networking_can_do = "\n".join(
        f"  - {item}" for item in NETWORKING_CAPABILITIES["can_do"]
    )
    networking_cannot = "\n".join(
        f"  - {m['category'].replace('_', ' ').title()}: e.g., {', '.join(m['patterns'][:3])}"
        for m in NETWORKING_CAPABILITIES["common_misconceptions"]
    )

    sections.append(f"""### Networking Task Boundaries
What Frank CAN do:
{networking_can_do}

What Frank CANNOT do (common misconceptions):
{networking_cannot}""")

    # Update capabilities
    update_can_do = "\n".join(
        f"  - {item}" for item in UPDATE_CAPABILITIES["can_do"]
    )
    update_cannot_lines = []
    for m in UPDATE_CAPABILITIES["common_misconceptions"]:
        line = f"  - {m['category'].replace('_', ' ').title()}: e.g., {', '.join(m['patterns'][:3])}"
        if "detection_rule" in m:
            line += f"\n    Detection: {m['detection_rule']}"
        update_cannot_lines.append(line)
    update_cannot = "\n".join(update_cannot_lines)

    sections.append(f"""### Update Task Boundaries
What Frank CAN do:
{update_can_do}

What Frank CANNOT do:
{update_cannot}""")

    # Group chat capabilities
    gc_can_do = "\n".join(
        f"  - {item}" for item in GROUPCHAT_MAINTENANCE_CAPABILITIES["can_do"]
    )
    gc_cannot = "\n".join(
        f"  - {m['category'].replace('_', ' ').title()}: e.g., {', '.join(m['patterns'][:2])}"
        for m in GROUPCHAT_MAINTENANCE_CAPABILITIES["common_misconceptions"]
    )

    sections.append(f"""### Group Chat Maintenance Task Boundaries
What Frank CAN do:
{gc_can_do}

What Frank CANNOT do:
{gc_cannot}""")

    return "\n\n".join(sections)


def get_all_misconception_patterns() -> List[Dict[str, Any]]:
    """Get all misconception patterns across all tasks.

    Returns:
        List of misconception dicts with patterns, category, and graceful_response.
    """
    all_misconceptions = []
    all_misconceptions.extend(NETWORKING_CAPABILITIES["common_misconceptions"])
    all_misconceptions.extend(UPDATE_CAPABILITIES["common_misconceptions"])
    all_misconceptions.extend(GROUPCHAT_MAINTENANCE_CAPABILITIES["common_misconceptions"])
    return all_misconceptions


def format_cannot_fulfill_for_synthesis(cannot_fulfill: Dict[str, Any]) -> str:
    """Format cannot_fulfill data for inclusion in synthesis prompt.

    Args:
        cannot_fulfill: Dict with components list and all_unfulfillable flag

    Returns:
        Formatted string for synthesis prompt context
    """
    if not cannot_fulfill:
        return ""

    components = cannot_fulfill.get("components", [])
    if not components:
        return ""

    lines = ["## Parts of the Request That Couldn't Be Fulfilled\n"]
    for comp in components:
        request_text = comp.get("request_text", "unknown request")
        category = comp.get("category", "unknown")
        hint = comp.get("graceful_decline_hint", "")
        lines.append(f"- Request: \"{request_text}\"")
        lines.append(f"  Category: {category.replace('_', ' ')}")
        if hint:
            lines.append(f"  Hint for response: {hint}")
        lines.append("")

    lines.append("""When responding:
- CRITICAL: Do NOT say you did something you didn't do. If no action was taken, don't claim you took action.
  - BAD: "got it, changed yincheng's school to drexel" (action wasn't taken!)
  - GOOD: "i can only update your own profile, not someone else's"
- If actions_summary says "No actions taken", then NOTHING was accomplished
- You MAY offer an alternative (e.g., "can't send your resume, but i can connect you directly")
- If offering to do something, phrase it as a question/offer, NOT as "i'm doing it now"
  - GOOD: "want me to connect you two instead?"
  - BAD: "i'll reach out to him now" (implies action taken when it wasn't)
- Don't apologize excessively - be matter-of-fact like frank would
- Keep frank's casual tone - lowercase, no emojis, 2-4 sentences""")

    return "\n".join(lines)
