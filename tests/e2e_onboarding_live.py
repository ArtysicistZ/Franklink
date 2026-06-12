from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
import os
import sys
import uuid
import random

from dotenv import load_dotenv

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, REPO_ROOT)


def _build_default_messages() -> list[str]:
    return [
        "hey, i am alex",
        "i go to usc",
        "i am into product and ai",
        "done, connected",
        "i want to meet product leads at consumer startups, mainly for feedback and early internships",
        "ideally in la or remote, next 2-3 months",
        "i built a campus marketplace used by 1.8k students and ran growth to 12 percent weekly",
        "i can share playbooks on onboarding funnels and growth loops, plus i have contacts at two seed funds",
        "i also ran a student builder club and can connect people to a few solid engineers",
    ]


def _read_script(path: str) -> list[str]:
    messages: list[str] = []
    with open(path, "r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            messages.append(line)
    return messages


def _random_test_phone() -> str:
    suffix = random.randint(0, 9_999_999)
    return f"+1555{suffix:07d}"


def _format_state_line(state: dict) -> str:
    profile = (state.get("user_profile") or {}) if isinstance(state, dict) else {}
    stage = profile.get("onboarding_stage")
    waiting_for = state.get("waiting_for")
    intro_fee = profile.get("intro_fee_cents")
    return f"state: onboarding_stage={stage} waiting_for={waiting_for} intro_fee_cents={intro_fee}"


async def _run_turn(
    *,
    agent,
    db,
    phone_number: str,
    message: str,
    turn_index: int,
    include_state: bool,
) -> list[str]:
    transcript_lines: list[str] = []
    user = await db.get_or_create_user(phone_number)
    result = await agent.process_message(
        phone_number=phone_number,
        message_content=message,
        user=user,
        webhook_data={
            "message_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "media_url": None,
            "chat_guid": None,
        },
    )

    transcript_lines.append(f"-- turn {turn_index} --")
    transcript_lines.append(f"user: {message}")

    if not result.get("success", False):
        transcript_lines.append(f"frank: [error] {result.get('error')}")
        return transcript_lines

    responses = result.get("responses")
    if isinstance(responses, list) and responses:
        for item in responses:
            response_text = str(item.get("response_text") or "").strip()
            if response_text:
                transcript_lines.append(f"frank: {response_text}")
            outbound = item.get("outbound_messages", []) or []
            if isinstance(outbound, list) and outbound:
                for msg in outbound:
                    msg_text = str(msg or "").strip()
                    if msg_text:
                        transcript_lines.append(f"frank: {msg_text}")
    else:
        response_text = str(result.get("response_text") or "").strip()
        if response_text:
            transcript_lines.append(f"frank: {response_text}")
        outbound = (result.get("state") or {}).get("temp_data", {}).get("outbound_messages", []) or []
        if isinstance(outbound, list) and outbound:
            for msg in outbound:
                msg_text = str(msg or "").strip()
                if msg_text:
                    transcript_lines.append(f"frank: {msg_text}")

    if include_state:
        transcript_lines.append(_format_state_line(result.get("state") or {}))

    return transcript_lines


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a live onboarding E2E script with real LLM + DB calls.",
    )
    parser.add_argument(
        "--phone-number",
        default=None,
        help="Test phone number (defaults to a random +1555XXXXXXX).",
    )
    parser.add_argument(
        "--script",
        default=None,
        help="Path to a newline-delimited script of user messages.",
    )
    parser.add_argument(
        "--message",
        action="append",
        default=[],
        help="User message (can be provided multiple times).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path to write the transcript (defaults under support/scripts/output).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Seconds to sleep between turns.",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Prompt for each user message instead of using a script.",
    )
    parser.add_argument(
        "--include-state",
        action="store_true",
        help="Include onboarding stage and fee state after each turn.",
    )
    args = parser.parse_args()

    load_dotenv()

    from app.agents.interaction.agent import InteractionAgent
    from app.database.client import DatabaseClient
    from app.integrations.azure_openai_client import AzureOpenAIClient
    from app.integrations.photon_client import PhotonClient

    phone_number = args.phone_number or _random_test_phone()
    transcript_lines: list[str] = []
    transcript_lines.append("onboarding e2e transcript")
    transcript_lines.append(f"started_at: {datetime.utcnow().isoformat()}Z")
    transcript_lines.append(f"phone_number: {phone_number}")

    if args.interactive:
        messages: list[str] = []
        print("interactive mode; press enter on a blank line to stop.")
        while True:
            user_input = input("user> ").strip()
            if not user_input:
                break
            messages.append(user_input)
    elif args.message:
        messages = [m.strip() for m in args.message if m.strip()]
    elif args.script:
        messages = _read_script(args.script)
    else:
        messages = _build_default_messages()

    if not messages:
        print("no messages provided; exiting.")
        return 1

    db = DatabaseClient()
    openai = AzureOpenAIClient()
    photon = PhotonClient()
    agent = InteractionAgent(db=db, photon=photon, openai=openai)

    for idx, msg in enumerate(messages, start=1):
        turn_lines = await _run_turn(
            agent=agent,
            db=db,
            phone_number=phone_number,
            message=msg,
            turn_index=idx,
            include_state=args.include_state,
        )
        transcript_lines.extend(turn_lines)
        transcript_lines.append("")
        if args.sleep > 0:
            await asyncio.sleep(args.sleep)

    output_path = args.output
    if not output_path:
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_dir = os.path.join(os.path.dirname(__file__), "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f"onboarding_transcript_{stamp}.txt")

    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(transcript_lines).rstrip() + "\n")

    print(f"transcript written to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
