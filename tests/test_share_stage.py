"""
Test script to verify share_to_complete stage handles image uploads correctly.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import os
import sys
import uuid

from dotenv import load_dotenv

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, REPO_ROOT)


async def main() -> int:
    load_dotenv()

    from app.agents.interaction.agent import InteractionAgent
    from app.database.client import DatabaseClient
    from app.integrations.azure_openai_client import AzureOpenAIClient
    from app.integrations.photon_client import PhotonClient

    db = DatabaseClient()
    openai = AzureOpenAIClient()
    photon = PhotonClient()
    agent = InteractionAgent(db=db, photon=photon, openai=openai)

    # Use a unique test phone number
    phone_number = f"+15550{uuid.uuid4().hex[:6]}"
    print(f"Testing with phone: {phone_number}")

    async def run_turn(message: str, media_url: str | None, turn_num: int):
        user = await db.get_or_create_user(phone_number)
        result = await agent.process_message(
            phone_number=phone_number,
            message_content=message,
            user=user,
            webhook_data={
                "message_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "media_url": media_url,
                "chat_guid": None,
            },
        )

        profile = (result.get("state", {}) or {}).get("user_profile", {})
        stage = profile.get("onboarding_stage")
        is_onboarded = profile.get("is_onboarded")
        intro_fee = profile.get("intro_fee_cents")

        responses = result.get("responses", [])
        response_text = ""
        if responses:
            response_text = responses[-1].get("response_text", "")[:150]
        else:
            response_text = (result.get("response_text") or "")[:150]

        print(f"\n--- Turn {turn_num} ---")
        print(f"User: {message[:60]}{'...' if len(message) > 60 else ''}")
        if media_url:
            print(f"Media: {media_url}")
        print(f"Frank: {response_text}...")
        print(f"Stage: {stage} | Onboarded: {is_onboarded} | Fee: {intro_fee}")

        return result, stage, is_onboarded

    # Messages designed to get through onboarding quickly
    messages = [
        ("hey, i am Alex Chen", None),
        ("MIT", None),
        ("AI and machine learning startups", None),
        ("done, connected", None),
        ("i want to meet technical cofounders and AI researchers for my startup", None),
        ("i'm based in SF, looking in the next month", None),
        # Strong value signals
        ("i'm a former Google ML engineer, published at NeurIPS, built and sold an AI company for $2M", None),
        ("i have direct intros to partners at a16z, sequoia, and greylock. i've helped 5 startups raise seed rounds.", None),
        ("i also advise 3 YC companies on their ML architecture", None),
    ]

    # Run through initial onboarding
    turn = 1
    reached_share = False
    for msg, media in messages:
        result, stage, is_onboarded = await run_turn(msg, media, turn)
        turn += 1
        await asyncio.sleep(0.3)

        # Check if we've reached share_to_complete
        if stage == "share_to_complete":
            print("\n" + "="*60)
            print("REACHED share_to_complete STAGE!")
            print("="*60)
            reached_share = True
            break

    if not reached_share:
        print("\n" + "="*60)
        print("WARNING: Did not reach share_to_complete stage")
        print("Current stage:", stage)
        print("Continuing with screenshot test anyway...")
        print("="*60)

    # Now test: send a screenshot (simulated with media_url)
    print("\n" + "="*60)
    print("TESTING: Sending screenshot in current stage")
    print("="*60)

    result, stage, is_onboarded = await run_turn(
        "",  # Empty message - just an image
        "/path/to/screenshot.png",  # Simulated media URL
        turn
    )

    print("\n" + "="*60)
    print("FINAL RESULT:")
    print("="*60)

    if is_onboarded and stage == "complete":
        profile = result.get("state", {}).get("user_profile", {})
        intro_fee = profile.get("intro_fee_cents")
        print(f"SUCCESS! User is onboarded with intro_fee={intro_fee} cents")
        if intro_fee == 0:
            print("Fee correctly set to $0 for screenshot share!")
        return 0
    elif reached_share:
        print(f"FAILURE! Stage={stage}, is_onboarded={is_onboarded}")
        print("Expected: stage=complete, is_onboarded=True after screenshot")
        return 1
    else:
        print(f"Could not complete test - never reached share_to_complete")
        print(f"Final stage: {stage}")
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
