#!/usr/bin/env python3
"""
E2E test for email context system redesign.

Tests that:
1. Email context is only injected when relevant to conversation
2. User-specific answers get acknowledged first
3. Email references are natural, not forced

Usage:
    python support/scripts/test_email_context_e2e.py --connected-account-id ca_IkJAa2JWlHLA
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv
load_dotenv()

from app.integrations.composio_client import ComposioClient
from app.agents.execution.onboarding.utils.email_context import (
    has_topical_overlap,
    check_user_message_specificity,
    build_email_signals,
)
from app.agents.execution.onboarding.utils.need_proof import (
    build_initial_need_prompt,
    evaluate_user_need,
    seed_need_state,
)
from app.agents.execution.onboarding.utils.value_proof import (
    build_initial_gate_prompt,
    evaluate_user_value,
)


def print_header(title: str) -> None:
    print("\n" + "=" * 70)
    print(f" {title}")
    print("=" * 70)


def print_section(title: str) -> None:
    print(f"\n{'─' * 70}")
    print(f" {title}")
    print(f"{'─' * 70}")


def print_response(response: str, label: str = "Frank") -> None:
    """Print Frank's response with bubble formatting."""
    print(f"\n{label}:")
    bubbles = response.split("\n\n")
    for i, bubble in enumerate(bubbles, 1):
        print(f"  [{i}] {bubble.strip()}")
    print(f"\n  (Total: {len(bubbles)} bubbles)")


def check_banned_phrases(response: str) -> List[str]:
    """Check for banned phrases that reveal email source."""
    response_lower = response.lower()
    banned = [
        "inbox", "your inbox", "emails", "your emails",
        "i see", "i noticed", "based on", "looks like",
        "i can tell", "it seems", "appears", "suggests",
        "from what i know", "your emails show"
    ]
    found = [phrase for phrase in banned if phrase in response_lower]
    return found


async def fetch_emails_via_composio(
    connected_account_id: str,
    test_user_id: str = "test-email-context-user"
) -> Dict[str, Any]:
    """Fetch emails using connected_account_id directly."""
    print_header("STEP 1: Fetch Emails via Composio")

    client = ComposioClient()
    if not client.is_available():
        print("❌ Composio client not available")
        return {"status": "error", "emails": []}

    print(f"Connected Account ID: {connected_account_id}")
    print(f"Test User ID: {test_user_id}")

    # Fetch emails
    threads = await client.fetch_recent_threads(
        user_id=test_user_id,
        connected_account_id=connected_account_id,
        query="newer_than:90d",
        limit=10,
    )

    print(f"\nFetched {len(threads)} email threads")

    if not threads:
        print("❌ No emails fetched")
        return {"status": "error", "emails": [], "error": "no_emails"}

    # Build email signals
    signals = build_email_signals(
        threads=threads,
        query="newer_than:90d",
        max_evidence=10,
    )

    emails = signals.get("emails", [])
    print(f"Processed {len(emails)} emails into signals")

    if emails:
        print("\nEmail samples:")
        for i, email in enumerate(emails[:5], 1):
            print(f"  {i}. {email.get('subject', 'N/A')[:50]}")
            print(f"     From: {email.get('sender', 'N/A')[:40]}")

    print("\n✅ PASSED: Emails fetched successfully")
    return signals


async def test_conditional_email_injection(email_signals: Dict[str, Any]) -> None:
    """Test that email context is only injected when relevant."""
    print_header("STEP 2: Test Conditional Email Injection")

    emails = email_signals.get("emails", [])
    if not emails:
        print("❌ No emails to test with")
        return

    # Test Case 1: Vague message - should check for relevant emails
    print_section("Test Case 1: Vague User Message")
    vague_message = "I like startups"
    is_specific = check_user_message_specificity(vague_message)
    relevant = [e for e in emails if has_topical_overlap(e, vague_message)]
    should_include = not is_specific and len(relevant) > 0

    print(f"  Message: '{vague_message}'")
    print(f"  Is Specific: {is_specific}")
    print(f"  Relevant Emails: {len(relevant)}")
    print(f"  Should Include Email Context: {should_include}")
    print(f"  {'✅' if not is_specific else '❌'} Correctly identified as vague")

    # Test Case 2: Specific message with numbers - should skip email context
    print_section("Test Case 2: Specific User Message")
    specific_message = "I built a product with 500 users and raised $100k"
    is_specific = check_user_message_specificity(specific_message)
    relevant = [e for e in emails if has_topical_overlap(e, specific_message)]
    should_include = not is_specific and len(relevant) > 0

    print(f"  Message: '{specific_message}'")
    print(f"  Is Specific: {is_specific}")
    print(f"  Relevant Emails: {len(relevant)}")
    print(f"  Should Include Email Context: {should_include}")
    print(f"  {'✅' if is_specific else '❌'} Correctly identified as specific")
    print(f"  {'✅' if not should_include else '❌'} Will skip email context (focus on user answer)")

    # Test Case 3: The screenshot scenario
    print_section("Test Case 3: Screenshot Scenario")
    screenshot_message = "I can do vibe coding and do user research"
    is_specific = check_user_message_specificity(screenshot_message)
    relevant = [e for e in emails if has_topical_overlap(e, screenshot_message)]
    should_include = not is_specific and len(relevant) > 0

    print(f"  Message: '{screenshot_message}'")
    print(f"  Is Specific: {is_specific}")
    print(f"  Relevant Emails: {len(relevant)}")
    print(f"  Should Include Email Context: {should_include}")
    print(f"  {'✅' if not is_specific else '❌'} Correctly identified as vague")
    if len(relevant) == 0:
        print(f"  {'✅'} No relevant emails - will NOT force email references")
    else:
        print(f"  Found {len(relevant)} potentially relevant emails")


async def test_need_stage_responses(email_signals: Dict[str, Any]) -> Dict[str, Any]:
    """Test need stage with new email context system."""
    print_header("STEP 3: Test Need Stage Responses")

    user_profile = {
        "user_id": "test-email-context-user",
        "name": "Test User",
        "university": "Penn",
        "career_interests": ["startups", "product management"],
        "personal_facts": {
            "email_signals": email_signals,
        },
    }

    # Test initial prompt
    print_section("Initial Need Prompt")
    initial_prompt = await build_initial_need_prompt(user_profile=user_profile)
    print_response(initial_prompt, "Frank (Initial)")

    # Check for banned phrases
    banned = check_banned_phrases(initial_prompt)
    if banned:
        print(f"  ❌ Found banned phrases: {banned}")
    else:
        print(f"  ✅ No banned phrases found")

    # Test follow-up with specific user answer
    print_section("Follow-up with Specific Answer")
    specific_answer = "I want to meet early-stage VCs who invest in fintech startups. I'm building a payments product for SMBs and have 200 beta users"
    print(f"User: {specific_answer}")

    prior_state = seed_need_state(first_prompt=initial_prompt)
    result = await evaluate_user_need(
        user_message=specific_answer,
        user_profile=user_profile,
        prior_state=prior_state,
    )

    print_response(result.get("response_text", ""), "Frank (Follow-up)")

    # Check if Frank acknowledged user's specific details
    response = result.get("response_text", "").lower()
    acknowledged = any(word in response for word in ["200", "fintech", "payments", "smb", "beta"])

    print(f"\n  Decision: {result.get('decision')}")
    print(f"  {'✅' if acknowledged else '⚠️'} {'Acknowledged user specifics' if acknowledged else 'May not have acknowledged user specifics'}")

    banned = check_banned_phrases(result.get("response_text", ""))
    if banned:
        print(f"  ❌ Found banned phrases: {banned}")
    else:
        print(f"  ✅ No banned phrases found")

    return {
        "initial_prompt": initial_prompt,
        "user_need": result.get("user_need", {}),
    }


async def test_value_stage_responses(
    email_signals: Dict[str, Any],
    need_result: Dict[str, Any],
) -> None:
    """Test value stage with new email context system."""
    print_header("STEP 4: Test Value Stage Responses")

    user_profile = {
        "user_id": "test-email-context-user",
        "name": "Test User",
        "university": "Penn",
        "career_interests": ["startups", "product management"],
        "personal_facts": {
            "email_signals": email_signals,
        },
        "need_eval_state": {
            "status": "accepted",
            "user_need": need_result.get("user_need", {
                "targets": ["VCs", "startup founders"],
                "outcomes": ["funding", "mentorship"],
            }),
        },
    }

    # Test initial value gate
    print_section("Initial Value Gate")
    initial_gate = await build_initial_gate_prompt(
        phone_number="+15551234567",
        user_profile=user_profile,
    )
    print_response(initial_gate, "Frank (Value Gate)")

    banned = check_banned_phrases(initial_gate)
    if banned:
        print(f"  ❌ Found banned phrases: {banned}")
    else:
        print(f"  ✅ No banned phrases found")

    # Test follow-up with specific user answer (the screenshot scenario)
    print_section("Follow-up: Screenshot Scenario")
    screenshot_answer = "I can do vibe coding and do user research"
    print(f"User: {screenshot_answer}")

    prior_state = {
        "status": "pending",
        "mode": "evaluating",
        "asked_questions": [initial_gate],
        "turn_history": [{"role": "frank", "content": initial_gate}],
        "user_value": {},
        "intro_fee_cents": 9900,
    }

    result = await evaluate_user_value(
        phone_number="+15551234567",
        user_message=screenshot_answer,
        user_profile=user_profile,
        prior_state=prior_state,
    )

    print_response(result.get("response_text", ""), "Frank (Value Follow-up)")

    # Check if Frank pivoted to unrelated email signals (bad) or acknowledged answer (good)
    response = result.get("response_text", "").lower()

    # Check for acknowledgment of user's answer
    acknowledged_coding = any(word in response for word in ["vibe", "coding", "code"])
    acknowledged_research = "research" in response
    acknowledged = acknowledged_coding or acknowledged_research

    print(f"\n  Decision: {result.get('decision')}")
    print(f"  Intro Fee: ${result.get('intro_fee_cents', 0) / 100:.2f}")
    print(f"  {'✅' if acknowledged else '⚠️'} {'Acknowledged user answer' if acknowledged else 'May not have acknowledged user answer'}")

    banned = check_banned_phrases(result.get("response_text", ""))
    if banned:
        print(f"  ❌ Found banned phrases: {banned}")
    else:
        print(f"  ✅ No banned phrases found")

    # Test with very specific answer
    print_section("Follow-up: Very Specific Answer")
    specific_answer = "I built a payments SDK at Stripe that processed $10M in transactions. I also shipped 3 features to production and grew the team from 5 to 12 engineers"
    print(f"User: {specific_answer}")

    prior_state["turn_history"].append({"role": "user", "content": screenshot_answer})
    prior_state["turn_history"].append({"role": "frank", "content": result.get("response_text", "")})
    prior_state["asked_questions"].append(result.get("response_text", ""))
    prior_state["intro_fee_cents"] = result.get("intro_fee_cents", 990)

    result2 = await evaluate_user_value(
        phone_number="+15551234567",
        user_message=specific_answer,
        user_profile=user_profile,
        prior_state=prior_state,
    )

    print_response(result2.get("response_text", ""), "Frank (Specific Answer)")

    # Check if Frank acknowledged the specific details
    response2 = result2.get("response_text", "").lower()
    specific_acknowledged = any(word in response2 for word in ["stripe", "$10m", "10m", "sdk", "payments", "features", "engineers", "12", "team"])

    print(f"\n  Decision: {result2.get('decision')}")
    print(f"  Intro Fee: ${result2.get('intro_fee_cents', 0) / 100:.2f}")
    print(f"  {'✅' if specific_acknowledged else '⚠️'} {'Acknowledged specific details' if specific_acknowledged else 'May not have acknowledged specific details'}")

    banned2 = check_banned_phrases(result2.get("response_text", ""))
    if banned2:
        print(f"  ❌ Found banned phrases: {banned2}")
    else:
        print(f"  ✅ No banned phrases found")


async def run_e2e_test(connected_account_id: str) -> None:
    """Run full E2E test."""
    print_header("EMAIL CONTEXT SYSTEM REDESIGN - E2E TEST")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print(f"Connected Account ID: {connected_account_id}")

    # Step 1: Fetch emails
    email_signals = await fetch_emails_via_composio(connected_account_id)
    if email_signals.get("status") == "error":
        print("\n" + "=" * 70)
        print(" TEST ABORTED: Could not fetch email context")
        print("=" * 70)
        return

    # Step 2: Test conditional injection logic
    await test_conditional_email_injection(email_signals)

    # Step 3: Test need stage
    need_result = await test_need_stage_responses(email_signals)

    # Step 4: Test value stage
    await test_value_stage_responses(email_signals, need_result)

    # Summary
    print_header("TEST COMPLETE")
    print(f"Completed: {datetime.now(timezone.utc).isoformat()}")
    print("\nKey behaviors to verify:")
    print("  1. Email context is only used when relevant to user's message")
    print("  2. Specific user answers are acknowledged FIRST")
    print("  3. No banned phrases that reveal email source")
    print("  4. No forced pivots to unrelated email signals")


def main():
    parser = argparse.ArgumentParser(description="E2E test for email context system redesign")
    parser.add_argument(
        "--connected-account-id",
        required=True,
        help="Composio connected account ID (e.g., ca_IkJAa2JWlHLA)"
    )
    args = parser.parse_args()

    asyncio.run(run_e2e_test(args.connected_account_id))


if __name__ == "__main__":
    main()
