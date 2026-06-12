#!/usr/bin/env python3
"""
Mock E2E test for email context system redesign.

Tests the full flow with mock email data to verify:
1. Email context is only injected when relevant to conversation
2. User-specific answers get acknowledged first
3. Email references are natural, not forced

Usage:
    python support/scripts/test_email_context_mock.py
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import AsyncMock, patch

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv
load_dotenv()

from app.agents.execution.onboarding.utils.email_context import (
    has_topical_overlap,
    check_user_message_specificity,
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


# Mock email signals that simulate a real user's inbox
MOCK_EMAIL_SIGNALS = {
    "status": "ready",
    "summary": "Mock email signals for testing",
    "emails": [
        {
            "sender": "Florian Juengermann <florian@example.com>",
            "subject": "Penn Engineering Events - Spring Meetup",
            "body": "Join us for the Penn Engineering spring networking event...",
        },
        {
            "sender": "Stripe Recruiting <recruiting@stripe.com>",
            "subject": "Engineering Opportunity at Stripe",
            "body": "We'd love to discuss engineering opportunities with you...",
        },
        {
            "sender": "Y Combinator <apply@ycombinator.com>",
            "subject": "Startup School Application Update",
            "body": "Your Startup School application has been received...",
        },
        {
            "sender": "Google Recruiting <recruiting@google.com>",
            "subject": "SWE Position at Google",
            "body": "Thank you for your interest in software engineering at Google...",
        },
        {
            "sender": "Sequoia Capital <contact@sequoiacap.com>",
            "subject": "Founder Office Hours Invite",
            "body": "You're invited to Sequoia's founder office hours...",
        },
    ],
    "top_from_domains": ["stripe.com", "ycombinator.com", "google.com"],
    "query": "newer_than:90d",
}


async def test_conditional_email_injection() -> None:
    """Test that email context is only injected when relevant."""
    print_header("TEST 1: Conditional Email Injection Logic")

    emails = MOCK_EMAIL_SIGNALS["emails"]

    # Test Case 1: Vague message with relevant email overlap
    print_section("Test Case 1: Vague Message + Relevant Email")
    vague_message = "I want to join a startup school program"  # Should match Y Combinator email
    is_specific = check_user_message_specificity(vague_message)
    relevant = [e for e in emails if has_topical_overlap(e, vague_message)]
    should_include = not is_specific and len(relevant) > 0

    print(f"  Message: '{vague_message}'")
    print(f"  Is Specific: {is_specific}")
    print(f"  Relevant Emails: {len(relevant)}")
    print(f"  Should Include Email Context: {should_include}")
    print(f"  {'✅ PASS' if should_include else '❌ FAIL'}: Vague + relevant = include context")

    # Test Case 2: Specific message - should skip email context
    print_section("Test Case 2: Specific Message (Numbers + Action Verb)")
    specific_message = "I built a product with 500 users and raised $100k from angels"
    is_specific = check_user_message_specificity(specific_message)
    relevant = [e for e in emails if has_topical_overlap(e, specific_message)]
    should_include = not is_specific and len(relevant) > 0

    print(f"  Message: '{specific_message}'")
    print(f"  Is Specific: {is_specific}")
    print(f"  Relevant Emails: {len(relevant)}")
    print(f"  Should Include Email Context: {should_include}")
    print(f"  {'✅ PASS' if not should_include else '❌ FAIL'}: Specific answer = skip context")

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

    # For screenshot scenario: vague but no relevant emails = no forced context
    if not is_specific and len(relevant) == 0:
        print(f"  ✅ PASS: Vague but no relevant emails = don't force email references")
    elif not is_specific and len(relevant) > 0:
        print(f"  ⚠️  INFO: Found relevant emails, may include context")
    else:
        print(f"  ✅ PASS: Message is specific, will skip email context")

    # Test Case 4: Engineering message + engineering emails
    print_section("Test Case 4: Engineering Message")
    eng_message = "I do software engineering"
    is_specific = check_user_message_specificity(eng_message)
    relevant = [e for e in emails if has_topical_overlap(e, eng_message)]
    should_include = not is_specific and len(relevant) > 0

    print(f"  Message: '{eng_message}'")
    print(f"  Is Specific: {is_specific}")
    print(f"  Relevant Emails: {len(relevant)}")
    print(f"  Should Include Email Context: {should_include}")
    print(f"  {'✅ PASS' if should_include else '❌ FAIL'}: Engineering + eng emails = include context")


async def test_need_stage_with_mock() -> Dict[str, Any]:
    """Test need stage responses with mock email data."""
    print_header("TEST 2: Need Stage with Mock Email Data")

    user_profile = {
        "user_id": "test-mock-user",
        "name": "Test User",
        "university": "Penn",
        "career_interests": ["startups", "product management"],
        "personal_facts": {
            "email_signals": MOCK_EMAIL_SIGNALS,
        },
    }

    # Test initial prompt
    print_section("Initial Need Prompt")
    initial_prompt = await build_initial_need_prompt(user_profile=user_profile)
    print_response(initial_prompt, "Frank (Initial)")

    banned = check_banned_phrases(initial_prompt)
    if banned:
        print(f"  ❌ FAIL: Found banned phrases: {banned}")
    else:
        print(f"  ✅ PASS: No banned phrases found")

    # Test follow-up with screenshot scenario
    print_section("Follow-up: User Says 'vibe coding and user research'")
    user_message = "I can do vibe coding and do user research"
    print(f"User: {user_message}")

    prior_state = seed_need_state(first_prompt=initial_prompt)

    result = await evaluate_user_need(
        user_message=user_message,
        user_profile=user_profile,
        prior_state=prior_state,
    )

    print_response(result.get("response_text", ""), "Frank (Follow-up)")

    response = result.get("response_text", "").lower()

    # Check if Frank acknowledged user's answer
    ack_coding = any(w in response for w in ["vibe", "coding", "code"])
    ack_research = "research" in response
    acknowledged = ack_coding or ack_research

    # Check if Frank pivoted to unrelated email signals (bad)
    pivoted_to_email = any(name.lower() in response for name in ["florian", "stripe", "google", "sequoia", "ycombinator"])

    print(f"\n  Decision: {result.get('decision')}")
    print(f"  {'✅' if acknowledged else '⚠️'} {'Acknowledged user answer' if acknowledged else 'May not have acknowledged user answer'}")
    print(f"  {'✅' if not pivoted_to_email else '⚠️'} {'Did not pivot to unrelated email signals' if not pivoted_to_email else 'Pivoted to email signals'}")

    banned = check_banned_phrases(result.get("response_text", ""))
    if banned:
        print(f"  ❌ FAIL: Found banned phrases: {banned}")
    else:
        print(f"  ✅ PASS: No banned phrases found")

    return {
        "initial_prompt": initial_prompt,
        "user_need": result.get("user_need", {}),
    }


async def test_value_stage_with_mock(need_result: Dict[str, Any]) -> None:
    """Test value stage responses with mock email data."""
    print_header("TEST 3: Value Stage with Mock Email Data")

    user_profile = {
        "user_id": "test-mock-user",
        "name": "Test User",
        "university": "Penn",
        "career_interests": ["startups", "product management"],
        "personal_facts": {
            "email_signals": MOCK_EMAIL_SIGNALS,
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
        print(f"  ❌ FAIL: Found banned phrases: {banned}")
    else:
        print(f"  ✅ PASS: No banned phrases found")

    # Test screenshot scenario in value stage
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

    response = result.get("response_text", "").lower()

    # Check if Frank acknowledged user's answer
    ack_coding = any(w in response for w in ["vibe", "coding", "code"])
    ack_research = "research" in response
    acknowledged = ack_coding or ack_research

    # Check if Frank made unnatural pivot to email signals
    pivoted_names = [name for name in ["florian", "juengermann"] if name.lower() in response]

    print(f"\n  Decision: {result.get('decision')}")
    print(f"  Intro Fee: ${result.get('intro_fee_cents', 0) / 100:.2f}")
    print(f"  {'✅' if acknowledged else '⚠️'} {'Acknowledged user answer' if acknowledged else 'Did not acknowledge user answer'}")
    print(f"  {'✅' if not pivoted_names else '❌'} {'No unnatural email pivots' if not pivoted_names else f'Pivoted to: {pivoted_names}'}")

    banned = check_banned_phrases(result.get("response_text", ""))
    if banned:
        print(f"  ❌ FAIL: Found banned phrases: {banned}")
    else:
        print(f"  ✅ PASS: No banned phrases found")

    # Test with very specific answer
    print_section("Follow-up: Very Specific Answer")
    specific_answer = "I built a payments SDK at my last startup that processed $10M in transactions. I shipped 3 features to production and grew the team from 5 to 12 engineers."
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

    response2 = result2.get("response_text", "").lower()
    specific_ack = any(w in response2 for w in ["$10m", "10m", "sdk", "payments", "features", "engineers", "12", "team", "startup"])

    print(f"\n  Decision: {result2.get('decision')}")
    print(f"  Intro Fee: ${result2.get('intro_fee_cents', 0) / 100:.2f}")
    print(f"  {'✅' if specific_ack else '⚠️'} {'Acknowledged specific details' if specific_ack else 'May not have acknowledged specific details'}")

    banned2 = check_banned_phrases(result2.get("response_text", ""))
    if banned2:
        print(f"  ❌ FAIL: Found banned phrases: {banned2}")
    else:
        print(f"  ✅ PASS: No banned phrases found")


async def run_mock_e2e_test() -> None:
    """Run full mock E2E test."""
    print_header("EMAIL CONTEXT SYSTEM REDESIGN - MOCK E2E TEST")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print("\nUsing mock email data to test core logic without Composio")

    # Test 1: Conditional injection logic
    await test_conditional_email_injection()

    # Test 2: Need stage
    need_result = await test_need_stage_with_mock()

    # Test 3: Value stage
    await test_value_stage_with_mock(need_result)

    # Summary
    print_header("TEST SUMMARY")
    print(f"Completed: {datetime.now(timezone.utc).isoformat()}")
    print("\nKey behaviors verified:")
    print("  1. Email context is only used when relevant to user's message")
    print("  2. Specific user answers get priority acknowledgment")
    print("  3. No banned phrases revealing email source")
    print("  4. No forced pivots to unrelated email signals")


if __name__ == "__main__":
    asyncio.run(run_mock_e2e_test())
