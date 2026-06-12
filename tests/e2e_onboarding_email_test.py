#!/usr/bin/env python3
"""
E2E test for Frank onboarding with email context.

Tests that Frank explicitly references email content in need/value stage responses.

Usage:
    python support/scripts/e2e_onboarding_email_test.py

Requirements:
    - Valid Composio API key with a connected Gmail account
    - Set USER_ID env var or use default test user
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.agents.execution.onboarding.utils.email_context import (
    fetch_email_signals,
    select_email_context_for_prompt,
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


# Test configuration
TEST_USER_ID = os.environ.get("TEST_USER_ID", "94947aec-432e-40e1-94ea-89d2859997c6")
TEST_PHONE = os.environ.get("TEST_PHONE", "+15551234567")
VERBOSE = os.environ.get("VERBOSE", "1") == "1"


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


def check_email_reference(response: str, emails: list) -> Dict[str, Any]:
    """Check if response references email content."""
    response_lower = response.lower()

    references_found = []
    for email in emails:
        sender = email.get("sender", "").lower()
        subject = email.get("subject", "").lower()
        body = email.get("body", "").lower()

        # Check for sender references
        if sender:
            # Extract name/domain from sender
            parts = sender.replace('"', '').replace('<', ' ').replace('>', ' ').split()
            for part in parts:
                if len(part) > 3 and part in response_lower:
                    references_found.append(f"sender: {part}")
                    break

        # Check for subject keywords (meaningful words)
        if subject:
            words = [w for w in subject.split() if len(w) > 4 and w.isalpha()]
            for word in words[:5]:
                if word in response_lower:
                    references_found.append(f"subject: {word}")
                    break

    # Check for generic email reference patterns
    generic_patterns = [
        "inbox", "your inbox", "i see", "you've been",
        "looks like", "based on", "your recent", "emails show",
        "wuec", "wharton", "penn engineering", "zapier",
        "club fair", "master", "recruiting", "interview",
    ]
    generic_found = [p for p in generic_patterns if p in response_lower]

    return {
        "explicit_references": references_found,
        "generic_patterns": generic_found,
        "has_explicit": len(references_found) > 0,
        "has_generic": len(generic_found) > 0,
        "score": len(references_found) * 2 + len(generic_found),
    }


async def test_email_fetch() -> Optional[Dict[str, Any]]:
    """Test email fetching from Composio."""
    print_header("STEP 1: Fetch Email Context")

    print(f"User ID: {TEST_USER_ID}")

    signals = await fetch_email_signals(user_id=TEST_USER_ID)

    status = signals.get("status")
    print(f"Status: {status}")
    print(f"Summary: {signals.get('summary')}")

    emails = signals.get("emails", [])
    print(f"Emails fetched: {len(emails)}")

    if status != "ready" or not emails:
        print("\n❌ FAILED: Could not fetch emails")
        print(f"   Error: {signals.get('error', 'unknown')}")
        return None

    print("\nEmail samples:")
    for i, email in enumerate(emails[:3], 1):
        print(f"  {i}. {email.get('subject', 'N/A')[:50]}")
        print(f"     From: {email.get('sender', 'N/A')[:40]}")
        body_preview = email.get('body', '')[:80].replace('\n', ' ')
        print(f"     Body: {body_preview}...")

    print("\n✅ PASSED: Emails fetched successfully")
    return signals


async def test_need_stage(email_signals: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Test need stage with email context."""
    print_header("STEP 2: Need Stage - Initial Prompt")

    # Build user profile with email signals
    user_profile = {
        "user_id": TEST_USER_ID,
        "name": "Test User",
        "university": "Penn",
        "career_interests": ["startups", "product management"],
        "personal_facts": {
            "email_signals": email_signals,
        },
    }

    # Get initial need prompt
    print("Generating initial need prompt...")
    initial_prompt = await build_initial_need_prompt(user_profile=user_profile)

    print_response(initial_prompt, "Frank (Initial Need)")

    # Check email references
    emails = email_signals.get("emails", [])
    ref_check = check_email_reference(initial_prompt, emails)

    print(f"\nEmail Reference Check:")
    print(f"  Explicit references: {ref_check['explicit_references']}")
    print(f"  Generic patterns: {ref_check['generic_patterns']}")
    print(f"  Score: {ref_check['score']}")

    if ref_check["has_explicit"]:
        print("\n✅ PASSED: Frank explicitly referenced email content")
    elif ref_check["has_generic"]:
        print("\n⚠️  PARTIAL: Frank used generic email patterns but no explicit references")
    else:
        print("\n❌ FAILED: Frank did not reference email context")

    # Test need evaluation with user response
    print_section("Need Stage - User Response")

    user_message = "I want to meet VCs and startup founders for my fintech idea"
    print(f"User: {user_message}")

    prior_state = seed_need_state(first_prompt=initial_prompt)

    result = await evaluate_user_need(
        user_message=user_message,
        user_profile=user_profile,
        prior_state=prior_state,
    )

    print_response(result.get("response_text", ""), "Frank (Follow-up)")
    print(f"\nDecision: {result.get('decision')}")
    print(f"User Need: {json.dumps(result.get('user_need', {}), indent=2)}")
    print(f"Confidence: {result.get('confidence')}")

    # Check email references in follow-up
    ref_check2 = check_email_reference(result.get("response_text", ""), emails)
    print(f"\nEmail Reference Check (Follow-up):")
    print(f"  Explicit references: {ref_check2['explicit_references']}")
    print(f"  Score: {ref_check2['score']}")

    return {
        "initial_prompt": initial_prompt,
        "user_need": result.get("user_need", {}),
        "ref_score": ref_check["score"] + ref_check2["score"],
    }


async def test_value_stage(
    email_signals: Dict[str, Any],
    need_result: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Test value stage with email context."""
    print_header("STEP 3: Value Stage - Initial Gate")

    # Build user profile with need state embedded
    user_profile = {
        "user_id": TEST_USER_ID,
        "name": "Test User",
        "university": "Penn",
        "career_interests": ["startups", "product management"],
        "personal_facts": {
            "email_signals": email_signals,
        },
        # Need state for value stage
        "need_eval_state": {
            "status": "accepted",
            "user_need": need_result.get("user_need", {
                "targets": ["VCs", "startup founders"],
                "outcomes": ["funding", "mentorship"],
            }),
        },
    }

    # Get initial value gate prompt
    print("Generating initial value gate prompt...")
    initial_gate = await build_initial_gate_prompt(
        phone_number=TEST_PHONE,
        user_profile=user_profile,
    )

    print_response(initial_gate, "Frank (Initial Value)")

    # Check email references
    emails = email_signals.get("emails", [])
    ref_check = check_email_reference(initial_gate, emails)

    print(f"\nEmail Reference Check:")
    print(f"  Explicit references: {ref_check['explicit_references']}")
    print(f"  Generic patterns: {ref_check['generic_patterns']}")
    print(f"  Score: {ref_check['score']}")

    if ref_check["has_explicit"]:
        print("\n✅ PASSED: Frank explicitly referenced email content")
    elif ref_check["has_generic"]:
        print("\n⚠️  PARTIAL: Frank used generic email patterns but no explicit references")
    else:
        print("\n❌ FAILED: Frank did not reference email context")

    # Test value evaluation with user response
    print_section("Value Stage - User Response")

    user_message = "I'm a software engineer with experience building AI products at a startup"
    print(f"User: {user_message}")

    # Build prior state for value eval
    prior_state = {
        "status": "pending",
        "mode": "evaluating",
        "asked_questions": [initial_gate],
        "turn_history": [{"role": "frank", "content": initial_gate}],
        "user_value": {},
        "intro_fee_cents": 9900,
    }

    result = await evaluate_user_value(
        phone_number=TEST_PHONE,
        user_message=user_message,
        user_profile=user_profile,
        prior_state=prior_state,
    )

    print_response(result.get("response_text", ""), "Frank (Value Follow-up)")
    print(f"\nDecision: {result.get('decision')}")
    print(f"Intro Fee: ${result.get('intro_fee_cents', 0) / 100:.2f}")
    print(f"Confidence: {result.get('confidence')}")

    # Check email references in follow-up
    ref_check2 = check_email_reference(result.get("response_text", ""), emails)
    print(f"\nEmail Reference Check (Follow-up):")
    print(f"  Explicit references: {ref_check2['explicit_references']}")
    print(f"  Score: {ref_check2['score']}")

    return {
        "initial_gate": initial_gate,
        "ref_score": ref_check["score"] + ref_check2["score"],
    }


async def run_e2e_test() -> None:
    """Run full E2E onboarding test."""
    print_header("E2E ONBOARDING EMAIL CONTEXT TEST")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print(f"Test User: {TEST_USER_ID}")

    results = {
        "email_fetch": False,
        "need_stage": False,
        "value_stage": False,
        "total_ref_score": 0,
    }

    # Step 1: Fetch emails
    email_signals = await test_email_fetch()
    if not email_signals:
        print("\n" + "=" * 70)
        print(" TEST ABORTED: Could not fetch email context")
        print("=" * 70)
        return

    results["email_fetch"] = True

    # Step 2: Test need stage
    need_result = await test_need_stage(email_signals)
    if need_result:
        results["need_stage"] = True
        results["total_ref_score"] += need_result.get("ref_score", 0)

    # Step 3: Test value stage
    value_result = await test_value_stage(email_signals, need_result or {})
    if value_result:
        results["value_stage"] = True
        results["total_ref_score"] += value_result.get("ref_score", 0)

    # Summary
    print_header("TEST SUMMARY")

    print(f"Email Fetch:  {'✅ PASSED' if results['email_fetch'] else '❌ FAILED'}")
    print(f"Need Stage:   {'✅ PASSED' if results['need_stage'] else '❌ FAILED'}")
    print(f"Value Stage:  {'✅ PASSED' if results['value_stage'] else '❌ FAILED'}")
    print(f"\nTotal Email Reference Score: {results['total_ref_score']}")

    if results["total_ref_score"] >= 4:
        print("\n🎉 EXCELLENT: Frank is explicitly referencing email content!")
    elif results["total_ref_score"] >= 2:
        print("\n⚠️  NEEDS IMPROVEMENT: Frank has some email references but could be more explicit")
    else:
        print("\n❌ POOR: Frank is not adequately referencing email content")

    # Bubble count check
    print("\nBubble Count Check:")
    if need_result:
        bubbles = need_result.get("initial_prompt", "").count("\n\n") + 1
        status = "✅" if bubbles >= 3 else "⚠️"
        print(f"  Need Stage Initial: {bubbles} bubbles {status} (target: 3-4)")
    if value_result:
        bubbles = value_result.get("initial_gate", "").count("\n\n") + 1
        status = "✅" if bubbles >= 3 else "⚠️"
        print(f"  Value Stage Initial: {bubbles} bubbles {status} (target: 3-4)")

    print(f"\nCompleted: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    asyncio.run(run_e2e_test())
