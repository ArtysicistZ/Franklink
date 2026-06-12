#!/usr/bin/env python3
"""
Multi-turn E2E test for Frank onboarding with email context.

Tests that Frank:
1. References different email content across turns (variety)
2. Asks value questions related to user's professional emails
3. Connects email context to user's claims
4. Never reveals how he knows (no "i see", "your inbox", etc.)

Usage:
    python support/scripts/e2e_multi_turn_test.py
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.agents.execution.onboarding.utils.email_context import fetch_email_signals
from app.database.client import DatabaseClient
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

# Simulated user responses for multi-turn conversation
VALUE_STAGE_USER_RESPONSES = [
    "I'm a software engineer with experience building AI products at a startup",
    "I built a recommendation engine that increased user engagement by 40%",
    "I also have connections to several YC founders from my time at the startup",
    "I can help others with technical architecture and system design",
]

# Phrases that reveal email source (should NOT appear)
REVEAL_PHRASES = [
    "i see", "i noticed", "your inbox", "your emails", "based on your",
    "looks like you", "i can see", "from your emails", "your email shows",
    "i've seen", "according to your",
]


def print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def print_section(title: str) -> None:
    print(f"\n{'─' * 80}")
    print(f" {title}")
    print("─" * 80)


def print_response(response: str, label: str = "Frank") -> None:
    """Print Frank's response with bubble formatting."""
    print(f"\n{label}:")
    bubbles = response.split("\n\n")
    for i, bubble in enumerate(bubbles, 1):
        print(f"  [{i}] {bubble.strip()}")
    print(f"\n  (Total: {len(bubbles)} bubbles)")


def extract_email_keywords(emails: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
    """Extract searchable keywords from emails."""
    keywords = {
        "senders": set(),
        "subjects": set(),
        "topics": set(),
    }

    topic_keywords = [
        "wuec", "wharton", "zapier", "penn", "engineering", "master",
        "accelerated", "entrepreneurship", "startup", "ai", "automation",
        "recruiting", "interview", "club", "fair", "events", "newsletter",
    ]

    for email in emails:
        # Extract sender keywords
        sender = email.get("sender", "").lower()
        for part in sender.replace('"', '').replace('<', ' ').replace('>', ' ').split():
            if len(part) > 3 and part.isalpha():
                keywords["senders"].add(part)

        # Extract subject keywords
        subject = email.get("subject", "").lower()
        for word in subject.split():
            if len(word) > 3 and word.isalpha():
                keywords["subjects"].add(word)

        # Extract topic keywords from body
        body = email.get("body", "").lower()
        for topic in topic_keywords:
            if topic in body or topic in subject or topic in sender:
                keywords["topics"].add(topic)

    return keywords


def check_email_references(response: str, email_keywords: Dict[str, Set[str]]) -> Dict[str, Any]:
    """Check which email keywords are referenced in the response."""
    response_lower = response.lower()

    found = {
        "senders": [],
        "subjects": [],
        "topics": [],
    }

    for sender in email_keywords["senders"]:
        if sender in response_lower:
            found["senders"].append(sender)

    for subject in email_keywords["subjects"]:
        if subject in response_lower:
            found["subjects"].append(subject)

    for topic in email_keywords["topics"]:
        if topic in response_lower:
            found["topics"].append(topic)

    total_refs = len(found["senders"]) + len(found["subjects"]) + len(found["topics"])

    return {
        "found": found,
        "total_refs": total_refs,
        "has_refs": total_refs > 0,
    }


def check_reveal_phrases(response: str) -> List[str]:
    """Check if response reveals how Frank knows things."""
    response_lower = response.lower()
    violations = []
    for phrase in REVEAL_PHRASES:
        if phrase in response_lower:
            violations.append(phrase)
    return violations


def analyze_variety(all_responses: List[str], email_keywords: Dict[str, Set[str]]) -> Dict[str, Any]:
    """Analyze variety of email references across all responses."""
    all_topics_used = set()
    topics_per_turn = []

    for response in all_responses:
        refs = check_email_references(response, email_keywords)
        turn_topics = set(refs["found"]["topics"])
        topics_per_turn.append(turn_topics)
        all_topics_used.update(turn_topics)

    # Check if different topics are used across turns
    unique_topics_count = len(all_topics_used)
    repeated_only = all(t == topics_per_turn[0] for t in topics_per_turn) if topics_per_turn else True

    return {
        "all_topics_used": list(all_topics_used),
        "unique_topics_count": unique_topics_count,
        "topics_per_turn": [list(t) for t in topics_per_turn],
        "has_variety": unique_topics_count >= 2 and not repeated_only,
    }


async def run_multi_turn_test() -> None:
    """Run multi-turn E2E onboarding test."""
    print_header("MULTI-TURN E2E ONBOARDING TEST")
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")
    print(f"Test User: {TEST_USER_ID}")

    # =========================================================================
    # STEP 1: Fetch emails
    # =========================================================================
    print_header("STEP 1: Fetch Email Context")

    email_signals = await fetch_email_signals(user_id=TEST_USER_ID)

    if email_signals.get("status") != "ready":
        print(f"❌ FAILED: Could not fetch emails - {email_signals.get('error', 'unknown')}")
        return

    emails = email_signals.get("emails", [])
    print(f"✅ Fetched {len(emails)} emails")

    # Store emails to database so value evaluation can access them
    if emails:
        try:
            db = DatabaseClient()
            await db.store_user_emails(user_id=TEST_USER_ID, emails=emails)
            print(f"✅ Stored {len(emails)} emails to database")
        except Exception as e:
            print(f"⚠️  Failed to store emails to database: {e}")

    print("\nAvailable email context:")
    for i, email in enumerate(emails[:5], 1):
        print(f"  {i}. {email.get('subject', 'N/A')[:60]}")
        print(f"     From: {email.get('sender', 'N/A')[:50]}")

    email_keywords = extract_email_keywords(emails)
    print(f"\nExtracted topics: {sorted(email_keywords['topics'])}")

    # Build user profile
    user_profile = {
        "user_id": TEST_USER_ID,
        "name": "Test User",
        "university": "Penn",
        "career_interests": ["startups", "product management"],
        "personal_facts": {"email_signals": email_signals},
    }

    # =========================================================================
    # STEP 2: Need Stage
    # =========================================================================
    print_header("STEP 2: Need Stage")

    initial_need = await build_initial_need_prompt(user_profile=user_profile)
    print_response(initial_need, "Frank (Need Initial)")

    # Check for reveal phrases
    violations = check_reveal_phrases(initial_need)
    if violations:
        print(f"\n⚠️  VIOLATION: Frank revealed source with: {violations}")
    else:
        print("\n✅ No source-revealing phrases")

    # User responds
    print_section("User Response")
    user_need_msg = "I want to meet VCs and startup founders for my fintech idea"
    print(f"User: {user_need_msg}")

    prior_state = seed_need_state(first_prompt=initial_need)
    need_result = await evaluate_user_need(
        user_message=user_need_msg,
        user_profile=user_profile,
        prior_state=prior_state,
    )

    print_response(need_result.get("response_text", ""), "Frank (Need Follow-up)")
    print(f"Decision: {need_result.get('decision')}")
    print(f"User Need: {json.dumps(need_result.get('user_need', {}), indent=2)}")

    # =========================================================================
    # STEP 3: Value Stage - Multiple Turns
    # =========================================================================
    print_header("STEP 3: Value Stage - Multi-Turn Evaluation")

    # Update profile with need state
    user_profile["need_eval_state"] = {
        "status": "accepted",
        "user_need": need_result.get("user_need", {
            "targets": ["VCs", "startup founders"],
            "outcomes": ["funding", "mentorship"],
        }),
    }

    # Initial value gate
    print_section("Value Turn 0: Initial Gate")
    initial_gate = await build_initial_gate_prompt(
        phone_number=TEST_PHONE,
        user_profile=user_profile,
    )
    print_response(initial_gate, "Frank")

    # Track all Frank responses for variety analysis
    all_frank_responses = [initial_gate]
    all_violations = []

    violations = check_reveal_phrases(initial_gate)
    if violations:
        all_violations.extend(violations)
        print(f"⚠️  VIOLATION: {violations}")

    refs = check_email_references(initial_gate, email_keywords)
    print(f"Email refs: {refs['found']['topics']}")

    # Build prior state for value evaluation
    prior_state = {
        "status": "pending",
        "mode": "evaluating",
        "asked_questions": [initial_gate],
        "turn_history": [{"role": "frank", "content": initial_gate}],
        "user_value": {},
        "intro_fee_cents": 9900,
    }

    # Run multiple value turns
    for turn_num, user_msg in enumerate(VALUE_STAGE_USER_RESPONSES, 1):
        print_section(f"Value Turn {turn_num}")
        print(f"User: {user_msg}")

        # Add user message to history
        prior_state["turn_history"].append({"role": "user", "content": user_msg})

        result = await evaluate_user_value(
            phone_number=TEST_PHONE,
            user_message=user_msg,
            user_profile=user_profile,
            prior_state=prior_state,
        )

        response_text = result.get("response_text", "")
        print_response(response_text, "Frank")

        print(f"Decision: {result.get('decision')}")
        print(f"Fee: ${result.get('intro_fee_cents', 0) / 100:.2f}")
        print(f"Confidence: {result.get('confidence')}")

        # Check for violations
        violations = check_reveal_phrases(response_text)
        if violations:
            all_violations.extend(violations)
            print(f"⚠️  VIOLATION: {violations}")

        # Check email references
        refs = check_email_references(response_text, email_keywords)
        print(f"Email refs: {refs['found']['topics']}")

        all_frank_responses.append(response_text)

        # Update prior state for next turn
        prior_state["asked_questions"].append(response_text)
        prior_state["turn_history"].append({"role": "frank", "content": response_text})
        prior_state["intro_fee_cents"] = result.get("intro_fee_cents", prior_state["intro_fee_cents"])

        # Merge user value
        if result.get("user_value"):
            for k, v in result["user_value"].items():
                if k not in prior_state["user_value"]:
                    prior_state["user_value"][k] = v
                elif isinstance(v, list) and isinstance(prior_state["user_value"][k], list):
                    prior_state["user_value"][k].extend(v)

        # Stop if accepted
        if result.get("decision") == "accept":
            print("\n🎉 User accepted!")
            break

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print_header("TEST SUMMARY")

    # Analyze variety
    variety = analyze_variety(all_frank_responses, email_keywords)

    print("Email Context Variety:")
    print(f"  Topics used across all turns: {variety['all_topics_used']}")
    print(f"  Unique topics count: {variety['unique_topics_count']}")
    print(f"  Topics per turn: {variety['topics_per_turn']}")
    print(f"  Has variety: {'✅ YES' if variety['has_variety'] else '❌ NO'}")

    print("\nSource Reveal Violations:")
    if all_violations:
        print(f"  ❌ Found {len(all_violations)} violations: {set(all_violations)}")
    else:
        print("  ✅ No violations - Frank never revealed his source")

    # Calculate overall score
    total_refs = sum(
        check_email_references(r, email_keywords)["total_refs"]
        for r in all_frank_responses
    )

    print(f"\nOverall Metrics:")
    print(f"  Total turns: {len(all_frank_responses)}")
    print(f"  Total email references: {total_refs}")
    print(f"  Avg refs per turn: {total_refs / len(all_frank_responses):.1f}")
    print(f"  Source violations: {len(all_violations)}")

    # Final verdict
    print("\n" + "=" * 80)
    if variety["has_variety"] and not all_violations and total_refs >= len(all_frank_responses):
        print(" 🎉 EXCELLENT: Frank uses varied email context magically!")
    elif total_refs >= len(all_frank_responses) and not all_violations:
        print(" ⚠️  GOOD: Frank references emails but could use more variety")
    elif all_violations:
        print(" ❌ FAILED: Frank revealed his email source")
    else:
        print(" ❌ FAILED: Frank doesn't reference email context enough")
    print("=" * 80)

    print(f"\nCompleted: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    asyncio.run(run_multi_turn_test())
