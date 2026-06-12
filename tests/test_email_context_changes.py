#!/usr/bin/env python3
"""
Test suite for Email Context System Redesign.

Tests the new conditional email injection logic:
1. has_topical_overlap() function
2. check_user_message_specificity() function
3. Conditional email context injection in prompts

Usage:
    python support/scripts/test_email_context_changes.py
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.agents.execution.onboarding.utils.email_context import (
    has_topical_overlap,
    check_user_message_specificity,
)


def print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def print_test(name: str, passed: bool, details: str = "") -> None:
    status = "PASS" if passed else "FAIL"
    icon = "✅" if passed else "❌"
    print(f"  {icon} {status}: {name}")
    if details:
        print(f"       {details}")


class TestResults:
    def __init__(self):
        self.total = 0
        self.passed = 0
        self.failed = 0
        self.failures = []

    def record(self, name: str, passed: bool, details: str = ""):
        self.total += 1
        if passed:
            self.passed += 1
        else:
            self.failed += 1
            self.failures.append((name, details))
        print_test(name, passed, details)


# ============================================================================
# Test: has_topical_overlap()
# ============================================================================

def test_topical_overlap(results: TestResults) -> None:
    """Test the has_topical_overlap function."""
    print_header("TEST: has_topical_overlap()")

    # Test 1: Direct keyword match
    email = {
        "sender": "Stripe Recruiting",
        "subject": "Engineering role at Stripe",
        "body": "We'd love to talk about engineering opportunities"
    }
    user_message = "I can help with engineering and coding"
    result = has_topical_overlap(email, user_message)
    results.record(
        "Direct keyword match (engineering)",
        result == True,
        f"Expected True, got {result}"
    )

    # Test 2: No overlap - completely different topics
    email = {
        "sender": "Marketing Team",
        "subject": "Brand strategy meeting",
        "body": "Discuss Q1 marketing campaign"
    }
    user_message = "I can do vibe coding and user research"
    result = has_topical_overlap(email, user_message)
    results.record(
        "No overlap (marketing vs coding)",
        result == False,
        f"Expected False, got {result}"
    )

    # Test 3: Partial match in sender
    email = {
        "sender": "Startup Founders Club",
        "subject": "Monthly meetup",
        "body": "Join us for networking"
    }
    user_message = "I want to meet startup founders"
    result = has_topical_overlap(email, user_message)
    results.record(
        "Match in sender (startup/founders)",
        result == True,
        f"Expected True, got {result}"
    )

    # Test 4: Short/stopword-only message should not match
    email = {
        "sender": "Random Company",
        "subject": "Important update",
        "body": "Please review this document"
    }
    user_message = "yes I can"
    result = has_topical_overlap(email, user_message)
    results.record(
        "Short message no match",
        result == False,
        f"Expected False, got {result}"
    )

    # Test 5: Match in body content
    email = {
        "sender": "Conference Team",
        "subject": "Registration confirmed",
        "body": "Your registration for the AI research conference is confirmed"
    }
    user_message = "I do AI research and machine learning"
    result = has_topical_overlap(email, user_message)
    results.record(
        "Match in body (research)",
        result == True,
        f"Expected True, got {result}"
    )

    # Test 6: Case insensitive matching
    email = {
        "sender": "VENTURE CAPITAL FIRM",
        "subject": "Investment Opportunity",
        "body": "Looking to invest"
    }
    user_message = "i want to meet venture capital investors"
    result = has_topical_overlap(email, user_message)
    results.record(
        "Case insensitive match (venture)",
        result == True,
        f"Expected True, got {result}"
    )

    # Test 7: Empty email fields
    email = {
        "sender": "",
        "subject": "",
        "body": ""
    }
    user_message = "I can help with coding"
    result = has_topical_overlap(email, user_message)
    results.record(
        "Empty email fields - no match",
        result == False,
        f"Expected False, got {result}"
    )

    # Test 8: Email about user research should match
    email = {
        "sender": "User Research Team",
        "subject": "User interview schedule",
        "body": "Please confirm your availability for user research sessions"
    }
    user_message = "I can do vibe coding and user research"
    result = has_topical_overlap(email, user_message)
    results.record(
        "Match user research topic",
        result == True,
        f"Expected True, got {result}"
    )


# ============================================================================
# Test: check_user_message_specificity()
# ============================================================================

def test_message_specificity(results: TestResults) -> None:
    """Test the check_user_message_specificity function."""
    print_header("TEST: check_user_message_specificity()")

    # Test 1: Message with numbers - should be specific
    user_message = "I built an app with 500 users and raised $50k"
    result = check_user_message_specificity(user_message)
    results.record(
        "Message with numbers is specific",
        result == True,
        f"Expected True, got {result}"
    )

    # Test 2: Message with action verbs + length - should be specific
    user_message = "I launched a product that helps students find internships and it grew to serve over 200 universities"
    result = check_user_message_specificity(user_message)
    results.record(
        "Message with action verbs + length is specific",
        result == True,
        f"Expected True, got {result}"
    )

    # Test 3: Vague short message - should NOT be specific
    user_message = "I can help people"
    result = check_user_message_specificity(user_message)
    results.record(
        "Vague short message not specific",
        result == False,
        f"Expected False, got {result}"
    )

    # Test 4: Message with just action verbs but short - might not be specific
    user_message = "I built something"
    result = check_user_message_specificity(user_message)
    results.record(
        "Short message with one action verb",
        result == False,
        f"Expected False, got {result}"
    )

    # Test 5: Long detailed message with action verbs - should be specific
    user_message = "I worked at 3 startups where I built products and launched features. I also led a team of 5 engineers"
    result = check_user_message_specificity(user_message)
    results.record(
        "Long message with action verbs + numbers is specific",
        result == True,
        f"Expected True, got {result}"
    )

    # Test 6: Message with URL + action verb - should be specific
    user_message = "I built a project you can check out at https://myproject.com"
    result = check_user_message_specificity(user_message)
    results.record(
        "Message with URL + action verb is specific",
        result == True,
        f"Expected True, got {result}"
    )

    # Test 7: Generic tagline energy - should NOT be specific
    user_message = "I am passionate about tech"
    result = check_user_message_specificity(user_message)
    results.record(
        "Generic tagline not specific",
        result == False,
        f"Expected False, got {result}"
    )

    # Test 8: The screenshot scenario - "vibe coding and user research"
    user_message = "I can do vibe coding and do user research"
    result = check_user_message_specificity(user_message)
    results.record(
        "Screenshot scenario (vibe coding + user research)",
        result == False,  # This is vague - no numbers, no strong action verbs
        f"Expected False (vague answer), got {result}"
    )

    # Test 9: Message with just email address (no action) - not specific
    user_message = "you can reach me at test@example.com"
    result = check_user_message_specificity(user_message)
    results.record(
        "Message with just email (no action) not specific",
        result == False,  # Just contact info without accomplishment
        f"Expected False, got {result}"
    )

    # Test 10: Action verb + number combo
    user_message = "I shipped 3 products last year"
    result = check_user_message_specificity(user_message)
    results.record(
        "Action verb + number is specific",
        result == True,
        f"Expected True, got {result}"
    )


# ============================================================================
# Test: Integration - Should Email Context Be Included?
# ============================================================================

def test_email_context_decision(results: TestResults) -> None:
    """Test the combined decision logic for email context inclusion."""
    print_header("TEST: Email Context Inclusion Decision")

    # Simulated email context
    emails = [
        {
            "sender": "Florian Juengermann",
            "subject": "Penn Engineering Events",
            "body": "Join us for the next engineering meetup"
        },
        {
            "sender": "Stripe Recruiting",
            "subject": "Engineering opportunity",
            "body": "We'd love to discuss engineering roles"
        },
        {
            "sender": "Y Combinator",
            "subject": "Startup School application",
            "body": "Your application to Startup School"
        }
    ]

    # Test 1: Vague message about startups + relevant email = SHOULD include context
    user_message = "I'm interested in startup founders"
    user_is_specific = check_user_message_specificity(user_message)
    relevant_emails = [e for e in emails if has_topical_overlap(e, user_message)]
    should_include = not user_is_specific and len(relevant_emails) > 0
    results.record(
        "Vague + relevant email = include context",
        should_include == True,
        f"specific={user_is_specific}, relevant={len(relevant_emails)}, include={should_include}"
    )

    # Test 2: Specific message = should NOT include context regardless of emails
    user_message = "I built a startup that raised $2M and has 10,000 users"
    user_is_specific = check_user_message_specificity(user_message)
    relevant_emails = [e for e in emails if has_topical_overlap(e, user_message)]
    should_include = not user_is_specific and len(relevant_emails) > 0
    results.record(
        "Specific message = skip context (focus on user)",
        should_include == False,
        f"specific={user_is_specific}, relevant={len(relevant_emails)}, include={should_include}"
    )

    # Test 3: Vague message + NO relevant emails = should NOT include context
    user_message = "I can help people"
    user_is_specific = check_user_message_specificity(user_message)
    relevant_emails = [e for e in emails if has_topical_overlap(e, user_message)]
    should_include = not user_is_specific and len(relevant_emails) > 0
    results.record(
        "Vague + no relevant emails = skip context",
        should_include == False,
        f"specific={user_is_specific}, relevant={len(relevant_emails)}, include={should_include}"
    )

    # Test 4: The screenshot scenario
    user_message = "I can do vibe coding and do user research"
    user_is_specific = check_user_message_specificity(user_message)
    relevant_emails = [e for e in emails if has_topical_overlap(e, user_message)]
    should_include = not user_is_specific and len(relevant_emails) > 0
    results.record(
        "Screenshot: 'vibe coding + user research'",
        should_include == False,  # No overlap with the emails, so don't force it
        f"specific={user_is_specific}, relevant={len(relevant_emails)}, include={should_include}"
    )

    # Test 5: Engineering message + engineering emails
    user_message = "I do software engineering"
    user_is_specific = check_user_message_specificity(user_message)
    relevant_emails = [e for e in emails if has_topical_overlap(e, user_message)]
    should_include = not user_is_specific and len(relevant_emails) > 0
    results.record(
        "Engineering message + engineering emails = include",
        should_include == True,
        f"specific={user_is_specific}, relevant={len(relevant_emails)}, include={should_include}"
    )


# ============================================================================
# Main
# ============================================================================

def run_all_tests() -> None:
    """Run all test suites."""
    print("\n" + "=" * 80)
    print(" EMAIL CONTEXT SYSTEM REDESIGN - TEST SUITE")
    print(" Testing conditional email injection logic")
    print("=" * 80)
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")

    results = TestResults()

    # Run tests
    test_topical_overlap(results)
    test_message_specificity(results)
    test_email_context_decision(results)

    # Summary
    print_header("TEST SUMMARY")
    print(f"Total tests: {results.total}")
    print(f"Passed: {results.passed} ✅")
    print(f"Failed: {results.failed} ❌")

    if results.failures:
        print("\nFailed tests:")
        for name, details in results.failures:
            print(f"  - {name}: {details}")

    print(f"\nCompleted: {datetime.now(timezone.utc).isoformat()}")

    if results.failed > 0:
        print("\n" + "=" * 80)
        print(" ❌ SOME TESTS FAILED - Investigate the failures above")
        print("=" * 80)
        sys.exit(1)
    else:
        print("\n" + "=" * 80)
        print(" ✅ ALL TESTS PASSED")
        print("=" * 80)


if __name__ == "__main__":
    run_all_tests()
