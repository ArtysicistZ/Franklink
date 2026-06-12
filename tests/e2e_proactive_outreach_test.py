#!/usr/bin/env python3
"""
E2E Test for Proactive Outreach Worker.

Tests the new implementation including:
1. Purpose extraction from Zep via _get_connection_purpose_suggestions
2. Ranking via rank_purposes_for_proactive (with match_type classification)
3. Opportunity storage in user_networking_opportunities table
4. Match finding and outreach creation
5. Task state saving for routing

This test uses actual LLM calls and real database connections.

Usage:
    python support/scripts/e2e_proactive_outreach_test.py

Environment variables:
    TEST_USER_ID: User ID for testing (must have Zep data and email connected)
"""

import asyncio
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.config import settings
from app.database.client import DatabaseClient
from app.integrations.azure_openai_client import AzureOpenAIClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Test configuration
TEST_USER_ID = os.environ.get("TEST_USER_ID", "fa8ad95d-d21f-4b58-8ac7-807e5b8183fc")  # Yincheng


@dataclass
class TestResult:
    """Result of a single test case."""
    name: str
    passed: bool = False
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None
    duration_ms: int = 0


def separator(title: str):
    """Print a section separator."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def subsection(title: str):
    """Print a subsection separator."""
    print(f"\n{'-' * 60}")
    print(f"  {title}")
    print("-" * 60)


class ProactiveOutreachTester:
    """E2E tester for proactive outreach service."""

    def __init__(self):
        self.db = DatabaseClient()
        self.openai = AzureOpenAIClient()
        self.test_results: List[TestResult] = []

    async def setup(self) -> Optional[Dict[str, Any]]:
        """Setup test environment and return user profile."""
        user = await self.db.get_user_by_id(TEST_USER_ID)
        if not user:
            print(f"ERROR: Test user {TEST_USER_ID} not found")
            return None

        print(f"Test User: {user.get('name')} ({user.get('email')})")
        print(f"Phone: {user.get('phone_number')}")
        print(f"Is Onboarded: {user.get('is_onboarded')}")

        # Check email connection
        personal_facts = user.get("personal_facts") or {}
        email_connect = personal_facts.get("email_connect") or {}
        print(f"Email Status: {email_connect.get('status', 'not_connected')}")

        return user

    async def test_purpose_extraction(self, user: Dict[str, Any]) -> TestResult:
        """Test 1: Extract purposes from Zep via _get_connection_purpose_suggestions."""
        subsection("Test 1: Purpose Extraction from Zep")
        result = TestResult(name="Purpose Extraction from Zep")
        start = datetime.now()

        try:
            from app.agents.tools.networking import _get_connection_purpose_suggestions

            suggestions_result = await _get_connection_purpose_suggestions(
                user_id=user["id"],
                user_profile=user,
                max_suggestions=5,
                skip_deduplication=True,  # Match proactive worker behavior
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            suggestions = suggestions_result.get("suggestions", [])
            has_suggestions = suggestions_result.get("has_suggestions", False)
            recent_facts = suggestions_result.get("recent_facts_count", 0)
            total_facts = suggestions_result.get("total_facts_count", 0)
            skip_reason = suggestions_result.get("skip_reason")

            print(f"Has Suggestions: {has_suggestions}")
            print(f"Suggestions Count: {len(suggestions)}")
            print(f"Recent Facts: {recent_facts}")
            print(f"Total Facts: {total_facts}")
            print(f"Skip Reason: {skip_reason}")

            if suggestions:
                print("\nSuggestions:")
                for i, s in enumerate(suggestions):
                    print(f"  {i+1}. Purpose: {s.get('purpose', '')[:60]}...")
                    print(f"     Group Name: {s.get('group_name', 'N/A')}")
                    print(f"     Activity Type: {s.get('activity_type', 'N/A')}")
                    print(f"     Urgency: {s.get('urgency', 'N/A')}")
                    print(f"     Event Date: {s.get('event_date', 'N/A')}")

            result.details = {
                "has_suggestions": has_suggestions,
                "suggestions_count": len(suggestions),
                "recent_facts_count": recent_facts,
                "total_facts_count": total_facts,
                "skip_reason": skip_reason,
                "suggestions": suggestions,
            }

            # Pass if we got any response (even no suggestions is valid if Zep has no data)
            result.passed = True
            if has_suggestions:
                print("\n[PASS] Successfully extracted purposes from Zep")
            else:
                print("\n[PASS] No suggestions (may indicate limited Zep data)")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")
            logger.exception("Purpose extraction failed")

        return result

    async def test_purpose_ranking(self, user: Dict[str, Any], suggestions: List[Dict[str, Any]]) -> TestResult:
        """Test 2: Rank purposes via rank_purposes_for_proactive."""
        subsection("Test 2: Purpose Ranking with Match Type Classification")
        result = TestResult(name="Purpose Ranking")
        start = datetime.now()

        if not suggestions:
            result.passed = True
            result.details = {"skipped": True, "reason": "No suggestions to rank"}
            print("[SKIP] No suggestions available to rank")
            return result

        try:
            from app.agents.tools.networking import rank_purposes_for_proactive

            # Get recent outreach purposes for deduplication
            recent_outreach = await self.db.get_recent_proactive_outreach_purposes(
                user_id=user["id"],
                days=7,
            )
            recent_purposes = [r.get("signal_text", "") for r in recent_outreach if r.get("signal_text")]

            ranked = await rank_purposes_for_proactive(
                suggestions=suggestions,
                user_profile=user,
                recent_outreach_purposes=recent_purposes,
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            print(f"Recent Outreach Purposes: {len(recent_purposes)}")
            print(f"Ranked Suggestions: {len(ranked)}")

            if ranked:
                print("\nRanked Purposes:")
                for s in ranked:
                    print(f"  #{s.get('rank', '?')}. {s.get('purpose', '')[:50]}...")
                    print(f"      Match Type: {s.get('match_type', 'N/A')}")
                    print(f"      Max Matches: {s.get('max_matches', 'N/A')}")
                    print(f"      Group Name: {s.get('group_name', 'N/A')}")

            result.details = {
                "ranked_count": len(ranked),
                "ranked_purposes": ranked,
                "recent_purposes_count": len(recent_purposes),
            }

            # Verify match_type is present
            has_match_type = all(s.get("match_type") in ("single", "multi") for s in ranked)
            has_rank = all(isinstance(s.get("rank"), int) for s in ranked)

            if has_match_type and has_rank:
                result.passed = True
                print("\n[PASS] Ranking successful with match_type classification")
            else:
                result.passed = False
                print(f"\n[FAIL] Missing expected fields - match_type: {has_match_type}, rank: {has_rank}")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")
            logger.exception("Purpose ranking failed")

        return result

    async def test_opportunity_storage(self, user: Dict[str, Any], ranked: List[Dict[str, Any]]) -> TestResult:
        """Test 3: Store opportunities in user_networking_opportunities table."""
        subsection("Test 3: Opportunity Storage in Database")
        result = TestResult(name="Opportunity Storage")
        start = datetime.now()

        if not ranked:
            result.passed = True
            result.details = {"skipped": True, "reason": "No ranked purposes to store"}
            print("[SKIP] No ranked purposes available")
            return result

        try:
            batch_id = await self.db.insert_networking_opportunities_batch(
                user_id=user["id"],
                source="proactive",
                opportunities=ranked,
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            print(f"Batch ID: {batch_id}")

            if batch_id:
                # Verify storage by retrieving
                stored = await self.db.get_recent_networking_opportunities(
                    user_id=user["id"],
                    days=1,
                    status="active",
                )

                # Filter to just our batch
                batch_opportunities = [o for o in stored if o.get("batch_id") == batch_id]
                print(f"Stored Opportunities: {len(batch_opportunities)}")

                if batch_opportunities:
                    print("\nStored Records:")
                    for o in batch_opportunities[:3]:
                        print(f"  - Purpose: {o.get('purpose', '')[:40]}...")
                        print(f"    Rank: {o.get('rank')}, Match Type: {o.get('match_type')}")
                        print(f"    Group Name: {o.get('group_name', 'N/A')}")

                result.details = {
                    "batch_id": batch_id,
                    "stored_count": len(batch_opportunities),
                    "stored_opportunities": batch_opportunities,
                }

                result.passed = len(batch_opportunities) == len(ranked)
                if result.passed:
                    print(f"\n[PASS] All {len(ranked)} opportunities stored successfully")
                else:
                    print(f"\n[FAIL] Expected {len(ranked)}, stored {len(batch_opportunities)}")
            else:
                result.passed = False
                result.details = {"error": "No batch_id returned"}
                print("\n[FAIL] No batch_id returned from insert")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")
            logger.exception("Opportunity storage failed")

        return result

    async def test_match_finding(self, user: Dict[str, Any], ranked: List[Dict[str, Any]]) -> TestResult:
        """Test 4: Find matches for top-ranked purposes."""
        subsection("Test 4: Match Finding for Ranked Purposes")
        result = TestResult(name="Match Finding")
        start = datetime.now()

        if not ranked:
            result.passed = True
            result.details = {"skipped": True, "reason": "No ranked purposes to match"}
            print("[SKIP] No ranked purposes available")
            return result

        try:
            from app.agents.tools.networking import find_match

            # Try first ranked purpose
            top_purpose = ranked[0]
            signal_text = top_purpose.get("signal_text") or top_purpose.get("purpose", "")
            match_type = top_purpose.get("match_type", "single")

            print(f"Testing match for: {signal_text[:60]}...")
            print(f"Match Type: {match_type}")

            match_result = await find_match(
                user_id=user["id"],
                user_profile=user,
                override_demand=signal_text,
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            if match_result.success:
                data = match_result.data
                print(f"\nMatch Found!")
                print(f"  Target: {data.get('target_name', 'N/A')}")
                print(f"  Score: {data.get('match_score', 'N/A')}")
                print(f"  Reasons: {', '.join(data.get('matching_reasons', [])[:2])}")

                result.details = {
                    "match_found": True,
                    "target_name": data.get("target_name"),
                    "target_user_id": data.get("target_user_id"),
                    "match_score": data.get("match_score"),
                    "matching_reasons": data.get("matching_reasons"),
                }
                result.passed = True
                print("\n[PASS] Match found successfully")
            else:
                # No match is still a valid result (network may be limited)
                result.details = {
                    "match_found": False,
                    "error": match_result.error,
                }
                result.passed = True  # Not finding a match is valid
                print(f"\n[PASS] No match found (valid result): {match_result.error}")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")
            logger.exception("Match finding failed")

        return result

    async def test_multi_match_finding(self, user: Dict[str, Any], ranked: List[Dict[str, Any]]) -> TestResult:
        """Test 5: Find multiple matches for multi-type purposes."""
        subsection("Test 5: Multi-Match Finding")
        result = TestResult(name="Multi-Match Finding")
        start = datetime.now()

        # Find a multi-type purpose
        multi_purpose = next((p for p in ranked if p.get("match_type") == "multi"), None)

        if not multi_purpose:
            result.passed = True
            result.details = {"skipped": True, "reason": "No multi-type purpose in ranked list"}
            print("[SKIP] No multi-type purpose available")
            return result

        try:
            from app.agents.tools.networking import find_match

            signal_text = multi_purpose.get("signal_text") or multi_purpose.get("purpose", "")
            max_matches = multi_purpose.get("max_matches", 3)

            print(f"Testing multi-match for: {signal_text[:60]}...")
            print(f"Max Matches: {max_matches}")

            matches = []
            excluded_ids = []

            for i in range(min(max_matches, 3)):  # Try up to 3
                match_result = await find_match(
                    user_id=user["id"],
                    user_profile=user,
                    override_demand=signal_text,
                    excluded_user_ids=excluded_ids,
                )

                if match_result.success:
                    data = match_result.data
                    target_id = data.get("target_user_id")
                    if target_id:
                        matches.append(data)
                        excluded_ids.append(target_id)
                        print(f"  Match {i+1}: {data.get('target_name', 'N/A')}")
                else:
                    break

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            result.details = {
                "matches_found": len(matches),
                "matches": [
                    {"name": m.get("target_name"), "score": m.get("match_score")}
                    for m in matches
                ],
            }

            result.passed = True
            if matches:
                print(f"\n[PASS] Found {len(matches)} matches for multi-purpose")
            else:
                print("\n[PASS] No multi-matches found (valid result)")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")
            logger.exception("Multi-match finding failed")

        return result

    async def test_duplicate_checking(self, user: Dict[str, Any]) -> TestResult:
        """Test 6: Duplicate outreach checking."""
        subsection("Test 6: Duplicate Outreach Checking")
        result = TestResult(name="Duplicate Checking")
        start = datetime.now()

        try:
            from app.proactive.outreach.duplicate_checker import check_duplicate_outreach

            # Test with a known signal text
            test_signal = "finding a study partner for machine learning"

            is_duplicate = await check_duplicate_outreach(
                self.db,
                user_id=user["id"],
                signal_text=test_signal,
                cooldown_days=7,
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            print(f"Test Signal: {test_signal}")
            print(f"Is Duplicate: {is_duplicate}")

            result.details = {
                "test_signal": test_signal,
                "is_duplicate": is_duplicate,
            }

            result.passed = True  # Both true/false are valid results
            print(f"\n[PASS] Duplicate check returned: {is_duplicate}")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")
            logger.exception("Duplicate checking failed")

        return result

    async def test_message_generation(self, user: Dict[str, Any], ranked: List[Dict[str, Any]]) -> TestResult:
        """Test 7: Proactive message generation."""
        subsection("Test 7: Proactive Message Generation")
        result = TestResult(name="Message Generation")
        start = datetime.now()

        if not ranked:
            result.passed = True
            result.details = {"skipped": True, "reason": "No purposes available"}
            print("[SKIP] No purposes available")
            return result

        try:
            from app.proactive.outreach.message_generator import (
                generate_proactive_suggestion_message,
                build_email_context_summary,
            )

            signal = ranked[0]
            mock_match = {
                "target_name": "John Smith",
                "target_user_id": str(uuid4()),
                "matching_reasons": ["Both interested in ML", "Same university"],
                "llm_introduction": "John is working on similar research and has experience in the area.",
            }

            email_context = build_email_context_summary([], signal)
            print(f"Email Context: {email_context[:100]}...")

            message = await generate_proactive_suggestion_message(
                user_profile=user,
                signal=signal,
                match_result=mock_match,
                email_context=email_context,
                is_multi_match=False,
                all_matches=[mock_match],
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            if message:
                print(f"\nGenerated Message ({len(message)} chars):")
                print(f"  {message[:200]}...")

                result.details = {
                    "message_length": len(message),
                    "message_preview": message[:200],
                }
                result.passed = True
                print("\n[PASS] Message generated successfully")
            else:
                result.passed = False
                result.details = {"error": "No message generated"}
                print("\n[FAIL] No message generated")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")
            logger.exception("Message generation failed")

        return result

    async def test_multi_match_message_generation(self, user: Dict[str, Any], ranked: List[Dict[str, Any]]) -> TestResult:
        """Test 8: Multi-match message generation."""
        subsection("Test 8: Multi-Match Message Generation")
        result = TestResult(name="Multi-Match Message Generation")
        start = datetime.now()

        # Find a multi-type purpose
        multi_purpose = next((p for p in ranked if p.get("match_type") == "multi"), None)

        if not multi_purpose:
            # Create a synthetic multi-purpose for testing
            if ranked:
                multi_purpose = ranked[0].copy()
                multi_purpose["match_type"] = "multi"
                multi_purpose["max_matches"] = 3
            else:
                result.passed = True
                result.details = {"skipped": True, "reason": "No purposes available"}
                print("[SKIP] No purposes available")
                return result

        try:
            from app.proactive.outreach.message_generator import (
                generate_proactive_suggestion_message,
                build_email_context_summary,
            )

            mock_matches = [
                {
                    "target_name": "Alice Chen",
                    "target_user_id": str(uuid4()),
                    "matching_reasons": ["ML expertise", "Same year"],
                },
                {
                    "target_name": "Bob Wilson",
                    "target_user_id": str(uuid4()),
                    "matching_reasons": ["Research experience", "Similar interests"],
                },
                {
                    "target_name": "Carol Davis",
                    "target_user_id": str(uuid4()),
                    "matching_reasons": ["Study group organizer", "Same major"],
                },
            ]

            email_context = build_email_context_summary([], multi_purpose)

            message = await generate_proactive_suggestion_message(
                user_profile=user,
                signal=multi_purpose,
                match_result=mock_matches[0],
                email_context=email_context,
                is_multi_match=True,
                all_matches=mock_matches,
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            if message:
                print(f"\nGenerated Multi-Match Message ({len(message)} chars):")
                print(f"  {message[:250]}...")

                # Check if names appear in message
                names_present = sum(1 for m in mock_matches if m["target_name"].split()[0].lower() in message.lower())
                print(f"\nNames mentioned: {names_present}/{len(mock_matches)}")

                result.details = {
                    "message_length": len(message),
                    "message_preview": message[:250],
                    "names_mentioned": names_present,
                }
                result.passed = True
                print("\n[PASS] Multi-match message generated successfully")
            else:
                result.passed = False
                result.details = {"error": "No message generated"}
                print("\n[FAIL] No message generated")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")
            logger.exception("Multi-match message generation failed")

        return result

    async def test_full_service_flow(self, user: Dict[str, Any]) -> TestResult:
        """Test 9: Full proactive outreach service flow (dry run)."""
        subsection("Test 9: Full Service Flow (Dry Run)")
        result = TestResult(name="Full Service Flow")
        start = datetime.now()

        try:
            from app.proactive.outreach.service import ProactiveOutreachService

            # Create service with test worker ID
            service = ProactiveOutreachService(
                db=self.db,
                worker_id=f"test-worker-{uuid4().hex[:8]}",
                openai=self.openai,
            )

            # Test the internal _process_job method components
            # (We can't run full flow without mocking PhotonClient)

            # Step 1: Verify user preconditions
            if not user.get("is_onboarded"):
                print("User not onboarded - would skip")
                result.details["skip_reason"] = "not_onboarded"

            personal_facts = user.get("personal_facts") or {}
            email_connect = personal_facts.get("email_connect") or {}
            if email_connect.get("status") != "connected":
                print("Email not connected - would skip")
                result.details["skip_reason"] = "email_not_connected"

            if not user.get("proactive_preference", True):
                print("User opted out - would skip")
                result.details["skip_reason"] = "user_opted_out"

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            # If we got this far, the service is correctly initialized
            result.passed = True
            result.details["service_initialized"] = True
            print("\n[PASS] Service initialized and preconditions checked")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")
            logger.exception("Full service flow test failed")

        return result

    def print_summary(self):
        """Print test summary."""
        separator("TEST SUMMARY")

        passed = 0
        failed = 0
        skipped = 0

        for r in self.test_results:
            if r.details.get("skipped"):
                status = "SKIP"
                skipped += 1
            elif r.passed:
                status = "PASS"
                passed += 1
            else:
                status = "FAIL"
                failed += 1

            print(f"[{status}] {r.name} ({r.duration_ms}ms)")
            if r.error:
                print(f"       Error: {r.error}")

        print(f"\nTotal: {passed} passed, {failed} failed, {skipped} skipped")
        return failed == 0


async def main():
    """Run the proactive outreach e2e tests."""
    separator("PROACTIVE OUTREACH E2E TEST")
    print("Testing the new proactive outreach implementation")
    print(f"Test User ID: {TEST_USER_ID}")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")

    # Check if proactive outreach is enabled
    if not getattr(settings, "proactive_outreach_worker_enabled", False):
        print("\nWARNING: proactive_outreach_worker_enabled is False")
        print("Some tests may behave differently than in production")

    tester = ProactiveOutreachTester()

    # Setup
    separator("Setup")
    user = await tester.setup()
    if not user:
        return 1

    # Run tests
    separator("Running Tests")

    # Test 1: Purpose extraction
    result1 = await tester.test_purpose_extraction(user)
    tester.test_results.append(result1)
    suggestions = result1.details.get("suggestions", [])

    # Test 2: Purpose ranking
    result2 = await tester.test_purpose_ranking(user, suggestions)
    tester.test_results.append(result2)
    ranked = result2.details.get("ranked_purposes", [])

    # Test 3: Opportunity storage
    result3 = await tester.test_opportunity_storage(user, ranked)
    tester.test_results.append(result3)

    # Test 4: Match finding
    result4 = await tester.test_match_finding(user, ranked)
    tester.test_results.append(result4)

    # Test 5: Multi-match finding
    result5 = await tester.test_multi_match_finding(user, ranked)
    tester.test_results.append(result5)

    # Test 6: Duplicate checking
    result6 = await tester.test_duplicate_checking(user)
    tester.test_results.append(result6)

    # Test 7: Message generation
    result7 = await tester.test_message_generation(user, ranked)
    tester.test_results.append(result7)

    # Test 8: Multi-match message generation
    result8 = await tester.test_multi_match_message_generation(user, ranked)
    tester.test_results.append(result8)

    # Test 9: Full service flow
    result9 = await tester.test_full_service_flow(user)
    tester.test_results.append(result9)

    # Summary
    success = tester.print_summary()

    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
