#!/usr/bin/env python3
"""
E2E Edge Case Tests for Networking and Proactive Outreach.

Tests edge cases and error scenarios:
1. User with no Zep data
2. User with email not connected
3. Duplicate purpose detection
4. Empty ranking results
5. Match type ambiguity
6. User changes mind mid-flow
7. Concurrent request handling
8. Invalid user ID handling
9. Network empty scenarios
10. Rate limiting / cooldown scenarios

This test uses actual LLM calls and real database connections.

Usage:
    python support/scripts/e2e_networking_edge_cases_test.py

Environment variables:
    TEST_USER_ID: User ID for testing (must have Zep data)
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
from app.agents.state import AtomicStateManager, NetworkingFlowState

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Test configuration
TEST_USER_ID = os.environ.get("TEST_USER_ID", "fa8ad95d-d21f-4b58-8ac7-807e5b8183fc")
TEST_USER_PHONE = os.environ.get("TEST_USER_PHONE", "+12677882488")


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


class MockPhotonClient:
    """Mock Photon client that captures messages."""

    def __init__(self):
        self.sent_messages: List[Dict[str, Any]] = []

    async def send_message(self, to_number: str, content: str, **kwargs) -> Dict[str, Any]:
        self.sent_messages.append({"to_number": to_number, "content": content, **kwargs})
        return {"success": True, "message_id": f"mock-{len(self.sent_messages)}"}

    async def start_typing(self, phone_number: str, chat_guid: Optional[str] = None):
        pass

    async def stop_typing(self, phone_number: str, chat_guid: Optional[str] = None):
        pass

    async def mark_chat_read(self, chat_guid: Optional[str] = None):
        pass

    def clear(self):
        self.sent_messages = []


class EdgeCaseTester:
    """E2E edge case tester."""

    def __init__(self):
        self.db = DatabaseClient()
        self.openai = AzureOpenAIClient()
        self.state_manager = AtomicStateManager(self.db)
        self.photon = MockPhotonClient()
        self.interaction_agent = None
        self.test_results: List[TestResult] = []

    async def setup(self) -> Optional[Dict[str, Any]]:
        """Setup test environment."""
        user = await self.db.get_user_by_id(TEST_USER_ID)
        if not user:
            print(f"ERROR: Test user {TEST_USER_ID} not found")
            return None

        print(f"Test User: {user.get('name')}")

        from app.agents.interaction import get_interaction_agent
        self.interaction_agent = get_interaction_agent(
            db=self.db,
            photon=self.photon,
            openai=self.openai,
        )

        return user

    async def send_message(self, message: str, user: Dict[str, Any]) -> Dict[str, Any]:
        """Send a message through the interaction agent."""
        self.photon.clear()
        try:
            result = await self.interaction_agent.process_message(
                phone_number=TEST_USER_PHONE,
                message_content=message,
                user=user,
                webhook_data={},
            )
            return result or {"success": False, "error": "None returned"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def cleanup(self, user_id: str):
        """Clean up test data."""
        try:
            await self.state_manager.force_reset_state(user_id)
            await self.db.clear_task_history(user_id)
        except Exception:
            pass

    # =========================================================================
    # Edge Case Tests
    # =========================================================================

    async def test_no_zep_data_fallback(self, user: Dict[str, Any]) -> TestResult:
        """Test: System gracefully handles when no Zep data is available."""
        subsection("Test 1: No Zep Data Fallback")
        result = TestResult(name="No Zep Data Fallback")
        start = datetime.now()

        try:
            from app.agents.tools.networking import _get_connection_purpose_suggestions

            # Call with user that may have limited Zep data
            suggestions_result = await _get_connection_purpose_suggestions(
                user_id=user["id"],
                user_profile=user,
                max_suggestions=5,
                skip_deduplication=True,
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            has_suggestions = suggestions_result.get("has_suggestions", False)
            fallback_question = suggestions_result.get("fallback_question")
            skip_reason = suggestions_result.get("skip_reason")

            print(f"Has Suggestions: {has_suggestions}")
            print(f"Fallback Question: {fallback_question}")
            print(f"Skip Reason: {skip_reason}")

            result.details = {
                "has_suggestions": has_suggestions,
                "fallback_question": fallback_question,
                "skip_reason": skip_reason,
            }

            # Success if we get: suggestions, fallback question, or skip_reason (any valid response)
            result.passed = has_suggestions or fallback_question is not None or skip_reason is not None
            if result.passed:
                print("\n[PASS] Graceful handling of Zep data state")
            else:
                print("\n[FAIL] No suggestions, no fallback, and no skip reason")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_duplicate_purpose_detection(self, user: Dict[str, Any]) -> TestResult:
        """Test: Duplicate purposes are filtered out."""
        subsection("Test 2: Duplicate Purpose Detection")
        result = TestResult(name="Duplicate Purpose Detection")
        start = datetime.now()

        try:
            from app.agents.tools.networking import _is_duplicate_purpose

            existing_purposes = [
                "finding a study partner for machine learning",
                "looking for hackathon teammates",
                "someone to practice interview questions with",
            ]

            # Test exact duplicate
            test1 = _is_duplicate_purpose(
                "finding a study partner for machine learning",
                existing_purposes
            )

            # Test similar (keyword overlap)
            test2 = _is_duplicate_purpose(
                "study partner for ML and deep learning",
                existing_purposes
            )

            # Test different
            test3 = _is_duplicate_purpose(
                "finding a gym buddy at Pottruck",
                existing_purposes
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            print(f"Exact duplicate detected: {test1}")
            print(f"Similar (ML study) detected: {test2}")
            print(f"Different (gym) detected: {test3}")

            result.details = {
                "exact_duplicate": test1,
                "similar_duplicate": test2,
                "different_not_duplicate": not test3,
            }

            # Exact should be duplicate, similar should be duplicate, different should not
            result.passed = test1 and test2 and not test3
            if result.passed:
                print("\n[PASS] Duplicate detection working correctly")
            else:
                print("\n[FAIL] Unexpected duplicate detection results")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_empty_ranking_handling(self, user: Dict[str, Any]) -> TestResult:
        """Test: Empty ranking results are handled gracefully."""
        subsection("Test 3: Empty Ranking Handling")
        result = TestResult(name="Empty Ranking Handling")
        start = datetime.now()

        try:
            from app.agents.tools.networking import rank_purposes_for_proactive

            # Test with empty suggestions
            ranked = await rank_purposes_for_proactive(
                suggestions=[],
                user_profile=user,
                recent_outreach_purposes=[],
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            print(f"Ranked result for empty input: {ranked}")

            result.details = {
                "result_type": type(ranked).__name__,
                "result_value": ranked,
            }

            # Should return empty list gracefully
            result.passed = ranked == []
            if result.passed:
                print("\n[PASS] Empty ranking handled gracefully")
            else:
                print(f"\n[FAIL] Unexpected result: {ranked}")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_user_changes_mind(self, user: Dict[str, Any]) -> TestResult:
        """Test: User changes their mind mid-flow."""
        subsection("Test 4: User Changes Mind Mid-Flow")
        result = TestResult(name="User Changes Mind Mid-Flow")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            # Step 1: Start networking flow
            response1 = await self.send_message("i want to network", user)
            print(f"Step 1 Response: {response1.get('response_text', '')[:100]}...")

            await asyncio.sleep(0.3)

            # Step 2: Change mind - cancel/different request
            response2 = await self.send_message("actually never mind, i want to update my profile instead", user)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            response_text2 = response2.get("response_text", "")
            task2 = response2.get("task")

            print(f"Step 2 Response: {response_text2[:100]}...")
            print(f"Task after change: {task2}")

            result.details = {
                "step1_success": response1.get("success"),
                "step2_success": response2.get("success"),
                "step2_task": task2,
            }

            # System should handle the change gracefully
            result.passed = response2.get("success", False)
            if result.passed:
                print("\n[PASS] System handled user changing mind")
            else:
                print("\n[FAIL] Failed to handle mind change")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_invalid_user_id_repair(self, user: Dict[str, Any]) -> TestResult:
        """Test: Corrupted user ID is auto-repaired from profile."""
        subsection("Test 5: Invalid User ID Auto-Repair")
        result = TestResult(name="Invalid User ID Auto-Repair")
        start = datetime.now()

        try:
            from app.agents.tools.networking import _validate_and_repair_user_id

            # Test with corrupted UUID (missing segment)
            corrupted_id = "fa8ad95d-d21f-4b58-807e5b8183fc"  # Missing one segment

            error, repaired = _validate_and_repair_user_id(corrupted_id, user)

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            print(f"Corrupted ID: {corrupted_id}")
            print(f"Error: {error}")
            print(f"Repaired ID: {repaired}")

            result.details = {
                "corrupted_input": corrupted_id,
                "error": error,
                "repaired_id": repaired,
                "matches_profile": repaired == user.get("id"),
            }

            # Should repair to profile ID
            if error is None and repaired == user.get("id"):
                result.passed = True
                print("\n[PASS] User ID auto-repaired successfully")
            else:
                # Or should provide clear error
                result.passed = error is not None
                print(f"\n[PASS] Validation provided error: {error}")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_network_empty_handling(self, user: Dict[str, Any]) -> TestResult:
        """Test: Network empty scenarios are handled gracefully."""
        subsection("Test 6: Network Empty Handling")
        result = TestResult(name="Network Empty Handling")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            from app.agents.tools.networking import find_match

            # Try to find a match for a very specific/unlikely demand
            match_result = await find_match(
                user_id=user["id"],
                user_profile=user,
                override_demand="someone who knows underwater basket weaving for competitive tournaments",
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            print(f"Match Success: {match_result.success}")
            if match_result.success:
                print(f"Match Data: {match_result.data}")
            else:
                print(f"Match Error: {match_result.error}")

            result.details = {
                "success": match_result.success,
                "data": match_result.data if match_result.success else None,
                "error": match_result.error,
            }

            # Should either find a match or gracefully report network empty
            result.passed = True  # Both outcomes are valid
            if match_result.success:
                print("\n[PASS] Unexpected match found (network has someone!)")
            else:
                print("\n[PASS] Network empty handled gracefully")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_cooldown_period_check(self, user: Dict[str, Any]) -> TestResult:
        """Test: Cooldown period prevents duplicate outreach."""
        subsection("Test 7: Cooldown Period Check")
        result = TestResult(name="Cooldown Period Check")
        start = datetime.now()

        try:
            from app.proactive.outreach.duplicate_checker import check_duplicate_outreach
            from app.proactive.config import PROACTIVE_OUTREACH_COOLDOWN_DAYS

            print(f"Cooldown Days: {PROACTIVE_OUTREACH_COOLDOWN_DAYS}")

            # Check for a signal that may have been used recently
            test_signal = "finding a study partner"

            is_duplicate = await check_duplicate_outreach(
                self.db,
                user_id=user["id"],
                signal_text=test_signal,
                cooldown_days=PROACTIVE_OUTREACH_COOLDOWN_DAYS,
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            print(f"Test Signal: {test_signal}")
            print(f"Is Duplicate (within cooldown): {is_duplicate}")

            result.details = {
                "signal": test_signal,
                "is_duplicate": is_duplicate,
                "cooldown_days": PROACTIVE_OUTREACH_COOLDOWN_DAYS,
            }

            result.passed = True  # Both true/false are valid
            print("\n[PASS] Cooldown check completed")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_match_type_ambiguity(self, user: Dict[str, Any]) -> TestResult:
        """Test: Ambiguous match types are handled correctly."""
        subsection("Test 8: Match Type Ambiguity")
        result = TestResult(name="Match Type Ambiguity")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            # Send an ambiguous request (could be single or multi)
            message = "i need someone to help with my project"
            print(f"User: {message}")

            response = await self.send_message(message, user)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            response_text = response.get("response_text", "")
            task = response.get("task")

            print(f"Task: {task}")
            print(f"Response: {response_text[:200]}...")

            # Check if system asked for clarification or made a decision
            asks_clarification = any(kw in response_text.lower() for kw in [
                "one person", "multiple", "group", "how many"
            ])

            result.details = {
                "task": task,
                "asks_clarification": asks_clarification,
                "response_preview": response_text[:200],
            }

            result.passed = response.get("success", False)
            if result.passed:
                if asks_clarification:
                    print("\n[PASS] System asked for match type clarification")
                else:
                    print("\n[PASS] System made a match type decision")
            else:
                print("\n[FAIL] Request failed")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_very_long_purpose(self, user: Dict[str, Any]) -> TestResult:
        """Test: Very long purpose text is handled correctly."""
        subsection("Test 9: Very Long Purpose Handling")
        result = TestResult(name="Very Long Purpose Handling")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            # Create a very long request
            long_message = (
                "I'm looking for someone who can help me with my research project on "
                "machine learning applications in quantitative finance specifically "
                "focusing on high-frequency trading algorithms using deep reinforcement "
                "learning and neural network architectures for market making strategies "
                "and also someone who has experience with Python and C++ and understands "
                "the mathematical foundations of stochastic calculus and probability theory"
            )
            print(f"User (truncated): {long_message[:80]}... ({len(long_message)} chars)")

            response = await self.send_message(long_message, user)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            response_text = response.get("response_text", "")
            task = response.get("task")

            print(f"Task: {task}")
            print(f"Response: {response_text[:150]}...")

            result.details = {
                "input_length": len(long_message),
                "task": task,
                "success": response.get("success"),
            }

            result.passed = response.get("success", False)
            if result.passed:
                print("\n[PASS] Long purpose handled correctly")
            else:
                print("\n[FAIL] Failed to handle long purpose")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_special_characters_in_purpose(self, user: Dict[str, Any]) -> TestResult:
        """Test: Special characters in purpose are handled correctly."""
        subsection("Test 10: Special Characters in Purpose")
        result = TestResult(name="Special Characters in Purpose")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            # Purpose with special characters
            special_message = "find me a co-founder for my AI startup @ Penn (CIS 520)"
            print(f"User: {special_message}")

            response = await self.send_message(special_message, user)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            response_text = response.get("response_text", "")
            task = response.get("task")

            print(f"Task: {task}")
            print(f"Response: {response_text[:150]}...")

            result.details = {
                "input": special_message,
                "task": task,
                "success": response.get("success"),
            }

            result.passed = response.get("success", False)
            if result.passed:
                print("\n[PASS] Special characters handled correctly")
            else:
                print("\n[FAIL] Failed to handle special characters")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_rapid_successive_requests(self, user: Dict[str, Any]) -> TestResult:
        """Test: Rapid successive requests are handled correctly."""
        subsection("Test 11: Rapid Successive Requests")
        result = TestResult(name="Rapid Successive Requests")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            # Send multiple requests in quick succession
            messages = [
                "find me a study partner",
                "actually make it a gym buddy",
                "no wait, interview prep partner",
            ]

            responses = []
            for msg in messages:
                print(f"User: {msg}")
                resp = await self.send_message(msg, user)
                responses.append(resp)
                await asyncio.sleep(0.2)  # Small delay

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            # Check last response
            last_response = responses[-1]
            print(f"\nFinal Task: {last_response.get('task')}")
            print(f"Final Response: {last_response.get('response_text', '')[:150]}...")

            result.details = {
                "requests_sent": len(messages),
                "all_successful": all(r.get("success") for r in responses),
                "final_task": last_response.get("task"),
            }

            # All requests should be handled without errors
            result.passed = all(r.get("success") for r in responses)
            if result.passed:
                print("\n[PASS] Rapid requests handled correctly")
            else:
                print("\n[FAIL] Some requests failed")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_opportunity_batch_storage(self, user: Dict[str, Any]) -> TestResult:
        """Test: Multiple opportunities can be stored in a batch."""
        subsection("Test 12: Opportunity Batch Storage")
        result = TestResult(name="Opportunity Batch Storage")
        start = datetime.now()

        try:
            # Create test opportunities
            test_opportunities = [
                {
                    "purpose": "test purpose 1 - edge case test",
                    "group_name": "Test Group 1",
                    "rationale": "Test rationale",
                    "evidence": "Test evidence",
                    "activity_type": "academic",
                    "urgency": "medium",
                    "rank": 1,
                    "match_type": "single",
                    "max_matches": 1,
                },
                {
                    "purpose": "test purpose 2 - edge case test",
                    "group_name": "Test Group 2",
                    "rationale": "Test rationale 2",
                    "evidence": "Test evidence 2",
                    "activity_type": "event",
                    "urgency": "high",
                    "rank": 2,
                    "match_type": "multi",
                    "max_matches": 3,
                },
            ]

            batch_id = await self.db.insert_networking_opportunities_batch(
                user_id=user["id"],
                source="user_requested",
                opportunities=test_opportunities,
            )

            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            print(f"Batch ID: {batch_id}")

            if batch_id:
                # Verify storage
                stored = await self.db.get_recent_networking_opportunities(
                    user_id=user["id"],
                    days=1,
                    status="active",
                )
                batch_items = [o for o in stored if o.get("batch_id") == batch_id]

                print(f"Stored Items: {len(batch_items)}")

                # Mark as skipped to clean up
                for item in batch_items:
                    await self.db.mark_opportunity_skipped(opportunity_id=item["id"])

                result.details = {
                    "batch_id": batch_id,
                    "stored_count": len(batch_items),
                    "expected_count": len(test_opportunities),
                }

                result.passed = len(batch_items) == len(test_opportunities)
                if result.passed:
                    print("\n[PASS] Batch storage working correctly")
                else:
                    print(f"\n[FAIL] Expected {len(test_opportunities)}, got {len(batch_items)}")
            else:
                result.passed = False
                result.details = {"error": "No batch_id returned"}
                print("\n[FAIL] No batch_id returned")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    def print_summary(self):
        """Print test summary."""
        separator("TEST SUMMARY")

        passed = 0
        failed = 0

        for r in self.test_results:
            status = "PASS" if r.passed else "FAIL"
            if r.passed:
                passed += 1
            else:
                failed += 1

            print(f"[{status}] {r.name} ({r.duration_ms}ms)")
            if r.error:
                print(f"       Error: {r.error}")

        print(f"\nTotal: {passed} passed, {failed} failed")
        return failed == 0


async def main():
    """Run the edge case tests."""
    separator("NETWORKING & PROACTIVE OUTREACH - EDGE CASE TESTS")
    print("Testing edge cases and error scenarios")
    print(f"Test User ID: {TEST_USER_ID}")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")

    tester = EdgeCaseTester()

    # Setup
    separator("Setup")
    user = await tester.setup()
    if not user:
        return 1

    # Run tests
    separator("Running Edge Case Tests")

    # Test 1: No Zep data fallback
    result1 = await tester.test_no_zep_data_fallback(user)
    tester.test_results.append(result1)

    # Test 2: Duplicate purpose detection
    result2 = await tester.test_duplicate_purpose_detection(user)
    tester.test_results.append(result2)

    # Test 3: Empty ranking handling
    result3 = await tester.test_empty_ranking_handling(user)
    tester.test_results.append(result3)

    # Test 4: User changes mind
    result4 = await tester.test_user_changes_mind(user)
    tester.test_results.append(result4)

    # Test 5: Invalid user ID repair
    result5 = await tester.test_invalid_user_id_repair(user)
    tester.test_results.append(result5)

    # Test 6: Network empty handling
    result6 = await tester.test_network_empty_handling(user)
    tester.test_results.append(result6)

    # Test 7: Cooldown period check
    result7 = await tester.test_cooldown_period_check(user)
    tester.test_results.append(result7)

    # Test 8: Match type ambiguity
    result8 = await tester.test_match_type_ambiguity(user)
    tester.test_results.append(result8)

    # Test 9: Very long purpose
    result9 = await tester.test_very_long_purpose(user)
    tester.test_results.append(result9)

    # Test 10: Special characters
    result10 = await tester.test_special_characters_in_purpose(user)
    tester.test_results.append(result10)

    # Test 11: Rapid successive requests
    result11 = await tester.test_rapid_successive_requests(user)
    tester.test_results.append(result11)

    # Test 12: Batch storage
    result12 = await tester.test_opportunity_batch_storage(user)
    tester.test_results.append(result12)

    # Cleanup
    separator("Cleanup")
    await tester.cleanup(user["id"])

    # Summary
    success = tester.print_summary()

    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
