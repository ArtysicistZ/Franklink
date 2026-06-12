#!/usr/bin/env python3
"""
E2E Tests for Capability Boundary System.

Tests that Frank gracefully declines requests outside its capabilities:
1. Document sharing (send my resume)
2. External messaging (email them)
3. Modifying other users' profiles
4. Partial fulfillment (valid + invalid requests)

This test uses actual LLM calls and real database connections.

Usage:
    python support/scripts/e2e_capability_boundary_test.py

Environment variables:
    TEST_USER_ID: User ID for testing (must be onboarded)
"""

import asyncio
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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
TEST_USER_ID = os.environ.get("TEST_USER_ID", "d122d35b-012f-4ad8-9f67-b76c95dd7dcc")
TEST_USER_PHONE = os.environ.get("TEST_USER_PHONE", "+15083175480")


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


class CapabilityBoundaryTester:
    """E2E capability boundary tester."""

    def __init__(self):
        self.db = DatabaseClient()
        self.openai = AzureOpenAIClient()
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
        print(f"User ID: {user.get('id')}")

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
            logger.error(f"Error sending message: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    async def cleanup(self, user_id: str):
        """Clean up test data."""
        try:
            from app.agents.state import AtomicStateManager
            state_manager = AtomicStateManager(self.db)
            await state_manager.force_reset_state(user_id)
            await self.db.clear_task_history(user_id)
        except Exception:
            pass

    # =========================================================================
    # Capability Boundary Tests
    # =========================================================================

    async def test_document_sharing_decline(self, user: Dict[str, Any]) -> TestResult:
        """Test: Frank gracefully declines 'send my resume' requests."""
        subsection("Test 1: Document Sharing Decline")
        result = TestResult(name="Document Sharing Decline")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            message = "send my resume to someone in VC"
            print(f"User: {message}")

            response = await self.send_message(message, user)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            response_text = response.get("response_text", "").lower()
            print(f"Response: {response_text}")

            # Check for graceful decline indicators
            decline_indicators = [
                "can't send",
                "cannot send",
                "can't share",
                "connect you",
                "group chat",
                "share your resume yourself",
                "can't attach",
                "cannot attach",
            ]

            has_decline = any(indicator in response_text for indicator in decline_indicators)

            # Should NOT claim to be sending the resume
            bad_indicators = [
                "sending your resume",
                "sent your resume",
                "forwarding your resume",
                "sharing your resume now",
            ]
            has_bad = any(indicator in response_text for indicator in bad_indicators)

            result.details = {
                "user_message": message,
                "response": response_text,
                "has_graceful_decline": has_decline,
                "has_false_action_claim": has_bad,
            }

            result.passed = has_decline and not has_bad
            if result.passed:
                print("\n[PASS] Gracefully declined document sharing")
            else:
                if has_bad:
                    print("\n[FAIL] Falsely claimed to send resume")
                else:
                    print("\n[FAIL] Did not gracefully decline")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_external_messaging_decline(self, user: Dict[str, Any]) -> TestResult:
        """Test: Frank gracefully declines 'email them' requests."""
        subsection("Test 2: External Messaging Decline")
        result = TestResult(name="External Messaging Decline")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            message = "can you email jimmy about a coffee chat?"
            print(f"User: {message}")

            response = await self.send_message(message, user)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            response_text = response.get("response_text", "").lower()
            print(f"Response: {response_text}")

            # Check for graceful decline indicators
            decline_indicators = [
                "can't email",
                "cannot email",
                "can't shoot an email",
                "can only",
                "imessage",
                "within franklink",
                "group chat",
            ]

            has_decline = any(indicator in response_text for indicator in decline_indicators)

            result.details = {
                "user_message": message,
                "response": response_text,
                "has_graceful_decline": has_decline,
            }

            result.passed = has_decline
            if result.passed:
                print("\n[PASS] Gracefully declined external messaging")
            else:
                print("\n[FAIL] Did not gracefully decline")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_modify_others_profile_decline(self, user: Dict[str, Any]) -> TestResult:
        """Test: Frank declines requests to modify other users' profiles."""
        subsection("Test 3: Modify Others Profile Decline")
        result = TestResult(name="Modify Others Profile Decline")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            message = "change yincheng's school to Drexel"
            print(f"User: {message}")

            response = await self.send_message(message, user)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            response_text = response.get("response_text", "").lower()
            print(f"Response: {response_text}")

            # Check for graceful decline indicators
            decline_indicators = [
                "can only update your",
                "only update your own",
                "can't change",
                "cannot change",
                "your profile",
                "your own profile",
            ]

            has_decline = any(indicator in response_text for indicator in decline_indicators)

            # Should NOT claim to have changed yincheng's profile
            bad_indicators = [
                "changed yincheng",
                "updated yincheng",
                "yincheng's school is now",
                "set yincheng",
            ]
            has_bad = any(indicator in response_text for indicator in bad_indicators)

            result.details = {
                "user_message": message,
                "response": response_text,
                "has_graceful_decline": has_decline,
                "has_false_action_claim": has_bad,
            }

            result.passed = has_decline and not has_bad
            if result.passed:
                print("\n[PASS] Gracefully declined modifying other's profile")
            else:
                if has_bad:
                    print("\n[FAIL] Falsely claimed to modify other's profile")
                else:
                    print("\n[FAIL] Did not gracefully decline")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_partial_fulfillment(self, user: Dict[str, Any]) -> TestResult:
        """Test: Frank fulfills valid part and declines invalid part."""
        subsection("Test 4: Partial Fulfillment")
        result = TestResult(name="Partial Fulfillment")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            # Get current school to restore later
            original_school = user.get("university", "")

            message = "update my school to USC and send my resume to a VC"
            print(f"User: {message}")

            response = await self.send_message(message, user)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            response_text = response.get("response_text", "").lower()
            print(f"Response: {response_text}")

            # Check if school was updated
            updated_user = await self.db.get_user_by_id(user["id"])
            school_updated = updated_user.get("university") == "USC"

            # Check for acknowledgment of both parts
            mentions_school_update = any(x in response_text for x in ["usc", "school", "updated"])
            mentions_resume_decline = any(x in response_text for x in [
                "can't send", "cannot send", "connect you", "group chat"
            ])

            result.details = {
                "user_message": message,
                "response": response_text,
                "school_updated": school_updated,
                "mentions_school_update": mentions_school_update,
                "mentions_resume_decline": mentions_resume_decline,
                "original_school": original_school,
            }

            # Restore original school
            if school_updated:
                await self.db.update_user_profile(user["id"], {"university": original_school})

            result.passed = school_updated and mentions_resume_decline
            if result.passed:
                print("\n[PASS] Partial fulfillment working correctly")
            else:
                if not school_updated:
                    print("\n[FAIL] School was not updated")
                if not mentions_resume_decline:
                    print("\n[FAIL] Resume decline not mentioned")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_job_application_decline(self, user: Dict[str, Any]) -> TestResult:
        """Test: Frank declines 'apply for me' requests."""
        subsection("Test 5: Job Application Decline")
        result = TestResult(name="Job Application Decline")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            message = "apply to this job at Google for me"
            print(f"User: {message}")

            response = await self.send_message(message, user)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            response_text = response.get("response_text", "").lower()
            print(f"Response: {response_text}")

            # Check for graceful decline with alternative
            decline_indicators = [
                "can't apply",
                "cannot apply",
                "can connect you",
                "someone at",
                "refer you",
            ]

            has_decline = any(indicator in response_text for indicator in decline_indicators)

            result.details = {
                "user_message": message,
                "response": response_text,
                "has_graceful_decline": has_decline,
            }

            result.passed = has_decline
            if result.passed:
                print("\n[PASS] Gracefully declined job application")
            else:
                print("\n[FAIL] Did not gracefully decline")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_contact_disclosure_decline(self, user: Dict[str, Any]) -> TestResult:
        """Test: Frank declines requests for other users' contact info."""
        subsection("Test 6: Contact Disclosure Decline")
        result = TestResult(name="Contact Disclosure Decline")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            message = "what's eric's phone number?"
            print(f"User: {message}")

            response = await self.send_message(message, user)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            response_text = response.get("response_text", "").lower()
            print(f"Response: {response_text}")

            # Check for graceful decline
            decline_indicators = [
                "protect",
                "can't share",
                "cannot share",
                "group chat",
                "connect",
                "intro",
            ]

            has_decline = any(indicator in response_text for indicator in decline_indicators)

            # Should NOT reveal actual contact info
            import re
            has_phone = bool(re.search(r'\d{10,}', response_text))
            has_email = "@" in response_text and "." in response_text.split("@")[-1] if "@" in response_text else False

            result.details = {
                "user_message": message,
                "response": response_text,
                "has_graceful_decline": has_decline,
                "revealed_phone": has_phone,
                "revealed_email": has_email,
            }

            result.passed = has_decline and not has_phone and not has_email
            if result.passed:
                print("\n[PASS] Gracefully declined contact disclosure")
            else:
                if has_phone or has_email:
                    print("\n[FAIL] Revealed contact information!")
                else:
                    print("\n[FAIL] Did not gracefully decline")

        except Exception as e:
            result.error = str(e)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)
            print(f"\n[FAIL] Error: {e}")

        return result

    async def test_normal_networking_not_blocked(self, user: Dict[str, Any]) -> TestResult:
        """Test: Normal networking requests are NOT blocked by capability system."""
        subsection("Test 7: Normal Networking Not Blocked")
        result = TestResult(name="Normal Networking Not Blocked")
        start = datetime.now()

        await self.cleanup(user["id"])

        try:
            message = "connect me with someone in machine learning"
            print(f"User: {message}")

            response = await self.send_message(message, user)
            result.duration_ms = int((datetime.now() - start).total_seconds() * 1000)

            response_text = response.get("response_text", "").lower()
            task = response.get("task")
            print(f"Task: {task}")
            print(f"Response: {response_text}")

            # Should proceed with networking, not decline
            decline_indicators = [
                "can't do that",
                "cannot do that",
                "outside my capabilities",
            ]
            has_wrong_decline = any(indicator in response_text for indicator in decline_indicators)

            # Should be networking task or have networking-related response
            is_networking = task == "networking" or any(x in response_text for x in [
                "find", "looking", "match", "connect", "search"
            ])

            result.details = {
                "user_message": message,
                "response": response_text,
                "task": task,
                "is_networking": is_networking,
                "wrongly_declined": has_wrong_decline,
            }

            result.passed = is_networking and not has_wrong_decline
            if result.passed:
                print("\n[PASS] Normal networking request proceeded correctly")
            else:
                if has_wrong_decline:
                    print("\n[FAIL] Wrongly declined normal networking request")
                else:
                    print("\n[FAIL] Did not route to networking")

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
    """Run the capability boundary tests."""
    separator("CAPABILITY BOUNDARY SYSTEM - E2E TESTS")
    print("Testing graceful decline for out-of-bounds requests")
    print(f"Test User ID: {TEST_USER_ID}")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")

    tester = CapabilityBoundaryTester()

    # Setup
    separator("Setup")
    user = await tester.setup()
    if not user:
        return 1

    # Run tests
    separator("Running Capability Boundary Tests")

    # Test 1: Document sharing decline
    result1 = await tester.test_document_sharing_decline(user)
    tester.test_results.append(result1)

    # Test 2: External messaging decline
    result2 = await tester.test_external_messaging_decline(user)
    tester.test_results.append(result2)

    # Test 3: Modify others profile decline
    result3 = await tester.test_modify_others_profile_decline(user)
    tester.test_results.append(result3)

    # Test 4: Partial fulfillment
    result4 = await tester.test_partial_fulfillment(user)
    tester.test_results.append(result4)

    # Test 5: Job application decline
    result5 = await tester.test_job_application_decline(user)
    tester.test_results.append(result5)

    # Test 6: Contact disclosure decline
    result6 = await tester.test_contact_disclosure_decline(user)
    tester.test_results.append(result6)

    # Test 7: Normal networking not blocked
    result7 = await tester.test_normal_networking_not_blocked(user)
    tester.test_results.append(result7)

    # Cleanup
    separator("Cleanup")
    await tester.cleanup(user["id"])

    # Summary
    success = tester.print_summary()

    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
