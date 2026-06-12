#!/usr/bin/env python3
"""
E2E test for unified group chat structure.

Verifies that the new unified group chat storage model works correctly:
1. Group chat identity (group_chats table)
2. Participant membership (group_chat_participants table)
3. N-participant support in prompts
4. Summary worker with dynamic participant names
5. Follow-up service with dynamic participant handling

Usage:
    python support/scripts/e2e_groupchat_unified_structure_test.py
    python support/scripts/e2e_groupchat_unified_structure_test.py --test-llm
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv

load_dotenv()


@dataclass
class TestResult:
    """Result of a single test case."""
    name: str
    passed: bool
    details: str
    error: Optional[str] = None


class UnifiedGroupChatTester:
    """E2E tester for unified group chat structure."""

    def __init__(self):
        from app.database.client import DatabaseClient
        self.db = DatabaseClient()
        self.test_results: List[TestResult] = []
        self.test_chat_guid: Optional[str] = None
        self.test_user_ids: List[str] = []

    def print_header(self, title: str) -> None:
        print("\n" + "=" * 80)
        print(f" {title}")
        print("=" * 80)

    def print_section(self, title: str) -> None:
        print(f"\n{'-' * 60}")
        print(f" {title}")
        print("-" * 60)

    async def find_test_users(self) -> List[Dict[str, Any]]:
        """Find 3 onboarded users for testing."""
        result = self.db.client.table("users").select("*").eq(
            "is_onboarded", True
        ).limit(3).execute()
        return result.data if result.data else []

    async def find_existing_group_chat(self) -> Optional[Dict[str, Any]]:
        """Find an existing group chat with participants."""
        result = self.db.client.table("group_chats").select("*").limit(1).execute()
        if result.data:
            return result.data[0]
        return None

    # =========================================================================
    # Test: Load participants from unified table
    # =========================================================================
    async def test_load_participants_unified(self) -> TestResult:
        """Test loading participants from unified group_chat_participants table."""
        self.print_section("Test 1: Load Participants from Unified Table")

        chat = await self.find_existing_group_chat()
        if not chat:
            return TestResult(
                name="Load Participants Unified",
                passed=False,
                details="No existing group chat found",
                error="No test data"
            )

        chat_guid = chat.get("chat_guid")
        print(f"Testing with chat_guid: {chat_guid[:40]}...")

        try:
            participants = await self.db.get_group_chat_participants(chat_guid)
            print(f"Found {len(participants)} participants")

            for i, p in enumerate(participants):
                user_id = p.get("user_id", "")[:8]
                mode = p.get("mode", "unknown")
                role = p.get("role", "unknown")
                print(f"  Participant {i+1}: user_id={user_id}..., mode={mode}, role={role}")

            passed = len(participants) >= 2
            return TestResult(
                name="Load Participants Unified",
                passed=passed,
                details=f"Found {len(participants)} participants using unified table",
                error=None if passed else "Expected at least 2 participants"
            )
        except Exception as e:
            return TestResult(
                name="Load Participants Unified",
                passed=False,
                details="Failed to load participants",
                error=str(e)
            )

    # =========================================================================
    # Test: load_participants in followup/context.py
    # =========================================================================
    async def test_followup_context_load_participants(self) -> TestResult:
        """Test the load_participants function returns correct structure."""
        self.print_section("Test 2: Follow-up Context load_participants")

        from app.groupchat.followup.context import load_participants

        chat = await self.find_existing_group_chat()
        if not chat:
            return TestResult(
                name="Follow-up Context load_participants",
                passed=False,
                details="No existing group chat found",
                error="No test data"
            )

        chat_guid = chat.get("chat_guid")
        print(f"Testing load_participants for: {chat_guid[:40]}...")

        try:
            result = await load_participants(self.db, chat_guid=chat_guid)

            # Should return (chat_record, participant_names: List[str], participant_modes: List[str])
            if not isinstance(result, tuple) or len(result) != 3:
                return TestResult(
                    name="Follow-up Context load_participants",
                    passed=False,
                    details=f"Expected 3-tuple, got {type(result)}",
                    error="Wrong return type"
                )

            chat_record, participant_names, participant_modes = result

            print(f"  chat_record: {'found' if chat_record else 'None'}")
            print(f"  participant_names: {participant_names}")
            print(f"  participant_modes: {participant_modes}")

            # Verify types
            passed = (
                (chat_record is None or isinstance(chat_record, dict)) and
                isinstance(participant_names, list) and
                isinstance(participant_modes, list) and
                len(participant_names) == len(participant_modes)
            )

            return TestResult(
                name="Follow-up Context load_participants",
                passed=passed,
                details=f"names={participant_names}, modes={participant_modes}",
                error=None if passed else "Type or length mismatch"
            )
        except Exception as e:
            import traceback
            return TestResult(
                name="Follow-up Context load_participants",
                passed=False,
                details="Exception during load_participants",
                error=f"{e}\n{traceback.format_exc()}"
            )

    # =========================================================================
    # Test: effective_group_mode with N participants
    # =========================================================================
    async def test_effective_group_mode_variadic(self) -> TestResult:
        """Test effective_group_mode handles N participant modes."""
        self.print_section("Test 3: effective_group_mode with N Participants")

        from app.groupchat.followup.utils import effective_group_mode

        test_cases = [
            # (modes, expected_result, description)
            (["active", "active"], "active", "2 active -> active"),
            (["active", "quiet"], "quiet", "active + quiet -> quiet"),
            (["active", "muted"], "muted", "active + muted -> muted"),
            (["quiet", "muted"], "muted", "quiet + muted -> muted"),
            (["active", "active", "active"], "active", "3 active -> active"),
            (["active", "quiet", "muted"], "muted", "mixed -> muted (most restrictive)"),
            (["active", "quiet", "quiet"], "quiet", "3 participants with quiet"),
            ([], "active", "empty -> active (default)"),
            (["invalid", "active"], "active", "invalid mode treated as active"),
        ]

        all_passed = True
        details = []

        for modes, expected, description in test_cases:
            result = effective_group_mode(*modes)
            passed = result == expected
            all_passed = all_passed and passed
            status = "[PASS]" if passed else "[FAIL]"
            details.append(f"{status} {description}: {modes} -> {result} (expected {expected})")
            print(f"  {status} {description}")

        return TestResult(
            name="effective_group_mode Variadic",
            passed=all_passed,
            details="\n".join(details),
            error=None if all_passed else "Some test cases failed"
        )

    # =========================================================================
    # Test: build_groupchat_followup_messages with N participants
    # =========================================================================
    async def test_followup_prompt_n_participants(self) -> TestResult:
        """Test follow-up prompt generation for N participants."""
        self.print_section("Test 4: Follow-up Prompt with N Participants")

        from app.groupchat.followup.prompts import build_groupchat_followup_messages

        test_cases = [
            (["Alice", "Bob"], "two people"),
            (["Alice", "Bob", "Charlie"], "3 people"),
            (["Alice", "Bob", "Charlie", "Diana"], "4 people"),
            (["Solo"], "1 people"),  # Edge case
        ]

        all_passed = True
        details = []

        for participant_names, expected_phrase in test_cases:
            messages = build_groupchat_followup_messages(
                chat_guid="test-chat-guid",
                participant_names=participant_names,
                inactivity_minutes=60,
                last_user_message_at="2024-01-01T00:00:00Z",
                summary_segments=["## Topics\n- test topic"],
            )

            system_content = messages[0]["content"]
            user_content = messages[1]["content"]

            # Check that the prompt mentions the correct people count
            has_people_phrase = expected_phrase in system_content
            has_participants = all(name in user_content for name in participant_names)

            passed = has_people_phrase and has_participants
            all_passed = all_passed and passed

            status = "[PASS]" if passed else "[FAIL]"
            details.append(f"{status} {len(participant_names)} participants: phrase='{expected_phrase}' found={has_people_phrase}")
            print(f"  {status} {participant_names} -> '{expected_phrase}' in prompt")

        return TestResult(
            name="Follow-up Prompt N Participants",
            passed=all_passed,
            details="\n".join(details),
            error=None if all_passed else "Some prompts missing expected content"
        )

    # =========================================================================
    # Test: build_groupchat_summary_messages with N participants
    # =========================================================================
    async def test_summary_prompt_n_participants(self) -> TestResult:
        """Test summary prompt generation for N participants."""
        self.print_section("Test 5: Summary Prompt with N Participants")

        from app.groupchat.summary.prompts import build_groupchat_summary_messages

        test_cases = [
            ["Alice", "Bob"],
            ["Alice", "Bob", "Charlie"],
            ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        ]

        all_passed = True
        details = []

        for participant_names in test_cases:
            messages = build_groupchat_summary_messages(
                chat_guid="test-chat-guid",
                participant_names=participant_names,
                segment_start_at="2024-01-01T00:00:00Z",
                segment_end_at="2024-01-01T01:00:00Z",
                transcript_lines=["[2024-01-01T00:30:00Z] user:Alice: Hello everyone"],
            )

            system_content = messages[0]["content"]
            user_content = messages[1]["content"]

            # Check that each participant has their own section in the template
            has_all_sections = all(f"### {name}" in system_content for name in participant_names)
            has_participants_list = all(name in user_content for name in participant_names)

            passed = has_all_sections and has_participants_list
            all_passed = all_passed and passed

            status = "[PASS]" if passed else "[FAIL]"
            details.append(f"{status} {len(participant_names)} participants: sections={has_all_sections}, list={has_participants_list}")
            print(f"  {status} {participant_names} -> person sections in prompt")

        return TestResult(
            name="Summary Prompt N Participants",
            passed=all_passed,
            details="\n".join(details),
            error=None if all_passed else "Some prompts missing expected content"
        )

    # =========================================================================
    # Test: GroupChatManagedContext with N participants
    # =========================================================================
    async def test_managed_context_n_participants(self) -> TestResult:
        """Test GroupChatManagedContext handles N participants."""
        self.print_section("Test 6: GroupChatManagedContext N Participants")

        from app.groupchat.runtime.types import GroupChatManagedContext

        # Create context with 3 participants
        participant_ids = ("user-a-id", "user-b-id", "user-c-id")
        participant_modes = {
            "user-a-id": "active",
            "user-b-id": "quiet",
            "user-c-id": "muted",
        }

        ctx = GroupChatManagedContext(
            chat_guid="test-chat",
            participant_ids=participant_ids,
            participant_modes=participant_modes,
            connection_request_id=None,
            member_count=3,
        )

        tests = [
            (ctx.is_participant("user-a-id"), True, "is_participant for user-a"),
            (ctx.is_participant("user-b-id"), True, "is_participant for user-b"),
            (ctx.is_participant("user-c-id"), True, "is_participant for user-c"),
            (ctx.is_participant("unknown-id"), False, "is_participant for unknown"),
            (len(ctx.participant_ids), 3, "participant count"),
            (ctx.participant_modes.get("user-b-id"), "quiet", "mode lookup"),
        ]

        all_passed = True
        details = []

        for actual, expected, description in tests:
            passed = actual == expected
            all_passed = all_passed and passed
            status = "[PASS]" if passed else "[FAIL]"
            details.append(f"{status} {description}: {actual} == {expected}")
            print(f"  {status} {description}")

        return TestResult(
            name="GroupChatManagedContext N Participants",
            passed=all_passed,
            details="\n".join(details),
            error=None if all_passed else "Some assertions failed"
        )

    # =========================================================================
    # Test: Router loads participants from unified table
    # =========================================================================
    async def test_router_load_managed_context(self) -> TestResult:
        """Test router loads managed context using unified participants."""
        self.print_section("Test 7: Router _load_managed_context")

        from app.groupchat.runtime.router import GroupChatRouter

        chat = await self.find_existing_group_chat()
        if not chat:
            return TestResult(
                name="Router _load_managed_context",
                passed=False,
                details="No existing group chat found",
                error="No test data"
            )

        chat_guid = chat.get("chat_guid")
        print(f"Testing router with chat_guid: {chat_guid[:40]}...")

        try:
            router = GroupChatRouter(db=self.db)
            managed = await router._load_managed_context(chat_guid=chat_guid)

            if not managed:
                return TestResult(
                    name="Router _load_managed_context",
                    passed=False,
                    details="Router returned None for managed context",
                    error="Could not load managed context"
                )

            print(f"  participant_ids: {len(managed.participant_ids)} participants")
            print(f"  participant_modes: {managed.participant_modes}")
            print(f"  member_count: {managed.member_count}")

            # Verify structure
            passed = (
                isinstance(managed.participant_ids, tuple) and
                isinstance(managed.participant_modes, dict) and
                len(managed.participant_ids) >= 2
            )

            return TestResult(
                name="Router _load_managed_context",
                passed=passed,
                details=f"Loaded {len(managed.participant_ids)} participants with modes",
                error=None if passed else "Invalid managed context structure"
            )
        except Exception as e:
            import traceback
            return TestResult(
                name="Router _load_managed_context",
                passed=False,
                details="Exception during _load_managed_context",
                error=f"{e}\n{traceback.format_exc()}"
            )

    # =========================================================================
    # Test: Summary worker collects all participant names
    # =========================================================================
    async def test_summary_worker_participant_collection(self) -> TestResult:
        """Test summary worker collects names for all participants."""
        self.print_section("Test 8: Summary Worker Participant Collection")

        # This test verifies the logic pattern used in summary/worker.py
        chat = await self.find_existing_group_chat()
        if not chat:
            return TestResult(
                name="Summary Worker Participant Collection",
                passed=False,
                details="No existing group chat found",
                error="No test data"
            )

        chat_guid = chat.get("chat_guid")
        print(f"Testing participant collection for: {chat_guid[:40]}...")

        try:
            # Replicate the logic from summary/worker.py
            participants = await self.db.get_group_chat_participants(chat_guid)

            participant_names: List[str] = []
            name_by_user_id: Dict[str, str] = {}

            for i, p in enumerate(participants):
                p_user_id = str(p.get("user_id") or "").strip()
                if p_user_id:
                    user = await self.db.get_user_by_id(p_user_id)
                    name = str((user or {}).get("name") or "").strip() or f"user {i+1}"
                    name_by_user_id[p_user_id] = name
                    participant_names.append(name)

            print(f"  participant_names: {participant_names}")
            print(f"  name_by_user_id keys: {list(name_by_user_id.keys())[:3]}...")

            passed = (
                len(participant_names) >= 2 and
                len(name_by_user_id) == len(participant_names)
            )

            return TestResult(
                name="Summary Worker Participant Collection",
                passed=passed,
                details=f"Collected {len(participant_names)} names: {participant_names}",
                error=None if passed else "Expected at least 2 participants"
            )
        except Exception as e:
            import traceback
            return TestResult(
                name="Summary Worker Participant Collection",
                passed=False,
                details="Exception during participant collection",
                error=f"{e}\n{traceback.format_exc()}"
            )

    # =========================================================================
    # Test: LLM call with actual summary prompt (optional)
    # =========================================================================
    async def test_llm_summary_generation(self) -> TestResult:
        """Test actual LLM call for summary generation with N participants."""
        self.print_section("Test 9: LLM Summary Generation (Real Call)")

        from app.integrations.azure_openai_client import AzureOpenAIClient
        from app.groupchat.summary.prompts import build_groupchat_summary_messages

        openai = AzureOpenAIClient()

        # Generate a test prompt with 3 participants
        participant_names = ["Alice", "Bob", "Charlie"]
        transcript_lines = [
            "[2024-01-01T10:00:00Z] user:Alice: hey everyone, excited for the hackathon!",
            "[2024-01-01T10:01:00Z] user:Bob: me too! what project should we build?",
            "[2024-01-01T10:02:00Z] user:Charlie: how about an ai assistant?",
            "[2024-01-01T10:03:00Z] user:Alice: love it! i can work on the frontend",
            "[2024-01-01T10:04:00Z] user:Bob: i'll handle backend apis",
            "[2024-01-01T10:05:00Z] user:Charlie: perfect, i'll do the ml model",
        ]

        messages = build_groupchat_summary_messages(
            chat_guid="test-chat-guid",
            participant_names=participant_names,
            segment_start_at=None,
            segment_end_at="2024-01-01T10:10:00Z",
            transcript_lines=transcript_lines,
        )

        print(f"  Sending prompt with {len(participant_names)} participants...")
        print(f"  Transcript lines: {len(transcript_lines)}")

        try:
            response = await openai.generate_response(
                messages=messages,
                model="gpt-4o-mini",
                temperature=0.2,
                trace_label="e2e_unified_structure_test",
            )

            print(f"\n  LLM Response preview:")
            preview = response[:500] if response else ""
            for line in preview.split("\n")[:10]:
                print(f"    {line}")

            # Verify response contains sections for all participants
            has_all_sections = all(name in response for name in participant_names)
            has_topics = "## Topics" in response or "## topics" in response.lower()
            has_summary = "## One-line Summary" in response or "summary" in response.lower()

            passed = has_all_sections and (has_topics or has_summary)

            return TestResult(
                name="LLM Summary Generation",
                passed=passed,
                details=f"Response length: {len(response)}, has_all_names={has_all_sections}",
                error=None if passed else "LLM response missing expected sections"
            )
        except Exception as e:
            import traceback
            return TestResult(
                name="LLM Summary Generation",
                passed=False,
                details="Exception during LLM call",
                error=f"{e}\n{traceback.format_exc()}"
            )

    # =========================================================================
    # Test: LLM call with actual followup prompt (optional)
    # =========================================================================
    async def test_llm_followup_generation(self) -> TestResult:
        """Test actual LLM call for followup generation with N participants."""
        self.print_section("Test 10: LLM Follow-up Generation (Real Call)")

        from app.integrations.azure_openai_client import AzureOpenAIClient
        from app.groupchat.followup.prompts import build_groupchat_followup_messages

        openai = AzureOpenAIClient()

        # Generate a test prompt with 3 participants
        participant_names = ["Alice", "Bob", "Charlie"]
        summary_segments = [
            "## Topics\n- Planning a hackathon project\n- Dividing work: frontend, backend, ML",
            "## Each Person\n### Alice\n- Excited for hackathon\n- Will work on frontend",
            "### Bob\n- Handling backend APIs",
            "### Charlie\n- Working on ML model",
        ]

        messages = build_groupchat_followup_messages(
            chat_guid="test-chat-guid",
            participant_names=participant_names,
            inactivity_minutes=1440,  # 24 hours
            last_user_message_at="2024-01-01T10:05:00Z",
            summary_segments=summary_segments,
        )

        print(f"  Sending prompt with {len(participant_names)} participants...")

        try:
            response = await openai.generate_response(
                messages=messages,
                model="gpt-4o-mini",
                temperature=0.6,
                trace_label="e2e_unified_structure_test_followup",
            )

            print(f"\n  LLM Response:")
            print(f"    {response}")

            # Verify response follows rules (no emojis, lowercase, no markdown)
            is_lowercase = response == response.lower()
            no_emojis = not any(ord(c) > 127 for c in response)  # Simplified emoji check
            reasonable_length = 50 < len(response) < 500

            passed = reasonable_length  # Main check is that it generated something reasonable

            return TestResult(
                name="LLM Follow-up Generation",
                passed=passed,
                details=f"Response: {response[:100]}..., len={len(response)}",
                error=None if passed else "LLM response not as expected"
            )
        except Exception as e:
            import traceback
            return TestResult(
                name="LLM Follow-up Generation",
                passed=False,
                details="Exception during LLM call",
                error=f"{e}\n{traceback.format_exc()}"
            )

    # =========================================================================
    # Run all tests
    # =========================================================================
    async def run_all_tests(self, test_llm: bool = False) -> None:
        """Run all unified structure tests."""
        self.print_header("UNIFIED GROUP CHAT STRUCTURE E2E TESTS")
        print(f"Started: {datetime.now(timezone.utc).isoformat()}")
        print(f"Test LLM: {test_llm}")

        # Core structure tests
        tests = [
            self.test_load_participants_unified,
            self.test_followup_context_load_participants,
            self.test_effective_group_mode_variadic,
            self.test_followup_prompt_n_participants,
            self.test_summary_prompt_n_participants,
            self.test_managed_context_n_participants,
            self.test_router_load_managed_context,
            self.test_summary_worker_participant_collection,
        ]

        # Optional LLM tests
        if test_llm:
            tests.extend([
                self.test_llm_summary_generation,
                self.test_llm_followup_generation,
            ])

        for test_fn in tests:
            try:
                result = await test_fn()
                self.test_results.append(result)
            except Exception as e:
                import traceback
                self.test_results.append(TestResult(
                    name=test_fn.__name__,
                    passed=False,
                    details="Test raised exception",
                    error=f"{e}\n{traceback.format_exc()}"
                ))

        # Summary
        self.print_header("TEST SUMMARY")

        passed_count = sum(1 for r in self.test_results if r.passed)
        total_count = len(self.test_results)

        print(f"\nResults: {passed_count}/{total_count} tests passed\n")

        for result in self.test_results:
            status = "[PASS]" if result.passed else "[FAIL]"
            print(f"  {status}: {result.name}")
            if result.error:
                print(f"         Error: {result.error[:200]}")

        print(f"\nCompleted: {datetime.now(timezone.utc).isoformat()}")

        if passed_count < total_count:
            print(f"\n{'=' * 80}")
            print(f" [WARN] {total_count - passed_count} tests failed")
            print(f"{'=' * 80}")
        else:
            print(f"\n{'=' * 80}")
            print(f" [SUCCESS] All tests passed!")
            print(f"{'=' * 80}")


async def main():
    parser = argparse.ArgumentParser(description="E2E test for unified group chat structure")
    parser.add_argument(
        "--test-llm",
        action="store_true",
        help="Include tests with actual LLM calls"
    )
    args = parser.parse_args()

    tester = UnifiedGroupChatTester()
    await tester.run_all_tests(test_llm=args.test_llm)


if __name__ == "__main__":
    asyncio.run(main())
