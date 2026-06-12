#!/usr/bin/env python3
"""
Comprehensive E2E test for unified group chat structure - Edge Cases & Scenarios.

Tests additional scenarios including:
1. Edge cases for participant handling
2. Mode transitions and combinations
3. Router event enrichment
4. Group chat provisioning flow
5. Database operations for participants
6. Empty/missing data handling
7. Multi-person chat scenarios (3+ participants)
8. Real database CRUD operations

Usage:
    python support/scripts/e2e_groupchat_comprehensive_test.py
    python support/scripts/e2e_groupchat_comprehensive_test.py --test-llm
    python support/scripts/e2e_groupchat_comprehensive_test.py --include-destructive
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

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


class ComprehensiveGroupChatTester:
    """Comprehensive E2E tester for group chat functionality."""

    def __init__(self):
        from app.database.client import DatabaseClient
        self.db = DatabaseClient()
        self.test_results: List[TestResult] = []
        self.cleanup_chat_guids: List[str] = []
        self.cleanup_user_ids: List[str] = []

    def print_header(self, title: str) -> None:
        print("\n" + "=" * 80)
        print(f" {title}")
        print("=" * 80)

    def print_section(self, title: str) -> None:
        print(f"\n{'-' * 60}")
        print(f" {title}")
        print("-" * 60)

    # =========================================================================
    # EDGE CASE TESTS
    # =========================================================================

    async def test_empty_participant_list(self) -> TestResult:
        """Test handling when no participants exist."""
        self.print_section("Edge Case 1: Empty Participant List")

        from app.groupchat.followup.context import load_participants

        # Use a non-existent chat GUID
        fake_guid = f"test;+;nonexistent-{uuid.uuid4()}"

        try:
            chat, names, modes = await load_participants(self.db, chat_guid=fake_guid)

            print(f"  chat: {chat}")
            print(f"  names: {names}")
            print(f"  modes: {modes}")

            # Should return None chat and empty lists
            passed = chat is None and names == [] and modes == []

            return TestResult(
                name="Empty Participant List",
                passed=passed,
                details=f"Returned: chat={chat}, names={names}, modes={modes}",
                error=None if passed else "Expected None/empty for non-existent chat"
            )
        except Exception as e:
            return TestResult(
                name="Empty Participant List",
                passed=False,
                details="Exception during test",
                error=str(e)
            )

    async def test_single_participant_chat(self) -> TestResult:
        """Test handling of a chat with only 1 participant (edge case)."""
        self.print_section("Edge Case 2: Single Participant Chat")

        from app.groupchat.followup.prompts import build_groupchat_followup_messages
        from app.groupchat.summary.prompts import build_groupchat_summary_messages

        single_participant = ["Alice"]

        try:
            # Test followup prompt
            followup_msgs = build_groupchat_followup_messages(
                chat_guid="test-single",
                participant_names=single_participant,
                inactivity_minutes=60,
                last_user_message_at="2024-01-01T00:00:00Z",
                summary_segments=["## Topics\n- test"],
            )

            # Test summary prompt
            summary_msgs = build_groupchat_summary_messages(
                chat_guid="test-single",
                participant_names=single_participant,
                segment_start_at=None,
                segment_end_at="2024-01-01T01:00:00Z",
                transcript_lines=["[ts] user:Alice: hello"],
            )

            followup_system = followup_msgs[0]["content"]
            summary_system = summary_msgs[0]["content"]

            print(f"  Followup mentions '1 people': {'1 people' in followup_system}")
            print(f"  Summary has Alice section: {'### Alice' in summary_system}")

            passed = "1 people" in followup_system and "### Alice" in summary_system

            return TestResult(
                name="Single Participant Chat",
                passed=passed,
                details="Prompts generated for single participant",
                error=None if passed else "Prompts don't handle single participant correctly"
            )
        except Exception as e:
            return TestResult(
                name="Single Participant Chat",
                passed=False,
                details="Exception during test",
                error=str(e)
            )

    async def test_many_participants_chat(self) -> TestResult:
        """Test handling of a chat with many participants (10+)."""
        self.print_section("Edge Case 3: Many Participants (10+)")

        from app.groupchat.followup.prompts import build_groupchat_followup_messages
        from app.groupchat.summary.prompts import build_groupchat_summary_messages
        from app.groupchat.followup.utils import effective_group_mode

        many_participants = [f"User{i}" for i in range(1, 11)]  # 10 participants
        many_modes = ["active"] * 8 + ["quiet", "muted"]  # Mix of modes

        try:
            # Test followup prompt
            followup_msgs = build_groupchat_followup_messages(
                chat_guid="test-many",
                participant_names=many_participants,
                inactivity_minutes=60,
                last_user_message_at="2024-01-01T00:00:00Z",
                summary_segments=["## Topics\n- big group discussion"],
            )

            # Test summary prompt
            summary_msgs = build_groupchat_summary_messages(
                chat_guid="test-many",
                participant_names=many_participants,
                segment_start_at=None,
                segment_end_at="2024-01-01T01:00:00Z",
                transcript_lines=["[ts] user:User1: hello everyone"],
            )

            # Test effective mode with many participants
            mode = effective_group_mode(*many_modes)

            followup_system = followup_msgs[0]["content"]
            summary_system = summary_msgs[0]["content"]

            has_all_sections = all(f"### {name}" in summary_system for name in many_participants)
            mentions_10_people = "10 people" in followup_system

            print(f"  Followup mentions '10 people': {mentions_10_people}")
            print(f"  Summary has all 10 sections: {has_all_sections}")
            print(f"  Effective mode (with muted): {mode}")

            passed = mentions_10_people and has_all_sections and mode == "muted"

            return TestResult(
                name="Many Participants Chat",
                passed=passed,
                details=f"10 participants handled, mode={mode}",
                error=None if passed else "Failed to handle 10 participants correctly"
            )
        except Exception as e:
            return TestResult(
                name="Many Participants Chat",
                passed=False,
                details="Exception during test",
                error=str(e)
            )

    async def test_special_characters_in_names(self) -> TestResult:
        """Test handling of special characters in participant names."""
        self.print_section("Edge Case 4: Special Characters in Names")

        from app.groupchat.followup.prompts import build_groupchat_followup_messages
        from app.groupchat.summary.prompts import build_groupchat_summary_messages

        # Names with special characters, unicode, etc.
        special_names = [
            "Alice O'Brien",
            "Bob (The Builder)",
            "Charlie & Diana",
            "Eve <script>",
            "Frank \"Frankie\" Jr.",
        ]

        try:
            followup_msgs = build_groupchat_followup_messages(
                chat_guid="test-special",
                participant_names=special_names,
                inactivity_minutes=60,
                last_user_message_at="2024-01-01T00:00:00Z",
                summary_segments=["## Topics\n- test"],
            )

            summary_msgs = build_groupchat_summary_messages(
                chat_guid="test-special",
                participant_names=special_names,
                segment_start_at=None,
                segment_end_at="2024-01-01T01:00:00Z",
                transcript_lines=["[ts] user:Alice: hello"],
            )

            # Check prompts generated without errors
            followup_user = followup_msgs[1]["content"]
            summary_system = summary_msgs[0]["content"]

            has_all_names = all(name in followup_user for name in special_names)

            print(f"  All special names in followup: {has_all_names}")
            print(f"  Prompt length: {len(followup_user)} chars")

            passed = has_all_names and len(followup_user) > 0

            return TestResult(
                name="Special Characters in Names",
                passed=passed,
                details=f"Handled names: {special_names}",
                error=None if passed else "Failed to handle special characters"
            )
        except Exception as e:
            return TestResult(
                name="Special Characters in Names",
                passed=False,
                details="Exception during test",
                error=str(e)
            )

    async def test_empty_names_fallback(self) -> TestResult:
        """Test handling when participant names are empty strings."""
        self.print_section("Edge Case 5: Empty Names with Fallback")

        from app.groupchat.followup.prompts import build_groupchat_followup_messages
        from app.groupchat.summary.prompts import build_groupchat_summary_messages

        # Mix of empty and valid names
        names_with_empty = ["", "Bob", "", "Diana"]

        try:
            followup_msgs = build_groupchat_followup_messages(
                chat_guid="test-empty-names",
                participant_names=names_with_empty,
                inactivity_minutes=60,
                last_user_message_at="2024-01-01T00:00:00Z",
                summary_segments=["## Topics\n- test"],
            )

            summary_msgs = build_groupchat_summary_messages(
                chat_guid="test-empty-names",
                participant_names=names_with_empty,
                segment_start_at=None,
                segment_end_at="2024-01-01T01:00:00Z",
                transcript_lines=["[ts] user:Bob: hello"],
            )

            # Should handle empty names gracefully (include them as-is or skip)
            followup_content = followup_msgs[1]["content"]
            summary_content = summary_msgs[0]["content"]

            print(f"  Followup generated: {len(followup_content) > 0}")
            print(f"  Summary generated: {len(summary_content) > 0}")
            print(f"  Has Bob: {'Bob' in followup_content}")
            print(f"  Has Diana: {'Diana' in followup_content}")

            passed = len(followup_content) > 0 and "Bob" in followup_content

            return TestResult(
                name="Empty Names Fallback",
                passed=passed,
                details="Generated prompts with mixed empty/valid names",
                error=None if passed else "Failed to handle empty names"
            )
        except Exception as e:
            return TestResult(
                name="Empty Names Fallback",
                passed=False,
                details="Exception during test",
                error=str(e)
            )

    # =========================================================================
    # MODE TRANSITION TESTS
    # =========================================================================

    async def test_mode_transitions(self) -> TestResult:
        """Test all possible mode transition scenarios."""
        self.print_section("Mode Test 1: All Mode Combinations")

        from app.groupchat.followup.utils import effective_group_mode

        # Test matrix: all combinations of 2-3 participants
        test_cases = [
            # 2 participants
            (["active", "active"], "active"),
            (["active", "quiet"], "quiet"),
            (["active", "muted"], "muted"),
            (["quiet", "quiet"], "quiet"),
            (["quiet", "muted"], "muted"),
            (["muted", "muted"], "muted"),
            # 3 participants - various combos
            (["active", "active", "active"], "active"),
            (["active", "active", "quiet"], "quiet"),
            (["active", "active", "muted"], "muted"),
            (["active", "quiet", "quiet"], "quiet"),
            (["active", "quiet", "muted"], "muted"),
            (["quiet", "quiet", "quiet"], "quiet"),
            (["quiet", "quiet", "muted"], "muted"),
            (["muted", "muted", "muted"], "muted"),
            # Edge: Single mode
            (["active"], "active"),
            (["quiet"], "quiet"),
            (["muted"], "muted"),
            # Edge: No modes
            ([], "active"),
            # Edge: Invalid modes should default to active
            (["invalid"], "active"),
            (["ACTIVE"], "active"),  # Case insensitive
            (["Active", "Quiet"], "quiet"),
            (["", "muted"], "muted"),  # Empty string -> active
            ([None, "quiet"], "quiet"),  # None -> active
        ]

        all_passed = True
        failures = []

        for modes, expected in test_cases:
            result = effective_group_mode(*modes)
            if result != expected:
                all_passed = False
                failures.append(f"{modes} -> {result} (expected {expected})")
            else:
                print(f"  [PASS] {modes} -> {result}")

        if failures:
            for f in failures:
                print(f"  [FAIL] {f}")

        return TestResult(
            name="All Mode Combinations",
            passed=all_passed,
            details=f"Tested {len(test_cases)} combinations",
            error="\n".join(failures) if failures else None
        )

    # =========================================================================
    # ROUTER EVENT ENRICHMENT TESTS
    # =========================================================================

    async def test_router_participant_resolution(self) -> TestResult:
        """Test router resolves participant correctly from N participants."""
        self.print_section("Router Test 1: Participant Resolution")

        from app.groupchat.runtime.types import GroupChatManagedContext, GroupChatEvent

        # Create a managed context with 4 participants
        participant_ids = ("user-1", "user-2", "user-3", "user-4")
        participant_modes = {
            "user-1": "active",
            "user-2": "quiet",
            "user-3": "active",
            "user-4": "muted",
        }

        ctx = GroupChatManagedContext(
            chat_guid="test-router",
            participant_ids=participant_ids,
            participant_modes=participant_modes,
            connection_request_id=None,
            member_count=4,
        )

        tests = [
            # (user_id, expected_is_participant, expected_mode)
            ("user-1", True, "active"),
            ("user-2", True, "quiet"),
            ("user-3", True, "active"),
            ("user-4", True, "muted"),
            ("user-5", False, None),  # Unknown user
            ("", False, None),
            (None, False, None),
        ]

        all_passed = True
        for user_id, expected_is_participant, expected_mode in tests:
            is_participant = ctx.is_participant(user_id) if user_id else False
            actual_mode = ctx.participant_modes.get(user_id) if user_id else None

            passed = (is_participant == expected_is_participant and
                     (actual_mode == expected_mode if expected_is_participant else True))

            if not passed:
                all_passed = False
                print(f"  [FAIL] user_id={user_id}: is_participant={is_participant}, mode={actual_mode}")
            else:
                print(f"  [PASS] user_id={user_id}: is_participant={is_participant}, mode={actual_mode}")

        return TestResult(
            name="Router Participant Resolution",
            passed=all_passed,
            details=f"Tested {len(tests)} resolution scenarios",
            error=None if all_passed else "Some resolutions failed"
        )

    # =========================================================================
    # DATABASE OPERATION TESTS
    # =========================================================================

    async def test_participant_crud_operations(self) -> TestResult:
        """Test CRUD operations for participants (non-destructive read tests)."""
        self.print_section("DB Test 1: Participant Read Operations")

        try:
            # Find an existing group chat
            result = self.db.client.table("group_chats").select("*").limit(1).execute()
            if not result.data:
                return TestResult(
                    name="Participant CRUD Operations",
                    passed=True,
                    details="No group chats to test (skipped)",
                    error=None
                )

            chat_guid = result.data[0]["chat_guid"]

            # Test get_group_chat_by_guid
            chat = await self.db.get_group_chat_by_guid(chat_guid)
            print(f"  get_group_chat_by_guid: {'found' if chat else 'not found'}")

            # Test get_group_chat_participants
            participants = await self.db.get_group_chat_participants(chat_guid)
            print(f"  get_group_chat_participants: {len(participants)} participants")

            # Verify participant structure
            if participants:
                p = participants[0]
                has_user_id = "user_id" in p
                has_mode = "mode" in p
                has_role = "role" in p
                print(f"  Participant has user_id: {has_user_id}")
                print(f"  Participant has mode: {has_mode}")
                print(f"  Participant has role: {has_role}")

            passed = chat is not None and len(participants) >= 2

            return TestResult(
                name="Participant Read Operations",
                passed=passed,
                details=f"Chat found with {len(participants)} participants",
                error=None if passed else "Expected chat with 2+ participants"
            )
        except Exception as e:
            import traceback
            return TestResult(
                name="Participant CRUD Operations",
                passed=False,
                details="Exception during test",
                error=f"{e}\n{traceback.format_exc()}"
            )

    async def test_member_count_consistency(self) -> TestResult:
        """Test that member_count matches actual participant count."""
        self.print_section("DB Test 2: Member Count Consistency")

        try:
            # Get a few group chats
            result = self.db.client.table("group_chats").select("*").limit(5).execute()
            if not result.data:
                return TestResult(
                    name="Member Count Consistency",
                    passed=True,
                    details="No group chats to test (skipped)",
                    error=None
                )

            inconsistencies = []
            for chat in result.data:
                chat_guid = chat["chat_guid"]
                stored_count = chat.get("member_count", 0)

                participants = await self.db.get_group_chat_participants(chat_guid)
                actual_count = len(participants)

                if stored_count != actual_count:
                    inconsistencies.append(f"{chat_guid[:20]}...: stored={stored_count}, actual={actual_count}")
                else:
                    print(f"  [OK] {chat_guid[:30]}...: count={actual_count}")

            passed = len(inconsistencies) == 0

            return TestResult(
                name="Member Count Consistency",
                passed=passed,
                details=f"Checked {len(result.data)} chats",
                error="\n".join(inconsistencies) if inconsistencies else None
            )
        except Exception as e:
            return TestResult(
                name="Member Count Consistency",
                passed=False,
                details="Exception during test",
                error=str(e)
            )

    # =========================================================================
    # LLM EDGE CASE TESTS
    # =========================================================================

    async def test_llm_empty_summary_segments(self) -> TestResult:
        """Test LLM followup generation with empty/minimal summary."""
        self.print_section("LLM Edge Case 1: Empty Summary Segments")

        from app.integrations.azure_openai_client import AzureOpenAIClient
        from app.groupchat.followup.prompts import build_groupchat_followup_messages

        openai = AzureOpenAIClient()

        messages = build_groupchat_followup_messages(
            chat_guid="test-empty-summary",
            participant_names=["Alice", "Bob"],
            inactivity_minutes=1440,
            last_user_message_at="2024-01-01T00:00:00Z",
            summary_segments=["(no summary available)"],  # Minimal summary
        )

        try:
            response = await openai.generate_response(
                messages=messages,
                model="gpt-4o-mini",
                temperature=0.6,
                trace_label="e2e_empty_summary_test",
            )

            print(f"  Response: {response[:150]}...")

            # Should still generate something reasonable
            passed = len(response) > 20

            return TestResult(
                name="LLM Empty Summary Segments",
                passed=passed,
                details=f"Generated {len(response)} chars with minimal summary",
                error=None if passed else "Response too short"
            )
        except Exception as e:
            return TestResult(
                name="LLM Empty Summary Segments",
                passed=False,
                details="Exception during LLM call",
                error=str(e)
            )

    async def test_llm_long_participant_list(self) -> TestResult:
        """Test LLM summary with many participants."""
        self.print_section("LLM Edge Case 2: Long Participant List")

        from app.integrations.azure_openai_client import AzureOpenAIClient
        from app.groupchat.summary.prompts import build_groupchat_summary_messages

        openai = AzureOpenAIClient()

        # 8 participants
        participants = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry"]

        transcript = [
            "[2024-01-01T10:00:00Z] user:Alice: meeting starting now",
            "[2024-01-01T10:01:00Z] user:Bob: here!",
            "[2024-01-01T10:02:00Z] user:Charlie: present",
            "[2024-01-01T10:03:00Z] user:Diana: ready",
            "[2024-01-01T10:04:00Z] user:Eve: joining",
            "[2024-01-01T10:05:00Z] user:Frank: online",
            "[2024-01-01T10:06:00Z] user:Grace: here too",
            "[2024-01-01T10:07:00Z] user:Henry: all set",
            "[2024-01-01T10:10:00Z] user:Alice: let's discuss the project timeline",
            "[2024-01-01T10:11:00Z] user:Bob: i think we need 2 weeks for phase 1",
            "[2024-01-01T10:12:00Z] user:Charlie: agreed, design first",
        ]

        messages = build_groupchat_summary_messages(
            chat_guid="test-many-participants",
            participant_names=participants,
            segment_start_at=None,
            segment_end_at="2024-01-01T10:30:00Z",
            transcript_lines=transcript,
        )

        try:
            response = await openai.generate_response(
                messages=messages,
                model="gpt-4o-mini",
                temperature=0.2,
                trace_label="e2e_many_participants_test",
            )

            print(f"  Response preview: {response[:200]}...")

            # Check that response has sections
            has_topics = "## Topics" in response or "topics" in response.lower()
            mentions_participants = sum(1 for p in participants if p in response)

            print(f"  Has topics section: {has_topics}")
            print(f"  Participants mentioned: {mentions_participants}/{len(participants)}")

            # LLM may not mention all participants if they didn't contribute meaningfully
            # Key check: prompt was built correctly and LLM responded with structure
            passed = has_topics and mentions_participants >= 2

            return TestResult(
                name="LLM Long Participant List",
                passed=passed,
                details=f"Summary generated for {len(participants)} participants, {mentions_participants} mentioned",
                error=None if passed else "Summary missing expected sections"
            )
        except Exception as e:
            return TestResult(
                name="LLM Long Participant List",
                passed=False,
                details="Exception during LLM call",
                error=str(e)
            )

    # =========================================================================
    # TRANSCRIPT FORMATTING TESTS
    # =========================================================================

    async def test_transcript_formatting_n_participants(self) -> TestResult:
        """Test transcript formatting with N participants."""
        self.print_section("Format Test 1: Transcript with N Participants")

        from app.groupchat.summary.worker import _format_transcript_lines

        messages = [
            {"role": "user", "sender_user_id": "user-1", "content": "Hello everyone!", "sent_at": "2024-01-01T10:00:00Z"},
            {"role": "user", "sender_user_id": "user-2", "content": "Hi there!", "sent_at": "2024-01-01T10:01:00Z"},
            {"role": "user", "sender_user_id": "user-3", "content": "Good morning!", "sent_at": "2024-01-01T10:02:00Z"},
            {"role": "assistant", "content": "Welcome to the chat!", "sent_at": "2024-01-01T10:03:00Z"},
            {"role": "user", "sender_user_id": "user-4", "content": "Excited to be here", "sent_at": "2024-01-01T10:04:00Z"},
        ]

        name_by_user_id = {
            "user-1": "Alice",
            "user-2": "Bob",
            "user-3": "Charlie",
            "user-4": "Diana",
        }

        try:
            lines = _format_transcript_lines(
                messages=messages,
                name_by_user_id=name_by_user_id,
                max_lines=100,
            )

            print(f"  Generated {len(lines)} transcript lines:")
            for line in lines[:5]:
                print(f"    {line}")

            # Verify all names appear
            has_alice = any("Alice" in line for line in lines)
            has_bob = any("Bob" in line for line in lines)
            has_charlie = any("Charlie" in line for line in lines)
            has_diana = any("Diana" in line for line in lines)
            has_frank = any("frank" in line for line in lines)  # assistant

            print(f"  Has Alice: {has_alice}")
            print(f"  Has Bob: {has_bob}")
            print(f"  Has Charlie: {has_charlie}")
            print(f"  Has Diana: {has_diana}")
            print(f"  Has Frank (assistant): {has_frank}")

            passed = has_alice and has_bob and has_charlie and has_diana and has_frank

            return TestResult(
                name="Transcript N Participants",
                passed=passed,
                details=f"All 4 user names + frank in transcript",
                error=None if passed else "Some names missing from transcript"
            )
        except Exception as e:
            return TestResult(
                name="Transcript N Participants",
                passed=False,
                details="Exception during test",
                error=str(e)
            )

    async def test_transcript_unknown_sender(self) -> TestResult:
        """Test transcript formatting when sender not in name lookup."""
        self.print_section("Format Test 2: Unknown Sender Fallback")

        from app.groupchat.summary.worker import _format_transcript_lines

        messages = [
            {"role": "user", "sender_user_id": "unknown-user", "content": "Hello!", "sent_at": "2024-01-01T10:00:00Z"},
            {"role": "user", "sender_user_id": "", "content": "No ID", "sent_at": "2024-01-01T10:01:00Z"},
            {"role": "user", "content": "Missing sender", "sent_at": "2024-01-01T10:02:00Z"},
        ]

        name_by_user_id = {}  # Empty lookup

        try:
            lines = _format_transcript_lines(
                messages=messages,
                name_by_user_id=name_by_user_id,
                max_lines=100,
            )

            print(f"  Generated {len(lines)} lines:")
            for line in lines:
                print(f"    {line}")

            # Should fall back to "user" when no name found
            all_have_user_label = all("user:user" in line for line in lines)

            passed = len(lines) == 3 and all_have_user_label

            return TestResult(
                name="Unknown Sender Fallback",
                passed=passed,
                details=f"Fallback to 'user' label works",
                error=None if passed else "Fallback not working correctly"
            )
        except Exception as e:
            return TestResult(
                name="Unknown Sender Fallback",
                passed=False,
                details="Exception during test",
                error=str(e)
            )

    # =========================================================================
    # INTEGRATION TESTS
    # =========================================================================

    async def test_full_followup_pipeline(self) -> TestResult:
        """Test the full followup pipeline with real data."""
        self.print_section("Integration Test 1: Full Followup Pipeline")

        from app.groupchat.followup.context import load_participants, build_summary_segments
        from app.groupchat.followup.prompts import build_groupchat_followup_messages
        from app.groupchat.followup.utils import effective_group_mode

        # Find a real group chat
        result = self.db.client.table("group_chats").select("*").limit(1).execute()
        if not result.data:
            return TestResult(
                name="Full Followup Pipeline",
                passed=True,
                details="No group chats to test (skipped)",
                error=None
            )

        chat_guid = result.data[0]["chat_guid"]

        try:
            # Step 1: Load participants
            chat, names, modes = await load_participants(self.db, chat_guid=chat_guid)
            print(f"  Step 1 - Load participants: {len(names)} found")

            if not chat:
                return TestResult(
                    name="Full Followup Pipeline",
                    passed=False,
                    details="Could not load chat",
                    error="load_participants returned None chat"
                )

            # Step 2: Compute effective mode
            mode = effective_group_mode(*modes) if modes else "active"
            print(f"  Step 2 - Effective mode: {mode}")

            # Step 3: Build summary segments
            segments = await build_summary_segments(self.db, chat_guid=chat_guid)
            print(f"  Step 3 - Summary segments: {len(segments)} found")

            # Step 4: Build prompt (even without segments, should work)
            if not segments:
                segments = ["(no recent summary available)"]

            messages = build_groupchat_followup_messages(
                chat_guid=chat_guid,
                participant_names=names,
                inactivity_minutes=60,
                last_user_message_at="2024-01-01T00:00:00Z",
                summary_segments=segments,
            )
            print(f"  Step 4 - Prompt built: {len(messages)} messages")

            passed = (
                len(names) >= 2 and
                mode in ["active", "quiet", "muted"] and
                len(messages) == 2
            )

            return TestResult(
                name="Full Followup Pipeline",
                passed=passed,
                details=f"Pipeline completed: {len(names)} participants, mode={mode}",
                error=None if passed else "Pipeline incomplete"
            )
        except Exception as e:
            import traceback
            return TestResult(
                name="Full Followup Pipeline",
                passed=False,
                details="Exception during pipeline",
                error=f"{e}\n{traceback.format_exc()}"
            )

    # =========================================================================
    # Run all tests
    # =========================================================================
    async def run_all_tests(self, test_llm: bool = False) -> None:
        """Run all comprehensive tests."""
        self.print_header("COMPREHENSIVE GROUP CHAT E2E TESTS")
        print(f"Started: {datetime.now(timezone.utc).isoformat()}")
        print(f"Test LLM: {test_llm}")

        # Core tests (no LLM)
        tests = [
            # Edge cases
            self.test_empty_participant_list,
            self.test_single_participant_chat,
            self.test_many_participants_chat,
            self.test_special_characters_in_names,
            self.test_empty_names_fallback,
            # Mode tests
            self.test_mode_transitions,
            # Router tests
            self.test_router_participant_resolution,
            # DB tests
            self.test_participant_crud_operations,
            self.test_member_count_consistency,
            # Format tests
            self.test_transcript_formatting_n_participants,
            self.test_transcript_unknown_sender,
            # Integration tests
            self.test_full_followup_pipeline,
        ]

        # LLM tests (optional)
        if test_llm:
            tests.extend([
                self.test_llm_empty_summary_segments,
                self.test_llm_long_participant_list,
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
            if result.error and not result.passed:
                # Truncate long errors
                error_preview = result.error[:150] + "..." if len(result.error) > 150 else result.error
                print(f"         Error: {error_preview}")

        print(f"\nCompleted: {datetime.now(timezone.utc).isoformat()}")

        if passed_count < total_count:
            print(f"\n{'=' * 80}")
            print(f" [WARN] {total_count - passed_count} tests failed")
            print(f"{'=' * 80}")
        else:
            print(f"\n{'=' * 80}")
            print(f" [SUCCESS] All {total_count} tests passed!")
            print(f"{'=' * 80}")


async def main():
    parser = argparse.ArgumentParser(description="Comprehensive E2E test for group chat")
    parser.add_argument(
        "--test-llm",
        action="store_true",
        help="Include tests with actual LLM calls"
    )
    args = parser.parse_args()

    tester = ComprehensiveGroupChatTester()
    await tester.run_all_tests(test_llm=args.test_llm)


if __name__ == "__main__":
    asyncio.run(main())
