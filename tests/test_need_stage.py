#!/usr/bin/env python3
"""
Test suite for Need Stage to identify error/fallback issues.

Tests:
1. JSON parsing with various malformed inputs
2. LLM response handling
3. State transitions
4. Error fallback paths

Usage:
    python support/scripts/test_need_stage.py
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict
from unittest.mock import AsyncMock, patch, MagicMock

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.agents.execution.onboarding.utils.need_proof import (
    _safe_load_json,
    _extract_json_payload,
    _as_clean_str,
    _normalize_need_ask_response,
    _normalize_need_accept_response,
    _merge_user_need,
    build_initial_need_prompt,
    evaluate_user_need,
    seed_need_state,
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
# Test: JSON Parsing
# ============================================================================

def test_json_parsing(results: TestResults) -> None:
    """Test JSON extraction and parsing functions."""
    print_header("TEST: JSON Parsing")

    # Test 1: Valid JSON
    valid_json = '{"decision": "ask", "response_text": "who do you want to meet?"}'
    result = _safe_load_json(valid_json)
    results.record(
        "Valid JSON parsing",
        result.get("decision") == "ask" and "response_text" in result,
        f"Got: {result}"
    )

    # Test 2: JSON with markdown code block
    markdown_json = '```json\n{"decision": "accept", "response_text": "got it."}\n```'
    result = _safe_load_json(markdown_json)
    results.record(
        "Markdown-wrapped JSON",
        result.get("decision") == "accept",
        f"Got: {result}"
    )

    # Test 3: JSON with extra text before
    prefixed_json = 'Here is my response:\n{"decision": "ask", "response_text": "test"}'
    result = _safe_load_json(prefixed_json)
    results.record(
        "JSON with prefix text",
        result.get("decision") == "ask",
        f"Got: {result}"
    )

    # Test 4: Invalid JSON
    invalid_json = '{"decision": "ask", response_text: "missing quotes"}'
    result = _safe_load_json(invalid_json)
    results.record(
        "Invalid JSON returns empty dict",
        result == {},
        f"Got: {result}"
    )

    # Test 5: Empty string
    result = _safe_load_json("")
    results.record(
        "Empty string returns empty dict",
        result == {},
        f"Got: {result}"
    )

    # Test 6: None value
    result = _safe_load_json(None)  # type: ignore
    results.record(
        "None value returns empty dict",
        result == {},
        f"Got: {result}"
    )

    # Test 7: Nested JSON
    nested_json = '{"decision": "ask", "user_need": {"targets": ["VCs"], "outcomes": ["funding"]}}'
    result = _safe_load_json(nested_json)
    results.record(
        "Nested JSON parsing",
        isinstance(result.get("user_need"), dict) and "targets" in result.get("user_need", {}),
        f"Got: {result}"
    )

    # Test 8: JSON with unicode
    unicode_json = '{"decision": "ask", "response_text": "who\u2019s your target?"}'
    result = _safe_load_json(unicode_json)
    results.record(
        "Unicode in JSON",
        "target" in result.get("response_text", ""),
        f"Got: {result}"
    )

    # Test 9: JSON with newlines in string
    newline_json = '{"decision": "ask", "response_text": "line1\\n\\nline2"}'
    result = _safe_load_json(newline_json)
    results.record(
        "Newlines in JSON string",
        result.get("response_text") == "line1\n\nline2",
        f"Got: {result}"
    )

    # Test 10: Truncated JSON
    truncated_json = '{"decision": "ask", "response_text": "test'
    result = _safe_load_json(truncated_json)
    results.record(
        "Truncated JSON returns empty dict",
        result == {},
        f"Got: {result}"
    )


# ============================================================================
# Test: Response Normalization
# ============================================================================

def test_response_normalization(results: TestResults) -> None:
    """Test response text normalization functions."""
    print_header("TEST: Response Normalization")

    # Test 1: ASK response with single question mark
    ask_response = "okay cool.\n\nwho do you want to meet?"
    result = _normalize_need_ask_response(ask_response)
    question_count = result.count("?")
    results.record(
        "ASK: Single question mark",
        question_count == 1,
        f"Question marks: {question_count}, Text: {repr(result)}"
    )

    # Test 2: ASK response with multiple question marks gets fixed
    multi_q = "who? what do you want?"
    result = _normalize_need_ask_response(multi_q)
    question_count = result.count("?")
    results.record(
        "ASK: Multiple questions normalized to one",
        question_count == 1,
        f"Question marks: {question_count}, Text: {repr(result)}"
    )

    # Test 3: ASK response gets bubble structure
    single_line = "who do you want to meet and what do you want from them?"
    result = _normalize_need_ask_response(single_line)
    bubble_count = len(result.split("\n\n"))
    results.record(
        "ASK: Single line gets bubble structure",
        bubble_count >= 2,
        f"Bubbles: {bubble_count}, Text: {repr(result)}"
    )

    # Test 4: ACCEPT response removes question marks
    accept_response = "got it. you want VCs?"
    result = _normalize_need_accept_response(accept_response)
    question_count = result.count("?")
    results.record(
        "ACCEPT: Question marks removed",
        question_count == 0,
        f"Question marks: {question_count}, Text: {repr(result)}"
    )

    # Test 5: Empty string handling
    result = _normalize_need_ask_response("")
    results.record(
        "Empty string handling",
        result == "" or "?" in result,  # Either empty or fallback with question
        f"Got: {repr(result)}"
    )


# ============================================================================
# Test: User Need Merging
# ============================================================================

def test_user_need_merging(results: TestResults) -> None:
    """Test merging of user need objects."""
    print_header("TEST: User Need Merging")

    # Test 1: Empty prior
    prior = {}
    new = {"targets": ["VCs"], "outcomes": ["funding"]}
    result = _merge_user_need(prior, new)
    results.record(
        "Empty prior merges new",
        result == new,
        f"Got: {result}"
    )

    # Test 2: Empty new
    prior = {"targets": ["VCs"]}
    new = {}
    result = _merge_user_need(prior, new)
    results.record(
        "Empty new keeps prior",
        result == prior,
        f"Got: {result}"
    )

    # Test 3: List merging without duplicates
    prior = {"targets": ["VCs", "founders"]}
    new = {"targets": ["VCs", "angels"]}
    result = _merge_user_need(prior, new)
    results.record(
        "List merging deduplicates",
        set(result.get("targets", [])) == {"VCs", "founders", "angels"},
        f"Got: {result}"
    )

    # Test 4: Nested dict merging
    prior = {"constraints": {"timeline": "3 months"}}
    new = {"constraints": {"budget": "$100k"}}
    result = _merge_user_need(prior, new)
    results.record(
        "Nested dict merging",
        result.get("constraints", {}).get("timeline") == "3 months"
        and result.get("constraints", {}).get("budget") == "$100k",
        f"Got: {result}"
    )

    # Test 5: None values ignored
    prior = {"targets": ["VCs"]}
    new = {"targets": None, "outcomes": ["funding"]}
    result = _merge_user_need(prior, new)
    results.record(
        "None values ignored",
        result.get("targets") == ["VCs"] and result.get("outcomes") == ["funding"],
        f"Got: {result}"
    )


# ============================================================================
# Test: State Seeding
# ============================================================================

def test_state_seeding(results: TestResults) -> None:
    """Test initial state creation."""
    print_header("TEST: State Seeding")

    # Test 1: Basic state creation
    first_prompt = "who do you want to meet?"
    state = seed_need_state(first_prompt=first_prompt)
    results.record(
        "Basic state has required fields",
        all(k in state for k in ["status", "mode", "asked_questions", "turn_history"]),
        f"Keys: {list(state.keys())}"
    )

    # Test 2: First prompt in asked_questions
    results.record(
        "First prompt stored in asked_questions",
        state.get("asked_questions") == [first_prompt],
        f"Got: {state.get('asked_questions')}"
    )

    # Test 3: Turn history initialized
    results.record(
        "Turn history has first prompt",
        len(state.get("turn_history", [])) == 1
        and state["turn_history"][0].get("role") == "frank",
        f"Got: {state.get('turn_history')}"
    )

    # Test 4: With prior state
    prior = {"user_need": {"targets": ["VCs"]}}
    state = seed_need_state(first_prompt=first_prompt, prior_state=prior)
    results.record(
        "Prior state preserved",
        state.get("user_need") == {"targets": ["VCs"]},
        f"Got: {state.get('user_need')}"
    )


# ============================================================================
# Test: LLM Call Simulation (Mocked)
# ============================================================================

async def test_llm_call_simulation(results: TestResults) -> None:
    """Test LLM call handling with mocked responses."""
    print_header("TEST: LLM Call Simulation (Mocked)")

    user_profile = {
        "user_id": "test-user-123",
        "name": "Test User",
        "university": "Penn",
        "career_interests": ["startups"],
        "personal_facts": {},
    }

    # Test 1: Successful LLM response
    mock_response = json.dumps({
        "decision": "ask",
        "response_text": "okay.\n\nwho specifically do you want to meet?",
        "question_type": "targets",
        "user_need": {"targets": ["VCs"]},
        "confidence": 0.7
    })

    with patch("app.agents.execution.onboarding.utils.need_proof.AzureOpenAIClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.generate_response = AsyncMock(return_value=mock_response)
        MockClient.return_value = mock_instance

        prior_state = seed_need_state(first_prompt="initial question")
        result = await evaluate_user_need(
            user_message="I want to meet VCs",
            user_profile=user_profile,
            prior_state=prior_state,
        )

        results.record(
            "Successful LLM response parsed",
            result.get("decision") == "ask" and "response_text" in result,
            f"Decision: {result.get('decision')}, Has response: {'response_text' in result}"
        )

    # Test 2: LLM returns malformed JSON
    with patch("app.agents.execution.onboarding.utils.need_proof.AzureOpenAIClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.generate_response = AsyncMock(return_value="This is not JSON")
        MockClient.return_value = mock_instance

        prior_state = seed_need_state(first_prompt="initial question")
        result = await evaluate_user_need(
            user_message="I want to meet VCs",
            user_profile=user_profile,
            prior_state=prior_state,
        )

        results.record(
            "Malformed JSON handled gracefully",
            result.get("decision") in ["ask", "accept"] and "response_text" in result,
            f"Decision: {result.get('decision')}, Response: {result.get('response_text', '')[:50]}"
        )

    # Test 3: LLM call raises exception
    with patch("app.agents.execution.onboarding.utils.need_proof.AzureOpenAIClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.generate_response = AsyncMock(side_effect=Exception("API Error"))
        MockClient.return_value = mock_instance

        prior_state = seed_need_state(first_prompt="initial question")
        result = await evaluate_user_need(
            user_message="I want to meet VCs",
            user_profile=user_profile,
            prior_state=prior_state,
        )

        results.record(
            "LLM exception triggers fallback",
            result.get("decision") in ["ask", "accept"]
            and result.get("response_text") is not None,
            f"Decision: {result.get('decision')}, Response: {result.get('response_text', '')[:50]}"
        )

    # Test 4: Empty user message
    with patch("app.agents.execution.onboarding.utils.need_proof.AzureOpenAIClient") as MockClient:
        mock_instance = AsyncMock()
        MockClient.return_value = mock_instance

        prior_state = seed_need_state(first_prompt="initial question")
        result = await evaluate_user_need(
            user_message="",
            user_profile=user_profile,
            prior_state=prior_state,
        )

        results.record(
            "Empty message returns first prompt",
            result.get("decision") == "ask"
            and result.get("response_text") == "initial question",
            f"Decision: {result.get('decision')}, Response: {result.get('response_text', '')[:50]}"
        )
        # Verify LLM was NOT called for empty message
        results.record(
            "LLM not called for empty message",
            not mock_instance.generate_response.called,
            f"LLM called: {mock_instance.generate_response.called}"
        )


# ============================================================================
# Test: Decision Constraints
# ============================================================================

async def test_decision_constraints(results: TestResults) -> None:
    """Test that decision constraints are properly enforced."""
    print_header("TEST: Decision Constraints")

    user_profile = {
        "user_id": "test-user-123",
        "name": "Test User",
        "university": "Penn",
        "career_interests": ["startups"],
        "personal_facts": {},
    }

    # Test 1: First turn must be "ask" (min followups not met)
    mock_response = json.dumps({
        "decision": "accept",  # LLM tries to accept too early
        "response_text": "got it.",
        "question_type": "targets",
        "user_need": {"targets": ["VCs"]},
        "confidence": 0.9
    })

    with patch("app.agents.execution.onboarding.utils.need_proof.AzureOpenAIClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.generate_response = AsyncMock(return_value=mock_response)
        MockClient.return_value = mock_instance

        # Prior state with 0 followups used (only initial question asked)
        prior_state = seed_need_state(first_prompt="initial question")
        result = await evaluate_user_need(
            user_message="I want to meet VCs",
            user_profile=user_profile,
            prior_state=prior_state,
        )

        results.record(
            "Early accept forced to ask",
            result.get("decision") == "ask",
            f"Decision: {result.get('decision')} (expected: ask)"
        )

    # Test 2: After max followups, must accept
    mock_response = json.dumps({
        "decision": "ask",  # LLM tries to ask more
        "response_text": "tell me more?",
        "question_type": "targets",
        "user_need": {"targets": ["VCs"]},
        "confidence": 0.5
    })

    with patch("app.agents.execution.onboarding.utils.need_proof.AzureOpenAIClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.generate_response = AsyncMock(return_value=mock_response)
        MockClient.return_value = mock_instance

        # Prior state with max followups used
        prior_state = {
            "status": "pending",
            "mode": "gathering",
            "asked_questions": ["q1", "q2"],  # 2 questions = max
            "turn_history": [
                {"role": "frank", "content": "q1"},
                {"role": "user", "content": "a1"},
                {"role": "frank", "content": "q2"},
            ],
            "user_need": {"targets": ["VCs"]},
        }
        result = await evaluate_user_need(
            user_message="more info",
            user_profile=user_profile,
            prior_state=prior_state,
        )

        results.record(
            "Max followups forces accept",
            result.get("decision") == "accept",
            f"Decision: {result.get('decision')} (expected: accept)"
        )


# ============================================================================
# Test: Edge Cases
# ============================================================================

async def test_edge_cases(results: TestResults) -> None:
    """Test various edge cases."""
    print_header("TEST: Edge Cases")

    user_profile = {
        "user_id": "test-user-123",
        "name": "Test User",
        "university": "Penn",
        "career_interests": ["startups"],
        "personal_facts": {},
    }

    # Test 1: Very long user message
    long_message = "I want to meet " + "VCs and founders " * 100

    mock_response = json.dumps({
        "decision": "ask",
        "response_text": "okay.\n\nbe more specific?",
        "question_type": "targets",
        "user_need": {},
        "confidence": 0.5
    })

    with patch("app.agents.execution.onboarding.utils.need_proof.AzureOpenAIClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.generate_response = AsyncMock(return_value=mock_response)
        MockClient.return_value = mock_instance

        prior_state = seed_need_state(first_prompt="initial question")
        result = await evaluate_user_need(
            user_message=long_message,
            user_profile=user_profile,
            prior_state=prior_state,
        )

        results.record(
            "Long message handled",
            result.get("decision") in ["ask", "accept"],
            f"Decision: {result.get('decision')}"
        )

    # Test 2: Special characters in message
    special_message = "I want to meet <VCs> & 'founders' \"today\" @startup!"

    with patch("app.agents.execution.onboarding.utils.need_proof.AzureOpenAIClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.generate_response = AsyncMock(return_value=mock_response)
        MockClient.return_value = mock_instance

        prior_state = seed_need_state(first_prompt="initial question")
        result = await evaluate_user_need(
            user_message=special_message,
            user_profile=user_profile,
            prior_state=prior_state,
        )

        results.record(
            "Special characters handled",
            result.get("decision") in ["ask", "accept"],
            f"Decision: {result.get('decision')}"
        )

    # Test 3: Unicode/emoji in message
    emoji_message = "I want to meet investors"

    with patch("app.agents.execution.onboarding.utils.need_proof.AzureOpenAIClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.generate_response = AsyncMock(return_value=mock_response)
        MockClient.return_value = mock_instance

        prior_state = seed_need_state(first_prompt="initial question")
        result = await evaluate_user_need(
            user_message=emoji_message,
            user_profile=user_profile,
            prior_state=prior_state,
        )

        results.record(
            "Unicode/emoji handled",
            result.get("decision") in ["ask", "accept"],
            f"Decision: {result.get('decision')}"
        )

    # Test 4: Whitespace-only message
    with patch("app.agents.execution.onboarding.utils.need_proof.AzureOpenAIClient") as MockClient:
        mock_instance = AsyncMock()
        MockClient.return_value = mock_instance

        prior_state = seed_need_state(first_prompt="initial question")
        result = await evaluate_user_need(
            user_message="   \n\t  ",
            user_profile=user_profile,
            prior_state=prior_state,
        )

        results.record(
            "Whitespace-only treated as empty",
            result.get("response_text") == "initial question",
            f"Response: {result.get('response_text', '')[:50]}"
        )


# ============================================================================
# Main
# ============================================================================

async def run_all_tests() -> None:
    """Run all test suites."""
    print("\n" + "=" * 80)
    print(" NEED STAGE TEST SUITE")
    print(" Testing error/fallback paths and edge cases")
    print("=" * 80)
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")

    results = TestResults()

    # Synchronous tests
    test_json_parsing(results)
    test_response_normalization(results)
    test_user_need_merging(results)
    test_state_seeding(results)

    # Async tests
    await test_llm_call_simulation(results)
    await test_decision_constraints(results)
    await test_edge_cases(results)

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
    asyncio.run(run_all_tests())
