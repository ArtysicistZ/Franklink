#!/usr/bin/env python3
"""
Comprehensive test for all three onboarding fixes:
1. Need stage error handling
2. Value stage fee drop under $10 on first response
3. Share stage screenshot completion

Usage:
    python support/scripts/test_onboarding_fixes.py
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.agents.execution.onboarding.utils.value_proof import (
    _compute_fee_ceiling,
    evaluate_user_value,
)
from app.agents.execution.onboarding.utils.need_proof import (
    evaluate_user_need,
    seed_need_state,
)
from app.agents.execution.onboarding.nodes.collect_share_to_complete import (
    _has_media_attachment,
    collect_share_to_complete,
)


def print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)


def print_result(name: str, passed: bool, details: str = "") -> None:
    status = "PASS" if passed else "FAIL"
    icon = "✅" if passed else "❌"
    print(f"  {icon} {status}: {name}")
    if details:
        print(f"       {details}")


async def test_issue_1_need_stage_error_handling() -> bool:
    """Test that need stage handles errors gracefully."""
    print_header("ISSUE 1: Need Stage Error Handling")

    all_passed = True
    user_profile = {
        "user_id": "test-user-123",
        "name": "Test User",
        "university": "Penn",
        "career_interests": ["startups"],
        "personal_facts": {},
    }

    # Test 1: LLM exception triggers fallback
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

        passed = result.get("decision") in ["ask", "accept"] and result.get("response_text") is not None
        all_passed = all_passed and passed
        print_result(
            "LLM exception triggers fallback",
            passed,
            f"Decision: {result.get('decision')}, Has response: {result.get('response_text') is not None}"
        )

    # Test 2: Malformed JSON handled
    with patch("app.agents.execution.onboarding.utils.need_proof.AzureOpenAIClient") as MockClient:
        mock_instance = AsyncMock()
        mock_instance.generate_response = AsyncMock(return_value="Not JSON at all")
        MockClient.return_value = mock_instance

        prior_state = seed_need_state(first_prompt="initial question")
        result = await evaluate_user_need(
            user_message="I want to meet VCs",
            user_profile=user_profile,
            prior_state=prior_state,
        )

        passed = result.get("decision") in ["ask", "accept"]
        all_passed = all_passed and passed
        print_result(
            "Malformed JSON handled gracefully",
            passed,
            f"Decision: {result.get('decision')}"
        )

    return all_passed


def test_issue_2_value_stage_fee_drop() -> bool:
    """Test that fee drops under $10 on first response."""
    print_header("ISSUE 2: Value Stage Fee Drop Under $10")

    all_passed = True

    # Test 1: First response drops to under $10
    result = _compute_fee_ceiling(
        current_fee=9900,  # $99
        floor=99,          # $0.99
        remaining_asks=4,
        followups_used=0   # First response
    )
    passed = result < 1000
    all_passed = all_passed and passed
    print_result(
        "First response fee drops under $10",
        passed,
        f"Fee: ${result/100:.2f} (expected < $10.00)"
    )

    # Test 2: Starting from various fees
    test_fees = [9900, 5000, 2500, 1500, 1000]
    for fee in test_fees:
        result = _compute_fee_ceiling(
            current_fee=fee,
            floor=99,
            remaining_asks=4,
            followups_used=0
        )
        passed = result < 1000
        all_passed = all_passed and passed
        print_result(
            f"From ${fee/100:.2f} drops under $10",
            passed,
            f"Result: ${result/100:.2f}"
        )

    # Test 3: Already under $10 stays under $10
    result = _compute_fee_ceiling(
        current_fee=500,  # $5
        floor=99,
        remaining_asks=4,
        followups_used=0
    )
    passed = result < 1000
    all_passed = all_passed and passed
    print_result(
        "Already under $10 stays under $10",
        passed,
        f"Fee: ${result/100:.2f}"
    )

    # Test 4: Subsequent responses continue to drop
    result = _compute_fee_ceiling(
        current_fee=990,  # $9.90
        floor=99,
        remaining_asks=3,
        followups_used=1  # Second response
    )
    passed = result < 990  # Should be lower than input
    all_passed = all_passed and passed
    print_result(
        "Subsequent responses continue to drop",
        passed,
        f"Fee: ${result/100:.2f} (should be < $9.90)"
    )

    return all_passed


async def test_issue_3_share_stage_screenshot() -> bool:
    """Test that screenshot sharing completes onboarding."""
    print_header("ISSUE 3: Share Stage Screenshot Completion")

    all_passed = True

    async def mock_update_profile(state, updates):
        pass

    async def mock_summarize_value(user_profile, value_state):
        return "Test user value summary"

    async def mock_apply_updates(db, user_id, value_update):
        return {"value_history": [{"text": value_update}]}

    async def mock_generate_embedding(state):
        pass

    # Test 1: Screenshot detection works
    state_with_media = {
        "current_message": {
            "content": "",
            "metadata": {"media_url": "https://example.com/screenshot.png"}
        }
    }
    passed = _has_media_attachment(state_with_media)
    all_passed = all_passed and passed
    print_result(
        "Screenshot detected in metadata",
        passed,
        f"Has media: {passed}"
    )

    # Test 2: Direct media_url also works
    state_with_direct = {
        "current_message": {
            "content": "",
            "media_url": "https://example.com/screenshot.png"
        }
    }
    passed = _has_media_attachment(state_with_direct)
    all_passed = all_passed and passed
    print_result(
        "Screenshot detected in direct media_url",
        passed,
        f"Has media: {passed}"
    )

    # Test 3: Screenshot sharing completes onboarding
    with patch("app.agents.execution.onboarding.nodes.collect_share_to_complete.update_user_profile", mock_update_profile), \
         patch("app.agents.execution.onboarding.nodes.collect_share_to_complete.summarize_user_value", mock_summarize_value), \
         patch("app.agents.execution.onboarding.nodes.collect_share_to_complete.apply_demand_value_updates", mock_apply_updates), \
         patch("app.agents.execution.onboarding.nodes.mark_complete.generate_career_interest_embedding", mock_generate_embedding):

        state = {
            "user_profile": {
                "user_id": "test-user-123",
                "personal_facts": {
                    "frank_share_stage": {"prompt_sent": True},
                    "frank_value_eval": {"user_value": {"lanes": ["engineering"]}},
                },
            },
            "current_message": {
                "content": "",
                "metadata": {"media_url": "https://example.com/screenshot.png"}
            },
        }

        result = await collect_share_to_complete(state)

        passed = result["user_profile"].get("is_onboarded") is True
        all_passed = all_passed and passed
        print_result(
            "Screenshot completes onboarding",
            passed,
            f"is_onboarded: {result['user_profile'].get('is_onboarded')}"
        )

        passed = result["user_profile"].get("onboarding_stage") == "complete"
        all_passed = all_passed and passed
        print_result(
            "Stage set to complete",
            passed,
            f"stage: {result['user_profile'].get('onboarding_stage')}"
        )

        passed = result["user_profile"].get("intro_fee_cents") == 0
        all_passed = all_passed and passed
        print_result(
            "Fee is $0 for screenshot share",
            passed,
            f"fee: ${result['user_profile'].get('intro_fee_cents', 0)/100:.2f}"
        )

    return all_passed


async def main() -> None:
    """Run all tests for the three fixes."""
    print("\n" + "=" * 80)
    print(" FRANKLINK ONBOARDING FIXES - VERIFICATION TESTS")
    print("=" * 80)
    print(f"Started: {datetime.now(timezone.utc).isoformat()}")

    results = {}

    # Test Issue 1: Need Stage Error Handling
    results["issue_1"] = await test_issue_1_need_stage_error_handling()

    # Test Issue 2: Value Stage Fee Drop
    results["issue_2"] = test_issue_2_value_stage_fee_drop()

    # Test Issue 3: Share Stage Screenshot
    results["issue_3"] = await test_issue_3_share_stage_screenshot()

    # Summary
    print_header("SUMMARY")

    for issue, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        issue_names = {
            "issue_1": "Need Stage Error Handling",
            "issue_2": "Value Stage Fee Drop Under $10",
            "issue_3": "Share Stage Screenshot Completion",
        }
        print(f"  {status}: {issue_names[issue]}")

    all_passed = all(results.values())

    print(f"\nCompleted: {datetime.now(timezone.utc).isoformat()}")

    if all_passed:
        print("\n" + "=" * 80)
        print(" ✅ ALL FIXES VERIFIED SUCCESSFULLY")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print(" ❌ SOME FIXES NEED ATTENTION")
        print("=" * 80)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
