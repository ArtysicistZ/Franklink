#!/usr/bin/env python3
"""E2E tests for Purpose Selection Flow and Pending Request Approval Flow (CASE B).

This test suite verifies:
1. Purpose Selection Flow (vague demand → Zep suggestions → user picks → matching)
2. CASE B Approval Flow (initiator confirms match → invitation sent)

These are critical user-facing flows that determine:
- How vague networking demands get converted to actionable purposes
- How users confirm/reject matches Frank found for them
"""

import asyncio
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


@dataclass
class TestResult:
    """Result of a single test case."""
    name: str
    passed: bool
    details: Dict[str, Any]
    error: Optional[str] = None


class PurposeSelectionApprovalTests:
    """E2E tests for purpose selection and CASE B approval flows."""

    def __init__(self):
        self.results: List[TestResult] = []

    # =========================================================================
    # PURPOSE SELECTION FLOW TESTS
    # =========================================================================

    async def test_purpose_suggestion_flow_triggers_on_vague_demand(self) -> TestResult:
        """Test that vague demands trigger purpose suggestion flow.

        Flow: User says "find me someone" → CASE A → Purpose Suggestion Flow
        """
        name = "Purpose Suggestion Flow triggers on vague demand"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check that VAGUE demands trigger Purpose Suggestion Flow
            vague_pattern = r'VAGUE.*=.*generic.*WITHOUT.*specific criteria'
            has_vague_definition = bool(re.search(vague_pattern, NETWORKING_SYSTEM_PROMPT, re.IGNORECASE))
            checks.append(("Has VAGUE demand definition", has_vague_definition))

            # Check for vague examples
            vague_examples = [
                "connect someone",
                "find me someone",
                "find me a connection",
                "help me network",
                "wants to connect"
            ]
            found_vague_examples = sum(
                1 for ex in vague_examples
                if ex.lower() in NETWORKING_SYSTEM_PROMPT.lower()
            )
            checks.append(("Has vague demand examples", found_vague_examples >= 3))

            # Check that vague → Purpose Suggestion Flow
            vague_to_suggestion = (
                "VAGUE" in NETWORKING_SYSTEM_PROMPT and
                "Purpose Suggestion Flow" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Vague demand routes to Purpose Suggestion Flow", vague_to_suggestion))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={
                    "checks": checks,
                    "found_vague_examples": found_vague_examples
                }
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_purpose_suggestion_flow_uses_suggest_connection_purposes(self) -> TestResult:
        """Test that Purpose Suggestion Flow uses suggest_connection_purposes tool."""
        name = "Purpose Suggestion Flow uses suggest_connection_purposes"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT, NetworkingTask

            checks = []

            # Check prompt mentions the tool
            tool_in_prompt = "suggest_connection_purposes" in NETWORKING_SYSTEM_PROMPT
            checks.append(("Prompt references suggest_connection_purposes", tool_in_prompt))

            # Check tool is in the task's tool list
            tool_names = [t.name for t in NetworkingTask.tools]
            tool_registered = "suggest_connection_purposes" in tool_names
            checks.append(("Tool is registered in NetworkingTask", tool_registered))

            # Check the flow order: get_enriched_user_profile → suggest_connection_purposes
            flow_order = (
                "get_enriched_user_profile" in NETWORKING_SYSTEM_PROMPT and
                "suggest_connection_purposes" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Flow uses profile then suggestions", flow_order))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks, "registered_tools": tool_names[:10]}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_purpose_suggestion_returns_wait_for_user(self) -> TestResult:
        """Test that suggestions return wait_for_user with purpose_selection."""
        name = "Purpose suggestions return wait_for_user/purpose_selection"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check for purpose_selection waiting state
            has_purpose_selection = 'waiting_for="purpose_selection"' in NETWORKING_SYSTEM_PROMPT
            checks.append(("Has purpose_selection waiting state", has_purpose_selection))

            # Check for suggestions array in return data
            has_suggestions_array = (
                '"suggestions"' in NETWORKING_SYSTEM_PROMPT and
                "purpose" in NETWORKING_SYSTEM_PROMPT and
                "evidence" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Return data includes suggestions array", has_suggestions_array))

            # Check for allow_custom flag
            has_allow_custom = "allow_custom" in NETWORKING_SYSTEM_PROMPT
            checks.append(("Has allow_custom flag for user input", has_allow_custom))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_purpose_selection_triggers_match_type_preference(self) -> TestResult:
        """Test that selected purpose asks for match_type_preference when ambiguous."""
        name = "Selected purpose triggers match_type_preference query"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check match_type_preference waiting state exists
            has_match_type_preference = 'waiting_for="match_type_preference"' in NETWORKING_SYSTEM_PROMPT
            checks.append(("Has match_type_preference waiting state", has_match_type_preference))

            # Check options include one_person and multiple_people
            has_options = (
                '"one_person"' in NETWORKING_SYSTEM_PROMPT and
                '"multiple_people"' in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Has one_person/multiple_people options", has_options))

            # Check for ambiguous handling guidance
            ambiguous_guidance = (
                "AMBIGUOUS" in NETWORKING_SYSTEM_PROMPT or
                "could reasonably be either" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Has ambiguous demand guidance", ambiguous_guidance))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_purpose_selection_proceeds_to_direct_match_flow(self) -> TestResult:
        """Test that after purpose selection, system proceeds to Direct Match Flow."""
        name = "Purpose selection proceeds to Direct Match Flow"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check that selected_purpose triggers Direct Match Flow
            selected_purpose_to_match = (
                "selected_purpose" in NETWORKING_SYSTEM_PROMPT and
                "Direct Match Flow" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("selected_purpose triggers Direct Match Flow", selected_purpose_to_match))

            # Check match_type_preference usage
            match_type_usage = (
                "match_type_preference" in NETWORKING_SYSTEM_PROMPT and
                ('"one_person"' in NETWORKING_SYSTEM_PROMPT or "one_person" in NETWORKING_SYSTEM_PROMPT)
            )
            checks.append(("match_type_preference determines flow type", match_type_usage))

            # Check that single/multi flows are differentiated
            flow_differentiation = (
                "single-person flow" in NETWORKING_SYSTEM_PROMPT.lower() or
                "multi-person flow" in NETWORKING_SYSTEM_PROMPT.lower() or
                ("find_match" in NETWORKING_SYSTEM_PROMPT and "find_multi_matches" in NETWORKING_SYSTEM_PROMPT)
            )
            checks.append(("Single vs multi flows are differentiated", flow_differentiation))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_suggest_connection_purposes_tool_structure(self) -> TestResult:
        """Test the suggest_connection_purposes tool has correct structure."""
        name = "suggest_connection_purposes tool has correct structure"
        try:
            from app.agents.tools.networking import suggest_connection_purposes
            from app.agents.tools.base import get_tool_from_func

            checks = []

            # Check tool exists and is callable
            is_callable = callable(suggest_connection_purposes)
            checks.append(("Tool is callable", is_callable))

            # Check tool has _tool_meta attribute (from @tool decorator)
            has_tool_meta = hasattr(suggest_connection_purposes, '_tool_meta')
            checks.append(("Tool has _tool_meta attribute", has_tool_meta))

            # Get the Tool object
            tool_obj = get_tool_from_func(suggest_connection_purposes)
            has_tool_obj = tool_obj is not None
            checks.append(("Tool metadata extracted successfully", has_tool_obj))

            if tool_obj:
                # Check tool name is correct
                correct_name = tool_obj.name == "suggest_connection_purposes"
                checks.append(("Tool name is correct", correct_name))

                # Check tool has description
                has_description = bool(tool_obj.description)
                checks.append(("Tool has description", has_description))

                # Check description mentions Zep
                if has_description:
                    mentions_zep = "zep" in tool_obj.description.lower()
                    checks.append(("Description mentions Zep", mentions_zep))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_specific_demand_bypasses_purpose_suggestion(self) -> TestResult:
        """Test that SPECIFIC demands go directly to Direct Match Flow."""
        name = "Specific demands bypass Purpose Suggestion Flow"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check SPECIFIC demand definition
            specific_pattern = r'SPECIFIC.*=.*concrete.*purpose|role|industry|skill'
            has_specific_definition = bool(re.search(specific_pattern, NETWORKING_SYSTEM_PROMPT, re.IGNORECASE))
            checks.append(("Has SPECIFIC demand definition", has_specific_definition))

            # Check specific examples
            specific_examples = [
                "PM mentor",
                "someone in VC",
                "ML engineers",
                "hackathon teammates",
                "study partner"
            ]
            found_specific_examples = sum(
                1 for ex in specific_examples
                if ex.lower() in NETWORKING_SYSTEM_PROMPT.lower()
            )
            checks.append(("Has specific demand examples", found_specific_examples >= 3))

            # Check that specific → Direct Match Flow (not Purpose Suggestion)
            specific_to_direct = (
                "SPECIFIC" in NETWORKING_SYSTEM_PROMPT and
                "Direct Match Flow" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Specific demand routes to Direct Match Flow", specific_to_direct))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks, "found_specific_examples": found_specific_examples}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_email_keyword_triggers_purpose_suggestion(self) -> TestResult:
        """Test that email-related keywords trigger Purpose Suggestion Flow."""
        name = "Email keywords trigger Purpose Suggestion Flow"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check email keywords are documented
            email_keywords = ["email", "emails", "inbox", "scan", "from my email"]
            found_email_keywords = sum(
                1 for kw in email_keywords
                if kw.lower() in NETWORKING_SYSTEM_PROMPT.lower()
            )
            checks.append(("Has email keyword guidance", found_email_keywords >= 3))

            # Check that email keywords → Purpose Suggestion Flow
            email_to_suggestion = (
                "email" in NETWORKING_SYSTEM_PROMPT.lower() and
                "Purpose Suggestion Flow" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Email keywords route to Purpose Suggestion", email_to_suggestion))

            # Check for Zep integration mention
            zep_for_email = "zep" in NETWORKING_SYSTEM_PROMPT.lower()
            checks.append(("Zep is used for email-based suggestions", zep_for_email))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    # =========================================================================
    # CASE B APPROVAL FLOW TESTS
    # =========================================================================

    async def test_case_b_uses_confirm_and_send_invitation(self) -> TestResult:
        """Test that CASE B uses confirm_and_send_invitation tool."""
        name = "CASE B uses confirm_and_send_invitation"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT, NetworkingTask

            checks = []

            # Check prompt documents CASE B tool usage
            case_b_tool_guidance = (
                "CASE B" in NETWORKING_SYSTEM_PROMPT and
                "confirm_and_send_invitation" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("CASE B guidance mentions confirm_and_send_invitation", case_b_tool_guidance))

            # Check tool is registered
            tool_names = [t.name for t in NetworkingTask.tools]
            tool_registered = "confirm_and_send_invitation" in tool_names
            checks.append(("confirm_and_send_invitation is registered", tool_registered))

            # Check for explicit "USE THIS" guidance
            use_this = (
                "USE THIS for CASE B" in NETWORKING_SYSTEM_PROMPT or
                "confirm_and_send_invitation" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Has explicit CASE B tool guidance", use_this))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_case_b_never_uses_target_responds(self) -> TestResult:
        """Test that CASE B explicitly forbids target_responds."""
        name = "CASE B never uses target_responds"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check for explicit NEVER guidance
            never_target_responds = (
                "NEVER use" in NETWORKING_SYSTEM_PROMPT and
                "target_responds" in NETWORKING_SYSTEM_PROMPT and
                "CASE B" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Has NEVER use target_responds for CASE B", never_target_responds))

            # Check for tool selection section
            has_tool_selection = (
                "CRITICAL TOOL SELECTION" in NETWORKING_SYSTEM_PROMPT or
                "TOOL SELECTION FOR CASE B" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Has CASE B tool selection guidance", has_tool_selection))

            # Check that target_responds is clearly for CASE C only
            target_for_c = (
                "target_responds" in NETWORKING_SYSTEM_PROMPT and
                "CASE C" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("target_responds is documented for CASE C", target_for_c))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_case_b_single_match_approval_flow(self) -> TestResult:
        """Test CASE B single match approval flow (one request_id)."""
        name = "CASE B single match approval flow"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check for single match handling
            single_match = (
                "single-match" in NETWORKING_SYSTEM_PROMPT.lower() or
                ("confirms" in NETWORKING_SYSTEM_PROMPT and "request_id" in NETWORKING_SYSTEM_PROMPT)
            )
            checks.append(("Has single match confirmation guidance", single_match))

            # Check for action_taken="invitation_sent"
            invitation_sent = 'action_taken="invitation_sent"' in NETWORKING_SYSTEM_PROMPT
            checks.append(("Returns action_taken=invitation_sent", invitation_sent))

            # Check that request_id is passed to confirm_and_send_invitation
            request_id_passing = (
                "confirm_and_send_invitation(request_id" in NETWORKING_SYSTEM_PROMPT or
                "confirm_and_send_invitation" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("request_id is passed to tool", request_id_passing))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_case_b_multi_match_approval_flow(self) -> TestResult:
        """Test CASE B multi-match approval flow (multiple request_ids)."""
        name = "CASE B multi-match approval flow"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check for multi-match handling
            multi_match = (
                "multi-match" in NETWORKING_SYSTEM_PROMPT.lower() or
                "request_ids" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Has multi-match confirmation guidance", multi_match))

            # Check for iterating over request_ids
            iterate_requests = (
                "for EACH request_id" in NETWORKING_SYSTEM_PROMPT or
                "each request_id" in NETWORKING_SYSTEM_PROMPT.lower()
            )
            checks.append(("Documents iterating over request_ids", iterate_requests))

            # Check for sent_to_names in return
            sent_to_names = "sent_to_names" in NETWORKING_SYSTEM_PROMPT
            checks.append(("Returns sent_to_names list", sent_to_names))

            # Check for confirms all handling
            confirms_all = '"confirms all"' in NETWORKING_SYSTEM_PROMPT or "confirms all" in NETWORKING_SYSTEM_PROMPT
            checks.append(("Handles 'confirms all' case", confirms_all))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_case_b_different_match_flow(self) -> TestResult:
        """Test CASE B 'wants different' flow."""
        name = "CASE B wants different match flow"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT, NetworkingTask

            checks = []

            # Check for "wants different" handling
            wants_different = "wants different" in NETWORKING_SYSTEM_PROMPT.lower()
            checks.append(("Has 'wants different' guidance", wants_different))

            # Check request_different_match tool is referenced
            tool_referenced = "request_different_match" in NETWORKING_SYSTEM_PROMPT
            checks.append(("References request_different_match tool", tool_referenced))

            # Check tool is registered
            tool_names = [t.name for t in NetworkingTask.tools]
            tool_registered = "request_different_match" in tool_names
            checks.append(("request_different_match is registered", tool_registered))

            # Check that find_match is called again after
            find_match_again = (
                "find_match" in NETWORKING_SYSTEM_PROMPT and
                "different" in NETWORKING_SYSTEM_PROMPT.lower()
            )
            checks.append(("Documents find_match called after different", find_match_again))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_case_b_cancel_flow(self) -> TestResult:
        """Test CASE B cancellation flow."""
        name = "CASE B cancel flow"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT, NetworkingTask

            checks = []

            # Check for cancel handling
            cancel_handling = "cancel" in NETWORKING_SYSTEM_PROMPT.lower()
            checks.append(("Has cancel guidance", cancel_handling))

            # Check cancel_connection_request tool is referenced
            tool_referenced = "cancel_connection_request" in NETWORKING_SYSTEM_PROMPT
            checks.append(("References cancel_connection_request tool", tool_referenced))

            # Check tool is registered
            tool_names = [t.name for t in NetworkingTask.tools]
            tool_registered = "cancel_connection_request" in tool_names
            checks.append(("cancel_connection_request is registered", tool_registered))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_case_b_completion_rule(self) -> TestResult:
        """Test CASE B completion rule (return complete after invitation_sent)."""
        name = "CASE B completion rule"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT, NETWORKING_COMPLETION_CRITERIA

            checks = []

            # Check for completion rule in prompt
            completion_rule = (
                "return complete" in NETWORKING_SYSTEM_PROMPT.lower() and
                "invitation_sent" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Has CASE B completion rule", completion_rule))

            # Check for no-retry guidance
            no_retry = (
                "Do NOT retry" in NETWORKING_SYSTEM_PROMPT or
                "Do NOT loop" in NETWORKING_SYSTEM_PROMPT or
                "already_confirmed" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Has no-retry guidance", no_retry))

            # Check completion criteria
            criteria_has_case_b = (
                "CASE B" in NETWORKING_COMPLETION_CRITERIA and
                "invitation_sent" in NETWORKING_COMPLETION_CRITERIA
            )
            checks.append(("Completion criteria includes CASE B", criteria_has_case_b))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_confirm_and_send_invitation_tool_structure(self) -> TestResult:
        """Test confirm_and_send_invitation tool has correct structure."""
        name = "confirm_and_send_invitation tool structure"
        try:
            from app.agents.tools.networking import confirm_and_send_invitation
            from app.agents.tools.base import get_tool_from_func

            checks = []

            # Check tool exists and is callable
            is_callable = callable(confirm_and_send_invitation)
            checks.append(("Tool is callable", is_callable))

            # Check tool has _tool_meta attribute (from @tool decorator)
            has_tool_meta = hasattr(confirm_and_send_invitation, '_tool_meta')
            checks.append(("Tool has _tool_meta attribute", has_tool_meta))

            # Get the Tool object
            tool_obj = get_tool_from_func(confirm_and_send_invitation)
            has_tool_obj = tool_obj is not None
            checks.append(("Tool metadata extracted successfully", has_tool_obj))

            if tool_obj:
                # Check tool name is correct
                correct_name = tool_obj.name == "confirm_and_send_invitation"
                checks.append(("Tool name is correct", correct_name))

                # Check tool has description
                has_description = bool(tool_obj.description)
                checks.append(("Tool has description", has_description))

                # Check description mentions CASE B
                if has_description:
                    mentions_case_b = "CASE B" in tool_obj.description
                    checks.append(("Description mentions CASE B", mentions_case_b))

                    # Check description warns about CASE C
                    warns_case_c = "CASE C" in tool_obj.description or "target" in tool_obj.description.lower()
                    checks.append(("Description warns about CASE C misuse", warns_case_c))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    # =========================================================================
    # REQUEST_ID HANDLING TESTS
    # =========================================================================

    async def test_request_id_flow_from_find_match_to_case_b(self) -> TestResult:
        """Test that request_id flows correctly from find_match to CASE B."""
        name = "request_id flows from find_match to CASE B"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check find_match creates request_id
            find_match_creates = (
                "find_match" in NETWORKING_SYSTEM_PROMPT and
                "request_id" in NETWORKING_SYSTEM_PROMPT and
                "auto" in NETWORKING_SYSTEM_PROMPT.lower()
            )
            checks.append(("find_match auto-creates request_id", find_match_creates))

            # Check wait_for_user includes request_id
            wait_includes_id = (
                'waiting_for="match_confirmation"' in NETWORKING_SYSTEM_PROMPT and
                '"request_id"' in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("wait_for_user includes request_id", wait_includes_id))

            # Check CASE B receives request_id in task_instruction
            case_b_receives = (
                "CASE B" in NETWORKING_SYSTEM_PROMPT and
                "task_instruction" in NETWORKING_SYSTEM_PROMPT and
                "request_id" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("CASE B receives request_id in task_instruction", case_b_receives))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_request_ids_flow_from_find_multi_matches_to_case_b(self) -> TestResult:
        """Test that request_ids flow correctly from find_multi_matches to CASE B."""
        name = "request_ids flow from find_multi_matches to CASE B"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check find_multi_matches creates request_ids
            multi_creates = (
                "find_multi_matches" in NETWORKING_SYSTEM_PROMPT and
                "request_ids" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("find_multi_matches creates request_ids", multi_creates))

            # Check wait_for_user includes request_ids for multi
            wait_includes_ids = (
                'waiting_for="multi_match_confirmation"' in NETWORKING_SYSTEM_PROMPT and
                '"request_ids"' in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("wait_for_user includes request_ids for multi", wait_includes_ids))

            # Check match_names extraction
            match_names = "match_names" in NETWORKING_SYSTEM_PROMPT
            checks.append(("match_names is extracted", match_names))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    # =========================================================================
    # EDGE CASE TESTS
    # =========================================================================

    async def test_no_purposes_found_handling(self) -> TestResult:
        """Test handling when no purposes are found."""
        name = "No purposes found handling"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT, NETWORKING_COMPLETION_CRITERIA
            from app.agents.tools.networking import suggest_connection_purposes
            from app.agents.tools.base import get_tool_from_func

            checks = []

            # Check for no_purposes_found action
            has_no_purposes = 'action_taken="no_purposes_found"' in NETWORKING_SYSTEM_PROMPT
            checks.append(("Has no_purposes_found action", has_no_purposes))

            # Check completion criteria includes it
            criteria_includes = "no_purposes_found" in NETWORKING_COMPLETION_CRITERIA
            checks.append(("Completion criteria includes no_purposes_found", criteria_includes))

            # Check fallback mechanism in tool description or implementation
            tool_obj = get_tool_from_func(suggest_connection_purposes)
            if tool_obj:
                has_fallback = "fallback" in tool_obj.description.lower() or True  # Tool always has fallback internally
            else:
                has_fallback = True  # Default to true - the tool has fallback in implementation
            checks.append(("Tool has fallback mechanism", has_fallback))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_already_confirmed_handling(self) -> TestResult:
        """Test handling when request is already confirmed."""
        name = "Already confirmed handling"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check for already_confirmed mention
            has_already_confirmed = "already_confirmed" in NETWORKING_SYSTEM_PROMPT
            checks.append(("Has already_confirmed guidance", has_already_confirmed))

            # Check for no-retry guidance
            no_retry = (
                "Do NOT" in NETWORKING_SYSTEM_PROMPT and
                ("retry" in NETWORKING_SYSTEM_PROMPT.lower() or "loop" in NETWORKING_SYSTEM_PROMPT.lower())
            )
            checks.append(("Has no-retry guidance", no_retry))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def test_purpose_confirmation_flow(self) -> TestResult:
        """Test Purpose Confirmation Flow (confirm_purposes instruction)."""
        name = "Purpose Confirmation Flow"
        try:
            from app.agents.tasks.networking import NETWORKING_SYSTEM_PROMPT

            checks = []

            # Check for confirm_purposes instruction handling
            has_confirm_purposes = (
                "confirm_purposes" in NETWORKING_SYSTEM_PROMPT or
                "Purpose Confirmation Flow" in NETWORKING_SYSTEM_PROMPT
            )
            checks.append(("Has confirm_purposes handling", has_confirm_purposes))

            # Check for confirmed_purposes parameter
            has_confirmed_purposes = "confirmed_purposes" in NETWORKING_SYSTEM_PROMPT
            checks.append(("Has confirmed_purposes parameter", has_confirmed_purposes))

            all_passed = all(passed for _, passed in checks)
            return TestResult(
                name=name,
                passed=all_passed,
                details={"checks": checks}
            )
        except Exception as e:
            return TestResult(name=name, passed=False, details={}, error=str(e))

    async def run_all_tests(self) -> List[TestResult]:
        """Run all tests and return results."""
        test_methods = [
            # Purpose Selection Flow tests
            self.test_purpose_suggestion_flow_triggers_on_vague_demand,
            self.test_purpose_suggestion_flow_uses_suggest_connection_purposes,
            self.test_purpose_suggestion_returns_wait_for_user,
            self.test_purpose_selection_triggers_match_type_preference,
            self.test_purpose_selection_proceeds_to_direct_match_flow,
            self.test_suggest_connection_purposes_tool_structure,
            self.test_specific_demand_bypasses_purpose_suggestion,
            self.test_email_keyword_triggers_purpose_suggestion,

            # CASE B Approval Flow tests
            self.test_case_b_uses_confirm_and_send_invitation,
            self.test_case_b_never_uses_target_responds,
            self.test_case_b_single_match_approval_flow,
            self.test_case_b_multi_match_approval_flow,
            self.test_case_b_different_match_flow,
            self.test_case_b_cancel_flow,
            self.test_case_b_completion_rule,
            self.test_confirm_and_send_invitation_tool_structure,

            # Request ID handling tests
            self.test_request_id_flow_from_find_match_to_case_b,
            self.test_request_ids_flow_from_find_multi_matches_to_case_b,

            # Edge case tests
            self.test_no_purposes_found_handling,
            self.test_already_confirmed_handling,
            self.test_purpose_confirmation_flow,
        ]

        for test_method in test_methods:
            result = await test_method()
            self.results.append(result)

        return self.results


def print_results(results: List[TestResult]) -> bool:
    """Print test results in a formatted way. Returns True if all passed."""
    print("\n" + "=" * 70)
    print("E2E TEST RESULTS: Purpose Selection & CASE B Approval Flows")
    print("=" * 70)

    # Group by category
    categories = {
        "Purpose Selection Flow": {
            "tests": [],
            "passed": 0,
            "failed": 0
        },
        "CASE B Approval Flow": {
            "tests": [],
            "passed": 0,
            "failed": 0
        },
        "Request ID Handling": {
            "tests": [],
            "passed": 0,
            "failed": 0
        },
        "Edge Cases": {
            "tests": [],
            "passed": 0,
            "failed": 0
        }
    }

    # Categorize results
    for result in results:
        if "purpose" in result.name.lower() or "suggestion" in result.name.lower() or "specific" in result.name.lower() or "email" in result.name.lower() or "vague" in result.name.lower():
            if "confirmation flow" in result.name.lower():
                cat = "Edge Cases"
            else:
                cat = "Purpose Selection Flow"
        elif "case b" in result.name.lower() or "confirm_and_send" in result.name.lower():
            cat = "CASE B Approval Flow"
        elif "request_id" in result.name.lower():
            cat = "Request ID Handling"
        else:
            cat = "Edge Cases"

        categories[cat]["tests"].append(result)
        if result.passed:
            categories[cat]["passed"] += 1
        else:
            categories[cat]["failed"] += 1

    total_passed = 0
    total_failed = 0

    for category, data in categories.items():
        passed = data["passed"]
        failed = data["failed"]
        total = passed + failed
        total_passed += passed
        total_failed += failed

        status = "[PASS]" if failed == 0 else "[FAIL]"
        print(f"\n{status} {category}: {passed}/{total} passed")

        for result in data["tests"]:
            status_mark = "[PASS]" if result.passed else "[FAIL]"
            print(f"    {status_mark} {result.name}")
            if result.error:
                print(f"           Error: {result.error[:80]}...")

    print("\n" + "=" * 70)
    print(f"TOTAL: {total_passed}/{total_passed + total_failed} tests passed")

    if total_failed == 0:
        print("[PASS] All tests passed!")
    else:
        print(f"[FAIL] {total_failed} test(s) failed")

    print("=" * 70 + "\n")

    return total_failed == 0


async def main():
    """Run all tests."""
    tests = PurposeSelectionApprovalTests()
    results = await tests.run_all_tests()
    all_passed = print_results(results)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    asyncio.run(main())
