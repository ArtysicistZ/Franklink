"""Generic Execution Agent with ReAct-style reasoning loop.

This agent executes tasks by:
1. REASON: Using LLM to decide what action to take
2. ACT: Executing a tool with parameters
3. OBSERVE: Recording the result and continuing

The agent is task-agnostic - it operates on any Task definition
with available tools.

IMPORTANT: This agent returns STRUCTURED DATA only, never user-facing text.
The Interaction Agent is responsible for synthesizing user-facing responses.
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional

from app.agents.base import BaseAgent
from app.agents.tasks.base import Task
from app.agents.memory.execution import ExecutionMemory
from app.agents.execution.state import ExecutionAction, ExecutionResult
from app.integrations.azure_openai_client import AzureOpenAIClient

logger = logging.getLogger(__name__)


class GenericExecutionAgent(BaseAgent):
    """Generic execution agent that runs tasks using a ReAct loop.

    The agent:
    1. Receives a Task with tools and instructions
    2. Reasons about what action to take
    3. Executes tools and observes results
    4. Continues until complete

    IMPORTANT: This agent NEVER generates user-facing text. It returns
    structured data that the Interaction Agent uses to synthesize responses.
    """

    def __init__(self, db: Any, openai: Optional[AzureOpenAIClient] = None):
        """Initialize the execution agent.

        Args:
            db: DatabaseClient instance
            openai: Optional OpenAI client (creates one if not provided)
        """
        super().__init__(agent_type="execution", db=db, openai=openai)
        self.openai = openai or AzureOpenAIClient()

    async def execute(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute from BaseAgent interface - delegates to execute_task."""
        # Extract task from state if present
        task = state.get("task")
        context = state.get("context", {})

        if not task:
            logger.error("[EXECUTION] No task provided in state")
            return state

        result = await self.execute_task(task, context)

        # Update state with result
        state["execution_result"] = result.to_dict()

        return state

    async def execute_task(
        self,
        task: Task,
        context: Dict[str, Any],
    ) -> ExecutionResult:
        """Execute a task using the ReAct loop.

        Args:
            task: Task definition with tools and instructions
            context: Initial context for the task

        Returns:
            ExecutionResult with structured data (NOT user-facing text)
        """
        # Initialize fresh memory for each execution
        memory = ExecutionMemory(
            context=context,
            task_name=task.name,
        )

        logger.info(f"[EXECUTION] Starting task: {task.name}")

        # Track actions for structured result
        actions_taken: List[Dict[str, Any]] = []

        # Pre-processing: Detect networking flow for CASE A before entering ReAct loop
        task_instruction = context.get("task_instruction", {})
        if task.name == "networking" and task_instruction.get("case") == "A":
            detected_flow = await self._detect_networking_flow(
                task_instruction,
                user_message=str(context.get("user_message") or ""),
            )
            memory.context["detected_flow"] = detected_flow
            logger.info(f"[EXECUTION] Networking flow detected: {detected_flow}")

            # Inject flow decision into scratchpad to guide the LLM
            if detected_flow == "purpose_suggestion":
                memory.add_thought(
                    "FLOW DECISION: Pre-analysis determined this is a VAGUE request or mentions EMAIL. "
                    "MUST use Purpose Suggestion Flow: call get_enriched_user_profile then suggest_connection_purposes. "
                    "Purpose suggestions are generated from Zep knowledge graph which contains user's email activity. "
                    "CRITICAL: After suggest_connection_purposes returns, you MUST STOP and return wait_for_user "
                    "with waiting_for='purpose_selection' to let the user choose which purpose(s) to pursue. "
                    "Do NOT call find_match in this execution - wait for user's purpose selection first."
                )
            elif detected_flow == "purpose_confirmation":
                memory.add_thought(
                    "FLOW DECISION: Pre-analysis determined this is a purpose confirmation. "
                    "User selected purposes to pursue. Find matches using find_match or find_multi_matches."
                )
            elif detected_flow == "direct_match":
                memory.add_thought(
                    "FLOW DECISION: Pre-analysis determined this is a SPECIFIC request. "
                    "Use Direct Match Flow: call get_enriched_user_profile then find_match or find_multi_matches."
                )

        for i in range(task.max_iterations):
            memory.increment_iteration()
            logger.debug(f"[EXECUTION] Iteration {memory.iteration}/{task.max_iterations}")

            try:
                # 1. REASON: Get next action from LLM
                thought, action = await self._reason(task, memory)
                memory.add_thought(thought)
                if task.name == "groupchat_maintenance":
                    logger.info(
                        "[EXECUTION][GROUPCHAT_MAINT] action=%s tool=%s waiting_for=%s",
                        action.type,
                        action.tool_name,
                        action.waiting_for,
                    )

                # 2. Check for terminal actions
                if action.type == "complete":
                    parse_error = (
                        isinstance(action.data, dict)
                        and (
                            "parse_error" in action.data
                            or "reasoning_failed" in str(action.data.get("status", "")).lower()
                        )
                    ) or (str(action.summary or "").strip().lower() in {
                        "failed to parse llm response",
                        "error occurred during reasoning",
                    })
                    memory.add_action("complete", metadata={
                        "summary": action.summary,
                        "data": action.data,
                    })
                    if parse_error:
                        logger.warning(
                            "[EXECUTION] Task %s failed due to parse error",
                            task.name,
                        )
                        return ExecutionResult(
                            status="failed",
                            actions_taken=actions_taken,
                            data_collected=action.data or {},
                            state_changes=memory.interim_results,
                            error="Failed to parse execution response",
                            memory=memory,
                            iterations_used=memory.iteration,
                            # Deprecated fields for backward compatibility
                            result=action.result,
                        )

                    logger.info(f"[EXECUTION] Task {task.name} completed")

                    return ExecutionResult(
                        status="complete",
                        actions_taken=actions_taken,
                        data_collected=action.data or {},
                        state_changes=memory.interim_results,
                        memory=memory,
                        iterations_used=memory.iteration,
                        # Deprecated fields for backward compatibility
                        result=action.result,
                    )

                if action.type == "wait_for_user":
                    memory.add_action("wait_for_user", metadata={
                        "waiting_for": action.waiting_for,
                        "data": action.data,
                    })
                    logger.info(f"[EXECUTION] Task {task.name} waiting for user: {action.waiting_for}")

                    return ExecutionResult(
                        status="waiting",
                        actions_taken=actions_taken,
                        data_collected=action.data or {},
                        state_changes=memory.interim_results,
                        memory=memory,
                        iterations_used=memory.iteration,
                        waiting_for=action.waiting_for,
                    )

                # 3. ACT: Execute the tool
                # Flow enforcement: Block purpose suggestion if direct matching already failed
                # This prevents the LLM from falling back to purpose suggestion when a specific
                # demand (like "machine learning mentor") finds no matches.
                if action.tool_name == "suggest_connection_purposes":
                    # Check if find_match or find_multi_matches was already called
                    direct_match_called = any(
                        a.get("tool_name") in ("find_match", "find_multi_matches")
                        for a in actions_taken
                    )
                    if direct_match_called:
                        # Direct matching was attempted - check if it succeeded
                        direct_match_succeeded = any(
                            a.get("tool_name") in ("find_match", "find_multi_matches")
                            and a.get("success")
                            for a in actions_taken
                        )
                        if not direct_match_succeeded:
                            # Direct matching failed - block purpose suggestion fallback
                            error_msg = (
                                "FLOW BLOCKED: Direct matching (find_match/find_multi_matches) was already "
                                "called and returned no matches. For specific demands, you MUST return "
                                "complete with action_taken='no_matches_found'. Do NOT fall back to purpose suggestion."
                            )
                            memory.add_observation(f"BLOCKED: {error_msg}")
                            logger.warning(f"[EXECUTION] Flow enforcement: {error_msg}")
                            actions_taken.append({
                                "tool_name": action.tool_name,
                                "success": False,
                                "error": error_msg,
                                "blocked_by": "flow_enforcement",
                            })
                            continue

                # Flow enforcement: Block find_match if this is a vague request (Purpose Suggestion Flow)
                # This ensures vague requests go through suggest_connection_purposes first
                if action.tool_name in ("find_match", "find_multi_matches"):
                    # CRITICAL: Always inject user_profile from context to ensure skills are included.
                    # The LLM-generated user_profile is incomplete - it doesn't have skills data.
                    context_user_profile = memory.context.get("user_profile", {})
                    if context_user_profile:
                        action.params["user_profile"] = context_user_profile
                        logger.info(
                            f"[EXECUTION] Injected user_profile from context for {action.tool_name} "
                            f"(seeking_skills={context_user_profile.get('seeking_skills')}, "
                            f"offering_skills={context_user_profile.get('offering_skills')})"
                        )

                    detected_flow = memory.context.get("detected_flow")
                    task_instruction = memory.context.get("task_instruction", {}) or {}
                    task_case = str(task_instruction.get("case", "")).upper()

                    # CASE A safety: ensure matching tools get explicit demand text
                    # from task instructions instead of implicit latest_demand fallback.
                    if task.name in ("networking", "groupchat_networking") and task_case == "A":
                        explicit_demand = self._derive_case_a_networking_demand(task_instruction)

                        if action.tool_name == "find_match":
                            existing_demand = action.params.get("override_demand")
                            if not str(existing_demand or "").strip() and explicit_demand:
                                action.params["override_demand"] = explicit_demand
                                logger.info(
                                    "[EXECUTION] Injected CASE A override_demand for find_match: %s",
                                    explicit_demand[:80],
                                )

                            if not str(action.params.get("override_demand") or "").strip():
                                error_msg = (
                                    "FLOW BLOCKED: find_match called without explicit override_demand "
                                    "for networking CASE A."
                                )
                                memory.add_observation(f"BLOCKED: {error_msg}")
                                logger.warning(f"[EXECUTION] Flow enforcement: {error_msg}")
                                actions_taken.append({
                                    "tool_name": action.tool_name,
                                    "success": False,
                                    "error": error_msg,
                                    "blocked_by": "flow_enforcement",
                                })
                                continue

                        elif action.tool_name == "find_multi_matches":
                            existing_signal = action.params.get("signal_text")
                            if not str(existing_signal or "").strip() and explicit_demand:
                                action.params["signal_text"] = explicit_demand
                                logger.info(
                                    "[EXECUTION] Injected CASE A signal_text for find_multi_matches: %s",
                                    explicit_demand[:80],
                                )

                            if not str(action.params.get("signal_text") or "").strip():
                                error_msg = (
                                    "FLOW BLOCKED: find_multi_matches called without explicit signal_text "
                                    "for networking CASE A."
                                )
                                memory.add_observation(f"BLOCKED: {error_msg}")
                                logger.warning(f"[EXECUTION] Flow enforcement: {error_msg}")
                                actions_taken.append({
                                    "tool_name": action.tool_name,
                                    "success": False,
                                    "error": error_msg,
                                    "blocked_by": "flow_enforcement",
                                })
                                continue

                    if detected_flow == "purpose_suggestion":
                        # Check if suggest_connection_purposes was already called in THIS execution
                        purpose_suggested_this_execution = any(
                            a.get("tool_name") == "suggest_connection_purposes"
                            for a in actions_taken
                        )

                        if not purpose_suggested_this_execution:
                            # Block: Haven't suggested purposes yet
                            error_msg = (
                                "FLOW BLOCKED: Pre-analysis detected this as a VAGUE request. "
                                "You MUST call suggest_connection_purposes first to suggest purposes "
                                "from Zep context. Do NOT call find_match directly for vague requests."
                            )
                            memory.add_observation(f"BLOCKED: {error_msg}")
                            logger.warning(f"[EXECUTION] Flow enforcement: {error_msg}")
                            actions_taken.append({
                                "tool_name": action.tool_name,
                                "success": False,
                                "error": error_msg,
                                "blocked_by": "flow_enforcement",
                            })
                            continue
                        else:
                            # Block: Suggested purposes in THIS execution, must wait for user selection
                            error_msg = (
                                "FLOW BLOCKED: suggest_connection_purposes was just called. "
                                "You MUST now return wait_for_user with waiting_for='purpose_selection' "
                                "to let the user choose which purpose(s) to pursue. Do NOT call find_match "
                                "until the user confirms their purpose selection in a SEPARATE message."
                            )
                            memory.add_observation(f"BLOCKED: {error_msg}")
                            logger.warning(f"[EXECUTION] Flow enforcement: {error_msg}")
                            actions_taken.append({
                                "tool_name": action.tool_name,
                                "success": False,
                                "error": error_msg,
                                "blocked_by": "flow_enforcement",
                            })
                            continue

                # Flow enforcement: prevent invitation sends when CASE B instruction is decline/cancel.
                if action.tool_name == "confirm_and_send_invitation":
                    task_instruction = memory.context.get("task_instruction", {}) or {}
                    task_case = str(task_instruction.get("case", "")).upper()
                    if task_case == "B" and self._networking_instruction_has_decline_intent(task_instruction):
                        error_msg = (
                            "FLOW BLOCKED: CASE B instruction indicates decline/cancel intent. "
                            "Do NOT call confirm_and_send_invitation."
                        )
                        memory.add_observation(f"BLOCKED: {error_msg}")
                        logger.warning(f"[EXECUTION] Flow enforcement: {error_msg}")
                        actions_taken.append({
                            "tool_name": action.tool_name,
                            "success": False,
                            "error": error_msg,
                            "blocked_by": "flow_enforcement",
                        })
                        continue

                # Flow enforcement: Block confirm_and_send_invitation in the same execution as find_match/find_multi_matches
                # User MUST confirm matches before invitations are sent. This prevents the LLM from
                # auto-confirming matches without user approval.
                if action.tool_name == "confirm_and_send_invitation":
                    # Check if find_match or find_multi_matches was called in THIS execution
                    match_found_this_execution = any(
                        a.get("tool_name") in ("find_match", "find_multi_matches")
                        and a.get("success")
                        for a in actions_taken
                    )

                    if match_found_this_execution:
                        error_msg = (
                            "FLOW BLOCKED: You cannot call confirm_and_send_invitation in the same execution "
                            "as find_match/find_multi_matches. After finding matches, you MUST return "
                            "wait_for_user with waiting_for='match_confirmation' or 'multi_match_confirmation' "
                            "to let the user confirm the matches first. The user will confirm in a SEPARATE message, "
                            "and ONLY THEN should confirm_and_send_invitation be called (in CASE B)."
                        )
                        memory.add_observation(f"BLOCKED: {error_msg}")
                        logger.warning(f"[EXECUTION] Flow enforcement: {error_msg}")
                        actions_taken.append({
                            "tool_name": action.tool_name,
                            "success": False,
                            "error": error_msg,
                            "blocked_by": "flow_enforcement",
                        })
                        continue

                # CASE C safety: if instruction says accept/decline explicitly, keep tool params aligned.
                if action.tool_name == "target_responds":
                    task_instruction = memory.context.get("task_instruction", {}) or {}
                    task_case = str(task_instruction.get("case", "")).upper()
                    if task_case == "C":
                        explicit_accept = task_instruction.get("accept")
                        if isinstance(explicit_accept, bool):
                            if "accept" not in action.params:
                                action.params["accept"] = explicit_accept
                                logger.info(
                                    "[EXECUTION] Injected CASE C accept=%s for target_responds",
                                    explicit_accept,
                                )
                            elif bool(action.params.get("accept")) != explicit_accept:
                                error_msg = (
                                    "FLOW BLOCKED: target_responds accept param contradicts CASE C task instruction."
                                )
                                memory.add_observation(f"BLOCKED: {error_msg}")
                                logger.warning(f"[EXECUTION] Flow enforcement: {error_msg}")
                                actions_taken.append({
                                    "tool_name": action.tool_name,
                                    "success": False,
                                    "error": error_msg,
                                    "blocked_by": "flow_enforcement",
                                })
                                continue

                tool = task.get_tool(action.tool_name)
                if not tool:
                    error_msg = f"Tool not found: {action.tool_name}"
                    memory.add_observation(f"Error: {error_msg}")
                    logger.warning(f"[EXECUTION] {error_msg}")
                    actions_taken.append({
                        "tool_name": action.tool_name,
                        "success": False,
                        "error": error_msg,
                    })
                    continue

                memory.add_action(
                    "tool",
                    tool_name=action.tool_name,
                    params=action.params,
                )

                # Execute tool
                tool_success = False
                result_summary = ""
                try:
                    result = await tool.func(**action.params)
                    observation = result.to_observation()
                    tool_success = result.success
                    result_summary = str(result.data)[:100] if result.data else ""
                except Exception as e:
                    observation = f"Tool execution error: {str(e)}"
                    logger.error(f"[EXECUTION] Tool {action.tool_name} failed: {e}", exc_info=True)

                # Track action
                actions_taken.append({
                    "tool_name": action.tool_name,
                    "success": tool_success,
                    "result_summary": result_summary,
                    "params": action.params,
                })

                # 4. OBSERVE: Record result
                memory.add_observation(observation)

                # Store interim results if successful
                if hasattr(result, "data") and result.success:
                    # For confirm_and_send_invitation, accumulate all target names since
                    # multiple invitations may be sent in a single execution (multi-match CASE B)
                    if action.tool_name == "confirm_and_send_invitation":
                        existing = memory.get_result("_sent_invitations", [])
                        existing.append(result.data)
                        memory.store_result("_sent_invitations", existing)
                        # Also track target names explicitly for response synthesis
                        target_name = result.data.get("target_name")
                        if target_name:
                            sent_to_names = memory.get_result("sent_to_names", [])
                            if target_name not in sent_to_names:
                                sent_to_names.append(target_name)
                            memory.store_result("sent_to_names", sent_to_names)
                    memory.store_result(action.tool_name, result.data)

            except Exception as e:
                logger.error(f"[EXECUTION] Iteration {memory.iteration} failed: {e}", exc_info=True)
                memory.add_observation(f"Error: {str(e)}")

        # Max iterations reached
        logger.warning(f"[EXECUTION] Task {task.name} hit max iterations")
        return ExecutionResult(
            status="failed",
            actions_taken=actions_taken,
            data_collected=memory.interim_results,
            error="Maximum iterations reached without completion",
            memory=memory,
            iterations_used=memory.iteration,
        )

    @staticmethod
    def _derive_case_a_networking_demand(task_instruction: Dict[str, Any]) -> str:
        """Derive explicit demand text for CASE A networking matching tools.

        Prevents accidental fallback to stale profile demand when the LLM omits
        override parameters.
        """
        selected_purpose = str(task_instruction.get("selected_purpose") or "").strip()
        if selected_purpose:
            return selected_purpose

        confirmed_purposes = task_instruction.get("confirmed_purposes")
        if isinstance(confirmed_purposes, list) and len(confirmed_purposes) == 1:
            single = str(confirmed_purposes[0] or "").strip()
            if single:
                return single

        instruction = str(task_instruction.get("instruction") or "").strip()
        if instruction.lower() in {"confirm_purposes", "confirm_and_create_multi_match"}:
            return ""
        return instruction

    @staticmethod
    def _networking_instruction_has_decline_intent(task_instruction: Dict[str, Any]) -> bool:
        """Detect explicit decline/cancel intent from networking task instruction."""
        instruction = str(task_instruction.get("instruction") or "").lower()
        if not instruction:
            return False

        normalized = re.sub(r"[^a-z0-9\s]", " ", instruction)
        normalized = re.sub(r"\s+", " ", normalized).strip()

        phrases = (
            "decline", "declines", "declined",
            "reject", "rejects", "rejected",
            "cancel", "cancels", "cancelled", "canceled",
            "not now", "pass", "skip", "never mind", "nevermind",
            "nah", "nope",
        )
        if any(p in normalized for p in phrases):
            return True

        return bool(re.search(r"\bno\b", normalized))


    async def _reason(
        self,
        task: Task,
        memory: ExecutionMemory,
    ) -> tuple[str, ExecutionAction]:
        """Use LLM to reason about the next action.

        Args:
            task: Current task
            memory: Execution memory with scratchpad

        Returns:
            Tuple of (thought string, ExecutionAction)
        """
        # Build system prompt
        system_prompt = task.build_system_prompt(memory.context)

        # Build user prompt with scratchpad - updated to use structured output format
        scratchpad = memory.get_scratchpad_text(max_entries=15)
        user_prompt = f"""## Current Scratchpad
{scratchpad if scratchpad else "No actions taken yet."}

## Available Tools
{json.dumps([t.to_llm_schema() for t in task.tools], indent=2)}

Based on the scratchpad and task instructions, decide the next action.

Return structured data only. The Interaction Agent handles user-facing messages.

Respond with JSON only:
{{
    "thought": "<your reasoning>",
    "action": {{
        "type": "<tool|complete|wait_for_user>",

        // For type="tool":
        "name": "<tool_name>",
        "params": {{...}},

        // For type="complete":
        "summary": "<what was accomplished - internal note>",
        "data": {{...}},  // structured data

        // For type="wait_for_user" (need user input before continuing):
        "waiting_for": "<match_confirmation|networking_clarification|...>",
        "data": {{...}}  // Context about what's pending (e.g., match details)
    }}
}}

Use "wait_for_user" when you need the user to confirm or provide input before proceeding.
For example, after finding a match with find_match or find_multi_matches, return wait_for_user with waiting_for="match_confirmation" or "multi_match_confirmation".
"""

        try:
            response = await self.openai.generate_response(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                model="gpt-4o-mini",
                temperature=0.3,
                max_tokens=500,
                trace_label=f"execution_{task.name}",
                response_format={"type": "json_object"},
            )

            # Parse response
            return self._parse_reasoning_response(response)

        except Exception as e:
            logger.error(f"[EXECUTION] Reasoning failed: {e}", exc_info=True)
            # Return a fallback action with structured data (not user-facing text)
            return (
                f"Reasoning error: {str(e)}",
                ExecutionAction(
                    type="complete",
                    summary="Error occurred during reasoning",
                    data={"error": str(e), "status": "reasoning_failed"},
                ),
            )

    def _parse_reasoning_response(self, response: str) -> tuple[str, ExecutionAction]:
        """Parse LLM response into thought and action.

        Args:
            response: Raw LLM response

        Returns:
            Tuple of (thought, ExecutionAction)
        """
        try:
            # Clean up response
            cleaned = response.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:]
            if cleaned.startswith("```"):
                cleaned = cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

            data = json.loads(cleaned)
            thought = data.get("thought", "No thought provided")
            action_data = data.get("action", {})

            action_type = action_data.get("type", "complete")

            if action_type == "tool":
                return thought, ExecutionAction(
                    type="tool",
                    tool_name=action_data.get("name"),
                    params=action_data.get("params", {}),
                )
            elif action_type == "wait_for_user":
                return thought, ExecutionAction(
                    type="wait_for_user",
                    waiting_for=action_data.get("waiting_for"),
                    data=action_data.get("data", {}),
                )
            else:  # complete
                return thought, ExecutionAction(
                    type="complete",
                    summary=action_data.get("summary"),
                    data=action_data.get("data", {}),
                    # Backward compatibility
                    result=action_data.get("result"),
                )

        except json.JSONDecodeError as e:
            logger.warning(f"[EXECUTION] Failed to parse response: {e}")
            # Try to repair common JSON issues
            repaired = self._attempt_json_repair(cleaned)
            if repaired:
                try:
                    data = json.loads(repaired)
                    thought = data.get("thought", "No thought provided")
                    action_data = data.get("action", {})
                    action_type = action_data.get("type", "complete")

                    if action_type == "tool":
                        return thought, ExecutionAction(
                            type="tool",
                            tool_name=action_data.get("name"),
                            params=action_data.get("params", {}),
                        )
                    elif action_type == "wait_for_user":
                        return thought, ExecutionAction(
                            type="wait_for_user",
                            waiting_for=action_data.get("waiting_for"),
                            data=action_data.get("data", {}),
                        )
                    else:
                        return thought, ExecutionAction(
                            type="complete",
                            summary=action_data.get("summary"),
                            data=action_data.get("data", {}),
                            result=action_data.get("result"),
                        )
                except json.JSONDecodeError:
                    pass  # Repair failed, fall through to error handling

            # Return a fallback with structured data (not user-facing text)
            # CRITICAL: Check if response was trying to return wait_for_user
            # This prevents incorrectly marking tasks as complete when the LLM
            # was actually trying to wait for user confirmation
            response_lower = response.lower()
            if "wait_for_user" in response_lower:
                # LLM was trying to return wait_for_user - extract waiting_for type
                waiting_for = "unknown"
                if "multi_match_confirmation" in response_lower:
                    waiting_for = "multi_match_confirmation"
                elif "match_confirmation" in response_lower:
                    waiting_for = "match_confirmation"
                elif "purpose_selection" in response_lower:
                    waiting_for = "purpose_selection"

                logger.warning(
                    f"[EXECUTION] JSON parse failed but detected wait_for_user intent, "
                    f"returning waiting_for={waiting_for}"
                )
                return response[:200], ExecutionAction(
                    type="wait_for_user",
                    waiting_for=waiting_for,
                    data={"parse_error": str(e), "raw_response": response[:500]},
                )

            return response[:200], ExecutionAction(
                type="complete",
                summary="Failed to parse LLM response",
                data={"parse_error": str(e), "raw_response": response[:500]},
            )

    def _attempt_json_repair(self, json_str: str) -> Optional[str]:
        """Attempt to repair common JSON formatting issues from LLM output.

        Common issues:
        - Trailing commas before closing braces/brackets
        - Missing commas between elements
        - Unescaped quotes in strings
        - Truncated JSON
        - Missing commas between object properties (e.g., "key": value "key2": value2)

        Args:
            json_str: Malformed JSON string

        Returns:
            Repaired JSON string or None if repair failed
        """
        import re

        if not json_str:
            return None

        repaired = json_str

        # Remove trailing commas before ] or }
        repaired = re.sub(r',\s*([}\]])', r'\1', repaired)

        # Try to fix missing commas between string values
        # Pattern: "value" "key" -> "value", "key"
        repaired = re.sub(r'"\s*\n\s*"', '",\n"', repaired)

        # Try to fix missing commas after } or ] before "
        repaired = re.sub(r'([}\]])\s*\n\s*"', r'\1,\n"', repaired)

        # Fix missing commas between object properties
        # Pattern: "value"\n        "key": -> "value",\n        "key":
        repaired = re.sub(r'"\s*\n(\s*)"', r'",\n\1"', repaired)

        # Fix missing commas after numbers/booleans before "key":
        # Pattern: 123\n        "key": -> 123,\n        "key":
        repaired = re.sub(r'(\d+)\s*\n(\s*)"', r'\1,\n\2"', repaired)
        repaired = re.sub(r'(true|false|null)\s*\n(\s*)"', r'\1,\n\2"', repaired)

        # Fix missing commas after } in arrays before {
        # Pattern: }\n        { -> },\n        {
        repaired = re.sub(r'}\s*\n(\s*){', r'},\n\1{', repaired)

        # If JSON appears truncated, try to close it
        open_braces = repaired.count('{') - repaired.count('}')
        open_brackets = repaired.count('[') - repaired.count(']')

        if open_braces > 0 or open_brackets > 0:
            # Try to find a reasonable truncation point and close
            # Remove any trailing incomplete key-value pair
            repaired = re.sub(r',\s*"[^"]*"\s*:\s*[^,}\]]*$', '', repaired)
            repaired = repaired.rstrip().rstrip(',')
            repaired += ']' * open_brackets + '}' * open_braces

        # Validate the repair worked
        try:
            json.loads(repaired)
            logger.info("[EXECUTION] Successfully repaired malformed JSON")
            return repaired
        except json.JSONDecodeError:
            return None

    @staticmethod
    def _is_generic_networking_request(text: str) -> bool:
        """Return True when text is a generic networking ask without criteria."""
        normalized = re.sub(r"[^a-z0-9\s]", " ", str(text or "").lower())
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if not normalized:
            return False

        # Explicit criteria by "in/with/who" should not be treated as generic.
        if re.search(r"\b(anyone|someone)\s+in\s+[a-z0-9]", normalized):
            return False
        if re.search(r"\b(anyone|someone)\s+(with|who)\s+[a-z0-9]", normalized):
            return False

        # These terms indicate concrete criteria and should not be treated as vague.
        specific_signals = (
            "for ",
            "who ",
            "about ",
            "mentor",
            "teammate",
            "hackathon",
            "study",
            "machine learning",
            "startup",
            "founder",
            "cofounder",
            "interview",
            "pm ",
            "engineer",
        )
        if any(s in normalized for s in specific_signals):
            return False

        uncertainty_patterns = (
            "not sure",
            "idk",
            "i dont know",
            "maybe anyone",
            "anyone interesting",
            "whoever",
            "whatever",
            "surprise me",
            "open to anything",
            "anything works",
        )
        if any(p in normalized for p in uncertainty_patterns):
            return True

        generic_patterns = (
            r"\bconnect me\b(?:\s+(?:with|to)\s+someone)?\b",
            r"\bfind me\b(?:\s+someone|\s+a connection)?\b",
            r"\bfind someone\b",
            r"\bmeet someone\b",
            r"\bhelp me network\b",
            r"\bnetwork for me\b",
            r"\bwant to meet someone\b",
        )
        return any(re.search(p, normalized) for p in generic_patterns)

    async def _detect_networking_flow(
        self,
        task_instruction: Dict[str, Any],
        user_message: str = "",
    ) -> str:
        """Use LLM to detect which networking flow to use based on instruction semantics.

        This pre-processing step runs BEFORE the ReAct loop to ensure the correct
        flow is used for networking requests.

        Args:
            task_instruction: The task instruction from InteractionAgent
            user_message: Raw user message text for anti-stale-context safeguards

        Returns:
            Flow type: "purpose_suggestion", "direct_match", or "purpose_confirmation"
        """
        instruction = str(task_instruction.get("instruction", "") or "")
        raw_message = str(user_message or "")

        # Anti-stale-context safeguard:
        # If current user message is a generic ask, always run purpose suggestion.
        # This prevents implicit fallback to prior demand context.
        message_for_detection = raw_message.strip() or instruction
        lowered_message = message_for_detection.lower()
        if raw_message.strip() and any(term in lowered_message for term in ("email", "emails", "inbox")):
            logger.info("[EXECUTION] Flow override: raw user message mentions email/inbox")
            return "purpose_suggestion"
        if raw_message.strip() and self._is_generic_networking_request(message_for_detection):
            logger.info("[EXECUTION] Flow override: raw user message is generic networking request")
            return "purpose_suggestion"

        # Handle special cases (no LLM needed)
        if task_instruction.get("confirmed_purposes"):
            return "purpose_confirmation"
        if task_instruction.get("selected_purpose"):
            return "direct_match"  # User already selected a purpose

        system_prompt = """You classify networking requests into flow types.

There are THREE flow types: VAGUE, EMAIL, and SPECIFIC.
Both VAGUE and EMAIL use Purpose Suggestion Flow (suggest_connection_purposes).
SPECIFIC uses Direct Match Flow (find_match/find_multi_matches).

---

VAGUE = Request is generic WITHOUT any specific criteria - no activity, role, skill, or goal mentioned.
VAGUE requests use generic phrasing without specifying WHO or WHAT they want.
Examples of VAGUE:
- "connect me with someone" (no criteria)
- "find me someone" (no criteria)
- "find me a connection" (no criteria)
- "can u find me someone" (no criteria)
- "help me network" (no criteria)
- "find connections" (no criteria)
- "wants to connect" (no criteria)
- "find someone" (no criteria)
- "meet someone" (no criteria)
- "I want to meet someone" (no criteria)
- "help me find a connection" (no criteria)
- "suggest some connections" (asks for suggestions, no criteria)
- "what networking opportunities do I have" (asks for suggestions, no criteria)

---

EMAIL = Request explicitly mentions emails, inbox, or wanting connections BASED ON emails.
Examples of EMAIL:
- "scan my emails", "check my inbox"
- "find connections from my emails", "based on my emails"
- "email opportunities", "opportunities from my emails"
- "any interesting connection opp from my emails"
- "connect me with someone from my emails"
- "what networking opportunities are in my emails"
- "connection opportunities in my inbox"
- "from my email", "in my emails", "my inbox"

---

SPECIFIC = Request contains a concrete purpose, activity, role, skill, industry, or goal.
Examples of SPECIFIC:
- Role/industry: "PM mentor", "ML engineers", "VC analyst", "someone in consulting", "software engineers at Google"
- Activity-based: "hackathon teammates", "teammates for hackathon", "study partner for CIS 520", "cofounder for startup", "study group for machine learning"
- Goal-based: "someone to practice interviews with", "gym buddy", "research collaborator", "cofounders for fintech startup"

IMPORTANT: Activity-based requests ARE specific!
- "hackathon teammates" = SPECIFIC (has activity: hackathon)
- "study partner for STAT 4050" = SPECIFIC (has course: STAT 4050)
- "cofounder for my startup" = SPECIFIC (has goal: startup)
- "teammates for the hackathon" = SPECIFIC (has activity: hackathon)

---

**DECISION RULE:**
1. If request mentions "email", "emails", "inbox" → EMAIL
2. Else if request has no specific criteria (no role, activity, skill, goal) → VAGUE
3. Else → SPECIFIC

Output JSON only: {"flow": "specific" | "vague" | "email", "reason": "<brief explanation>"}"""

        user_prompt = f'Classify this networking instruction:\n\n"{instruction}"'

        try:
            response = await self.openai.generate_response(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                model="gpt-4o-mini",
                temperature=0.0,
                max_tokens=100,
                trace_label="detect_networking_flow",
            )

            # Parse response
            cleaned = response.strip()
            if cleaned.startswith("```json"):
                cleaned = cleaned[7:]
            if cleaned.startswith("```"):
                cleaned = cleaned[3:]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

            result = json.loads(cleaned)
            flow = result.get("flow", "specific")
            reason = result.get("reason", "")

            logger.info(f"[EXECUTION] Flow classification: {flow} - {reason}")

            # Map to internal flow names
            # Both "vague" and "email" use Purpose Suggestion Flow
            if flow in ("vague", "email"):
                return "purpose_suggestion"
            else:
                return "direct_match"

        except Exception as e:
            logger.warning(f"[EXECUTION] Flow detection failed, defaulting to direct_match: {e}")
            return "direct_match"
