# Multi-Task Intent Routing Plan

This plan proposes a low-risk way to let one inbound user message drive multiple graphs in sequence, instead of only one. It preserves the existing LangGraph router behavior when the classifier returns a single task or when pending/onboarding gating applies.

## Current Behavior (Summary)
- `app/agents/interaction/router.py` routes a single message to exactly one graph via `classify_intent_node` (single label) -> `route_by_intent` -> execution agent.
- `app/models/state.py` stores only one `current_message.intent` and one `response` per run.
- `app/agents/interaction/agent.py` returns a single `response_text` and `next_graph`.
- `app/orchestrator.py` sends one reply message (plus optional resource URLs / outbound messages) and stores one bot message.

## Proposed Behavior (Multi-Task)
- The intent classifier returns JSON with a list of tasks, each mapped to a graph intent.
- Multi-task runs only for fully onboarded users (`is_onboarded == True` and `onboarding_stage == complete`).
- If the user is not fully onboarded, or if `waiting_for` is set, the router collapses to a single-task flow.
- The router builds a task queue, then runs graphs sequentially for each task.
- Each task produces a response + optional extras; results are collected in a list and returned to the orchestrator.
- The orchestrator sends each task response in order and persists each as its own bot message.
- Fallback: if JSON is invalid or empty, keep the current single-intent behavior.

## Data Model Changes (State)
Update `app/models/state.py`:
- Add a `TaskItem` typed dict (intent, task_text, optional task_id/source).
- Add a `TaskResult` typed dict (intent, graph, response_text, resource_urls, outbound_messages, waiting_for).
- Add fields to `GraphState`:
  - `task_queue: List[TaskItem]`
  - `active_task: Optional[TaskItem]`
  - `task_results: List[TaskResult]`
  - `original_message: Optional[str]` (or store in `graph_metadata` if preferred)

## Intent Classifier Output (Prompt + Parsing)
Update `app/utils/prompts.py`:
- Add `get_multi_intent_classification_prompt()` that instructs JSON-only output.
- Required JSON shape:
  ```json
  {
    "tasks": [
      {"intent": "recommendation", "task": "show me fintech internships"},
      {"intent": "networking", "task": "connect me with finance students"}
    ]
  }
  ```
- Constraints in prompt:
  - Allowed intents: general, recommendation, networking, onboarding
  - Split only if the message clearly contains multiple independent requests
  - Max tasks: 3
  - Avoid returning a standalone general task when other tasks exist

Parsing in `app/agents/interaction/router.py`:
- Extract JSON from LLM output (strip code fences; regex for `{...}` if needed).
- Validate tasks list and intent values.
- Enforce max 3 tasks (truncate extras deterministically, preserve order).
- If invalid or empty, fall back to a single task using the original message and a default intent (`general` or legacy `classify_intent` call).
- If multi-task is suppressed due to gating (not fully onboarded or `waiting_for`), store parsed tasks in `graph_metadata` for observability and future enablement.

## Router Flow Changes (LangGraph)
Update `app/agents/interaction/router.py`:
- `classify_intent_node`:
  - Call the new multi-task prompt.
  - Store `task_queue` (list), clear `task_results`, set `original_message`.
  - Keep `current_message.intent` set to the first task intent for compatibility.
  - If multi-task is not allowed, collapse the queue to a single task and store the full candidate list in `graph_metadata["multi_task_candidates"]` with `graph_metadata["multi_task_suppressed"]=<reason>`.
- Add a `select_next_task` node:
  - If `task_queue` has items, pop the next item into `active_task`.
  - Set `current_message.content` to `active_task.task` and `current_message.intent` to `active_task.intent`.
  - If no queue exists, construct a single-task queue from the current message.
- Add a `task_complete` node:
  - Append a `TaskResult` to `task_results` (response_text + extras).
  - Clear task-specific temp keys (e.g., `resource_urls`, `outbound_messages`) to avoid bleed into the next task.
  - Decide whether to continue or end.

Graph edges:
- Route after `pending_networking_gate` to `select_next_task` instead of directly to graph nodes.
- Route from `select_next_task` to the appropriate graph via `route_by_intent`.
- Route each graph node to `task_complete` instead of END.
- `task_complete` conditionally loops to `select_next_task` until the queue is empty (or stop condition triggers).

## Pending/Onboarding Safety Rules (Low Risk)
To keep behavior safe and predictable:
- Keep existing onboarding gate (`should_onboard`) as-is for now. If the user is mid-onboarding, route only to onboarding (no multi-task fan-out). This avoids conflicting prompts.
- Keep pending-action logic (waiting_for / pending networking gate). When `waiting_for` is set, skip multi-task and route only to the appropriate graph for that pending action.
- Future-ready hook: multi-task suppression reasons are recorded so enabling multi-task for `waiting_for` later is a localized change (one guard + tests).

If you want multi-task even during onboarding or pending flows, we can relax these rules later by adjusting the guard in `classify_intent_node` and in the `select_next_task` entry path.

## Orchestrator and Runner Changes
Update `app/agents/interaction/agent.py`:
- Return `responses` from `task_results` when present.
- Keep `response_text`, `intent`, and `next_graph` pointing to the last task for backward compatibility.

Update `app/orchestrator.py`:
- If `responses` list exists, send each `response_text` in order.
- For each task result, send resource URLs and outbound messages tied to that task.
- Store each sent response in DB with metadata including task index and graph.
- Keep reaction behavior tied to the last response (or make it explicit if you prefer per-task reactions).

## Manual Test Plan (No New Infra)
1. Multi-task (recommendation + networking):
   - "show me fintech internships and connect me with finance students"
   - Expect two responses: recommendations then networking prompt.
2. Multi-task with general filler:
   - "hey frank can you show internships and connect me with people in ai"
   - Expect two task responses, no extra greeting-only reply.
3. Onboarding + task (should stop at onboarding per safety rule):
   - New user: "im john at mit and also show me ai resources"
   - Expect onboarding response only (until we relax the rule).
4. Pending networking:
   - User with pending request: "yes also show me roles"
   - Expect networking flow only (until rules are relaxed).

## Decisions (Resolved)
- Multi-task is only enabled for fully onboarded users.
- Max tasks per message is 3.
- `waiting_for` suppresses multi-task for now; we keep a clear hook to enable it later.

## Implementation Checklist (After Approval)
1. Add task schema + fields to `app/models/state.py`.
2. Add multi-task intent prompt in `app/utils/prompts.py`.
3. Update `classify_intent_node` parsing and add `select_next_task` + `task_complete` nodes in `app/agents/interaction/router.py`.
4. Update router edges to loop through tasks.
5. Update `app/agents/interaction/agent.py` to return task results.
6. Update `app/orchestrator.py` to send/store multi-task outputs.
7. Run manual tests above and adjust prompt/validation as needed.

## Confidence Notes (Why This Is Safe)
- Existing onboarding and pending-action routing stays the default; multi-task only activates for fully onboarded users.
- Invalid or empty JSON falls back to the existing single-intent path.
- Hard cap of 3 tasks prevents excessive fan-out and keeps runtime bounded.
- Task results are isolated with per-task cleanup to avoid state bleed between graphs.
