# Group Chat Workflow Refactor (Raw History Recording First)

## 1) What "robust and correct" means (requirements)

This refactor is successful only if these guarantees hold for Franklink-managed group chats:

1) **Inbound is always recorded**
   - Any inbound group chat event that reaches the app is persisted to Zep **before** any business logic runs.
   - Recording must not depend on which feature handler is active.

2) **Outbound is always recorded**
   - Every Frank outbound group chat message goes through a single wrapper that records it to Zep.
   - No direct `PhotonClient.send_message_to_chat` usage in feature code.

3) **Idempotent processing**
   - Duplicate delivery must not create duplicate Zep writes or duplicate follow-up actions.
   - Idempotency must be based on a stable event key: `(chat_guid, message_id)` when available, otherwise a deterministic fallback key.

4) **Order-agnostic**
   - Inbound events may arrive out of order; handlers must not assume strict ordering.

5) **Multi-instance safe**
   - If you ever run multiple API instances, inbound processing remains safe via shared idempotency keys (Redis).

6) **Failure-tolerant**
   - Zep failures never block feature handling or message sends.
   - If you need truly lossless logging during Zep outages, you must add a durable fallback queue/store (optional extra).

## 2) Current baseline (how it works today)

### 2.1 Inbound flow
`PhotonListener` -> `MainOrchestrator.handle_message` -> (group chat branch) -> `GroupChatRouter.handle_inbound` -> (handler dispatch)

References:
- `app/integrations/photon_listener.py` (detects group chats and forwards payload)
- `app/orchestrator.py` (early-returns for group chats)
- `app/groupchat/runtime/router.py` (central routing + inbound recording)
- `app/groupchat/runtime/handlers/opinion_v1.py` (current handler adapter)
- `app/groupchat/features/opinion.py` (icebreaker follow-up + "frank ..." invocations)

### 2.2 Raw history recording today
History is written to Zep via `GroupChatMemoryService` (`app/groupchat/memory/zep.py`) and centralized via:
- Inbound: recorded first in `GroupChatRouter` using `GroupChatRecorder` (`app/groupchat/io/recorder.py`)
- Outbound: recorded by `GroupChatSender.send_and_record` (`app/groupchat/io/sender.py`)

### 2.3 Existing "group chat workflows" currently living under `networking/`
These are group chat runtime features and now live under `app/groupchat/`:
- **Seed on create**: welcome + post-intro icebreaker + poll (`app/groupchat/features/provisioning.py`, `app/groupchat/features/icebreaker.py`)
- **Follow-up on replies + invocations**: one-time opinion after both users respond; respond to explicit "frank ..." (`app/groupchat/features/opinion.py`)

## 3) Why the current approach will break as features grow

Concrete footguns already present:
- Inbound recording is coupled to one feature service; new features can accidentally skip logging.
- Messages can be returned early (e.g., sender mapping issues) before recording, so "raw history is complete" is not guaranteed.
- Outbound recording is manual; any new outbound feature can forget to write to Zep.
- Group chat handling is outside LangGraph, so adding multiple group workflows will increase "service sprawl" unless we introduce a central runtime layer.

## 4) Recommended architecture (clean + scalable)

### 4.1 Domain boundary: networking vs group chat runtime

Treat the system as two cooperating domains:

1) **Networking (handshake/matching)** (`app/agents/execution/networking/`)
   - Scope ends at: users matched + consent + group chat created.
   - May call a group chat runtime "seed" entry point once, after creation.

2) **Group chat runtime** (`app/groupchat/` - new package)
   - Owns: inbound routing, raw history recording, outbound sending wrappers, feature handlers (icebreaker, invocations, future features).

This boundary prevents group chat logic from continuing to accumulate in the "networking" folder.

### 4.2 Single entry point: GroupChatRouter

Add a single runtime entry point used by `MainOrchestrator` for all group chat messages:

`GroupChatRouter.handle(event)` pipeline:
1) Validate group chat format from `chat_guid`
2) Determine if chat is Franklink-managed
   - primary: Supabase `group_chats` record
   - optional fallback: Zep session metadata `kind=groupchat` (helps if DB row missing)
3) Resolve sender identity best-effort (do not block logging)
4) **Record inbound raw message to Zep first** (idempotent)
5) Dispatch to feature handlers (first handler that claims the event wins)

### 4.3 Centralize IO: GroupChatRecorder + GroupChatSender

1) `GroupChatRecorder` (wraps `GroupChatMemoryService`)
   - `record_inbound(event)` and `record_outbound(event)`
   - attaches consistent metadata (see section 6)
   - enforces idempotency for recording (Redis `SET NX` on `groupchat:seen:<chat_guid>:<event_id>`)

2) `GroupChatSender`
   - `send_and_record(chat_guid, text, metadata)`:
     - send via Photon
     - record to Zep best-effort
   - this is the only allowed send API for group chat features

### 4.4 Handler interface (plugin model)

Define a stable handler interface so new features don't require editing a monolith:
- `can_handle(ctx, event) -> bool`
- `handle(ctx, event) -> handled: bool`

Rules:
- Each handler must be **idempotent** for side effects using a stable key in Redis:
  - `groupchat:<feature>:<version>:<chat_guid>:<action>:<event_id>`
- Handlers may read from Zep/Redis but must not depend on strict message order.

### 4.5 File/folder layout (proposed)

Create `app/groupchat/` and move group chat runtime code there over time:
- `app/groupchat/runtime/router.py` (GroupChatRouter)
- `app/groupchat/runtime/types.py` (GroupChatEvent, GroupChatManagedContext)
- `app/groupchat/io/recorder.py` (GroupChatRecorder)
- `app/groupchat/io/sender.py` (GroupChatSender)
- `app/groupchat/memory/zep.py` (GroupChatMemoryService)
- `app/groupchat/runtime/handlers/` (handler registry + adapters)
- `app/groupchat/features/` (provisioning + current workflows)
  - future: `settings_v1.py`, `introductions_v1.py`, etc.

### 4.6 What belongs in `app/groupchat/` vs `app/agents/execution/networking/`

**Keep in `app/agents/execution/networking/` (handshake graph only):**
- Matching + consent + invitations + connection request state machine.
- The LangGraph networking workflow and its nodes (its scope ends at "group chat created").
- DM prompts related to invitations/accept/decline.

**Move into `app/groupchat/` (everything inside the chat after creation):**
- Inbound group message routing (single entry point for group chat events).
- Raw history recording (Zep) and consistent message metadata.
- Outbound sending wrapper for group chats (send + record).
- Icebreaker seeding + follow-up workflows.
- "frank ..." invocation behavior.
- Any future feature that reacts to group chat messages (settings, nudges, mini-workflows).

A simple boundary test:
- If it depends on `chat_guid` and happens *inside* the group chat, it belongs in `app/groupchat/`.
- If it depends on `connection_request_id` and happens *before* the group chat exists, it belongs in `app/agents/execution/networking/`.

### 4.7 Migration map (current files -> future home)

This is the concrete "move everything group chat related" checklist for implementation:
- `app/agents/execution/networking/nodes/create_group.py` -> calls `app/groupchat/features/provisioning.py`
- `app/orchestrator.py` group chat branch -> calls `app/groupchat/runtime/router.py`
- raw history -> `app/groupchat/memory/zep.py` + `app/groupchat/io/*`
- workflows -> `app/groupchat/features/*` (current) and `app/groupchat/runtime/handlers/*` (dispatch)

Note: `app/agents/execution/networking/nodes/create_group.py` stays in networking, but should call `app/groupchat/features/provisioning.py` so group chat creation logic is owned by the group chat domain.

## 5) Do we need a new LangGraph for group chats?

### Not required for correctness of raw history (Phase 1)
To guarantee raw history recording, a router + IO layer is enough and is the fastest safe fix.

### Plausible and useful later (Phase 2+), if complexity grows
Consider a dedicated "group_chat" graph only when you have multiple multi-step workflows that benefit from explicit nodes/edges (e.g., scheduling, structured onboarding inside group chat, multi-turn tool calls).

If you do it, the key design point is: group chat state must be keyed by `chat_guid` (multi-user), not by `user_id` like DM graphs.

## 6) Stable internal event model (must be passed everywhere)

The router should normalize Photon payloads into a single event shape and pass it to recorder + handlers:
- `chat_guid` (required)
- `message_id` (Photon guid when present; required for strong idempotency)
- `timestamp` (from Photon payload if provided; avoid generating "now" as the only timestamp)
- `sender_handle` (phone/email)
- `sender_user_id` (best-effort; can be "duplicate user row" today)
- `resolved_participant` ("user_a" | "user_b" | "unknown")
- `sender_name` (best-effort; fallback from handle if missing)
- `text` (may be empty if attachment-only)
- `media_url` (attachment path/url if present)
- `raw_payload` (optional, debug)

Recording metadata stored in Zep should include at minimum:
- `source=groupchat`
- `chat_guid`
- `message_id` (if available)
- `timestamp`
- `user_id` (best-effort)
- `name` (best-effort)
- `type` (e.g., `user_message`, `warm_intro`, `icebreaker_prompt`, `frank_invocation_reply`, etc.)

## 7) Implementation plan (robust, incremental, low-risk)

### Phase 0: Create the folder boundary (no behavior change)
- Add `app/groupchat/` skeleton and handler registry.
- Add shims to keep old imports working during migration.

### Phase 1: Centralize inbound recording (the core fix)
- Update `app/orchestrator.py` group chat branch to call `GroupChatRouter` instead of `GroupChatOpinionService` directly.
- Ensure the router receives `message_id`, `timestamp`, and `media_url` from the Photon payload (today the group path discards these).
- Router records inbound to Zep first (idempotent), then dispatches to current handlers (initially, a wrapper handler around existing `GroupChatOpinionService` logic is fine).

Success criteria:
- every inbound message in a managed group chat is written to Zep even if a handler ignores it

### Phase 2: Centralize outbound send + record
- Introduce `GroupChatSender.send_and_record(...)`.
- Replace direct `send_message_to_chat` usage in:
  - `app/groupchat/features/provisioning.py`
  - `app/groupchat/features/opinion.py`
- Ensure outbound metadata includes message id from Photon response (if returned).

Success criteria:
- all outbound Frank messages appear in Zep with consistent metadata

### Phase 3: Move existing workflows into handlers (make future work easy)
- Re-home icebreaker "seed" and "follow-up" into `app/groupchat/runtime/handlers/icebreaker_v1.py`
- Re-home "frank ..." invocation behavior into `app/groupchat/runtime/handlers/invocation_v1.py`
- Keep Redis keys and Zep metadata format stable during the move (no behavior change)

Success criteria:
- adding a new group chat feature is "add a handler + register it"

### Phase 4: Safety rails + validation
- Add structured logs: `chat_guid`, `event_id`, `managed`, `handler`, `recorded_inbound`, `recorded_outbound`
- Extend `support/scripts/groupchat_zep_check.py` to optionally show message_id + timestamps from metadata for auditing
- Add tests with Photon/Zep stubs (unit tests for router/recorder idempotency + handler idempotency)

## 8) Acceptance checklist (correctness gates)

Raw history:
- [ ] inbound group messages always recorded before any handler logic
- [ ] outbound Frank messages always recorded by the sender wrapper
- [ ] duplicate inbound events do not create duplicate Zep writes (idempotency works)
- [ ] attachments are captured in metadata (`media_url`) when present

Workflow scalability:
- [ ] group chat runtime code is under `app/groupchat/` (networking folder stops accumulating runtime code)
- [ ] features are handlers; no "one giant service" pattern
- [ ] handler side effects are idempotent and safe across restarts
