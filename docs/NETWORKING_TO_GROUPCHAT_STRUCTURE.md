# Networking -> Group Chat Structure (Current)

This doc explains the current folder structure and runtime flow that connects:

- DM-based LangGraph flows (interaction + execution agents) under `app/agents/`
- Frank-managed group chat runtime under `app/groupchat/`

It's written as a "mental model + map" so it's easy to know where code belongs and how messages move.

---

## 1) The mental model: two pipelines

Franklink has two distinct message-processing pipelines:

1) Direct messages (DMs) -> Interaction Agent (LangGraph)
   - Used for onboarding, recommendation, networking requests, and general chat.
   - Key idea: DM state is primarily keyed by user (phone/email) and persisted in Supabase.

2) Group chats -> GroupChat runtime
   - Used only once a group chat exists (Frank + two users).
   - Key idea: group chat state is keyed by `chat_guid` (multi-user), and message history is stored in Zep.

This split is deliberate: group chats behave very differently from DMs (multi-user, chat GUIDs, different idempotency needs, different "never DM reply to group chat" rule).

---

## 2) Quick folder map

### 2.1 High-level tree

```text
app/
  agents/                      # LangGraph agents (DM world)
    interaction/               # conductor + routing helpers
      agent.py
      graph.py
      router.py
    execution/                 # domain graphs
      onboarding/
      networking/
      recommendation/
      update/
      general/

  groupchat/                   # group chat runtime domain (chat_guid world)
    runtime/                   # single entrypoint + handler dispatch
      router.py
      handlers/
      deps.py
      types.py
    io/                        # centralized send + record wrappers
      recorder.py
      sender.py
    memory/                    # Zep session + metadata helpers
      zep.py
    summary/                   # background summarization (Supabase jobs + worker)
      scheduler.py
      worker.py
      prompts.py
      utils.py
    features/                  # group chat business workflows
      provisioning.py          # create chat + seed welcome/icebreaker
      icebreaker.py            # generate topic + poll content
      opinion.py               # follow-up + "frank ..." invocation behavior

  database/                    # Supabase client + models
  integrations/                # Photon, Azure OpenAI, Zep HTTP, Stripe, etc.
  utils/                       # Redis client, message chunker, shared helpers
```

### 2.2 The boundary between folders

- Networking ends when a group chat is created.
- Group chat runtime begins once messages are happening inside the group chat.

In code, the bridge is:

- `app/agents/execution/networking/nodes/create_group.py` -> calls -> `app/groupchat/features/provisioning.py`

---

## 3) End-to-end flow: "connect me" -> group chat -> ongoing conversation

### 3.1 Inbound message enters the system (Photon -> API)

Inbound messages typically arrive via the Photon Socket.IO listener:

1) `app/integrations/photon_listener.py` receives a Photon event
2) It forwards a normalized payload into `app/main.py`'s callback
3) The callback calls `MainOrchestrator.handle_message(...)` in `app/orchestrator.py`

### 3.2 Orchestrator decides: DM vs group chat

`app/orchestrator.py` has an early branch:

- If the inbound payload has a group chat GUID (`";+;"` or `"chat..."`) -> route to `GroupChatRouter`
- Otherwise -> process via InteractionAgent (router + execution agents)

This is the fork in the road between the two pipelines.

### 3.3 DM networking request (LangGraph path)

When the user DMs something like "connect me" or "network", the DM pipeline looks like:

1) `app/agents/interaction/agent.py` builds initial `GraphState` (`app/models/state.py`)
2) `app/agents/interaction/router.py`:
   - loads the user profile from Supabase
   - checks if there are pending actions (e.g., you're awaiting match confirmation)
   - classifies intent
   - routes to the correct execution agent: onboarding/recommendation/networking/general
3) If routed to networking:
   - `app/agents/execution/networking/graph.py` runs the handshake flow

### 3.4 Networking graph creates the match + handshake state

The networking graph is a multi-step handshake:

- `nodes/find_match.py`
  - resumes a pending request if one exists
  - otherwise runs `utils/three_stage_matcher.py`:
    1) same-university filter (hard constraint)
    2) semantic candidate search (embedding similarity)
    3) LLM selection + intro generation
  - creates a Supabase `connection_requests` row via `utils/handshake_manager.py`

- `nodes/present_match.py`
  - shows the selected match to User A and sets `waiting_for="initiator_confirmation"`

- `nodes/process_initiator.py`
  - classifies the reply (confirm/different/cancel/unclear)
  - updates the request status accordingly

- `nodes/send_invitation.py`
  - DMs User B via Photon and sets `waiting_for="target_response"`

- `nodes/process_target.py`
  - classifies accept/decline/unclear
  - updates the request status and notifies the initiator

### 3.5 The bridge: create the group chat

If User B accepts, the networking graph reaches:

- `app/agents/execution/networking/nodes/create_group.py`

That node calls into the group chat domain:

- `app/groupchat/features/provisioning.py` (`GroupChatService.create_group(...)`)

Provisioning does three things (best-effort for side effects that shouldn't block):

1) Calls Photon to create the group chat and send a warm welcome message
2) Stores a `group_chats` record in Supabase (participants + modes + request id)
3) Seeds Zep:
   - session metadata describing participants + chat kind
   - records the welcome message as outbound history
   - sends an initial icebreaker topic + a poll (idempotent)

At this point, the networking domain is done. We now have a managed group chat.

### 3.6 Ongoing group chat messages (GroupChat runtime path)

Once a group chat exists, inbound group chat messages go down the group chat runtime pipeline:

1) `app/orchestrator.py` detects group chat and calls:
   - `app/groupchat/runtime/router.py` (`GroupChatRouter.handle_inbound(...)`)
2) The router:
   - validates the GUID format
   - loads "managed context":
     - primary: Supabase `group_chats` row
     - fallback: Zep session metadata `kind=groupchat`
   - normalizes the inbound payload to a stable internal event (`GroupChatEvent`)
   - resolves sender identity best-effort (user_a vs user_b)
   - records inbound via `GroupChatRecorder` (idempotent; Zep + Redis keys)
   - (optional) schedules a summary job in Supabase (if enabled)
   - dispatches to feature handlers (first one to claim the event wins)

Today the handler stack is minimal:

- `app/groupchat/runtime/handlers/opinion_v1.py` adapts the existing opinion workflow (`app/groupchat/features/opinion.py`)

---

## 4) What each `app/groupchat/*` folder is for (intuitively)

### 4.1 `app/groupchat/runtime/` (the "traffic controller")

This is the single entry point for group chat inbound messages.

Key properties:
- Owns the "never DM-reply to group chats" rule.
- Normalizes Photon payloads into a stable internal event type.
- Records inbound history before business logic (when the chat is Frank-managed).
- Delegates feature behavior to handlers.

If you're thinking "where do I plug in a new group chat behavior?", start here:
- Add a new handler in `app/groupchat/runtime/handlers/`
- Register it in the router (or handler list)

### 4.2 `app/groupchat/io/` (the "always record" wrappers)

This folder exists to prevent "some features forget to record history".

- `GroupChatRecorder` records inbound/outbound message history to Zep, using Redis idempotency keys
- `GroupChatSender` sends via Photon and always records outbound messages using the recorder

Rule of thumb:
- group chat feature code should prefer `GroupChatSender.send_and_record(...)` instead of calling Photon directly.

### 4.3 `app/groupchat/memory/` (the "Zep adapter")

Group chat message history is not stored in Supabase today.

Instead:
- Zep session ID is derived from chat GUID (`groupchat_<sanitized_chat_guid>`)
- Zep stores:
  - raw user/assistant messages (with metadata)
  - structured session metadata (participants, icebreaker context, poll info, etc.)

### 4.4 `app/groupchat/summary/` (background summarization)

This folder owns the "5 minutes of user inactivity -> summarize" feature:
- A scheduler called from the router (`scheduler.py`)
- A worker process (`worker.py`) that:
  - claims due jobs from Supabase
  - fetches transcript from Zep
  - generates a Markdown summary using `AzureOpenAIClient`
  - appends a segment into the one-row-per-chat memory table

### 4.5 `app/groupchat/features/` (the "business workflows")

These are the user-facing behaviors inside the group chat:

- `provisioning.py`: create chat + seed welcome + icebreaker + poll
- `icebreaker.py`: generate a topic + poll content (LLM + resources DB)
- `opinion.py`: watch user replies; post one follow-up opinion; then respond only to explicit "frank ..."

This folder is allowed to evolve: features can be moved into dedicated handlers over time without changing the runtime entrypoint.

---

## 5) Persistence model (what data lives where)

### 5.1 Supabase (structured records)

Supabase is the system of record for:
- users (profile fields)
- connection requests (handshake state)
- group chats (the mapping from `chat_guid` -> participants + modes)
- group chat summary jobs + summary memory (if you enable summarization)

Key code:
- models: `app/database/models.py`
- db access: `app/database/client/` (exported as `app.database.client.DatabaseClient`)

### 5.2 Zep (raw transcript + session metadata)

Zep is used for group chat memory:
- raw messages (both inbound and outbound)
- structured metadata (participants, icebreaker context, poll info, etc.)

Key code:
- `app/groupchat/memory/zep.py`
- `app/integrations/zep_client_simple.py`

### 5.3 Redis (idempotency + small caches)

Redis is used for:
- idempotency keys (avoid duplicate side effects / duplicate recordings)
- small caches (e.g., cached chat GUIDs, debounces for icebreaker sending)

Key code:
- `app/utils/redis_client.py`

---

## 6) "Where should I put new code?"

### If it's about matching or consent before chat creation
Put it under:
- `app/agents/execution/networking/` (nodes/utils/prompts)

### If it's about behavior inside the group chat (after `chat_guid` exists)
Put it under:
- `app/groupchat/` (usually a handler + feature service)

### If it's about recording/sending group chat messages safely
Put it under:
- `app/groupchat/io/` or `app/groupchat/memory/`

### If it's summarization / background "segment workers"
Put it under:
- `app/groupchat/summary/`

### If it's a persistence concern (tables, queries, RPC, models)
Put it under:
- `app/database/`

---

## 7) Related docs

- Group chat runtime design + rationale: `docs/GROUP_CHAT_WORKFLOW_PLAN.md`
- Inactivity summary plan: `docs/GROUP_CHAT_SUMMARY_PLAN.md`
- Quick LangGraph overview (DM world): `support/docs/LANGGRAPH_ARCHITECTURE.md`
