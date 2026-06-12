## Context
- Group chat messages are routed via app/groupchat/runtime/router.py, with handlers list defaulting to GroupChatMaintenanceHandler and OpinionV1Handler.
- Group chat routing uses InteractionAgentNew with is_group_chat_context=True, which currently restricts tasks to groupchat_maintenance.
- DM networking workflow is already correct and must remain unchanged; the baseline is origin/proactivetry.
- Networking execution uses the handshake flow and connection_requests for consented invitations.
- Two-person chats are stored in group_chats with user_a_id/user_b_id; multi-person membership is stored in group_chat_participants.

## Goals / Non-Goals
- Goals:
  - Add a dedicated group chat networking agent (separate from DM networking) to invite new participants to an existing chat.
  - Keep DM networking workflow unchanged, with no behavior changes.
  - Only trigger inside group chats with explicit Frank invocation and explicit request to find a new person for that chat.
  - If demand is unclear, infer it from group chat context (summary + recent messages + participant profiles).
  - Invite targets via the existing handshake flow; only add to the chat after acceptance.
  - Block DM from triggering group chat expansion.
  - Use GROUP_CREATED status for accepted invites to an existing chat.
  - Always invite one person at a time (single-match flow).
  - When a 2-person chat adds a third person, store all participants in group_chat_participants (canonical for multi-person).
  - All routing and tool triggers are decided by the LLM (no keyword rules), except detecting explicit Frank invocation.
- Non-Goals:
  - Replacing the DM networking system.
  - Changing the participant storage schema.
  - Hardcoded fallback responses; InteractionAgent synthesis remains the only response generator.

## Decisions
- Create a new group chat networking agent by copying the DM networking agent code from origin/proactivetry and adapting it for group chat expansion.
  - New task: app/agents/tasks/groupchat_networking.py
  - New tools module: app/agents/tools/groupchat_networking.py
- Restore DM networking code to the origin/proactivetry baseline (reverse prior modifications).
- Group chat routing:
  - Add a new group chat handler for expansion requests that requires explicit Frank invocation and explicit add-person intent.
  - Group chat interaction prompt routes to groupchat_networking (not networking).
  - DM never initiates groupchat_networking; DM may route to groupchat_networking only for CASE C target responses when a pending request has group_chat_guid.
- Demand resolution:
  - Clear demand: use user request directly.
  - Unclear demand: derive demand from group chat context using summary + recent messages + participant profiles.
- Invite flow:
  - Reuse connection request + handshake flow for consent.
  - On acceptance, add participant to existing chat via GroupChatService.add_participant_to_group().
  - Set connection_requests.group_chat_guid to the existing chat and mark status GROUP_CREATED.
- Participant storage:
  - For 2-person chats that add a third person, backfill group_chat_participants for user_a and user_b, then add the new participant.
- Expansion is a group-level request:
  - Any member may initiate, but the request is on behalf of the group.
  - The initiator user_id is still recorded as connection_requests.initiator_user_id for auditability.

## Group Chat Networking Prompts (Explicit)
- InteractionAgent group chat decision prompt must include:
  - Explicit routing guidance for group chat expansion vs normal groupchat maintenance.
  - Required task_instructions fields for group chat expansion: chat_guid, demand (or demand_source=derived), group_context_summary, and initiator_user_id.
  - A clear directive that the LLM must decide routing/tools (no keyword matching), except explicit Frank invocation.
- Groupchat networking task prompt must include:
  - Single-match only: always use find_match.
  - Never create a new chat; must add to existing chat after acceptance.
  - Clear vs unclear demand behavior (derive when unclear using group context tool).
  - Candidate filtering: exclude current participants and previously invited targets.
- Demand derivation prompt must:
  - Use group chat summary + recent messages + participant profiles.
  - Output a single concise demand string aligned to the chat's purpose and tone.

## What Stays the Same
- DM networking flows and their cases (A/B/C/D) remain unchanged (baseline from origin/proactivetry).
- Connection request lifecycle uses existing statuses; GROUP_CREATED remains the terminal success state.
- Existing handshake flow and consent model remain unchanged.

## What Changes
- Add a new groupchat networking agent (task + tools + prompts) by copying DM networking and adapting it.
- Add group chat routing/handler to trigger groupchat networking in group chats only.
- Add group chat context tools for demand derivation and participant exclusion (groupchat networking module only).
- Add backfill behavior for 2-person chats when first expanding to 3+ members.

## Risks / Trade-offs
- Duplicating networking code increases maintenance cost; must keep DM and groupchat logic in sync where appropriate.
- Demand inference may be noisy; prompt constraints must be strict and LLM-driven.
- Dual storage (group_chats + group_chat_participants) requires careful synchronization when transitioning 2-person chats to multi-person.

## Migration Plan
- No new tables required.
- On-demand backfill of group_chat_participants when a third member is added.

## Open Questions
- What is the exact threshold for "explicit request" to add a person in group chat? (Encode in prompts, not code.)
