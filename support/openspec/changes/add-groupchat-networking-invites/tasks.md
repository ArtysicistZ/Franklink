## 1. Restore DM Networking Baseline (origin/proactivetry)
- [x] 1.1 Revert app/agents/tasks/networking.py to origin/proactivetry
- [x] 1.2 Revert app/agents/tools/networking.py to origin/proactivetry
- [x] 1.3 Revert app/agents/execution/networking/utils/handshake_manager.py to origin/proactivetry
- [x] 1.4 Revert any DM prompt changes in app/agents/interaction/prompts/base_persona.py to origin/proactivetry
- [x] 1.5 Revert any DM routing changes in app/agents/interaction/agent.py to origin/proactivetry
- [x] 1.6 Revert any provisioning changes in app/groupchat/features/provisioning.py to origin/proactivetry
- [x] 1.7 Revert any connection_requests changes in app/database/client/connection_requests.py to origin/proactivetry

## 2. New Groupchat Networking Agent (Copied from DM Networking)
- [x] 2.1 Create app/agents/tasks/groupchat_networking.py by copying origin/proactivetry networking task code
- [x] 2.2 Create app/agents/tools/groupchat_networking.py by copying origin/proactivetry networking tools
- [x] 2.3 Adapt groupchat_networking prompts for group chat expansion (clear vs unclear demand, existing chat only)
- [x] 2.4 Add group chat context tools for demand derivation and participant exclusion in groupchat_networking module
- [x] 2.5 Keep all DM networking code paths unchanged

## 3. Group Chat Routing and Prompting (Group Chat Only)
- [x] 3.1 Add a GroupChatNetworkingHandler and register it in app/groupchat/runtime/router.py
- [x] 3.2 Update group chat decision prompt to route to groupchat_networking (not networking)
- [x] 3.3 Ensure routing/tool triggers are LLM-driven (no keyword matching), except explicit Frank invocation
- [x] 3.4 Block DM from triggering group chat expansion (DM response is LLM-synthesized guidance)

## 4. Invite to Existing Chat Flow
- [x] 4.1 Groupchat_networking uses single-match flow only (find_match)
- [x] 4.2 Use group_chat_guid for existing chat and add participant on acceptance
- [x] 4.3 Set request status to GROUP_CREATED on acceptance
- [x] 4.4 Backfill group_chat_participants for 2-person chats before adding the third member

## 5. Guardrails & Observability
- [x] 5.1 Exclude current participants and previously invited targets from candidate selection
- [x] 5.2 Add logs/traces for groupchat expansion routing, matching, invitation, and add-participant outcome

## 6. Tests
- [ ] 6.1 Routing tests for group chat expansion intent detection (LLM prompt-driven)
- [ ] 6.2 Groupchat networking task tests: single-match, existing-chat only
- [ ] 6.3 Integration tests for invite -> accept -> add participant to existing chat
