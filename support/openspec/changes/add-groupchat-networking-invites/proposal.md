## Why
Users want to expand an existing group chat by asking Frank (inside the group chat) to find and invite new participants who match the group's goal. Current group chat routing only supports maintenance and opinion flows, and the DM networking workflow is already correct and should remain unchanged (baseline on `origin/proactivetry`).

## What Changes
- Introduce a new group chat networking agent by copying the DM networking agent code (from `origin/proactivetry`) and adapting it for group chat expansion.
- Keep existing DM networking workflow untouched (no behavior changes), and reverse any prior modifications to the original networking flow.
- Trigger only inside group chats with explicit Frank invocation and an explicit request to find a new person for the existing chat; DM cannot trigger this.
- Always invite one person at a time (single-match flow only), using group context when demand is unclear.
- On acceptance, add the target to the existing chat and set connection request status to GROUP_CREATED.
- If a 2-person chat adds a third person, ensure all participants are stored in group_chat_participants (canonical for multi-person).

## Impact
- Affected specs: groupchat-networking (new capability)
- Affected code: app/groupchat/runtime/router.py, app/groupchat/runtime/handlers/*, app/agents/interaction/agent.py, app/agents/interaction/prompts/base_persona.py, new app/agents/tasks/groupchat_networking.py, new app/agents/tools/groupchat_networking.py, app/agents/execution/networking/* (reused), app/groupchat/features/provisioning.py, app/database/client/connection_requests.py, app/database/models.py
