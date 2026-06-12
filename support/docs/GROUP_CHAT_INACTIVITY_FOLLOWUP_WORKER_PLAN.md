# Group Chat Inactivity Follow-up Worker Plan (v1)

## 0) Baseline (what exists today)

### 0.1 Group chat runtime flow
- Group chat messages branch early in `app/orchestrator.py` and route to `app/groupchat/runtime/router.py`.
- The router normalizes inbound events, records inbound messages via `app/groupchat/io/recorder.py`, and dispatches to handlers (currently `app/groupchat/runtime/handlers/opinion_v1.py` which adapts `app/groupchat/features/opinion.py`).
- Outbound group chat sends should go through `app/groupchat/io/sender.py` (send + record).

### 0.2 Supabase raw transcript + summary memory
- Raw transcript tail is stored per chat in `group_chat_raw_memory_v1` (arrays + `last_event_at`) and accessed through RPCs in `support/scripts/group_chat_summary_v1.sql`.
- Summary segments are stored per chat in `group_chat_summary_memory_v1` and exposed via `group_chat_summary_segments_v1`.
- The summarization worker is `app/groupchat/summary/worker.py` and uses DB helpers in `app/database/client/group_chat_summary.py`.

### 0.3 Worker patterns + deployment
- The summary worker uses a job table (`group_chat_summary_jobs`) and an RPC claim loop (multi-instance safe).
- Docker runs the summary worker via `infrastructure/supervisor/supervisord.conf` and via `docker-compose.yml`.
- ECS runs the summary worker as a separate container in `infrastructure/aws/ecs/taskdef-testing.json`.

## 1) Feature goal

Create a new background worker that:
- Detects group chats with **no user messages** for a configurable inactivity period (default: 1 day).
- Pulls **recent summary segments** from `group_chat_summary_memory_v1` to ground a relationship-maintenance follow-up.
- Uses the LLM to craft a brief, low-friction group message for both participants.
- Sends via `GroupChatSender` so the outbound message is always recorded in `group_chat_raw_memory_v1`.
- Runs safely every ~10 seconds under both Docker and AWS ECS.

## 2) Data model and scheduling (robust + multi-instance safe)

### 2.1 Add a new job table (recommended)
Create `group_chat_followup_jobs_v1` (one row per chat) to avoid scanning every raw transcript row every 10 seconds:
- `chat_guid text primary key references group_chats(chat_guid)`
- `status text check (queued|running|done|failed)`
- `last_user_message_at timestamptz not null`
- `last_user_event_id text not null`
- `run_after timestamptz not null` (last_user_message_at + inactivity interval)
- `last_nudge_at timestamptz` (when this worker last sent a follow-up)
- `last_nudge_event_id text` (idempotency anchor for the nudge)
- `attempts int`, `last_error text`
- `claimed_by text`, `claimed_at timestamptz`, `updated_at timestamptz`

### 2.2 RPCs (mirror summary worker pattern)
Implement RPCs similar to those in `support/scripts/group_chat_summary_v1.sql`:
- `schedule_group_chat_followup_job_v1(...)`  
  - Called on inbound **user** messages (same place the summary job is scheduled).
  - Updates `last_user_message_at`, `last_user_event_id`, and `run_after`.
  - Resets `attempts` and clears backoff for new activity.
- `claim_group_chat_followup_jobs_v1(worker_id, max_jobs, stale_after)`  
  - Uses `FOR UPDATE SKIP LOCKED` to be multi-instance safe.
- `complete_group_chat_followup_job_v1(...)`  
  - Marks `done`, sets `last_nudge_at`, `last_nudge_event_id`, and clears claim fields.
  - If `last_user_event_id` changed while running, release back to `queued`.
- `fail_group_chat_followup_job_v1(...)`  
  - Records error, applies backoff (exponential), and releases the claim.

### 2.3 Why a dedicated job table?
- Avoids full-table scans of `group_chat_raw_memory_v1` every 10 seconds.
- Centralizes idempotency (one nudge per last user event).
- Matches the proven pattern used by `group_chat_summary_jobs`.

## 3) Worker algorithm (step-by-step)

Use `app/groupchat/summary/worker.py` as the reference structure.

### 3.1 Claim loop
- Poll every `groupchat_followup_poll_seconds` (default 10).
- Claim due jobs via `claim_group_chat_followup_jobs_v1`.

### 3.2 Per-job checks (defensive)
For each claimed job:
1) **Verify chat exists and is managed**  
   - `app/database/client/group_chat.py` -> `get_group_chat_by_guid`.
2) **Check user modes**  
   - If either user is `muted`, skip (consistent with `app/groupchat/features/opinion.py`).
   - Decide whether `quiet` should also suppress (open question).
3) **Confirm inactivity against raw transcript**  
   - Use `get_group_chat_raw_messages_window_v1(chat_guid, limit=50)` and find the most recent `role='user'` `sent_at`.
   - If any user message is newer than `last_user_message_at` (job anchor), release the job and let it reschedule.

### 3.3 Build follow-up context (summary-first)
Primary source:
- Fetch recent segments from `group_chat_summary_segments_v1` (e.g., last 3–6).
Secondary fallback:
- If summary is empty, optionally use the raw transcript tail for light grounding or skip sending (safer).

### 3.4 LLM prompt rules
Follow the tone and safety style in `app/groupchat/features/opinion.py`:
- Address **both** participants.
- Keep it short, low-pressure, and practical (no creepy monitoring language).
- Reference a **specific topic** from summary segments (or a safe fallback topic if summaries are absent).
- Suggest a small, easy next step (e.g., quick question, tiny plan, or a 10-minute check-in agenda).

### 3.5 Send + record
- Send via `GroupChatSender.send_and_record(...)` with a distinct `msg_type` (e.g., `relationship_followup_nudge_v1`).
- Capture Photon `message_id` (if available) and store as `last_nudge_event_id`.

### 3.6 Mark completion
- Call `complete_group_chat_followup_job_v1(...)` with `expected_last_user_event_id` to avoid overwriting newer activity.
- If sending fails, call `fail_group_chat_followup_job_v1(...)` with backoff.

## 4) Configuration (single place to adjust)

Add new settings to `app/config.py`:
- `groupchat_followup_enabled: bool = True`
- `groupchat_followup_inactivity_hours: int = 24`  
  - This is the **single source** for the inactivity window (1 day default).
- `groupchat_followup_worker_max_jobs: int = 5`
- `groupchat_followup_worker_stale_minutes: int = 20`
- `groupchat_followup_poll_seconds: int = 10`
- `groupchat_followup_model: str = "gpt-4o-mini"` (optional)

## 5) Deployment plan (Docker + ECS)

### 5.1 Docker (supervisor)
Add a new supervisor program in `infrastructure/supervisor/supervisord.conf`:
- `command=python -m app.groupchat.followup.worker --loop --interval-seconds=%(ENV_GROUPCHAT_FOLLOWUP_POLL_SECONDS)s`
- Provide default `GROUPCHAT_FOLLOWUP_POLL_SECONDS=10` and log level env.

### 5.2 Docker Compose
Add a new service in `docker-compose.yml` mirroring `groupchat-summary-worker`:
- `groupchat-followup-worker` with the same image, env file, and loop command.

### 5.3 AWS ECS
Add a new container definition to `infrastructure/aws/ecs/taskdef-testing.json`:
- Name: `groupchat-followup-worker`
- Command: `python -m app.groupchat.followup.worker --loop --interval-seconds __GROUPCHAT_FOLLOWUP_WORKER_POLL_SECONDS__`
- Log stream prefix: `worker` or `followup-worker`
- Add the new placeholder to deployment scripts and docs (pattern in `infrastructure/aws/ecs/README.md`).

## 6) Observability + safeguards

- Log with structured tags: `chat_guid`, `last_user_message_at`, `run_after`, `nudge_event_id`.
- Include a guardrail to prevent duplicate sends:
  - If `last_nudge_at` >= `last_user_message_at`, skip.
  - If `last_nudge_event_id` already set for the same anchor, skip.
- Cap messages per chat (optional) with a rolling limit (e.g., max 1 nudge per 7 days).

## 7) Rollout checklist

1) Add DB schema + RPCs (new `group_chat_followup_jobs_v1` and functions).
2) Add DB client methods under `app/database/client/` (mirroring summary mixin).
3) Implement worker module under `app/groupchat/followup/worker.py` (pattern from `app/groupchat/summary/worker.py`).
4) Add LLM prompt template under `app/groupchat/followup/prompts.py`.
5) Wire scheduling into inbound user message path (same location as summary job scheduling in `app/groupchat/io/recorder.py`).
6) Update Docker + ECS configs.
7) Validate in staging with a short inactivity window (e.g., 2 minutes).

## 8) Open questions (confirm before implementation)

1) Should `quiet` mode suppress the proactive follow-up, or only `muted`?
2) If summary segments are missing (new chat), do we skip the nudge or use a generic prompt?
3) Do you want a hard cap (e.g., max 1 follow-up per 7 days) even if inactivity persists?
