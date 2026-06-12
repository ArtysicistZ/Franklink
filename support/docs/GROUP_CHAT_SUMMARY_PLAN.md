# Group Chat Summarization + Supabase Storage Plan (v1)

This plan adds a durable, multi-instance safe "conversation segment summarizer" for Frank-managed group chats.

Core idea:
- **Zep** remains the live transcript + runtime metadata store.
- **Supabase** becomes the durable research/memory store for summaries.
- A **Supabase-backed job table** replaces Celery (reliable on EC2).
- Summary memory is stored as **typed Postgres arrays** in **one row per `chat_guid`** (no `jsonb` timeline).

---

## 1) Current baseline (what exists today)

### 1.1 Where data lives
- **Supabase**: `group_chats` exists (chat GUID + participants + modes + connection_request_id). There is **no** table that stores group chat message history today.
- **Zep**: stores **raw group chat messages** (user + assistant) plus **session metadata** (participants, icebreaker context, follow-up state).
  - Group chat sessions use `session_id = groupchat_<sanitized_chat_guid>` in `app/groupchat/memory/zep.py`.

### 1.2 Code paths that write/read Zep
- Inbound recording: `app/groupchat/runtime/router.py` -> `app/groupchat/io/recorder.py` -> `app/groupchat/memory/zep.py`
- Outbound recording: `app/groupchat/io/sender.py`
- Feature reads:
  - `app/groupchat/features/opinion.py` reads recent Zep messages and Zep session metadata (`icebreaker_v1`, participants).
  - `app/groupchat/features/provisioning.py` writes participants + icebreaker context into Zep metadata.

---

## 2) Feature goal / requirements

When a "conversation segment" ends (defined as **no user messages for 5 minutes**), generate a short research-oriented summary and persist it to Supabase.

Non-negotiables:
- **Durable across restarts** (no in-memory timers).
- **Multi-instance safe** (multiple API/worker processes).
- **Idempotent** (no duplicate segment summaries).
- **Failure tolerant** (LLM/Zep failures should not break group chat runtime).

---

## 3) Segment definition (correctness)

### 3.1 What counts as activity
- Only **user messages** extend the segment.
- Assistant messages are included in the transcript we summarize, but do not "reset" the 5-minute timer.

### 3.2 Boundaries
Let `inactivity_window = 5 minutes`.

On each inbound user message at time `t_user`, schedule:
- `run_after = t_user + inactivity_window`

A segment uses the transcript window:
- `segment_start_at = last_segment_end_at` from Supabase summary memory for this `chat_guid` (or `NULL` for the first segment)
- `segment_end_at = run_after` (the close boundary)

We summarize messages where:
- `message_timestamp >= segment_start_at` (if `segment_start_at` is present)
- `message_timestamp < segment_end_at`

Why `segment_end_at = run_after` (and not `t_user`):
- It includes assistant replies that happen inside the inactivity window.
- The next segment starts exactly at the previous close boundary, preventing "overlap".

### 3.3 Idempotency anchor
The idempotency key for a segment is:
- `last_user_event_id`: the stable ID of the last user message that produced this `run_after`.

In today's recorder (`app/groupchat/io/recorder.py`), inbound messages include a stable `metadata.event_id` (prefers Photon `message_id` when present).

---

## 4) Supabase storage design (one row per chat)

We store summaries as typed arrays inside one row per chat to match your requirement ("one block per chat", no `jsonb` timeline).

### 4.1 Table: `group_chat_summary_memory_v1` (one row per chat)
- Purpose: durable "summary timeline" per `chat_guid`
- Storage: Postgres arrays (`timestamptz[]`, `text[]`) which TOAST-compress automatically

Minimal schema (recommended):
```sql
create table if not exists group_chat_summary_memory_v1 (
  chat_guid text primary key,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  -- Cached pointer to avoid scanning arrays when computing the next segment start.
  last_segment_end_at timestamptz,

  -- Per-segment arrays (must stay aligned by index).
  segment_end_at timestamptz[] not null default '{}'::timestamptz[],
  last_user_message_at timestamptz[] not null default '{}'::timestamptz[],
  last_user_event_id text[] not null default '{}'::text[],
  summary_md text[] not null default '{}'::text[],

  constraint group_chat_summary_memory_v1_len_check check (
    coalesce(array_length(segment_end_at, 1), 0) = coalesce(array_length(last_user_message_at, 1), 0)
    and coalesce(array_length(segment_end_at, 1), 0) = coalesce(array_length(last_user_event_id, 1), 0)
    and coalesce(array_length(segment_end_at, 1), 0) = coalesce(array_length(summary_md, 1), 0)
  )
);

create index if not exists group_chat_summary_memory_v1_last_segment_end_at_idx
  on group_chat_summary_memory_v1 (last_segment_end_at);
```

Notes on efficiency:
- Arrays avoid repeated JSON keys and JSON parsing.
- Summary timelines are append-only and small; "one row per chat" stays manageable (TOAST handles larger values).

### 4.2 View (optional): query segments as rows
This makes it easy to debug and to fetch "summary around time T" without changing storage:

```sql
create or replace view group_chat_summary_segments_v1 as
select
  m.chat_guid,
  u.segment_index,
  u.segment_end_at,
  u.last_user_message_at,
  u.last_user_event_id,
  u.summary_md
from group_chat_summary_memory_v1 m
cross join lateral unnest(
  m.segment_end_at,
  m.last_user_message_at,
  m.last_user_event_id,
  m.summary_md
) with ordinality as u(
  segment_end_at,
  last_user_message_at,
  last_user_event_id,
  summary_md,
  segment_index
);
```

Example query ("first segment ending after T"):
```sql
select *
from group_chat_summary_segments_v1
where chat_guid = :chat_guid and segment_end_at >= :t
order by segment_end_at asc
limit 1;
```

---

## 5) Durable scheduling (replace Celery with Supabase + systemd)

Celery is not required. We use a Postgres-backed job row per chat and a small worker process that can run on EC2 with systemd.

### 5.1 Table: `group_chat_summary_jobs` (debounced "next run")
```sql
create table if not exists group_chat_summary_jobs (
  chat_guid text primary key,
  status text not null check (status in ('queued', 'running', 'done', 'failed')),

  -- The last user message we've observed (the segment anchor).
  last_user_message_at timestamptz not null,
  last_user_event_id text not null,

  -- When it is safe to close/summarize the segment.
  run_after timestamptz not null,

  attempts int not null default 0,
  last_error text,

  claimed_by text,
  claimed_at timestamptz,
  updated_at timestamptz not null default now()
);

create index if not exists group_chat_summary_jobs_due_idx
  on group_chat_summary_jobs (status, run_after);
```

Design intent:
- One row per chat means "debounce" is cheap: each new user message just pushes `run_after` forward.
- Multi-worker safety comes from atomic claiming (`FOR UPDATE SKIP LOCKED`).
- The `done/failed` states prevent re-processing when there is no pending segment.

### 5.2 RPC: schedule on inbound user message (atomic upsert)
Call this from the group chat inbound path (after recording the message).

Key properties:
- out-of-order safe: only advances the "last user message" anchor
- debounced: pushes `run_after` forward
- preserves `running` so a second worker can't claim concurrently

```sql
create or replace function schedule_group_chat_summary_job_v1(
  p_chat_guid text,
  p_last_user_message_at timestamptz,
  p_last_user_event_id text,
  p_inactivity_window interval default '5 minutes'
)
returns group_chat_summary_jobs
language plpgsql
as $$
declare
  out_row group_chat_summary_jobs;
begin
  insert into group_chat_summary_jobs (
    chat_guid,
    status,
    last_user_message_at,
    last_user_event_id,
    run_after,
    attempts,
    last_error,
    updated_at
  ) values (
    p_chat_guid,
    'queued',
    p_last_user_message_at,
    p_last_user_event_id,
    p_last_user_message_at + p_inactivity_window,
    0,
    null,
    now()
  )
  on conflict (chat_guid) do update
  set
    -- Out-of-order safety: only move the "last user" anchor forward.
    last_user_message_at = case
      when excluded.last_user_message_at >= group_chat_summary_jobs.last_user_message_at then excluded.last_user_message_at
      else group_chat_summary_jobs.last_user_message_at
    end,
    last_user_event_id = case
      when excluded.last_user_message_at >= group_chat_summary_jobs.last_user_message_at then excluded.last_user_event_id
      else group_chat_summary_jobs.last_user_event_id
    end,
    run_after = greatest(
      group_chat_summary_jobs.run_after,
      excluded.last_user_message_at + p_inactivity_window
    ),

    -- New user activity clears backoff/errors.
    attempts = case
      when excluded.last_user_message_at >= group_chat_summary_jobs.last_user_message_at then 0
      else group_chat_summary_jobs.attempts
    end,
    last_error = case
      when excluded.last_user_message_at >= group_chat_summary_jobs.last_user_message_at then null
      else group_chat_summary_jobs.last_error
    end,

    -- Preserve running so another worker can't claim concurrently.
    status = case when group_chat_summary_jobs.status = 'running' then 'running' else 'queued' end,
    updated_at = now()
  returning * into out_row;

  return out_row;
end;
$$;
```

### 5.3 RPC: claim due jobs (multi-instance safe, reclaim stale)
```sql
create or replace function claim_group_chat_summary_jobs_v1(
  p_worker_id text,
  p_max_jobs int default 5,
  p_stale_after interval default '20 minutes'
)
returns setof group_chat_summary_jobs
language plpgsql
as $$
begin
  return query
  with candidates as (
    select chat_guid
    from group_chat_summary_jobs
    where (
      (status = 'queued' and run_after <= now() and (claimed_at is null or claimed_at <= now() - p_stale_after))
      or (status = 'running' and claimed_at <= now() - p_stale_after)
    )
    order by run_after asc
    for update skip locked
    limit p_max_jobs
  )
  update group_chat_summary_jobs j
  set
    status = 'running',
    claimed_by = p_worker_id,
    claimed_at = now(),
    updated_at = now()
  from candidates c
  where j.chat_guid = c.chat_guid
  returning j.*;
end;
$$;
```

---

## 6) Atomic append (idempotent one-row memory writes)

All summary appends must happen through a single RPC so arrays stay aligned and concurrent workers can't lose updates.

```sql
create or replace function append_group_chat_summary_memory_segment_v1(
  p_chat_guid text,
  p_last_user_event_id text,
  p_last_user_message_at timestamptz,
  p_segment_end_at timestamptz,
  p_summary_md text
)
returns group_chat_summary_memory_v1
language plpgsql
as $$
declare
  out_row group_chat_summary_memory_v1;
begin
  insert into group_chat_summary_memory_v1 (
    chat_guid,
    updated_at,
    last_segment_end_at,
    segment_end_at,
    last_user_message_at,
    last_user_event_id,
    summary_md
  ) values (
    p_chat_guid,
    now(),
    p_segment_end_at,
    array[p_segment_end_at],
    array[p_last_user_message_at],
    array[p_last_user_event_id],
    array[p_summary_md]
  )
  on conflict (chat_guid) do update
  set
    updated_at = now(),

    -- Idempotency: if we've already stored this segment, no-op.
    segment_end_at = case
      when p_last_user_event_id = any(group_chat_summary_memory_v1.last_user_event_id)
        then group_chat_summary_memory_v1.segment_end_at
      else array_append(group_chat_summary_memory_v1.segment_end_at, p_segment_end_at)
    end,
    last_user_message_at = case
      when p_last_user_event_id = any(group_chat_summary_memory_v1.last_user_event_id)
        then group_chat_summary_memory_v1.last_user_message_at
      else array_append(group_chat_summary_memory_v1.last_user_message_at, p_last_user_message_at)
    end,
    last_user_event_id = case
      when p_last_user_event_id = any(group_chat_summary_memory_v1.last_user_event_id)
        then group_chat_summary_memory_v1.last_user_event_id
      else array_append(group_chat_summary_memory_v1.last_user_event_id, p_last_user_event_id)
    end,
    summary_md = case
      when p_last_user_event_id = any(group_chat_summary_memory_v1.last_user_event_id)
        then group_chat_summary_memory_v1.summary_md
      else array_append(group_chat_summary_memory_v1.summary_md, p_summary_md)
    end,

    last_segment_end_at = case
      when group_chat_summary_memory_v1.last_segment_end_at is null then p_segment_end_at
      else greatest(group_chat_summary_memory_v1.last_segment_end_at, p_segment_end_at)
    end
  returning * into out_row;

  return out_row;
end;
$$;
```

---

## 7) Worker algorithm (end-to-end)

The worker is a small Python process (no Celery) that:
1) claims due jobs
2) slices the transcript for that segment
3) generates a summary with `AzureOpenAIClient` (`gpt-4o-mini`)
4) appends the segment into `group_chat_summary_memory_v1`
5) finalizes the job safely

### 7.1 Summary output template (store as `summary_md`)
Require the model to output Markdown in a fixed template (so you avoid fragile JSON parsing):
- **Topics**
- **What each person said / key positions**
- **Agreements / disagreements**
- **Decisions**
- **Action items**
- **Open questions**
- **One-line summary**

Grounding rule: *do not add facts not present in the transcript; omit if uncertain*.

### 7.2 Per-chat processing steps
Given a claimed job snapshot `{chat_guid, last_user_message_at, last_user_event_id, run_after, claimed_at}`:

1) Decide boundaries
- `segment_start_at = group_chat_summary_memory_v1.last_segment_end_at` for this `chat_guid` (or `NULL` for first segment)
- `segment_end_at = run_after` (must equal `last_user_message_at + 5 minutes`)

2) Idempotency guard
- If `last_user_event_id` is already present in `group_chat_summary_memory_v1.last_user_event_id[]`, the segment is already stored; mark job `done` and exit.

3) Fetch transcript
- Use Zep raw messages (`/api/v2/sessions/{id}/messages`, limit=200 today).
- Filter locally to `[segment_start_at, segment_end_at)` using `metadata.timestamp` when available.
- Include both `role=user` and `role=assistant`.

Important constraint (current code): the Zep client only supports `limit<=200` and does not expose pagination/time slicing. If segments can exceed this, add the optional Supabase transcript mirror (`group_chat_messages`) and slice from SQL instead.

4) Summarize
- Build prompt: participants + transcript lines + segment window.
- Call `AzureOpenAIClient.generate_response(model="gpt-4o-mini", ...)`.
- Store the returned Markdown as the per-segment `summary_md` element.

5) Persist (atomic append)
- Call `append_group_chat_summary_memory_segment_v1(...)`.
- If the RPC no-ops due to the dedupe key, treat as success.

6) Watermark Zep (best-effort)
- Update Zep session metadata with a lightweight watermark (e.g., `summary_sync_v1`) containing:
  - `last_user_event_id`
  - `last_segment_end_at`
  - `synced_at`

7) Finalize the job safely (handle "new messages arrived while running")
- Re-read the current job row (or do a conditional update).
- If `last_user_event_id` is unchanged from the claimed snapshot:
  - mark job `done` (clear claim fields)
- If it changed:
  - set job back to `queued` (clear claim fields)
  - do **not** overwrite `last_user_message_at/run_after` (the router already advanced them)

Retries:
- On failure, increment `attempts`, set `last_error`, and push `run_after` forward with exponential backoff (cap it). Always use `greatest(run_after, now()+backoff)` so you don't overwrite a newer schedule.

---

## 8) EC2 deployment (robust, proven)

Two proven options on EC2:

### Option A (recommended): systemd timer + oneshot worker
- Pros: simple, self-healing, easy logs in journald, no "stuck loop"
- Run every 60s, claim jobs, exit

### Option B: long-running systemd service
- Pros: near real-time processing, fewer cold starts
- Cons: you must manage sleeps/backoff in code

Both options are multi-instance safe because job claiming is atomic.

---

## 9) Zep cleanup / space reduction (future, not in v1)

Your idea ("after saving to Supabase, delete from Zep") is not safe yet because current runtime features depend on Zep:
- `app/groupchat/features/opinion.py` reads recent Zep messages + session metadata.
- `app/groupchat/features/provisioning.py` writes long-lived metadata into Zep (`participants`, `icebreaker_v1`, etc).

Also, the current Zep client only supports deleting an entire session (`delete_session`), not deleting messages older than N days.

Sustainable path:
1) v1 (this plan): summarize + watermark, do not delete Zep
2) v2 (optional): add a minimal Supabase mirror:
   - `group_chat_messages` (row-per-message, with retention like 10 days)
   - `group_chat_runtime_state_v1` (copy of the Zep session metadata you still need)
3) v3: implement rolling compaction:
   - rebuild Zep session from the last 10 days of `group_chat_messages`
   - restore metadata from `group_chat_runtime_state_v1`
   - keep Supabase `group_chat_summary_memory_v1` as the long-term memory store

---

## 10) Implementation checklist (practical order)

1) Add Supabase tables + RPCs:
- Apply `support/scripts/group_chat_summary_v1.sql`
  - Tables: `group_chat_summary_memory_v1`, `group_chat_summary_jobs`
  - RPCs: `schedule_group_chat_summary_job_v1`, `claim_group_chat_summary_jobs_v1`, `append_group_chat_summary_memory_segment_v1`
  - RPCs: `complete_group_chat_summary_job_v1`, `fail_group_chat_summary_job_v1`

2) Add a small worker module (new file):
- claim jobs -> summarize -> append -> finalize
  - Worker entrypoint: `python -m app.groupchat.summary.worker --loop`

3) Wire scheduling into inbound group chat path:
- after `GroupChatRecorder.record_inbound(...)` for user messages
  - Scheduler: `app/groupchat/summary/scheduler.py`

4) Add observability:
- logs + "stuck job" queries
- dashboard: count queued jobs, max lag (`now() - run_after`), last_error frequency

5) Enable in environment:
- `GROUPCHAT_SUMMARY_ENABLED=true` (see `app/config.py`)

6) Decide if/when you want transcript mirroring (`group_chat_messages`) to enable true Zep compaction and to avoid Zep `limit=200` constraints.
