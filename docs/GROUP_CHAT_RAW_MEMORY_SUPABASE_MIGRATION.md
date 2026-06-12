# Switch Group Chat Raw Memory from Zep → Supabase (One Row per Chat)

This document is a **design + migration plan** to remove **Zep** as the storage for *raw group chat transcripts* and make **Supabase** the source of truth.

You asked for:
- **No Zep for raw memory storage** (Zep should be graph memory only).
- **One chat = one row** for raw message storage (easy table management).
- A robust plan that explicitly lists **which files must change** to stop relying on Zep raw history.
- **No code changes yet** (plan only).

---

## 0) Why we must switch (observed failure mode)

We already hit the concrete failure:
- Worker claims a job, but fails with `anchor_not_found_in_zep_window`.
- Root issue: the job anchor (`group_chat_summary_jobs.last_user_event_id`) is not guaranteed to exist in Zep’s returned message window (Zep can return 0 messages even when we believe we wrote them, and/or the transcript window is inconsistent).

For a background summarization system, the transcript store must be:
- Durable (survives restarts)
- Deterministic to query (window queries always see what was written)
- Under our control (schema + retention + backfill)

Supabase (Postgres) is the correct “raw transcript source of truth”.

---

## 1) Goals / Non-goals

### 1.1 Goals
- Store group chat raw messages in **Supabase**, with **one row per `chat_guid`**.
- Guarantee the summary worker can always find the anchor message (no more Zep-window surprises).
- Allow efficient space usage by keeping **only the unsummarized (or recent) tail** in the raw row.
- Keep Zep (optional) for **graph memory only**, fed from summary segments (not from raw transcript).

### 1.2 Non-goals (for this migration plan)
- Migrating **DM** memory usage from Zep (there are other code paths using Zep for non-groupchat graphs).
- Implementing the graph-memory extractor/outbox in the same PR (we’ll specify it, but it can be a second phase).

---

## 2) Target architecture (high level)

### 2.1 Raw transcript (authoritative)
- **Supabase / Postgres**: `group_chat_raw_memory_v1` (one row per `chat_guid`).
- Writes happen on every inbound/outbound group chat message via a Postgres RPC:
  - atomic append
  - idempotent by `event_id`
  - optional trimming/pruning to cap size

### 2.2 Summaries (durable research memory)
- Keep the existing approach: `group_chat_summary_memory_v1` stores segment summaries in arrays (one row per chat).
- Worker reads raw transcript from Supabase, summarizes, appends summary segment, then prunes raw transcript.

### 2.3 Zep (non-authoritative, optional)
- Zep is not used for “raw transcript history”.
- Zep is only used for **graph memory**, updated from:
  - segment summaries stored in Supabase (preferred), or
  - derived facts/triples (outbox pattern), never blocking correctness.

---

## 3) Canonical event id (must be stable)

Everything (raw transcript append, job scheduling anchor, summarization window anchoring) must share the same stable `event_id`.

Rules:
1) If Photon provides `message_id/guid`, use it as `event_id`.
2) If Photon does NOT provide a guid, use a **stable hash** derived from fields that do not change across re-delivery:
   - `chat_guid`
   - `from_number`
   - `content`
   - `media_url`
   - **Do not include timestamp** if it’s set at receipt time (it breaks stability).

This prevents duplicate processing, prevents “anchor mismatch”, and makes the summary anchor reliable.

---

## 4) Supabase schema (one row per chat, no jsonb required)

### 4.0 Where to store the one-row transcript

You have two valid “one chat = one row” placement options:

**Option A (allowed): add columns directly on `group_chats`**
- Pros: simplest mental model (everything on the chat row).
- Cons: bloats `group_chats` (row becomes “hot” and large), mixes concerns (chat config vs transcript), harder to evolve/roll back.

**Option B (recommended): a dedicated one-row table keyed by `chat_guid`**
- Pros: keeps `group_chats` clean, isolates storage/retention logic, easier to migrate/rollback, still one row per chat.
- Cons: one extra table to manage.

This plan uses **Option B**.

### 4.1 Table: `group_chat_raw_memory_v1`

Purpose:
- Holds a **bounded** raw message tail for a chat (ideally “unsummarized tail”).
- One row per chat, append-only semantics, with pruning after summary segments are stored.

Recommended DDL (conceptual):

```sql
create table if not exists group_chat_raw_memory_v1 (
  chat_guid text primary key references group_chats(chat_guid) on delete cascade,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),

  -- Optional watermark to support pruning / sanity checks
  last_pruned_before timestamptz,
  last_event_at timestamptz,

  -- Parallel arrays (same length; index i describes one message)
  event_id text[] not null default '{}'::text[],
  message_id text[] not null default '{}'::text[],          -- Photon guid when present; else empty string
  role text[] not null default '{}'::text[],                -- 'user' | 'assistant'
  sender_user_id uuid[] not null default '{}'::uuid[],      -- null allowed for assistant messages
  sender_handle text[] not null default '{}'::text[],       -- phone/email string; empty allowed
  sent_at timestamptz[] not null default '{}'::timestamptz[],
  content text[] not null default '{}'::text[],
  media_url text[] not null default '{}'::text[],

  constraint group_chat_raw_memory_v1_len_check check (
    coalesce(array_length(event_id, 1), 0) = coalesce(array_length(role, 1), 0)
    and coalesce(array_length(event_id, 1), 0) = coalesce(array_length(sent_at, 1), 0)
    and coalesce(array_length(event_id, 1), 0) = coalesce(array_length(content, 1), 0)
    and coalesce(array_length(event_id, 1), 0) = coalesce(array_length(message_id, 1), 0)
    and coalesce(array_length(event_id, 1), 0) = coalesce(array_length(sender_user_id, 1), 0)
    and coalesce(array_length(event_id, 1), 0) = coalesce(array_length(sender_handle, 1), 0)
    and coalesce(array_length(event_id, 1), 0) = coalesce(array_length(media_url, 1), 0)
  )
);

create index if not exists group_chat_raw_memory_v1_last_event_at_idx
  on group_chat_raw_memory_v1 (last_event_at);
```

Why parallel arrays:
- Meets “one chat = one row”.
- Avoids jsonb.
- TOAST compresses large arrays automatically.
- Supports simple idempotency check: `p_event_id = any(event_id)`.

Tradeoffs (explicit):
- You lose “SQL-native analytics” over individual messages unless you add a view that unnests arrays.
- You must keep the row bounded (prune/trim), or it will grow without limit.

### 4.2 Debug view (optional but recommended)

This view makes the arrays readable as rows for debugging:

```sql
create or replace view group_chat_raw_messages_expanded_v1 as
select
  r.chat_guid,
  u.msg_index,
  u.event_id,
  u.message_id,
  u.role,
  u.sender_user_id,
  u.sender_handle,
  u.sent_at,
  u.content,
  u.media_url
from group_chat_raw_memory_v1 r
cross join lateral unnest(
  r.event_id,
  r.message_id,
  r.role,
  r.sender_user_id,
  r.sender_handle,
  r.sent_at,
  r.content,
  r.media_url
) with ordinality as u(
  event_id,
  message_id,
  role,
  sender_user_id,
  sender_handle,
  sent_at,
  content,
  media_url,
  msg_index
);
```

---

## 5) RPCs (atomic + idempotent + bounded storage)

### 5.1 `append_group_chat_raw_message_v1(...)`

Requirements:
- **Idempotent**: if `p_event_id` already exists, do nothing.
- **Atomic append**: append aligned values to all arrays.
- **Bounded**: keep only last `p_keep_last_n` messages (or keep “unsummarized tail” only).

Signature (recommended):

```sql
create or replace function append_group_chat_raw_message_v1(
  p_chat_guid text,
  p_event_id text,
  p_message_id text,
  p_role text,
  p_sender_user_id uuid,
  p_sender_handle text,
  p_sent_at timestamptz,
  p_content text,
  p_media_url text,
  p_keep_last_n int default 800
)
returns group_chat_raw_memory_v1
language plpgsql
as $$
declare
  out_row group_chat_raw_memory_v1;
  n int;
begin
  insert into group_chat_raw_memory_v1 (
    chat_guid,
    updated_at,
    last_event_at,
    event_id,
    message_id,
    role,
    sender_user_id,
    sender_handle,
    sent_at,
    content,
    media_url
  ) values (
    p_chat_guid,
    now(),
    p_sent_at,
    array[p_event_id],
    array[coalesce(p_message_id,'')],
    array[p_role],
    array[p_sender_user_id],
    array[coalesce(p_sender_handle,'')],
    array[p_sent_at],
    array[coalesce(p_content,'')],
    array[coalesce(p_media_url,'')]
  )
  on conflict (chat_guid) do update
  set
    updated_at = now(),
    last_event_at = greatest(group_chat_raw_memory_v1.last_event_at, p_sent_at),

    event_id = case
      when p_event_id = any(group_chat_raw_memory_v1.event_id) then group_chat_raw_memory_v1.event_id
      else array_append(group_chat_raw_memory_v1.event_id, p_event_id)
    end,
    message_id = case
      when p_event_id = any(group_chat_raw_memory_v1.event_id) then group_chat_raw_memory_v1.message_id
      else array_append(group_chat_raw_memory_v1.message_id, coalesce(p_message_id,''))
    end,
    role = case
      when p_event_id = any(group_chat_raw_memory_v1.event_id) then group_chat_raw_memory_v1.role
      else array_append(group_chat_raw_memory_v1.role, p_role)
    end,
    sender_user_id = case
      when p_event_id = any(group_chat_raw_memory_v1.event_id) then group_chat_raw_memory_v1.sender_user_id
      else array_append(group_chat_raw_memory_v1.sender_user_id, p_sender_user_id)
    end,
    sender_handle = case
      when p_event_id = any(group_chat_raw_memory_v1.event_id) then group_chat_raw_memory_v1.sender_handle
      else array_append(group_chat_raw_memory_v1.sender_handle, coalesce(p_sender_handle,''))
    end,
    sent_at = case
      when p_event_id = any(group_chat_raw_memory_v1.event_id) then group_chat_raw_memory_v1.sent_at
      else array_append(group_chat_raw_memory_v1.sent_at, p_sent_at)
    end,
    content = case
      when p_event_id = any(group_chat_raw_memory_v1.event_id) then group_chat_raw_memory_v1.content
      else array_append(group_chat_raw_memory_v1.content, coalesce(p_content,''))
    end,
    media_url = case
      when p_event_id = any(group_chat_raw_memory_v1.event_id) then group_chat_raw_memory_v1.media_url
      else array_append(group_chat_raw_memory_v1.media_url, coalesce(p_media_url,''))
    end
  returning * into out_row;

  -- Optional bounding: trim to keep last N.
  n := coalesce(array_length(out_row.event_id, 1), 0);
  if p_keep_last_n is not null and p_keep_last_n > 0 and n > p_keep_last_n then
    update group_chat_raw_memory_v1 r
    set
      event_id = r.event_id[(n - p_keep_last_n + 1):n],
      message_id = r.message_id[(n - p_keep_last_n + 1):n],
      role = r.role[(n - p_keep_last_n + 1):n],
      sender_user_id = r.sender_user_id[(n - p_keep_last_n + 1):n],
      sender_handle = r.sender_handle[(n - p_keep_last_n + 1):n],
      sent_at = r.sent_at[(n - p_keep_last_n + 1):n],
      content = r.content[(n - p_keep_last_n + 1):n],
      media_url = r.media_url[(n - p_keep_last_n + 1):n],
      updated_at = now()
    where r.chat_guid = p_chat_guid
    returning * into out_row;
  end if;

  return out_row;
end;
$$;
```

### 5.2 `prune_group_chat_raw_memory_before_v1(...)` (recommended)

Goal:
- After we store a summary segment ending at `segment_end_at`, we can drop any raw messages strictly before that end boundary (optionally keep a small overlap tail).

This keeps the one-row transcript bounded and prevents unbounded growth.

Signature (recommended):

```sql
create or replace function prune_group_chat_raw_memory_before_v1(
  p_chat_guid text,
  p_before timestamptz,
  p_keep_tail int default 40
)
returns group_chat_raw_memory_v1
language plpgsql
as $$
declare
  out_row group_chat_raw_memory_v1;
  keep_from_idx int;
  n int;
begin
  -- Find the first index whose sent_at >= p_before.
  select min(u.idx) into keep_from_idx
  from group_chat_raw_memory_v1 r
  cross join lateral unnest(r.sent_at) with ordinality as u(ts, idx)
  where r.chat_guid = p_chat_guid and u.ts >= p_before;

  if keep_from_idx is null then
    -- Everything is older than p_before; keep only the last p_keep_tail messages.
    select array_length(r.event_id, 1) into n from group_chat_raw_memory_v1 r where r.chat_guid = p_chat_guid;
    keep_from_idx := greatest(1, n - p_keep_tail + 1);
  else
    -- Keep a tail overlap by moving start backward.
    keep_from_idx := greatest(1, keep_from_idx - p_keep_tail);
  end if;

  update group_chat_raw_memory_v1 r
  set
    event_id = r.event_id[keep_from_idx:],
    message_id = r.message_id[keep_from_idx:],
    role = r.role[keep_from_idx:],
    sender_user_id = r.sender_user_id[keep_from_idx:],
    sender_handle = r.sender_handle[keep_from_idx:],
    sent_at = r.sent_at[keep_from_idx:],
    content = r.content[keep_from_idx:],
    media_url = r.media_url[keep_from_idx:],
    last_pruned_before = p_before,
    updated_at = now()
  where r.chat_guid = p_chat_guid
  returning * into out_row;

  return out_row;
end;
$$;
```

### 5.3 Strongly recommended invariant: “ingest + schedule” in one RPC

To eliminate “job exists but anchor message is missing” entirely, create:
- `ingest_group_chat_user_message_and_schedule_summary_v1(...)`

It should:
1) Append the inbound user message to `group_chat_raw_memory_v1`.
2) Schedule/upsert the job row in `group_chat_summary_jobs` (your existing function).

Both occur in a single Postgres transaction.

This is the most robust fix for the entire anchor class of bugs.

---

## 6) Code changes required (explicit file list)

This section lists **every groupchat file that currently relies on Zep raw memory** and what must change to switch to Supabase.

### 6.1 Replace “GroupChatMemoryService (Zep transcript)” with “SupabaseRawTranscriptService”

**New module(s) (recommended):**
- `app/groupchat/memory/raw_supabase.py` (raw transcript read/write)
- `app/groupchat/memory/graph_zep.py` (optional graph memory only)
- Keep `app/groupchat/memory/__init__.py` exporting the correct service(s)

**Files to update:**
- `app/groupchat/memory/zep.py`
  - Remove/stop using: `add_user_message`, `add_assistant_message`, `get_recent_messages`, `get_session_summary` for raw transcript.
  - Keep only graph-related utilities (if you still want Zep graph in groupchat), or deprecate the module entirely.

### 6.2 Write-path changes (stop writing raw messages to Zep)

- `app/groupchat/io/recorder.py`
  - Replace Zep writes with Supabase RPC call(s) to `append_group_chat_raw_message_v1`.
  - For inbound user messages: ideally call `ingest_group_chat_user_message_and_schedule_summary_v1` (single RPC).
  - Keep Redis idempotency as a fast-path if desired, but **DB idempotency must be the truth**.

- `app/groupchat/io/sender.py`
  - Outbound assistant messages should also append into Supabase raw transcript (so summaries include assistant context).

- `app/groupchat/runtime/router.py`
  - Ensure canonical `event_id` is stable (see Section 3).
  - Scheduling should be moved to the ingest RPC (preferred) or must happen after the transcript append.

### 6.3 Read-path changes (stop reading “recent messages” from Zep)

- `app/groupchat/features/opinion.py`
  - Replace calls that read “recent messages” / “recent user messages” from Zep with Supabase transcript reads.
  - Replace any “recover participant IDs from Zep metadata” with `group_chats` (Supabase) as truth.

- `app/groupchat/features/provisioning.py`
  - Today it writes groupchat context into Zep metadata.
  - If Zep is graph-only, store this state in Supabase instead:
    - Either add explicit columns on `group_chats` (preferred for small structured fields)
    - Or create `group_chat_state_v1` (one row per chat) for icebreaker/provisioning state (no raw transcript)

### 6.4 Summarization worker (stop depending on Zep transcript windows)

- `app/groupchat/summary/worker.py`
  - Replace `_fetch_recent_messages()` implementation to read from Supabase raw transcript (`group_chat_raw_memory_v1`).
  - Anchor check becomes: ensure `expected_event_id` exists in the raw arrays and is within the segment window (by `sent_at`).
  - After storing a summary segment, call `prune_group_chat_raw_memory_before_v1(chat_guid, segment_end_at)` to bound storage.
  - Remove “watermark Zep” step; instead watermark in Supabase:
    - `group_chat_summary_memory_v1.last_segment_end_at` already serves as a canonical watermark.
    - Optionally update `group_chat_raw_memory_v1.last_pruned_before`.

### 6.5 Documentation cleanups (after code cutover)

- `support/docs/GROUP_CHAT_SUMMARY_PLAN.md`
  - Update baseline: Zep is no longer the live transcript store.
- `app/groupchat/__init__.py`
  - Update wording: “raw history recording (Zep)” → “raw history recording (Supabase)”.

---

## 7) Data retention & space efficiency (critical for one-row design)

Storing every message ever in one row is not sustainable long-term.

Recommended policy:
- `group_chat_raw_memory_v1` stores only the **unsummarized tail** (plus a small overlap).
- Long-term memory lives in:
  - `group_chat_summary_memory_v1` (segment summaries)
  - optionally a separate analytics/archive system if needed later

Practical defaults:
- `append_group_chat_raw_message_v1(..., p_keep_last_n=800)` to cap raw row size.
- `prune_group_chat_raw_memory_before_v1(..., p_keep_tail=40)` after each summary segment.

Result:
- Space stays bounded per chat.
- Summary worker always has enough context.
- You still have durable “memory” in summary segments without storing the full raw transcript forever.

If you truly need full raw retention for compliance/auditing, do **not** use one-row arrays; use an append-only message log table (row-per-message) and optionally maintain a one-row “tail cache”. (We can plan that variant if needed.)

---

## 8) Migration rollout (safe, reversible)

### Phase A: DB-first (no behavior change)
1) Add table `group_chat_raw_memory_v1`.
2) Add RPCs:
   - `append_group_chat_raw_message_v1`
   - `prune_group_chat_raw_memory_before_v1`
   - (recommended) `ingest_group_chat_user_message_and_schedule_summary_v1`
3) Add the debug view `group_chat_raw_messages_expanded_v1`.

### Phase B: Dual-write (temporary)
1) On inbound/outbound groupchat messages: write to **Supabase raw memory** and continue writing to Zep temporarily.
2) Validate the Supabase row stays consistent (array lengths match, event_ids are unique, timestamps parse).

### Phase C: Switch worker read path
1) Make summary worker read transcript from Supabase raw memory.
2) Keep Zep reads as fallback for one release only (optional).
3) Enable pruning after successful summary.

### Phase D: Switch runtime features read path
1) Update `opinion.py` and other handlers to read recent context from Supabase raw memory.
2) Move provisioning/icebreaker state off Zep metadata into Supabase.

### Phase E: Remove Zep raw transcript usage
1) Stop writing raw groupchat messages to Zep.
2) Keep Zep only for graph memory fed from summary segments (outbox pattern if needed).

Rollback plan:
- Because dual-write exists in Phase B/C, you can temporarily revert reads back to Zep if needed (until Phase E).

---

## 9) Validation checklist (what “done” means)

### Runtime
- On each inbound groupchat user message:
  - Supabase raw row contains the new `event_id` (idempotent).
  - Job row is scheduled with `last_user_event_id = event_id` and correct `run_after`.

### Worker
- When `run_after <= now()`:
  - Worker claims the job.
  - Anchor is found in Supabase raw memory window.
  - Summary segment is appended to `group_chat_summary_memory_v1`.
  - Raw memory is pruned (bounded arrays).

### Zep independence
- With `ZEP_ENABLED=false`, groupchat still:
  - records messages
  - generates summaries
  - produces opinion replies using Supabase transcript context

---

## 10) Open questions (to confirm before implementation)

1) Raw retention requirement:
   - Is it acceptable that `group_chat_raw_memory_v1` only stores an **unsummarized tail** (recommended), not full history forever?
2) Provisioning state:
   - Do you want a `group_chat_state_v1` table, or add explicit columns to `group_chats` for icebreaker/provisioning fields?
3) Zep graph memory:
   - Do you want to push **summary segments** to Zep and let Zep derive facts, or do we extract facts ourselves and push triples?
