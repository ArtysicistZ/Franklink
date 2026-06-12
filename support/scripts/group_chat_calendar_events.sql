-- Group Chat Calendar Events
-- Stores calendar events created for group chats (idempotency + audit)

create table if not exists group_chat_calendar_events (
    id uuid primary key default gen_random_uuid(),
    chat_guid text not null,
    organizer_user_id uuid not null references users(id) on delete cascade,
    event_id text,
    title text not null,
    start_time timestamptz not null,
    end_time timestamptz not null,
    timezone text not null,
    attendees text[] not null default array[]::text[],
    event_link text,
    request_hash text not null,
    status text not null default 'created'
        check (status in ('created', 'cancelled', 'failed')),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists group_chat_calendar_events_request_hash_idx
    on group_chat_calendar_events (request_hash);

create index if not exists group_chat_calendar_events_chat_guid_idx
    on group_chat_calendar_events (chat_guid, created_at desc);
