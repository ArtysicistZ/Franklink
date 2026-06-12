create table if not exists user_email_intent_events (
    id uuid primary key default gen_random_uuid(),
    user_id uuid not null references users(id) on delete cascade,
    event_key text not null,
    status text not null,
    intent_summary text not null,
    first_seen_at timestamptz,
    last_seen_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists user_email_intent_events_user_key_idx
    on user_email_intent_events (user_id, event_key);

create index if not exists user_email_intent_events_user_last_seen_idx
    on user_email_intent_events (user_id, last_seen_at desc);
