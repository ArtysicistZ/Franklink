create table if not exists user_email_highlights (
    id uuid primary key default gen_random_uuid(),
    user_id uuid not null references users(id) on delete cascade,
    message_id text not null,
    direction text not null check (direction in ('inbound', 'outbound')),
    is_from_me boolean not null default false,
    sender text,
    sender_domain text,
    subject text,
    body_excerpt text,
    received_at timestamptz,
    fetched_at timestamptz,
    created_at timestamptz not null default now(),
    zep_synced_at timestamptz
);

create unique index if not exists user_email_highlights_user_message_idx
    on user_email_highlights (user_id, message_id);

create index if not exists user_email_highlights_user_created_idx
    on user_email_highlights (user_id, created_at desc);
