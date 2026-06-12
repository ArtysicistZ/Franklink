-- Discovery Conversations: agent-generated multi-party dialogue for match previews
-- Each row stores a conversation between matched users' AI agents that surfaces
-- why they should connect. Linked from iMessage via a capability URL (slug).

CREATE TABLE IF NOT EXISTS discovery_conversations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug TEXT UNIQUE NOT NULL,
    initiator_user_id UUID NOT NULL REFERENCES users(id),
    participant_user_ids UUID[] NOT NULL,
    connection_request_id UUID,
    connection_request_ids UUID[],
    turns JSONB NOT NULL DEFAULT '[]',
    teaser_summary TEXT NOT NULL,
    match_metadata JSONB DEFAULT '{}',
    quality_score FLOAT,
    flow_type TEXT NOT NULL DEFAULT 'reactive',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT valid_flow_type CHECK (flow_type IN ('reactive', 'proactive'))
);

CREATE INDEX idx_discovery_conversations_slug ON discovery_conversations(slug);
CREATE INDEX idx_discovery_conversations_initiator ON discovery_conversations(initiator_user_id);
