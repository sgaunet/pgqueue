-- migrate:up

-- Enable UUID extension for UUIDv7 generation
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Metadata table to track all queues (topics and channels)
CREATE TABLE IF NOT EXISTS pgqueue_metadata (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_type TEXT NOT NULL CHECK (queue_type IN ('pubsub', 'channel')),
    queue_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(queue_type, queue_name)
);

CREATE INDEX idx_pgqueue_metadata_type_name ON pgqueue_metadata(queue_type, queue_name);
CREATE INDEX idx_pgqueue_metadata_table_name ON pgqueue_metadata(table_name);

-- Subscribers table for pub/sub topics
CREATE TABLE IF NOT EXISTS pgqueue_subscribers (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_name TEXT NOT NULL,
    subscriber_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE(topic_name, subscriber_id)
);

CREATE INDEX idx_pgqueue_subscribers_topic ON pgqueue_subscribers(topic_name) WHERE active = TRUE;

-- Replay audit log
CREATE TABLE IF NOT EXISTS pgqueue_replay_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_type TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    replay_type TEXT NOT NULL CHECK (replay_type IN ('timestamp', 'message_id', 'dlq')),
    replay_params JSONB NOT NULL,
    message_count INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by TEXT
);

CREATE INDEX idx_pgqueue_replay_log_queue ON pgqueue_replay_log(queue_type, queue_name);
CREATE INDEX idx_pgqueue_replay_log_created_at ON pgqueue_replay_log(created_at);

-- migrate:down

DROP TABLE IF EXISTS pgqueue_replay_log;
DROP TABLE IF EXISTS pgqueue_subscribers;
DROP TABLE IF EXISTS pgqueue_metadata;
