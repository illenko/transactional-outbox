-- +goose Up
CREATE INDEX IF NOT EXISTS idx_outbox_messages_unprocessed
    ON outbox_messages (created_at, processed_at)
    INCLUDE (id, entity_id)
    WHERE processed_at IS NULL;