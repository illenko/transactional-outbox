-- +goose Up
CREATE TABLE outbox_messages
(
    id           UUID PRIMARY KEY,
    entity_id    INT          NOT NULL,
    content      JSONB        NOT NULL,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP    NULL,
    error        VARCHAR(256) NULL
);