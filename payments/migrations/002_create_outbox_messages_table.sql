-- +goose Up
CREATE TABLE outbox_messages
(
    id           UUID PRIMARY KEY,
    entity_id    INT       NOT NULL,
    payload      JSONB     NOT NULL,
    created_at   TIMESTAMP NOT NULL,
    processed_at TIMESTAMP NULL,
    error        BOOLEAN DEFAULT FALSE
);