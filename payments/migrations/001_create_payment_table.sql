-- +goose Up
CREATE TABLE payment
(
    id         SERIAL PRIMARY KEY,
    user_id    INT            NOT NULL,
    amount     DECIMAL(10, 2) NOT NULL,
    status     VARCHAR(50)    NOT NULL,
    created_at TIMESTAMP      NOT NULL,
    updated_at TIMESTAMP      NOT NULL
);