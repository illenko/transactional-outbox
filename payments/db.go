package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
)

func createPayment(ctx context.Context, dbpool *pgxpool.Pool, payment *Payment) error {
	now := time.Now()
	payment.CreatedAt = now
	payment.UpdatedAt = now

	tx, err := dbpool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	err = tx.QueryRow(
		ctx,
		"INSERT INTO payment (user_id, amount, status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5) RETURNING id",
		payment.UserID, payment.Amount, payment.Status, payment.CreatedAt, payment.UpdatedAt,
	).Scan(&payment.ID)
	if err != nil {
		return err
	}

	event := PaymentEvent{
		Type: "payment_created",
		Data: *payment,
	}

	if err := insertOutboxMessage(ctx, tx, payment.ID, event); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func updatePaymentStatus(ctx context.Context, dbpool *pgxpool.Pool, id string, status string) (*Payment, error) {
	tx, err := dbpool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	result, err := tx.Exec(
		ctx,
		"UPDATE payment SET status=$1, updated_at=$2 WHERE id=$3",
		status, time.Now(), id,
	)
	if err != nil {
		return nil, err
	}

	rowsAffected := result.RowsAffected()
	if rowsAffected == 0 {
		return nil, sql.ErrNoRows
	}

	var payment Payment
	err = tx.QueryRow(
		ctx,
		"SELECT id, user_id, amount, status, created_at, updated_at FROM payment WHERE id=$1",
		id,
	).Scan(&payment.ID, &payment.UserID, &payment.Amount, &payment.Status, &payment.CreatedAt, &payment.UpdatedAt)
	if err != nil {
		return nil, err
	}

	event := PaymentEvent{
		Type: "payment_updated",
		Data: payment,
	}

	if err := insertOutboxMessage(ctx, tx, payment.ID, event); err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return &payment, nil
}

func insertOutboxMessage(ctx context.Context, tx pgx.Tx, entityID int, event PaymentEvent) error {
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	outboxMessage := OutboxMessage{
		ID:        uuid.New(),
		EntityID:  entityID,
		Payload:   string(eventPayload),
		CreatedAt: time.Now(),
	}

	_, err = tx.Exec(
		ctx,
		"INSERT INTO outbox_messages (id, entity_id, payload, created_at) VALUES ($1, $2, $3, $4)",
		outboxMessage.ID, outboxMessage.EntityID, outboxMessage.Payload, outboxMessage.CreatedAt,
	)
	return err
}

func getConn() (*sql.DB, error) {
	db, err := sql.Open("postgres", "user=postgres password=postgres dbname=postgres sslmode=disable")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func runMigrations(db *sql.DB) error {
	return goose.Up(db, "migrations")
}

func getPool() (*pgxpool.Pool, error) {
	dbpool, err := pgxpool.New(context.Background(), "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		return nil, err
	}
	return dbpool, nil
}
