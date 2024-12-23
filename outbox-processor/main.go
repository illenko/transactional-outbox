package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxMessage struct {
	ID        string    `json:"id"`
	EntityID  int       `json:"entity_id"`
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	dbpool, err := getPool()
	if err != nil {
		log.Fatal(err)
	}
	defer dbpool.Close()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := processMessages(dbpool); err != nil {
				log.Printf("Error processing messages: %v", err)
			}
		}
	}
}

func processMessages(dbpool *pgxpool.Pool) error {
	log.Println("Processing outbox messages")

	tx, err := dbpool.Begin(context.Background())
	if err != nil {
		log.Fatalf("Error starting transaction: %v", err)
		return err
	}
	defer tx.Rollback(context.Background())

	rows, err := tx.Query(
		context.Background(),
		"SELECT id, entity_id, payload, created_at FROM outbox_messages WHERE processed_at IS NULL ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED",
	)
	if err != nil {
		log.Fatalf("Error querying outbox messages: %v", err)
		return err
	}
	defer rows.Close()

	var messages []OutboxMessage
	for rows.Next() {
		var outboxMessage OutboxMessage
		if err := rows.Scan(&outboxMessage.ID, &outboxMessage.EntityID, &outboxMessage.Payload, &outboxMessage.CreatedAt); err != nil {
			log.Fatalf("Error scanning outbox message: %v", err)
			return err
		}
		messages = append(messages, outboxMessage)
	}

	log.Printf("Found %d outbox messages", len(messages))

	for _, outboxMessage := range messages {
		log.Printf("Processing outbox message: %v", outboxMessage)

		_, err := tx.Exec(
			context.Background(),
			"UPDATE outbox_messages SET processed_at=$1 WHERE id=$2",
			time.Now(), outboxMessage.ID,
		)
		if err != nil {
			log.Fatalf("Error updating outbox message: %v", err)
			return err
		}
	}

	if err := tx.Commit(context.Background()); err != nil {
		log.Fatalf("Error committing transaction: %v", err)
		return err
	}

	return nil
}

func getPool() (*pgxpool.Pool, error) {
	dbpool, err := pgxpool.New(context.Background(), "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		return nil, err
	}
	return dbpool, nil
}
