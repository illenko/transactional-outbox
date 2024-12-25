package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type OutboxMessage struct {
	ID        string    `json:"id"`
	EntityID  int       `json:"entity_id"`
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

var (
	messagesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "messages_processed_total",
		Help: "The total number of processed messages",
	})
	messagesFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "messages_failed_total",
		Help: "The total number of failed messages",
	})
)

func main() {

	go func() {
		dbpool, err := getPool()
		if err != nil {
			log.Fatal(err)
		}
		defer dbpool.Close()

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		batchSize := 5
		for {
			select {
			case <-ticker.C:
				start := time.Now()
				if err := processMessages(dbpool, batchSize); err != nil {
					log.Printf("Error processing messages: %v", err)
				}
				elapsed := time.Since(start)
				log.Printf("processMessages took %s", elapsed)
			}
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":8081", nil)
}

func processMessages(dbpool *pgxpool.Pool, limit int) error {
	ctx := context.Background()

	tx, err := dbpool.Begin(ctx)
	if err != nil {
		log.Printf("Error starting transaction: %v", err)
		return err
	}
	defer tx.Rollback(ctx)

	query := "SELECT id, entity_id, payload, created_at FROM outbox_messages WHERE processed_at IS NULL ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED"

	rows, err := tx.Query(ctx, query, limit)

	if err != nil {
		log.Printf("Error querying outbox messages: %v", err)
		return err
	}
	defer rows.Close()

	var messages []OutboxMessage
	for rows.Next() {
		var outboxMessage OutboxMessage
		if err := rows.Scan(&outboxMessage.ID, &outboxMessage.EntityID, &outboxMessage.Payload, &outboxMessage.CreatedAt); err != nil {
			log.Printf("Error scanning outbox message: %v", err)
			return err
		}
		messages = append(messages, outboxMessage)
	}

	log.Printf("Found %d outbox messages", len(messages))

	for _, outboxMessage := range messages {
		log.Printf("Processing outbox message: %v", outboxMessage)

		_, err := tx.Exec(ctx, "UPDATE outbox_messages SET processed_at=$1 WHERE id=$2", time.Now(), outboxMessage.ID)
		if err != nil {
			log.Fatalf("Error updating outbox message: %v", err)
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Error committing transaction: %v", err)

		messagesFailed.Add(float64(len(messages)))
		return err
	}

	messagesProcessed.Add(float64(len(messages)))

	return nil
}

func getPool() (*pgxpool.Pool, error) {
	dbpool, err := pgxpool.New(context.Background(), "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		return nil, err
	}
	return dbpool, nil
}
