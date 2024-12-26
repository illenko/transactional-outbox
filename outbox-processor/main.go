package main

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
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
		dbpool, err := newPool()
		if err != nil {
			log.Fatal(err)
		}
		defer dbpool.Close()

		writer := newKafkaWriter("localhost:9092", "payment-events")
		defer writer.Close()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		batchSize := 100
		for {
			select {
			case <-ticker.C:
				start := time.Now()
				if err := processMessages(dbpool, writer, batchSize); err != nil {
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

func processMessages(dbpool *pgxpool.Pool, writer *kafka.Writer, limit int) error {
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

	var kafkaMessages []kafka.Message

	for _, m := range messages {
		start := time.Now()

		msg := kafka.Message{
			Key:   []byte(strconv.Itoa(m.EntityID)),
			Value: []byte(m.Payload),
		}

		kafkaMessages = append(kafkaMessages, msg)

		elapsed := time.Since(start)
		log.Printf("Preparing message took: (%s) %s", m.EntityID, elapsed)
	}

	start := time.Now()
	err = writer.WriteMessages(context.Background(), kafkaMessages...)
	elapsed := time.Since(start)
	log.Printf("Writing messages took %s", elapsed)

	if err != nil {
		messagesFailed.Add(float64(len(messages)))
		log.Printf("Error writing messages: %v", err)
	}

	b := &pgx.Batch{}

	for _, m := range messages {
		sql := `UPDATE outbox_messages SET processed_at = $1, error =  $2 WHERE id = $3`

		b.Queue(sql, time.Now(), err != nil, m.ID)
	}

	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		log.Printf("Error sending batch: %v", err)

		messagesFailed.Add(float64(len(messages)))
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("Error committing transaction: %v", err)

		messagesFailed.Add(float64(len(messages)))
		return err
	}

	messagesProcessed.Add(float64(len(messages)))

	return nil
}

func newPool() (*pgxpool.Pool, error) {
	dbpool, err := pgxpool.New(context.Background(), "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		return nil, err
	}
	return dbpool, nil
}

func newKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(kafkaURL),
		Topic:                  topic,
		Balancer:               &kafka.ReferenceHash{},
		BatchSize:              100,
		RequiredAcks:           kafka.RequireAll,
		BatchTimeout:           100 * time.Millisecond,
		Async:                  false,
		AllowAutoTopicCreation: false,
	}
}
