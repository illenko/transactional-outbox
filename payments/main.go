package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
)

type Payment struct {
	ID        int       `json:"id"`
	UserID    int       `json:"user_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type PaymentEvent struct {
	Type string  `json:"type"`
	Data Payment `json:"data"`
}

type OutboxMessage struct {
	ID        uuid.UUID `json:"id"`
	EntityID  int       `json:"entity_id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type PaymentUpdate struct {
	Status string `json:"status"`
}

func main() {
	db, err := getDbConn()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := runMigrations(db); err != nil {
		log.Fatal(err)
	}

	dbpool, err := getDbPool()
	if err != nil {
		log.Fatal(err)
	}
	defer dbpool.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", func(w http.ResponseWriter, r *http.Request) {
		var payment Payment
		if err := json.NewDecoder(r.Body).Decode(&payment); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		payment.CreatedAt = time.Now()
		payment.UpdatedAt = time.Now()

		tx, err := dbpool.Begin(context.Background())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer tx.Rollback(context.Background())

		err = tx.QueryRow(
			context.Background(),
			"INSERT INTO payment (user_id, amount, status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5) RETURNING id",
			payment.UserID, payment.Amount, payment.Status, payment.CreatedAt, payment.UpdatedAt,
		).Scan(&payment.ID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		event := PaymentEvent{
			Type: "payment_created",
			Data: payment,
		}

		eventContent, err := json.Marshal(event)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		outboxMessage := OutboxMessage{
			ID:        uuid.New(),
			EntityID:  payment.ID,
			Content:   string(eventContent),
			CreatedAt: time.Now(),
		}

		_, err = tx.Exec(
			context.Background(),
			"INSERT INTO outbox_messages (id, entity_id, content, created_at) VALUES ($1, $2, $3, $4)",
			outboxMessage.ID, outboxMessage.EntityID, outboxMessage.Content, outboxMessage.CreatedAt,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(context.Background()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(payment)
	})
	mux.HandleFunc("PATCH /payments/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")

		var paymentUpdate PaymentUpdate
		if err := json.NewDecoder(r.Body).Decode(&paymentUpdate); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		tx, err := dbpool.Begin(context.Background())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer tx.Rollback(context.Background())

		result, err := tx.Exec(
			context.Background(),
			"UPDATE payment SET status=$1, updated_at=$2 WHERE id=$3",
			paymentUpdate.Status, time.Now(), id,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rowsAffected := result.RowsAffected()

		if rowsAffected == 0 {
			http.Error(w, "Payment not found", http.StatusNotFound)
			return
		}

		var payment Payment
		err = tx.QueryRow(
			context.Background(),
			"SELECT id, user_id, amount, status, created_at, updated_at FROM payment WHERE id=$1",
			id,
		).Scan(&payment.ID, &payment.UserID, &payment.Amount, &payment.Status, &payment.CreatedAt, &payment.UpdatedAt)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		event := PaymentEvent{
			Type: "payment_updated",
			Data: payment,
		}

		eventContent, err := json.Marshal(event)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		outboxMessage := OutboxMessage{
			ID:        uuid.New(),
			EntityID:  payment.ID,
			Content:   string(eventContent),
			CreatedAt: time.Now(),
		}

		_, err = tx.Exec(
			context.Background(),
			"INSERT INTO outbox_messages (id, entity_id, content, created_at) VALUES ($1, $2, $3, $4)",
			outboxMessage.ID, outboxMessage.EntityID, outboxMessage.Content, outboxMessage.CreatedAt,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if err := tx.Commit(context.Background()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(payment)
	})

	log.Println("Server started at :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

func getDbConn() (*sql.DB, error) {
	db, err := sql.Open("postgres", "user=postgres password=postgres dbname=postgres sslmode=disable")
	if err != nil {
		return nil, err
	}
	return db, nil
}

func runMigrations(db *sql.DB) error {
	return goose.Up(db, "migrations")
}

func getDbPool() (*pgxpool.Pool, error) {
	dbpool, err := pgxpool.New(context.Background(), "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		return nil, err
	}
	return dbpool, nil
}
