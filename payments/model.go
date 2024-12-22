package main

import (
	"time"

	"github.com/google/uuid"
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
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

type PaymentUpdateRequest struct {
	Status string `json:"status"`
}
