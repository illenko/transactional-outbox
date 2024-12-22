package main

import (
	"encoding/json"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
)

func createPaymentHandler(dbpool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var payment Payment
		if err := json.NewDecoder(r.Body).Decode(&payment); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := createPayment(dbpool, &payment); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(payment)
	}
}

func updatePaymentHandler(dbpool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")

		var paymentUpdate PaymentUpdateRequest
		if err := json.NewDecoder(r.Body).Decode(&paymentUpdate); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		payment, err := updatePaymentStatus(dbpool, id, paymentUpdate.Status)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(payment)
	}
}
