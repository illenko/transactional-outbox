package main

import (
	"log"
	"net/http"

	_ "github.com/lib/pq"
)

func main() {
	db, err := getConn()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := runMigrations(db); err != nil {
		log.Fatal(err)
	}

	dbpool, err := getPool()
	if err != nil {
		log.Fatal(err)
	}
	defer dbpool.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /payments", createPaymentHandler(dbpool))
	mux.HandleFunc("PATCH /payments/{id}", updatePaymentHandler(dbpool))

	log.Println("Server started at :8080")
	log.Fatal(http.ListenAndServe(":8080", mux))
}
