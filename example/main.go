package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/AdarshNampally/idempotency"
)

var chargeSeq int64

func chargeHandler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	id := atomic.AddInt64(&chargeSeq, 1)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"charge_id": id,
		"payload":   string(body),
		"status":    "charged",
	})
}

func main() {
	store := idempotency.NewStore(10_000, 24*time.Hour)

	mux := http.NewServeMux()
	mux.Handle("POST /charge", idempotency.New(store, http.HandlerFunc(chargeHandler)))

	fmt.Println("Server running on :8080")
	fmt.Println()
	fmt.Println("Idempotent (same charge_id both times):")
	fmt.Println(`  curl -s -X POST -H "Idempotency-Key: pay-001" -d '100' http://localhost:8080/charge`)
	fmt.Println(`  curl -s -X POST -H "Idempotency-Key: pay-001" -d '100' http://localhost:8080/charge`)
	fmt.Println()
	fmt.Println("Conflict (same key, different body -> 422):")
	fmt.Println(`  curl -s -X POST -H "Idempotency-Key: pay-001" -d '999' http://localhost:8080/charge`)
	fmt.Println()
	fmt.Println("Without key (executes every time):")
	fmt.Println(`  curl -s -X POST -d '100' http://localhost:8080/charge`)

	log.Fatal(http.ListenAndServe(":8080", mux))
}
