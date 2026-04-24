package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func AggregateHandler(ch clickhouse.Conn, psql *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		dateStr := r.URL.Query().Get("date")
		if dateStr == "" {
			http.Error(w, `{"error": "missing date"}`, http.StatusBadRequest)
			return
		}

		date, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			http.Error(w, `{"error": "invalid date format"}`, http.StatusBadRequest)
			return
		}

		log.Printf("Manual aggregation for %s", dateStr)
		start := time.Now()

		err = aggregateForDate(date, ch, psql)
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error": "%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}

		elapsed := time.Since(start)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":   "ok",
			"date":     dateStr,
			"duration": elapsed.Seconds(),
		})
	}
}