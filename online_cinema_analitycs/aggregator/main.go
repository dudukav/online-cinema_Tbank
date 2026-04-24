package main

import (
	"database/sql"
	"log"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	ch, _ = clickhouse.Open(&clickhouse.Options{Addr: []string{"clickhouse:9000"}})

	psql, _ = sql.Open("postgres", "postgresql://user:pass@postgres:5432/analytics")

	go StartScheduler(10*time.Minute, func() time.Time {
		return time.Now().Add(-24 * time.Hour)
	}, ch, psql)

	mux := http.NewServeMux()
	mux.HandleFunc("/aggregate", AggregateHandler(ch, psql))
	log.Fatal(http.ListenAndServe(":8081", mux))
}