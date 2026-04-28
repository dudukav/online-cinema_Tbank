package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/lib/pq"
)

func main() {
	clickHouseAddr := getenv("CLICKHOUSE_ADDR", "clickhouse:9000")
	clickHouseUser := getenv("CLICKHOUSE_USER", "analytics")
	clickHousePassword := getenv("CLICKHOUSE_PASSWORD", "analytics")
	postgresURL := getenv("POSTGRES_URL", "postgresql://user:pass@postgres:5432/analytics?sslmode=disable")
	interval, err := time.ParseDuration(getenv("AGGREGATION_INTERVAL", "10m"))
	if err != nil {
		log.Fatalf("invalid AGGREGATION_INTERVAL: %v", err)
	}

	ch, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{clickHouseAddr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: clickHouseUser,
			Password: clickHousePassword,
		},
	})
	if err != nil {
		log.Fatalf("connect clickhouse: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ch.Ping(ctx); err != nil {
		log.Fatalf("ping clickhouse: %v", err)
	}

	psql, err := sql.Open("postgres", postgresURL)
	if err != nil {
		log.Fatalf("open postgres: %v", err)
	}
	if err := psql.Ping(); err != nil {
		log.Fatalf("ping postgres: %v", err)
	}

	go StartScheduler(interval, func() time.Time {
		return time.Now().Add(-24 * time.Hour)
	}, ch, psql)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/aggregate", AggregateHandler(ch, psql))
	log.Fatal(http.ListenAndServe(":8081", mux))
}

func getenv(name, fallback string) string {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	return value
}
