package main

import (
	"database/sql"
	"net/http"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func route(ch driver.Conn, psql *sql.DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", metricsMiddleware("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	mux.HandleFunc("/aggregate", metricsMiddleware("/aggregate", AggregateHandler(ch, psql)))
	mux.Handle("/metrics", promhttp.Handler())

	return mux
}