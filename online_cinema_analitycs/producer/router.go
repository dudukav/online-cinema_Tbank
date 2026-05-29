package main

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func route(kp *KafkaProducer) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", metricsMiddleware("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	
	mux.HandleFunc("/events", metricsMiddleware("/events", EventsHandler(kp)))
	mux.HandleFunc("/events/generate", metricsMiddleware("/events/generate", EventsGenerateHandler(kp)))
	mux.Handle("/metrics", promhttp.Handler())

	return mux
}