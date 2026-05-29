package main

import "github.com/prometheus/client_golang/prometheus"

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests.",
			ConstLabels: prometheus.Labels{
				"service": "producer",
			},
		},
		[]string{"method", "endpoint", "status"},
	)

	httpRequestErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_request_errors_total",
			Help: "Total number of HTTP request errors.",
			ConstLabels: prometheus.Labels{
				"service": "producer",
			},
		},
		[]string{"method", "endpoint", "error_type"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "http_request_duration_seconds",
			Help: "Duration of HTTP requests in seconds.",
			ConstLabels: prometheus.Labels{
				"service": "producer",
			},
		},
		[]string{"method", "endpoint"},
	)

	movieEventsProducedTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "movie_events_produced_total",
			Help: "Total number of movie events produced to Kafka.",
			ConstLabels: prometheus.Labels{
				"service": "producer",
			},
		},
		[]string{"event_type", "device_type"},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestErrorsTotal)
	prometheus.MustRegister(httpRequestDuration)
	prometheus.MustRegister(movieEventsProducedTotal)
}
