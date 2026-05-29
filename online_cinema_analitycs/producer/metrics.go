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
			Buckets: []float64{
				0.005,
				0.01,
				0.025,
				0.05,
				0.1,
				0.25,
				0.5,
				0.75,
				1,
				1.25,
				1.5,
				2,
				2.5,
				5,
			},
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
