package main

import "github.com/prometheus/client_golang/prometheus"

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests.",
			ConstLabels: prometheus.Labels{
				"service": "aggregator",
			},
		},
		[]string{"method", "endpoint", "status"},
	)

	httpRequestErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_request_errors_total",
			Help: "Total number of HTTP request errors.",
			ConstLabels: prometheus.Labels{
				"service": "aggregator",
			},
		},
		[]string{"method", "endpoint", "error_type"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "http_request_duration_seconds",
			Help: "Duration of HTTP requests in seconds.",
			ConstLabels: prometheus.Labels{
				"service": "aggregator",
			},
		},
		[]string{"method", "endpoint"},
	)

	aggregationRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aggregation_runs_total",
			Help: "Total number of aggregation runs.",
			ConstLabels: prometheus.Labels{
				"service": "aggregator",
			},
		},
		[]string{"status"},
	)

	aggregationDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "aggregation_duration_seconds",
			Help: "Duration of aggregation runs in seconds.",
			ConstLabels: prometheus.Labels{
				"service": "aggregator",
			},
		},
	)

	aggregationRecordsProcessedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aggregation_records_processed_total",
			Help: "Total number of raw records processed by aggregation runs.",
			ConstLabels: prometheus.Labels{
				"service": "aggregator",
			},
		},
	)
)

func init() {
	prometheus.MustRegister(httpRequestsTotal)
	prometheus.MustRegister(httpRequestErrorsTotal)
	prometheus.MustRegister(httpRequestDuration)
	prometheus.MustRegister(aggregationRunsTotal)
	prometheus.MustRegister(aggregationDuration)
	prometheus.MustRegister(aggregationRecordsProcessedTotal)
}
