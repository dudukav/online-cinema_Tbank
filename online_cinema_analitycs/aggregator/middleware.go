package main

import (
	"net/http"
	"strconv"
	"time"
)

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func metricsMiddleware(endpoint string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next(recorder, r)
		status := strconv.Itoa(recorder.statusCode)

		httpRequestsTotal.WithLabelValues(
			r.Method,
			endpoint,
			status,
		).Inc()

		httpRequestDuration.WithLabelValues(
			r.Method,
			endpoint,
		).Observe(time.Since(start).Seconds())

		if recorder.statusCode >= 400 {
			errorType := "client_error"
			if recorder.statusCode >= 500 {
				errorType = "server_error"
			}

			httpRequestErrorsTotal.WithLabelValues(
				r.Method,
				endpoint,
				errorType,
			).Inc()
		}
	}
}
