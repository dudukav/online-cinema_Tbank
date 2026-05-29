#!/usr/bin/env bash
set -euo pipefail

echo "Checking service metrics endpoints"

curl -fsS http://localhost:8080/metrics | grep "http_requests_total"
curl -fsS http://localhost:8080/metrics | grep "movie_events_produced_total"

curl -fsS http://localhost:8082/metrics | grep "http_requests_total"
curl -fsS http://localhost:8082/metrics | grep "aggregation_runs_total"

echo "Checking Prometheus readiness"
curl -fsS http://localhost:9090/-/ready

echo "Checking Prometheus targets"
curl -fsS "http://localhost:9090/api/v1/query?query=up" | grep '"status":"success"'
curl -fsS "http://localhost:9090/api/v1/query?query=up" | grep 'producer'
curl -fsS "http://localhost:9090/api/v1/query?query=up" | grep 'aggregator'

echo "Metrics check passed"