#!/usr/bin/env bash
set -euo pipefail

PROMETHEUS_URL="${PROMETHEUS_URL:-http://localhost:9090}"

query_prometheus() {
  local query="$1"
  curl -fsS -G "${PROMETHEUS_URL}/api/v1/query" --data-urlencode "query=${query}" |
    jq -r '.data.result[0].value[1] // empty'
}

assert_number() {
  local name="$1"
  local value="$2"
  if [[ -z "${value}" || "${value}" == "NaN" || "${value}" == "+Inf" || "${value}" == "-Inf" ]]; then
    echo "${name}: no numeric value returned"
    exit 1
  fi
}

assert_gte() {
  local name="$1"
  local value="$2"
  local threshold="$3"
  assert_number "${name}" "${value}"
  awk -v name="${name}" -v value="${value}" -v threshold="${threshold}" '
    BEGIN {
      printf "%s = %.6f, required >= %.6f\n", name, value, threshold
      if (value + 0 < threshold + 0) exit 1
    }'
}

assert_lte() {
  local name="$1"
  local value="$2"
  local threshold="$3"
  assert_number "${name}" "${value}"
  awk -v name="${name}" -v value="${value}" -v threshold="${threshold}" '
    BEGIN {
      printf "%s = %.6f, required <= %.6f\n", name, value, threshold
      if (value + 0 > threshold + 0) exit 1
    }'
}

echo "Checking system SLI values from Prometheus"

availability="$(query_prometheus '1 - ((sum(rate(http_request_errors_total{service="producer"}[1m])) or vector(0)) / clamp_min(sum(rate(http_requests_total{service="producer"}[1m])), 0.001))')"
latency_p95="$(query_prometheus 'histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{service="producer"}[1m])) by (le))')"
aggregation_success="$(query_prometheus '(sum(increase(aggregation_runs_total{status="success"}[30m])) or vector(0)) / clamp_min(sum(increase(aggregation_runs_total[30m])), 1)')"

assert_gte "API availability" "${availability}" "0.95"
assert_lte "API p95 latency seconds" "${latency_p95}" "1.0"
assert_gte "Aggregation success ratio" "${aggregation_success}" "0.95"

echo "SLI check passed"
