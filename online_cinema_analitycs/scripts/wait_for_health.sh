#!/usr/bin/env bash
set -euo pipefail

wait_url() {
  local name="$1"
  local url="$2"

  echo "Waiting for $name at $url"

  for i in {1..60}; do
    if curl -fsS "$url" >/dev/null; then
      echo "$name is ready"
      return 0
    fi

    sleep 2
  done

  echo "$name is not ready"
  exit 1
}

wait_url "producer" "http://localhost:8080/health"
wait_url "aggregator" "http://localhost:8082/health"
wait_url "prometheus" "http://localhost:9090/-/ready"