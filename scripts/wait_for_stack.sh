#!/usr/bin/env bash
# Poll Docker healthchecks until core lakehouse services are ready.
set -euo pipefail

TIMEOUT="${STACK_WAIT_TIMEOUT:-120}"
INTERVAL="${STACK_WAIT_INTERVAL:-3}"
HEALTHY_SERVICES=(redpanda spark-master minio)
deadline=$((SECONDS + TIMEOUT))

echo "Waiting for Docker services to become healthy (timeout ${TIMEOUT}s)..."

while (( SECONDS < deadline )); do
  all_ok=true

  for svc in "${HEALTHY_SERVICES[@]}"; do
    health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$svc" 2>/dev/null || echo "missing")"
    if [[ "$health" != "healthy" ]]; then
      all_ok=false
      echo "  ${svc}: ${health}"
    fi
  done

  worker_status="$(docker inspect --format '{{.State.Status}}' spark-worker 2>/dev/null || echo "missing")"
  if [[ "$worker_status" != "running" ]]; then
    all_ok=false
    echo "  spark-worker: ${worker_status}"
  fi

  if $all_ok; then
    echo "Stack ready (redpanda, spark-master, minio healthy; spark-worker running)."
    exit 0
  fi

  sleep "$INTERVAL"
done

echo "Timed out waiting for stack health after ${TIMEOUT}s."
docker compose ps
exit 1
