#!/bin/bash

# Convenience wrapper for running benchmarks on PostgreSQL profile.
# Usage:
#   ./run-benchmarks-pg.sh
#   ./run-benchmarks-pg.sh 500000
#
# For APP_DB_HOST localhost / 127.0.0.1, starts the Docker DB via ./run-database-pg.sh
# (same APP_DB_* as this script) and waits until PostgreSQL accepts connections.

set -e
cd "$(dirname "$0")"

# Default to currently running local PostgreSQL demo container.
APP_DB_NAME="${APP_DB_NAME:-benchmark}"
APP_DB_USER="${APP_DB_USER:-benchmark}"
APP_DB_PASSWORD="${APP_DB_PASSWORD:-benchmark}"
APP_DB_HOST="${APP_DB_HOST:-localhost}"
APP_DB_PORT="${APP_DB_PORT:-5432}"

BENCHMARK_DB_URL="jdbc:postgresql://${APP_DB_HOST}:${APP_DB_PORT}/${APP_DB_NAME}"
CONTAINER_NAME="postgres-benchmark"

echo "PostgreSQL benchmark target: ${BENCHMARK_DB_URL}"
echo "Credentials user: ${APP_DB_USER}"
echo "Tip: override APP_DB_* vars if your local DB differs."

if [ "$APP_DB_HOST" = "localhost" ] || [ "$APP_DB_HOST" = "127.0.0.1" ]; then
  export APP_DB_NAME APP_DB_USER APP_DB_PASSWORD APP_DB_MEMORY APP_DB_SHM_SIZE
  echo "Ensuring local PostgreSQL (Docker) via ./run-database-pg.sh ..."
  ./run-database-pg.sh
  echo "Waiting for PostgreSQL to accept connections..."
  deadline=$((SECONDS + 90))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if docker exec "$CONTAINER_NAME" pg_isready -U "$APP_DB_USER" -d postgres >/dev/null 2>&1; then
      echo "PostgreSQL is ready."
      break
    fi
    sleep 1
  done
  if ! docker exec "$CONTAINER_NAME" pg_isready -U "$APP_DB_USER" -d postgres >/dev/null 2>&1; then
    echo "Error: PostgreSQL in container '$CONTAINER_NAME' did not become ready in time." >&2
    echo "Try: docker logs $CONTAINER_NAME" >&2
    exit 1
  fi
else
  echo "APP_DB_HOST=${APP_DB_HOST} is not local; skipping Docker start (use an already reachable server)."
fi

exec env \
  APP_DB_NAME="${APP_DB_NAME}" \
  APP_DB_USER="${APP_DB_USER}" \
  APP_DB_PASSWORD="${APP_DB_PASSWORD}" \
  BENCHMARK_DB_PROFILE=postgres \
  BENCHMARK_DB_URL="${BENCHMARK_DB_URL}" \
  BENCHMARK_DB_USER="${APP_DB_USER}" \
  BENCHMARK_DB_PASSWORD="${APP_DB_PASSWORD}" \
  ./run-benchmarks.sh "$@"
