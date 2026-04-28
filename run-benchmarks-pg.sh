#!/bin/bash

# Convenience wrapper for running benchmarks on PostgreSQL profile.
# Usage:
#   ./run-benchmarks-postgresql.sh
#   ./run-benchmarks-postgresql.sh 500000
#
# Note: Ensure PostgreSQL is running first (for example via ./run-database-pg.sh).

set -e
cd "$(dirname "$0")"

# Default to currently running local PostgreSQL demo container.
APP_DB_NAME="${APP_DB_NAME:-demo}"
APP_DB_USER="${APP_DB_USER:-demo}"
APP_DB_PASSWORD="${APP_DB_PASSWORD:-changeit951}"
APP_DB_HOST="${APP_DB_HOST:-localhost}"
APP_DB_PORT="${APP_DB_PORT:-5432}"

BENCHMARK_DB_URL="jdbc:postgresql://${APP_DB_HOST}:${APP_DB_PORT}/${APP_DB_NAME}"

echo "PostgreSQL benchmark target: ${BENCHMARK_DB_URL}"
echo "Credentials user: ${APP_DB_USER}"
echo "Tip: override APP_DB_* vars if your local DB differs."

exec env \
  APP_DB_NAME="${APP_DB_NAME}" \
  APP_DB_USER="${APP_DB_USER}" \
  APP_DB_PASSWORD="${APP_DB_PASSWORD}" \
  BENCHMARK_DB_PROFILE=postgres \
  BENCHMARK_DB_URL="${BENCHMARK_DB_URL}" \
  BENCHMARK_DB_USER="${APP_DB_USER}" \
  BENCHMARK_DB_PASSWORD="${APP_DB_PASSWORD}" \
  ./run-benchmarks.sh "$@"
