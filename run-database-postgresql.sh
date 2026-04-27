#!/bin/bash

# PostgreSQL convenience wrapper with zero manual setup.
# Delegates lifecycle actions to run-database.sh while providing defaults.
#
# Usage:
#   ./run-database-postgresql.sh
#   ./run-database-postgresql.sh stop
#   ./run-database-postgresql.sh restart
#   ./run-database-postgresql.sh delete
#   ./run-database-postgresql.sh backup

set -e
cd "$(dirname "$0")"

# Default credentials (can still be overridden by caller if needed).
export APP_DB_NAME="${APP_DB_NAME:-benchmark}"
export APP_DB_USER="${APP_DB_USER:-benchmark}"
export APP_DB_PASSWORD="${APP_DB_PASSWORD:-benchmark}"

exec ./run-database.sh "$@"
