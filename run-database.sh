#!/bin/bash

# ==============================================================================
# PostgreSQL Database Container Management Script
#
# Required Environment Variables:
#   APP_DB_NAME           - Name of the database to create (default 'demo')
#   APP_DB_USER           - Username used by both App and DB Admin
#   APP_DB_PASSWORD       - Password used by both App and DB Admin
# Optional Environment Variables:
#   APP_DB_MEMORY         - Container memory limit (default '3g')
#   APP_DB_SHM_SIZE       - Shared memory size for PostgreSQL (default '1g')
#
# Usage:
#   ./run-database.sh          - Starts the container (builds image if missing)
#   ./run-database.sh stop     - Stops the running container
#   ./run-database.sh restart  - Restarts the container
#   ./run-database.sh delete   - Stops and removes the container
#   ./run-database.sh backup   - Creates a compressed binary database backup (.dump)
#   docker logs -f postgres-benchmark - Show logs
# ==============================================================================

# Exit immediately if a command exits with a non-zero status
set -e
cd "$(dirname "$0")"

# Static constants
CONTAINER_NAME="postgres-benchmark"
IMAGE_NAME="my-postgres-alpine"
DB_NAME="${APP_DB_NAME:-benchmark}"
DB_MEMORY="${APP_DB_MEMORY:-3g}"
DB_SHM_SIZE="${APP_DB_SHM_SIZE:-1g}"
BACKUP_DIR="backup"

# --- Utility functions ---

# Check if the container exists
container_exists() {
  docker ps -a --format '{{.Names}}' | grep -Eq "^${CONTAINER_NAME}$"
}

# Check if the container is running
is_running() {
  docker ps --format '{{.Names}}' | grep -Eq "^${CONTAINER_NAME}$"
}

# Check if the image exists locally
image_exists() {
  docker images -q "$IMAGE_NAME" | grep -q .
}

# --- Action functions ---

# Function for: ./run-database.sh (no parameter)
start_container() {
  if is_running; then
    echo "Container '$CONTAINER_NAME' is already running (DB: $DB_NAME)."
    return
  fi

  if container_exists; then
    echo "Container exists but is stopped. Starting now..."
    docker start "$CONTAINER_NAME"
  else
    if ! image_exists; then
      echo "Image '$IMAGE_NAME' not found locally. Building from Dockerfile..."
      docker build -t "$IMAGE_NAME" .
    fi

    echo "Creating and starting a new container '$CONTAINER_NAME'..."

    if [ -z "$APP_DB_USER" ] || [ -z "$APP_DB_PASSWORD" ]; then
      echo "Error: variables APP_DB_USER and APP_DB_PASSWORD are required." >&2
      exit 1
    fi

    docker run -d \
      --name "$CONTAINER_NAME" \
      --memory="$DB_MEMORY" \
      --shm-size="$DB_SHM_SIZE" \
      --restart unless-stopped \
      -p 5432:5432 \
      -e POSTGRES_DB="$DB_NAME" \
      -e POSTGRES_USER="$APP_DB_USER" \
      -e POSTGRES_PASSWORD="$APP_DB_PASSWORD" \
      -v pg_data_demo:/var/lib/postgresql \
      "$IMAGE_NAME"
  fi
}

# Function for: ./run-database.sh stop
stop_container() {
  echo "Stopping container $CONTAINER_NAME..."
  docker stop "$CONTAINER_NAME" 2>/dev/null || echo "Container was not running."
}

# Function for: ./run-database.sh restart
restart_container() {
  echo "Restarting container $CONTAINER_NAME..."
  docker restart "$CONTAINER_NAME"
}

# Function for: ./run-database.sh delete
delete_container() {
  echo "Stopping and removing container $CONTAINER_NAME..."
  docker stop "$CONTAINER_NAME" 2>/dev/null || true
  docker rm "$CONTAINER_NAME" 2>/dev/null || true
  echo "Container deleted. Data in volume remains persistent."
}

# Function for: ./run-database.sh backup
backup_database() {
  echo "Starting compressed backup for database $DB_NAME..."

  if ! is_running; then
    echo "Error: Container $CONTAINER_NAME is not running. Backup aborted." >&2
    exit 1
  fi

  if [ -z "$APP_DB_USER" ]; then
    echo "Error: APP_DB_USER environment variable is missing." >&2
    exit 1
  fi

  mkdir -p "$BACKUP_DIR"

  # Local variables within function for better scoping
  local CURRENT_DATE=$(date +%Y-%m-%d)
  local BACKUP_FILE="$BACKUP_DIR/${CURRENT_DATE}.dump"
  local TEMP_BACKUP_FILE="${BACKUP_FILE}.tmp"

  if docker exec "$CONTAINER_NAME" pg_dump -U "$APP_DB_USER" -Fc "$DB_NAME" > "$TEMP_BACKUP_FILE"; then
    mv "$TEMP_BACKUP_FILE" "$BACKUP_FILE"
    echo "Compressed backup saved to: $BACKUP_FILE"
  else
    echo "Error: Backup failed." >&2
    rm -f "$TEMP_BACKUP_FILE"
    exit 1
  fi
}

# --- Environment check ---

if ! docker info >/dev/null 2>&1; then
    echo "Error: Docker is not available or insufficient permissions." >&2
    exit 1
fi

# --- Main entry point ---
case "$1" in
  "")        start_container ;;
  stop)      stop_container ;;
  restart)   restart_container ;;
  delete)    delete_container ;;
  backup)    backup_database ;;
  *)
    echo "Invalid parameter: $1"
    echo "Usage: $0 {stop|restart|delete|backup| (no parameter to start)}"
    exit 1
    ;;
esac