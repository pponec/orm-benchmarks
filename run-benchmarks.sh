#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# The report file
REPORT="$HOME/ujo-benchmark.csv"

# Change the current working directory to the directory of this script
cd "$(dirname "$0")" || exit 1

# Define the main class to execute
MAIN_CLASS="org.benchmark.runner.BenchmarkRunner"
# Define the target submodule
MODULE="benchmark"

# Load the first argument as iterations (optional)
ITERATIONS=${1:-""}
DB_PROFILE=${BENCHMARK_DB_PROFILE:-h2}

echo "Building the project using Maven Wrapper..."
./mvnw clean install -U -DskipTests

echo "Starting the benchmark..."
echo "DB profile: ${DB_PROFILE}"
if [ "${DB_PROFILE}" = "postgres" ] || [ "${DB_PROFILE}" = "postgresql" ]; then
  echo "PostgreSQL mode enabled. Ensure DB is already running via ./run-database.sh"
  echo "Ensuring PostgreSQL JDBC driver is available in local Maven repository..."
  ./mvnw -q dependency:get -Dartifact=org.postgresql:postgresql:42.7.8
else
  echo "Ensuring H2 JDBC driver is available in local Maven repository..."
  ./mvnw -q dependency:get -Dartifact=com.h2database:h2:2.4.240
fi

export MAVEN_OPTS="-Xms4G -Xmx8G"

# List of the libraries
FRAMEWORKS="
  UJORM
  JDBI
  MYBATIS
  HIBERNATE
  EXPOSED
  QUERYDSL
  JOOQ
  EBEAN
"

for FRAMEWORK in ${FRAMEWORKS}; do
    echo "========================================================="
    echo "Running benchmark for ${FRAMEWORK}..."
    echo "========================================================="
    ./mvnw exec:java -pl "${MODULE}" -Dexec.classpathScope=test -Dexec.mainClass="${MAIN_CLASS}" -Dexec.args="${FRAMEWORK} ${ITERATIONS}" -Dbenchmark.db.profile="${DB_PROFILE}"
done

cat "$REPORT"
echo "Benchmark execution finished, see the report: $REPORT"
