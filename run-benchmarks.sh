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

echo "Building the project using Maven Wrapper..."
./mvnw clean install -U

echo "Starting the benchmark..."

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
    ./mvnw exec:java -pl "${MODULE}" -Dexec.mainClass="${MAIN_CLASS}" -Dexec.args="${FRAMEWORK} ${ITERATIONS}"
done

cat "$REPORT"
echo "Benchmark execution finished, see the report: $REPORT"