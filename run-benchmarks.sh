#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Change the current working directory to the directory of this script
cd "$(dirname "$0")" || exit 1

# Define the main class to execute
MAIN_CLASS="org.benchmark.runner.BenchmarkRunner"
# Define the target submodule
MODULE="benchmark"

# Load the first argument as iterations (optional)
ITERATIONS=${1:-""}

echo "Building the project using Maven Wrapper..."
./mvnw clean install

echo "Starting the benchmark..."

# Seznam všech testovaných frameworků
FRAMEWORKS=("HIBERNATE" "JDBI" "EXPOSED" "MYBATIS" "UJORM")

for FRAMEWORK in "${FRAMEWORKS[@]}"; do
    echo "========================================================="
    echo "Running benchmark for ${FRAMEWORK}..."
    echo "========================================================="
    ./mvnw exec:java -pl "${MODULE}" -Dexec.mainClass="${MAIN_CLASS}" -Dexec.args="${FRAMEWORK} ${ITERATIONS}"
done

echo "Benchmark execution finished."