#!/bin/bash

# Convenience wrapper for running benchmarks on H2 profile.
# Usage:
#   ./run-benchmarks-h2.sh
#   ./run-benchmarks-h2.sh 500000

set -e
cd "$(dirname "$0")"

exec env BENCHMARK_DB_PROFILE=h2 ./run-benchmarks.sh "$@"
