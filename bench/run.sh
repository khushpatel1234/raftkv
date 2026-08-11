#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)

host=${RAFTKV_BENCH_HOST:-127.0.0.1}
port=${RAFTKV_BENCH_PORT:-6379}
clients=${RAFTKV_BENCH_CLIENTS:-50}
duration=${RAFTKV_BENCH_DURATION:-30}
warmup=${RAFTKV_BENCH_WARMUP:-100}
keyspace=${RAFTKV_BENCH_KEYSPACE:-10000}
value_size=${RAFTKV_BENCH_VALUE_SIZE:-128}
run_id=$(date -u +%Y%m%dT%H%M%SZ)
output=${RAFTKV_BENCH_OUTPUT:-"${PROJECT_DIR}/bench/results/run-${run_id}.csv"}
label=${RAFTKV_BENCH_LABEL:-"three-node-local"}

if [[ -e ${output} ]]; then
  echo "error: refusing to overwrite existing results: ${output}" >&2
  echo "set RAFTKV_BENCH_OUTPUT to a new path" >&2
  exit 2
fi

mkdir -p -- "$(dirname -- "${output}")"
if command -v git >/dev/null 2>&1; then
  git_commit=$(git -C "${PROJECT_DIR}" rev-parse --short HEAD 2>/dev/null || true)
else
  git_commit=""
fi

common=(
  --host "${host}"
  --port "${port}"
  --clients "${clients}"
  --duration "${duration}"
  --warmup "${warmup}"
  --keyspace "${keyspace}"
  --value-size "${value_size}"
  --run-label "${label}"
  --git-commit "${git_commit}"
  --output "${output}"
)

echo "Targeting ${host}:${port}; this must be the current leader." >&2
python3 "${SCRIPT_DIR}/resp_bench.py" --operation get "${common[@]}"
python3 "${SCRIPT_DIR}/resp_bench.py" --operation set "${common[@]}" --append
echo "Benchmark complete: ${output}"
