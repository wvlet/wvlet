#!/usr/bin/env bash
# TPC-H on DuckDB 2.0.1: CPU vs GPU (Sirius), on a DGX Spark.
#
# Run on the Spark itself:
#   ./run_bench.sh [scale factor=10] [iterations=3] [queries=1-22]
# Or from a checkout on another machine (copies this folder to the host first):
#   HOST=spark ./run_bench.sh 10
#
# Data:    $BENCH_DIR/tpch-sf<SF>/*.parquet (generated once)
# Results: $BENCH_DIR/results/sf<SF>-<timestamp>/{cpu,sirius}.csv, report.md
set -euo pipefail

SF=${1:-10}
ITERATIONS=${2:-3}
QUERIES=${3:-1-22}
BENCH_DIR=${BENCH_DIR:-$HOME/wvlet-gpu-bench}
DUCKDB_VERSION=2.0.1
HERE=$(cd "$(dirname "$0")" && pwd)
# sirius-sf100.yaml gives the GPU more memory for SF >= 100
if [[ -z "${SIRIUS_CONFIG:-}" ]]; then
  if (( ${SF%.*} >= 100 )); then SIRIUS_CONFIG="$HERE/sirius-sf100.yaml"; else SIRIUS_CONFIG="$HERE/sirius.yaml"; fi
fi

if [[ -n "${HOST:-}" ]]; then
  ssh "$HOST" "mkdir -p ~/wvlet-gpu-bench/scripts"
  rsync -a "$HERE/" "$HOST:wvlet-gpu-bench/scripts/"
  exec ssh -t "$HOST" "cd ~/wvlet-gpu-bench/scripts && ./run_bench.sh $SF $ITERATIONS $QUERIES"
fi

echo "== $(hostname) $(uname -m)"
nvidia-smi --query-gpu=name,driver_version --format=csv,noheader
if [[ "$(cat /proc/sys/kernel/io_uring_disabled 2>/dev/null || echo 0)" != "0" ]]; then
  echo "warning: kernel.io_uring_disabled != 0; Sirius needs io_uring"
fi

mkdir -p "$BENCH_DIR"
VENV="$BENCH_DIR/venv-duckdb-$DUCKDB_VERSION"
if [[ ! -x "$VENV/bin/python" ]]; then
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install -q "duckdb==$DUCKDB_VERSION"
fi
PY="$VENV/bin/python -I"

DATA="$BENCH_DIR/tpch-sf$SF"
$PY "$HERE/gen_tpch.py" "$SF" "$DATA"

OUT="$BENCH_DIR/results/sf$SF-$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUT"
COMMON=(--data "$DATA" --out "$OUT" --iterations "$ITERATIONS" --queries "$QUERIES")

echo "== DuckDB CPU"
SIRIUS_DISABLE=1 $PY "$HERE/bench_duckdb.py" --mode cpu "${COMMON[@]}" 2>&1 | tee "$OUT/cpu.log"
echo "== DuckDB + Sirius"
SIRIUS_CONFIG_FILE="$SIRIUS_CONFIG" \
  $PY "$HERE/bench_duckdb.py" --mode sirius "${COMMON[@]}" 2>&1 | tee "$OUT/sirius.log"
echo "== DuckDB + Sirius, tables pinned in GPU memory"
SIRIUS_CONFIG_FILE="$SIRIUS_CONFIG" \
  $PY "$HERE/bench_duckdb.py" --mode sirius --pin gpu "${COMMON[@]}" 2>&1 | tee "$OUT/sirius-pin-gpu.log"

$PY "$HERE/report.py" "$OUT" | tee "$OUT/report.md"
echo "Results: $OUT"
