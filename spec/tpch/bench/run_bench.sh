#!/bin/bash
# Time each .sql file in a directory against a DuckDB database with EXPLAIN ANALYZE.
#
# Usage: run_bench.sh <db_file> <sql_dir> [runs] [threads]
#
# Prints a TSV with the min and median "Total Time" reported by EXPLAIN ANALYZE over <runs>
# executions per query (default 3). See website/docs/development/benchmark.md for how to build
# the database and the SQL directory.
set -eu
DB="$1"
DIR="$2"
RUNS="${3:-3}"
THREADS="${4:-8}"

printf "query\tmin_ms\tmedian_ms\tall_ms\n"
for f in "$DIR"/*.sql; do
  name=$(basename "$f" .sql)
  sql=$(cat "$f")
  times=()
  for _ in $(seq 1 "$RUNS"); do
    total=$(duckdb "$DB" -c "PRAGMA threads=${THREADS}; EXPLAIN ANALYZE ${sql}" 2>/dev/null \
      | grep -oE "Total Time: [0-9.]+s" | head -1)
    if [ -z "$total" ]; then
      echo "error: EXPLAIN ANALYZE failed for ${f}" >&2
      duckdb "$DB" -c "EXPLAIN ANALYZE ${sql}" >/dev/null || true
      exit 1
    fi
    seconds=${total#Total Time: }
    seconds=${seconds%s}
    times+=("$(awk -v t="$seconds" 'BEGIN { printf "%.1f", t * 1000 }')")
  done
  sorted=$(printf '%s\n' "${times[@]}" | sort -n)
  min_ms=$(echo "$sorted" | head -1)
  median_ms=$(echo "$sorted" | awk '{ a[NR] = $1 } END { print a[int((NR + 1) / 2)] }')
  printf "%s\t%s\t%s\t%s\n" "$name" "$min_ms" "$median_ms" "${times[*]}"
done
