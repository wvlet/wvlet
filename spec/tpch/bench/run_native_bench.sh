#!/bin/bash
# Run the TPC-H queries in spec/tpch on the in-process wvlet engine through a Scala Native `wvc`
# binary, one process per query, and report each query's wall time and peak RSS.
#
# Usage: run_native_bench.sh <wvc_binary> <data_dir> [rounds] [queries]
#
#   wvc_binary  e.g. target/out/native0.5/scala-*/wvc/wvc (build with `./sbt wvc/nativeLink`;
#               set SCALANATIVE_MODE=release-fast and SCALANATIVE_GC=immix|boehm to compare GCs)
#   data_dir    folder of <table>.parquet files: spec/tpch/data (tiny) or target/tpch-sf1 (made by
#               EngineTPCHBench, see website/docs/development/benchmark.md)
#   rounds      executions per query (default 3)
#   queries     space-separated query names (default: q1 ... q22)
#
# Prints a TSV row per execution: query, round, wall seconds, peak RSS in MB, exit status. A
# non-zero status (a crash or an error) also prints the process's stderr.
set -u
WVC="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
DATA="$(cd "$2" && pwd)"
ROUNDS="${3:-3}"
QUERIES="${4:-$(seq -f 'q%g' 1 22 | tr '\n' ' ')}"
SPEC="$(cd "$(dirname "$0")/.." && pwd)"

# A temporary work folder with the queries and a model per table bound to its Parquet file, so the
# run needs no profile (the native CLI reads ~/.wvlet from the passwd entry, not $HOME)
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT
cp "${SPEC}"/q*.wv "${WORK}/"
shopt -s nullglob
tables=("${DATA}"/*.parquet)
if [ ${#tables[@]} -eq 0 ]; then
  echo "error: no .parquet files in ${DATA}" >&2
  exit 1
fi
for f in "${tables[@]}"; do
  printf "model %s = {\n  from '%s'\n}\n\n" "$(basename "${f}" .parquet)" "${f}" >> "${WORK}/tables.wv"
done

# BSD time (macOS) reports max RSS in bytes with -l; GNU time reports KB with -v
if [ "$(uname)" = "Darwin" ]; then
  TIME_OPT=-l
else
  TIME_OPT=-v
fi

printf "query\tround\twall_s\tpeak_rss_mb\tstatus\n"
for r in $(seq 1 "${ROUNDS}"); do
  for q in ${QUERIES}; do
    err="${WORK}/.stderr"
    /usr/bin/time ${TIME_OPT} "${WVC}" run -t wvlet -w "${WORK}" -f "${q}.wv" --format csv > /dev/null 2> "${err}"
    status=$?
    if [ "$(uname)" = "Darwin" ]; then
      wall=$(awk '/ real /{ print $1; exit }' "${err}")
      rss=$(awk '/maximum resident set size/{ printf "%.1f", $1 / 1048576; exit }' "${err}")
    else
      wall=$(awk -F': ' '/Elapsed \(wall clock\)/{ n = split($2, t, ":"); s = 0;
        for (i = 1; i <= n; i++) s = s * 60 + t[i]; print s; exit }' "${err}")
      rss=$(awk -F': ' '/Maximum resident set size/{ printf "%.1f", $2 / 1024; exit }' "${err}")
    fi
    printf "%s\t%s\t%s\t%s\t%s\n" "${q}" "${r}" "${wall}" "${rss}" "${status}"
    if [ "${status}" -ne 0 ]; then
      # Drop the time report (BSD: "  123  label"; GNU: "\tLabel: value"), keep the program's own
      # output, including stack traces ("\tat ...")
      grep -v -E $'^ +[0-9.]+ +[a-z]|^\t[A-Z][^:]*: ' "${err}" | head -40 >&2
    fi
  done
done
