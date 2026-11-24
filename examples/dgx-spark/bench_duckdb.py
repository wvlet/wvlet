"""Run the 22 TPC-H queries on DuckDB, on the CPU or on the GPU through Sirius.

Run each mode in its own process: `cpu` never loads Sirius (SIRIUS_DISABLE=1 is also set by
run_bench.sh), and `sirius` loads the core extension and enables transparent GPU execution.

Usage:
  python bench_duckdb.py --mode cpu|sirius --data <parquet dir> --out <result dir>
      [--iterations 3] [--queries 1,6,14] [--timeout 600] [--pin none|gpu|host]

Writes <out>/<mode>.csv (sirius-pin-<tier>.csv with --pin), one row per (query, iteration),
with columns mode, query, iteration, runtime_s, rows, digest, on_gpu. The first iteration reads
Parquet with a cold Sirius cache; later iterations are hot. As in the Sirius benchmark, only
execute() is timed. Fetching the result rows into Python is excluded.

on_gpu (sirius mode only) is checked after the timed runs, by running the query once more with
enable_duckdb_fallback = false. "yes" means the whole query ran on the GPU. "no" means Sirius
rejected the query, so the timed runs fell back to the CPU.
"""

import argparse
import csv
import hashlib
import os
import threading
import time

import duckdb

TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]


def digest(rows):
    """An order-insensitive digest of the result, with floats rounded so CPU/GPU sums compare"""

    def norm(v):
        if isinstance(v, float):
            return f"{v:.4f}"
        return str(v)

    lines = sorted("|".join(norm(v) for v in r) for r in rows)
    return hashlib.sha1("\n".join(lines).encode()).hexdigest()[:12]


def run(con, sql, timeout):
    timer = threading.Timer(timeout, con.interrupt)
    timer.start()
    try:
        start = time.perf_counter()
        result = con.execute(sql)
        elapsed = time.perf_counter() - start
        rows = result.fetchall()
        return elapsed, rows
    finally:
        timer.cancel()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["cpu", "sirius"], required=True)
    p.add_argument("--data", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--iterations", type=int, default=3)
    p.add_argument("--queries", default="1-22")
    p.add_argument("--timeout", type=float, default=600)
    p.add_argument(
        "--pin",
        choices=["none", "gpu", "host"],
        default="none",
        help="sirius only: pin_table every table to this tier; results go to sirius-pin-<tier>.csv",
    )
    args = p.parse_args()

    if "-" in args.queries:
        lo, hi = args.queries.split("-")
        queries = list(range(int(lo), int(hi) + 1))
    else:
        queries = [int(q) for q in args.queries.split(",")]

    con = duckdb.connect(":memory:")
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    texts = dict(con.execute("SELECT query_nr, query FROM tpch_queries()").fetchall())
    if args.mode == "sirius":
        start = time.perf_counter()
        # LOAD sirius downloads its ~800 MB CUDA backend on first use. DuckDB's built-in HTTP
        # client dropped that download on the DGX Spark ("Failed to read connection"); with
        # httpfs loaded, extension downloads go through its client, which has timeouts and retries
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("SET http_timeout = 600")
        con.execute("SET http_retries = 10")
        con.execute("INSTALL sirius")
        con.execute("LOAD sirius")
        con.execute("SET gpu_execution = true")
        print(f"LOAD sirius: {time.perf_counter() - start:.1f}s", flush=True)
    for t in TABLES:
        path = os.path.join(os.path.abspath(args.data), f"{t}.parquet")
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{path}')")
        if args.mode == "sirius" and args.pin != "none":
            # Columns are loaded into the tier lazily, on the first query that reads them
            con.execute(f"CALL pin_table('{path}', name = '{t}', tier = '{args.pin}')").fetchall()
    label = args.mode if args.pin == "none" else f"{args.mode}-pin-{args.pin}"

    os.makedirs(args.out, exist_ok=True)
    with open(os.path.join(args.out, f"{label}.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["mode", "query", "iteration", "runtime_s", "rows", "digest", "on_gpu"])
        for q in queries:
            sql = texts[q]
            records = []
            for it in range(1, args.iterations + 1):
                try:
                    elapsed, rows = run(con, sql, args.timeout)
                    records.append([q, it, f"{elapsed:.4f}", len(rows), digest(rows)])
                    print(f"[{label}] q{q} iter{it}: {elapsed:.3f}s", flush=True)
                except Exception as e:  # a failed or timed-out query must not stop the suite
                    records.append([q, it, "nan", 0, "error"])
                    print(f"[{label}] q{q} iter{it}: FAILED {str(e)[:200]}", flush=True)
            on_gpu = ""
            if args.mode == "sirius":
                con.execute("SET enable_duckdb_fallback = false")
                try:
                    run(con, sql, args.timeout)
                    on_gpu = "yes"
                except Exception as e:
                    on_gpu = "no"
                    print(f"[sirius] q{q} needs CPU fallback: {str(e)[:200]}", flush=True)
                finally:
                    con.execute("SET enable_duckdb_fallback = true")
            for r in records:
                w.writerow([label] + r + [on_gpu])
            f.flush()


if __name__ == "__main__":
    main()
