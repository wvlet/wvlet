"""Generate TPC-H tables as Parquet files with DuckDB's tpch extension.

Usage: python gen_tpch.py <scale factor> <output dir>
Writes <output dir>/<table>.parquet for the 8 TPC-H tables.
"""

import os
import sys
import time

import duckdb

TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: gen_tpch.py <scale factor> <output dir>")
    sf, out = sys.argv[1], sys.argv[2]
    os.makedirs(out, exist_ok=True)
    if all(os.path.exists(os.path.join(out, f"{t}.parquet")) for t in TABLES):
        print(f"TPC-H sf={sf} already exists in {out}")
        return
    # Generate into a disk-backed database: SF100 is ~100 GB raw and would not fit in memory
    db = os.path.join(out, "_dbgen.duckdb")
    for f in (db, db + ".wal"):
        if os.path.exists(f):
            os.remove(f)
    con = duckdb.connect(db)
    con.execute("INSTALL tpch")
    con.execute("LOAD tpch")
    start = time.perf_counter()
    con.execute(f"CALL dbgen(sf = {float(sf)})")
    print(f"dbgen sf={sf}: {time.perf_counter() - start:.1f}s")
    for t in TABLES:
        start = time.perf_counter()
        path = os.path.join(out, f"{t}.parquet")
        # Write to a temporary name first, so an interrupted run never looks complete
        con.execute(f"COPY {t} TO '{path}.tmp' (FORMAT PARQUET, ROW_GROUP_SIZE 1000000)")
        os.replace(f"{path}.tmp", path)
        print(f"  {t}: {time.perf_counter() - start:.1f}s")
    con.close()
    os.remove(db)


if __name__ == "__main__":
    main()
