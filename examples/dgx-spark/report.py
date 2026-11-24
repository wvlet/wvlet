"""Summarize the <result dir>/*.csv files of bench_duckdb.py as Markdown tables.

Usage: python report.py <result dir>
Modes: cpu (the baseline), then every other CSV in the directory (sirius, sirius-pin-gpu, ...).
The first table has the best hot runtime per query and mode, with the speedup over cpu. A mark
after a GPU mode's time flags a problem: `*` means the query fell back to the CPU, `!` means its
result differs from cpu's. The second table has the first (cold) runs.
"""

import csv
import glob
import math
import os
import sys
from collections import defaultdict


def load(path):
    runs = defaultdict(list)
    with open(path) as f:
        for r in csv.DictReader(f):
            runs[int(r["query"])].append(r)
    return runs


def times(rs):
    ts = [float(r["runtime_s"]) for r in sorted(rs, key=lambda r: int(r["iteration"]))]
    if not ts:
        return math.nan, math.nan
    hot = [t for t in ts[1:] if math.isfinite(t)] or [ts[0]]
    return ts[0], min(hot)


def fmt(t):
    return "—" if not math.isfinite(t) else f"{t:.3f}"


def main():
    out = sys.argv[1]
    names = sorted(os.path.basename(p)[:-4] for p in glob.glob(os.path.join(out, "*.csv")))
    modes = (["cpu"] if "cpu" in names else []) + [n for n in names if n != "cpu"]
    runs = {m: load(os.path.join(out, f"{m}.csv")) for m in modes}
    queries = sorted(set().union(*[set(r) for r in runs.values()]))

    def table(title, pick):
        print(f"### {title}\n")
        header = ["query"] + [f"{m} (s)" for m in modes]
        header += [f"{m} speedup" for m in modes if m != "cpu" and "cpu" in runs]
        print("| " + " | ".join(header) + " |")
        print("|---|" + "---:|" * (len(header) - 1))
        total = defaultdict(float)
        for q in queries:
            t = {m: pick(times(runs[m].get(q, []))) for m in modes}
            cells = [f"q{q}"]
            for m in modes:
                rs = runs[m].get(q, [])
                mark = ""
                if m != "cpu" and rs:
                    if rs[0]["on_gpu"] == "no":
                        mark += "*"
                    base = {r["digest"] for r in runs.get("cpu", {}).get(q, [])}
                    if base and {r["digest"] for r in rs} != base:
                        mark += "!"
                cells.append(fmt(t[m]) + mark)
                if math.isfinite(t[m]):
                    total[m] += t[m]
            for m in modes:
                if m != "cpu" and "cpu" in runs:
                    ok = math.isfinite(t["cpu"]) and math.isfinite(t[m]) and t[m] > 0
                    cells.append(f"{t['cpu'] / t[m]:.2f}x" if ok else "—")
            print("| " + " | ".join(cells) + " |")
        cells = ["**total**"] + [f"{total[m]:.2f}" for m in modes]
        for m in modes:
            if m != "cpu" and "cpu" in runs:
                cells.append(f"**{total['cpu'] / total[m]:.2f}x**" if total[m] > 0 else "—")
        print("| " + " | ".join(cells) + " |\n")

    table("Hot (best of iterations 2..n)", lambda t: t[1])
    table("Cold (iteration 1)", lambda t: t[0])


if __name__ == "__main__":
    main()
