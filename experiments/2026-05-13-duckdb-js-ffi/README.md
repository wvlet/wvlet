# DuckDB-on-Node feasibility experiment

Quick spike to validate calling `libduckdb` synchronously from Node via the
[koffi](https://www.npmjs.com/package/koffi) FFI library — bypassing
[`@duckdb/node-api`](https://www.npmjs.com/package/@duckdb/node-api)'s Promise-only API.

Findings are in [`plans/2026-05-13-duckdb-js-ffi.md`](../../plans/2026-05-13-duckdb-js-ffi.md).

## Run it

```bash
brew install duckdb          # macOS — provides /opt/homebrew/lib/libduckdb.dylib
mkdir -p /tmp/duckdb-feas && cd /tmp/duckdb-feas
npm init -y && npm install koffi
cp <repo>/experiments/2026-05-13-duckdb-js-ffi/{experiment,bench}.mjs .

node experiment.mjs   # schemaOf a parquet + a JSON
node bench.mjs        # cold/warm timing breakdown
```

Expected output of `bench.mjs` on Apple Silicon:

```
koffi.load(libduckdb.dylib):    ~19ms
Bind 9 C functions + struct:    ~0.5ms
First schemaOf (cold parquet):  ~80ms
Warm avg (100 calls):           ~5ms
```

## Files

- `experiment.mjs` — first-cut driver. Mirrors the same ~9 C functions we bound on
  Scala Native in `wvlet-lang/.native/.../duckdb/DuckDBApi.scala`. Proves
  end-to-end: load lib → open in-memory DB → query parquet → read columns.
- `bench.mjs` — timing breakdown for setup vs cold call vs warm calls. Used to
  validate that the per-compile schema-inference overhead is acceptable.

Both scripts hardcode the repo path for the parquet fixture — adjust if you
clone elsewhere.
