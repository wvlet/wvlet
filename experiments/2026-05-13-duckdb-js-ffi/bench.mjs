// Timing breakdown for the FFI path: koffi-load vs per-call vs warm-call.
import koffi from "koffi";

const t_processStart = process.hrtime.bigint();

const t0 = process.hrtime.bigint();
const lib = koffi.load("/opt/homebrew/lib/libduckdb.dylib");
const t1 = process.hrtime.bigint();

const result_struct = koffi.struct("duckdb_result", {
  a: "uint64_t", b: "uint64_t", c: "uint64_t",
  d: "void *", e: "char *", f: "void *",
});

const duckdb_open = lib.func("int duckdb_open(const char *p, _Out_ void **out)");
const duckdb_close = lib.func("void duckdb_close(_Inout_ void **db)");
const duckdb_connect = lib.func("int duckdb_connect(void *db, _Out_ void **out)");
const duckdb_disconnect = lib.func("void duckdb_disconnect(_Inout_ void **c)");
const duckdb_query = lib.func("int duckdb_query(void *c, const char *q, _Out_ duckdb_result *out)");
const duckdb_destroy_result = lib.func("void duckdb_destroy_result(_Inout_ duckdb_result *r)");
const duckdb_column_count = lib.func("uint64_t duckdb_column_count(duckdb_result *r)");
const duckdb_column_name = lib.func("const char *duckdb_column_name(duckdb_result *r, uint64_t i)");
const duckdb_column_type = lib.func("int duckdb_column_type(duckdb_result *r, uint64_t i)");
const t2 = process.hrtime.bigint();

function schemaOf(path) {
  const dbBox = [null];
  duckdb_open(null, dbBox);
  const db = dbBox[0];
  const conBox = [null];
  duckdb_connect(db, conBox);
  const con = conBox[0];
  const r = {};
  duckdb_query(con, `select * from '${path}' limit 0`, r);
  const n = duckdb_column_count(r);
  const cols = [];
  for (let i = 0n; i < n; i++) {
    cols.push({ name: duckdb_column_name(r, i), type: duckdb_column_type(r, i) });
  }
  duckdb_destroy_result(r);
  duckdb_disconnect([con]);
  duckdb_close([db]);
  return cols;
}

const t3 = process.hrtime.bigint();

const repo = "/Users/leo/work/wvlet/.claude/worktrees/glowing-swimming-moore";
const parquet = `${repo}/spec/cdp_behavior/data/weblog_users_first_purchased_at/part-00000-90048009-4be7-472e-ad1a-019aed6bd6fa-c000.snappy.parquet`;

// Cold call
const c0 = process.hrtime.bigint();
schemaOf(parquet);
const c1 = process.hrtime.bigint();

// Warm calls
const w0 = process.hrtime.bigint();
for (let i = 0; i < 100; i++) schemaOf(parquet);
const w1 = process.hrtime.bigint();

const ms = (a, b) => `${(Number(b - a) / 1e6).toFixed(2)}ms`;

console.log(`Node startup + koffi import:    ${ms(t_processStart, t0)}`);
console.log(`koffi.load(libduckdb.dylib):    ${ms(t0, t1)}`);
console.log(`Bind 9 C functions + struct:    ${ms(t1, t2)}`);
console.log(`Setup total (Node→ready):       ${ms(t_processStart, t3)}`);
console.log(`First schemaOf (cold parquet):  ${ms(c0, c1)}`);
console.log(`100 warm schemaOf calls:        ${ms(w0, w1)}  (${ms(w0, w1).replace('ms','')}/100 = avg ${(Number(w1 - w0) / 1e6 / 100).toFixed(2)}ms)`);
