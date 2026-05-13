// Browser stub for the `koffi` FFI library. koffi only loads native .node addons on Node;
// in a browser context (the wvlet-ui-playground), it can never work and the bundler can't
// follow its dynamic `require("./build/koffi/<platform>/koffi.node")` calls anyway.
//
// `wvlet-lang.js`'s `DuckDBCompat` calls `Koffi.load(...)` lazily and wraps it in `Try(...)`,
// so throwing here cleanly lands as `DuckDB.isAvailable = false`. The playground never hits
// the DuckDB-backed compile path in practice (no `from 'foo.parquet'` style queries), so
// these stubs should never actually run.

export const load = () => {
  throw new Error("koffi is not available in the browser playground");
};

export const struct = () => null;
