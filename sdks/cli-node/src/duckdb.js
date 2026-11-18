// Locates the libduckdb shared library shipped in DuckDB's official per-platform npm package
// (`@duckdb/node-bindings-<os>-<arch>[-musl]`), which @wvlet/cli declares as an optional
// dependency. npm installs only the package matching the host, so `wvlet run` works without
// a system-wide DuckDB install.
import { createRequire } from "node:module";
import path from "node:path";

const LIB_FILE_NAMES = {
  darwin: "libduckdb.dylib",
  linux: "libduckdb.so",
  win32: "duckdb.dll",
};

/**
 * Candidate npm package names of DuckDB's prebuilt bindings for the platform. On Linux, npm
 * installs either the musl or the glibc package (by the host's libc), so both are tried; this
 * avoids detecting the libc on every start.
 */
export function duckdbPackageNames(platform = process.platform, arch = process.arch) {
  const base = `@duckdb/node-bindings-${platform}-${arch}`;
  return platform === "linux" ? [`${base}-musl`, base] : [base];
}

/**
 * Absolute path of the bundled libduckdb, or undefined when no platform package is installed
 * (unsupported platform, or installed with --omit=optional).
 */
export function resolveBundledLibDuckDB({
  platform = process.platform,
  arch = process.arch,
  require = createRequire(import.meta.url),
} = {}) {
  const libFile = LIB_FILE_NAMES[platform];
  if (!libFile) return undefined;
  for (const name of duckdbPackageNames(platform, arch)) {
    try {
      return path.join(path.dirname(require.resolve(`${name}/package.json`)), libFile);
    } catch {
      // Not installed; try the next candidate
    }
  }
  return undefined;
}
