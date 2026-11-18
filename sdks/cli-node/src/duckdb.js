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

/** True on Linux distributions using musl libc (e.g. Alpine), where glibc builds don't load. */
export function isMusl(report = process.report?.getReport?.()) {
  if (process.platform !== "linux") return false;
  return !report?.header?.glibcVersionRuntime;
}

/** The npm package name of DuckDB's prebuilt bindings for the given platform. */
export function duckdbPackageName(platform = process.platform, arch = process.arch, musl = isMusl()) {
  return `@duckdb/node-bindings-${platform}-${arch}${platform === "linux" && musl ? "-musl" : ""}`;
}

/**
 * Absolute path of the bundled libduckdb, or undefined when the platform package is not
 * installed (unsupported platform, or installed with --omit=optional).
 */
export function resolveBundledLibDuckDB({
  platform = process.platform,
  arch = process.arch,
  musl = isMusl(),
  require = createRequire(import.meta.url),
} = {}) {
  const libFile = LIB_FILE_NAMES[platform];
  if (!libFile) return undefined;
  try {
    const pkgJson = require.resolve(`${duckdbPackageName(platform, arch, musl)}/package.json`);
    return path.join(path.dirname(pkgJson), libFile);
  } catch {
    return undefined;
  }
}
