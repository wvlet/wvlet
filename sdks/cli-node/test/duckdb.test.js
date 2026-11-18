import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { duckdbPackageNames, resolveBundledLibDuckDB } from "../src/duckdb.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const pkg = JSON.parse(fs.readFileSync(path.join(here, "..", "package.json"), "utf8"));

describe("duckdbPackageNames", () => {
  it("tries the musl package before the glibc one on Linux only", () => {
    assert.deepEqual(duckdbPackageNames("darwin", "arm64"), ["@duckdb/node-bindings-darwin-arm64"]);
    assert.deepEqual(duckdbPackageNames("linux", "x64"), [
      "@duckdb/node-bindings-linux-x64-musl",
      "@duckdb/node-bindings-linux-x64",
    ]);
    assert.deepEqual(duckdbPackageNames("win32", "x64"), ["@duckdb/node-bindings-win32-x64"]);
  });

  it("names only packages declared as optional dependencies", () => {
    for (const [platform, arch] of [
      ["darwin", "arm64"],
      ["darwin", "x64"],
      ["linux", "x64"],
      ["linux", "arm64"],
      ["win32", "x64"],
      ["win32", "arm64"],
    ]) {
      for (const name of duckdbPackageNames(platform, arch)) {
        assert.ok(pkg.optionalDependencies[name], name);
      }
    }
  });
});

describe("resolveBundledLibDuckDB", () => {
  const fakeRequire = (installed) => ({
    resolve: (id) => {
      if (id === `${installed}/package.json`) return `/nm/${installed}/package.json`;
      throw new Error(`Cannot find module '${id}'`);
    },
  });

  it("returns the shared library inside the platform package", () => {
    const lib = resolveBundledLibDuckDB({
      platform: "linux",
      arch: "x64",
      require: fakeRequire("@duckdb/node-bindings-linux-x64"),
    });
    assert.equal(lib, path.join("/nm/@duckdb/node-bindings-linux-x64", "libduckdb.so"));
  });

  it("finds the musl package on musl Linux", () => {
    const lib = resolveBundledLibDuckDB({
      platform: "linux",
      arch: "arm64",
      require: fakeRequire("@duckdb/node-bindings-linux-arm64-musl"),
    });
    assert.equal(lib, path.join("/nm/@duckdb/node-bindings-linux-arm64-musl", "libduckdb.so"));
  });

  it("returns undefined when the optional package is not installed", () => {
    const lib = resolveBundledLibDuckDB({ platform: "linux", arch: "x64", require: fakeRequire("other") });
    assert.equal(lib, undefined);
  });

  it("finds an existing library for the host after pnpm install", () => {
    const lib = resolveBundledLibDuckDB();
    assert.ok(lib, "the host's @duckdb/node-bindings package should be installed");
    assert.ok(fs.existsSync(lib), lib);
  });
});

describe("DuckDB version", () => {
  it("matches the DuckDB version of the JVM build", () => {
    // libduckdb is loaded through its C API, whose struct layouts change across DuckDB
    // releases; keep the npm bindings on the same DuckDB release as duckdb_jdbc.
    const buildSbt = fs.readFileSync(path.join(here, "..", "..", "..", "build.sbt"), "utf8");
    const jdbc = /DUCKDB_JDBC_VERSION\s*=\s*"([^"]+)"/.exec(buildSbt)?.[1];
    assert.ok(jdbc, "DUCKDB_JDBC_VERSION not found in build.sbt");
    const release = (v) => v.split(/[.-]/).slice(0, 3).join(".");
    const versions = new Set(Object.values(pkg.optionalDependencies));
    assert.equal(versions.size, 1, "all @duckdb/node-bindings-* packages must use one version");
    const [npmVersion] = versions;
    assert.equal(
      release(npmVersion),
      release(jdbc),
      `@duckdb/node-bindings ${npmVersion} vs duckdb_jdbc ${jdbc}: update sdks/cli-node/package.json`,
    );
  });
});
