import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import { duckdbPackageName, resolveBundledLibDuckDB } from "../src/duckdb.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const pkg = JSON.parse(fs.readFileSync(path.join(here, "..", "package.json"), "utf8"));

describe("duckdbPackageName", () => {
  it("adds the musl suffix only on musl Linux", () => {
    assert.equal(duckdbPackageName("darwin", "arm64", false), "@duckdb/node-bindings-darwin-arm64");
    assert.equal(duckdbPackageName("linux", "x64", false), "@duckdb/node-bindings-linux-x64");
    assert.equal(duckdbPackageName("linux", "arm64", true), "@duckdb/node-bindings-linux-arm64-musl");
    assert.equal(duckdbPackageName("win32", "x64", true), "@duckdb/node-bindings-win32-x64");
  });

  it("names a package declared as an optional dependency", () => {
    for (const [platform, arch, musl] of [
      ["darwin", "arm64", false],
      ["darwin", "x64", false],
      ["linux", "x64", false],
      ["linux", "arm64", false],
      ["linux", "x64", true],
      ["linux", "arm64", true],
      ["win32", "x64", false],
      ["win32", "arm64", false],
    ]) {
      assert.ok(pkg.optionalDependencies[duckdbPackageName(platform, arch, musl)], `${platform}-${arch}`);
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
      musl: false,
      require: fakeRequire("@duckdb/node-bindings-linux-x64"),
    });
    assert.equal(lib, path.join("/nm/@duckdb/node-bindings-linux-x64", "libduckdb.so"));
  });

  it("returns undefined when the optional package is not installed", () => {
    const lib = resolveBundledLibDuckDB({ platform: "linux", arch: "x64", musl: false, require: fakeRequire("other") });
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
