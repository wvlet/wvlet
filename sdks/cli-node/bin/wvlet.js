#!/usr/bin/env node
// Entry point of the @wvlet/cli npm package.
//
// - `wvlet upgrade` is handled here (it is specific to the npm distribution).
// - Every other command runs the Scala.js CLI bundle: importing `lib/main.js` triggers the main
//   module initializer, which reads `process.argv` and dispatches through the wvlet launcher.
// - The libduckdb from the host's `@duckdb/node-bindings-*` package is passed to the bundle
//   via WVLET_BUNDLED_LIBDUCKDB, so `wvlet run` works without a system DuckDB.
// - After a successful command, a one-line update notice may be printed to stderr.
import { resolveBundledLibDuckDB } from "../src/duckdb.js";
import { checkForUpdate, runUpgrade } from "../src/update.js";

const args = process.argv.slice(2);

if (args[0] === "upgrade") {
  process.exit(runUpgrade());
}

const isHelp = args.length === 1 && (args[0] === "-h" || args[0] === "--help");

if (!process.env.WVLET_BUNDLED_LIBDUCKDB) {
  const lib = resolveBundledLibDuckDB();
  if (lib) process.env.WVLET_BUNDLED_LIBDUCKDB = lib;
}

let notice;
try {
  notice = checkForUpdate();
} catch {
  // The update check must never break a command
}

// beforeExit fires once the command's work (including async remote runs) is done, so the notice
// follows the output. It does not fire on process.exit(), which the CLI uses for errors.
let printed = false;
process.on("beforeExit", () => {
  if (printed) return;
  printed = true;
  if (notice && !process.exitCode) process.stderr.write(`\n${notice}\n`);
});

await import("../lib/main.js");

if (isHelp) {
  process.stdout.write("\n[npm commands]\n  upgrade    Upgrade wvlet to the latest npm release\n");
}
