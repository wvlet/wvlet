import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, it } from "node:test";
import {
  CHECK_INTERVAL_MS,
  compareVersions,
  isStale,
  isUpdateCheckDisabled,
  readCache,
  updateNotice,
  upgradeCommand,
  writeCache,
} from "../src/update.js";

describe("compareVersions", () => {
  it("orders calendar versions numerically", () => {
    assert.ok(compareVersions("2026.12.0", "2026.3.0") > 0);
    assert.ok(compareVersions("2026.3.10", "2026.3.9") > 0);
    assert.ok(compareVersions("2025.12.5", "2026.1.0") < 0);
    assert.equal(compareVersions("2026.3.0", "v2026.3.0"), 0);
  });

  it("sorts a release after its pre-releases", () => {
    assert.ok(compareVersions("2026.4.0", "2026.4.0-rc1") > 0);
    assert.ok(compareVersions("2026.4.0-rc2", "2026.4.0-rc10") < 0);
  });

  it("treats unparsable versions as equal", () => {
    assert.equal(compareVersions("abc", "2026.3.0"), 0);
    assert.equal(compareVersions(undefined, "2026.3.0"), 0);
  });
});

describe("updateNotice", () => {
  it("suggests wvlet upgrade when a newer release exists", () => {
    assert.equal(
      updateNotice("2026.3.0", "2026.4.0"),
      "wvlet 2026.4.0 is available (current 2026.3.0). Run 'wvlet upgrade' to update.",
    );
  });

  it("stays silent when current, newer, or unknown", () => {
    assert.equal(updateNotice("2026.4.0", "2026.4.0"), undefined);
    assert.equal(updateNotice("2026.5.0-rc1", "2026.4.0"), undefined);
    assert.equal(updateNotice("2026.4.0", undefined), undefined);
  });
});

describe("isStale", () => {
  const now = 1_800_000_000_000;
  it("refreshes a missing or day-old cache", () => {
    assert.equal(isStale({}, now), true);
    assert.equal(isStale({ checkedAt: now - CHECK_INTERVAL_MS }, now), true);
    assert.equal(isStale({ checkedAt: "garbage" }, now), true);
  });

  it("keeps a recent cache and distrusts a future timestamp", () => {
    assert.equal(isStale({ checkedAt: now - 1000 }, now), false);
    assert.equal(isStale({ checkedAt: now + 60_000 }, now), true);
  });
});

describe("isUpdateCheckDisabled", () => {
  const enabled = { env: {}, isTTY: true, version: "2026.3.0", packagePath: "/usr/lib/node_modules/@wvlet/cli" };

  it("runs for an interactive global install", () => {
    assert.equal(isUpdateCheckDisabled(enabled), false);
  });

  it("honors the opt-out and skips CI and non-TTY runs", () => {
    assert.equal(isUpdateCheckDisabled({ ...enabled, env: { WVLET_NO_UPDATE_CHECK: "1" } }), true);
    assert.equal(isUpdateCheckDisabled({ ...enabled, env: { CI: "true" } }), true);
    assert.equal(isUpdateCheckDisabled({ ...enabled, isTTY: false }), true);
  });

  it("skips dev builds and npx runs", () => {
    assert.equal(isUpdateCheckDisabled({ ...enabled, version: "0.0.0" }), true);
    const npx = path.join(path.sep, "home", "u", ".npm", "_npx", "abc", "node_modules", "@wvlet", "cli");
    assert.equal(isUpdateCheckDisabled({ ...enabled, packagePath: npx }), true);
  });
});

describe("cache file", () => {
  it("round-trips and tolerates a corrupt file", () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), "wvlet-update-"));
    const cachePath = path.join(dir, "nested", "update-check.json");
    assert.deepEqual(readCache(cachePath), {});
    writeCache({ checkedAt: 1, latest: "2026.4.0" }, cachePath);
    assert.deepEqual(readCache(cachePath), { checkedAt: 1, latest: "2026.4.0" });
    fs.writeFileSync(cachePath, "{not json");
    assert.deepEqual(readCache(cachePath), {});
    fs.rmSync(dir, { recursive: true, force: true });
  });
});

describe("upgradeCommand", () => {
  it("uses the package manager that owns the global install", () => {
    assert.deepEqual(upgradeCommand("/usr/local/lib/node_modules/@wvlet/cli"), {
      cmd: "npm",
      args: ["install", "-g", "--prefix", "/usr/local", "@wvlet/cli@latest"],
    });
    assert.deepEqual(upgradeCommand("C:\\Users\\u\\AppData\\Roaming\\npm\\node_modules\\@wvlet\\cli").args, [
      "install",
      "-g",
      "--prefix",
      "C:\\Users\\u\\AppData\\Roaming\\npm",
      "@wvlet/cli@latest",
    ]);
    // Not a recognizable global layout: let npm use its configured prefix
    assert.deepEqual(upgradeCommand("/opt/tools/wvlet").args, ["install", "-g", "@wvlet/cli@latest"]);
    assert.equal(
      upgradeCommand("/Users/u/Library/pnpm/global/5/.pnpm/@wvlet+cli@2026.3.0/node_modules/@wvlet/cli").cmd,
      "pnpm",
    );
    assert.equal(upgradeCommand("/Users/u/.bun/install/global/node_modules/@wvlet/cli").cmd, "bun");
  });
});
