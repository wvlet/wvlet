// Update notice and `wvlet upgrade` for the npm distribution of the wvlet CLI.
//
// The notice never blocks a command on the network: it is printed from a cached "latest
// version" in ~/.wvlet/update-check.json, and a stale cache (older than a day) is refreshed by
// a detached background process (refresh.js) for the next run.
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const PACKAGE_NAME = "@wvlet/cli";
export const REGISTRY_LATEST_URL = "https://registry.npmjs.org/@wvlet/cli/latest";
export const CHECK_INTERVAL_MS = 24 * 60 * 60 * 1000;

const packageRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

export function defaultCachePath() {
  return path.join(os.homedir(), ".wvlet", "update-check.json");
}

/** The installed @wvlet/cli version, read fresh from disk (so it reflects a just-run upgrade). */
export function installedVersion(root = packageRoot) {
  try {
    return JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8")).version;
  } catch {
    return undefined;
  }
}

// wvlet uses YYYY.(milestone).(patch) with an optional pre-release suffix such as `-rc1`.
function parseVersion(v) {
  const m = /^v?(\d+)\.(\d+)\.(\d+)(?:-(.+))?$/.exec(String(v ?? "").trim());
  if (!m) return undefined;
  return { nums: [Number(m[1]), Number(m[2]), Number(m[3])], pre: m[4] };
}

/** Compare two versions: negative if a < b, 0 if equal, positive if a > b. Unparsable → 0. */
export function compareVersions(a, b) {
  const va = parseVersion(a);
  const vb = parseVersion(b);
  if (!va || !vb) return 0;
  for (let i = 0; i < 3; i++) {
    if (va.nums[i] !== vb.nums[i]) return va.nums[i] - vb.nums[i];
  }
  // A release sorts after its pre-releases (2026.4.0-rc1 < 2026.4.0)
  if (va.pre === vb.pre) return 0;
  if (va.pre === undefined) return 1;
  if (vb.pre === undefined) return -1;
  return va.pre.localeCompare(vb.pre, "en", { numeric: true });
}

/** True when the update check must not run: opted out, CI, non-interactive, or a dev build. */
export function isUpdateCheckDisabled({
  env = process.env,
  isTTY = process.stderr.isTTY,
  version,
  packagePath = packageRoot,
} = {}) {
  if (env.WVLET_NO_UPDATE_CHECK) return true;
  if (env.CI) return true;
  if (!isTTY) return true;
  // Unbuilt or source checkouts carry the 0.0.0 placeholder. npx runs are already up to date,
  // and a project dependency is updated through the project, not `wvlet upgrade`.
  if (!version || version === "0.0.0") return true;
  return installKind(packagePath) !== "global";
}

/**
 * How this copy was installed: "npx" (npx cache), "project" (a dependency in some project's
 * node_modules), or "global" (npm/pnpm/bun global install; the default).
 */
export function installKind(packagePath, exists = fs.existsSync) {
  const parts = packagePath.split(/[\\/]/);
  if (parts.includes("_npx")) return "npx";
  // pnpm and bun keep their global packages in a directory with its own package.json
  const isPnpmGlobal = parts.some((p, i) => (p === "pnpm" || p === ".pnpm") && parts[i + 1] === "global");
  const isBunGlobal = parts.some((p, i) => p === ".bun" && parts[i + 1] === "install" && parts[i + 2] === "global");
  if (isPnpmGlobal || isBunGlobal) return "global";
  const nm = parts.indexOf("node_modules");
  if (nm > 0) {
    const owner = parts.slice(0, nm).join(path.sep) || path.sep;
    if (exists(path.join(owner, "package.json"))) return "project";
  }
  return "global";
}

export function readCache(cachePath = defaultCachePath()) {
  try {
    const data = JSON.parse(fs.readFileSync(cachePath, "utf8"));
    return typeof data === "object" && data !== null ? data : {};
  } catch {
    return {};
  }
}

export function writeCache(cache, cachePath = defaultCachePath()) {
  try {
    fs.mkdirSync(path.dirname(cachePath), { recursive: true });
    fs.writeFileSync(cachePath, JSON.stringify(cache, null, 2) + "\n");
  } catch {
    // Best-effort: an unwritable home directory only means the next run checks again
  }
}

export function isStale(cache, now = Date.now()) {
  const checkedAt = Number(cache.checkedAt);
  return !Number.isFinite(checkedAt) || now - checkedAt >= CHECK_INTERVAL_MS || checkedAt > now;
}

/** The one-line notice to show, or undefined when the installed version is current. */
export function updateNotice(current, latest) {
  if (!latest || compareVersions(latest, current) <= 0) return undefined;
  return `wvlet ${latest} is available (current ${current}). Run 'wvlet upgrade' to update.`;
}

/**
 * Called once per CLI run (not for `upgrade`). Returns the notice from the cached result, and
 * starts a background refresh of the cache when it is stale.
 */
export function checkForUpdate({ cachePath = defaultCachePath(), now = Date.now() } = {}) {
  const current = installedVersion();
  if (isUpdateCheckDisabled({ version: current })) return undefined;
  const cache = readCache(cachePath);
  if (isStale(cache, now)) {
    // Record the attempt first so concurrent runs don't each spawn a refresh
    writeCache({ ...cache, checkedAt: now }, cachePath);
    spawnRefresh(cachePath);
  }
  return updateNotice(current, cache.latest);
}

function spawnRefresh(cachePath) {
  try {
    const child = spawn(process.execPath, [path.join(packageRoot, "src", "refresh.js"), cachePath], {
      detached: true,
      stdio: "ignore",
      windowsHide: true,
    });
    child.on("error", () => {});
    child.unref();
  } catch {
    // Ignore: the notice is a convenience
  }
}

/** Fetch the `latest` dist-tag version from the npm registry (pre-releases are never `latest`). */
export async function fetchLatestVersion({ url = REGISTRY_LATEST_URL, timeoutMs = 5000 } = {}) {
  const res = await fetch(url, {
    signal: AbortSignal.timeout(timeoutMs),
    headers: { accept: "application/json" },
  });
  if (!res.ok) throw new Error(`npm registry returned HTTP ${res.status}`);
  const body = await res.json();
  if (typeof body.version !== "string") throw new Error("npm registry response has no version");
  return body.version;
}

/**
 * The global install command for the package manager that owns this installation, judged from
 * where the running package lives.
 */
export function upgradeCommand(packagePath = realPackageRoot()) {
  const parts = packagePath.split(/[\\/]/);
  const target = `${PACKAGE_NAME}@latest`;
  if (parts.includes(".bun")) return { cmd: "bun", args: ["add", "-g", target] };
  if (parts.some((p) => p === "pnpm" || p === ".pnpm")) return { cmd: "pnpm", args: ["add", "-g", target] };
  // Upgrade the same global prefix this copy lives in, even when another npm comes first on
  // PATH (e.g. after switching Node versions): <prefix>/lib/node_modules/@wvlet/cli on POSIX,
  // <prefix>\node_modules\@wvlet\cli on Windows
  const n = parts.length;
  if (n >= 4 && parts[n - 3] === "node_modules" && parts[n - 2] === "@wvlet" && parts[n - 1] === "cli") {
    const sep = packagePath.includes("\\") ? "\\" : "/";
    const prefixParts = parts.slice(0, parts[n - 4] === "lib" ? n - 4 : n - 3);
    return { cmd: "npm", args: ["install", "-g", "--prefix", prefixParts.join(sep) || sep, target] };
  }
  return { cmd: "npm", args: ["install", "-g", target] };
}

function realPackageRoot() {
  try {
    return fs.realpathSync(packageRoot);
  } catch {
    return packageRoot;
  }
}

/** `wvlet upgrade`: reinstall the latest release with the owning package manager. */
export function runUpgrade({ cachePath = defaultCachePath() } = {}) {
  const before = installedVersion();
  const root = realPackageRoot();
  const kind = installKind(root);
  if (kind === "npx") {
    console.error(`This wvlet runs through npx. Run 'npx ${PACKAGE_NAME}@latest' to use the latest release.`);
    return 1;
  }
  if (kind === "project") {
    console.error(`This wvlet is a project dependency. Update ${PACKAGE_NAME} in that project's package.json instead.`);
    return 1;
  }
  if (!before || before === "0.0.0") {
    console.error("This wvlet is a development build. Rebuild it from source to update.");
    return 1;
  }
  const { cmd, args } = upgradeCommand(root);
  const quoted = args.map(quoteArg);
  const manual = `${cmd} ${quoted.join(" ")}`;
  console.error(`Running: ${manual}`);
  // npm, pnpm, and bun are .cmd shims on Windows, which spawn can only start through a shell.
  // The shell joins arguments with spaces, so paths with spaces must be quoted.
  const useShell = process.platform === "win32";
  const result = spawnSync(cmd, useShell ? quoted : args, { stdio: "inherit", shell: useShell });
  if (result.error || result.status !== 0) {
    console.error(`Upgrade failed. Run it manually: ${manual}`);
    return result.status || 1;
  }
  const after = installedVersionAfterUpgrade(cmd, root);
  writeCache({ checkedAt: Date.now(), latest: after }, cachePath);
  if (after && compareVersions(after, before) > 0) {
    console.log(`wvlet upgraded: ${before} -> ${after}`);
  } else {
    console.log(`wvlet ${after ?? before} is up to date`);
  }
  return 0;
}

/** Double-quote an argument containing whitespace, for display and for Windows shells. */
export function quoteArg(arg) {
  return /\s/.test(arg) ? `"${arg}"` : arg;
}

/**
 * The version installed by the upgrade. pnpm installs each version into its own folder, so
 * the folder this process runs from still holds the old version; look up pnpm's global root.
 */
function installedVersionAfterUpgrade(cmd, root) {
  if (cmd === "pnpm") {
    const r = spawnSync("pnpm", ["root", "-g"], { encoding: "utf8", shell: process.platform === "win32" });
    const globalRoot = r.status === 0 ? r.stdout.trim() : "";
    if (globalRoot) {
      const v = installedVersion(path.join(globalRoot, "@wvlet", "cli"));
      if (v) return v;
    }
  }
  return installedVersion(root);
}
