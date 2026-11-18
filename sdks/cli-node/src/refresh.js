// Detached background process started by update.js: fetch the latest @wvlet/cli version from
// the npm registry and store it in the update-check cache. Failures are silent; the attempt
// time was already recorded by the parent, so the next check happens a day later.
import { fetchLatestVersion, readCache, writeCache } from "./update.js";

const cachePath = process.argv[2];
try {
  const latest = await fetchLatestVersion();
  writeCache({ ...readCache(cachePath), checkedAt: Date.now(), latest }, cachePath);
} catch {
  // Offline or registry unavailable: keep the previous cache
}
