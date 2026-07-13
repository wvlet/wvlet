// Packaging guard for the @wvlet/wvlet npm package (#1894).
//
// Runs as prepublishOnly: fails the publish when the package would ship without the
// optimized Scala.js bundle, the hand-written type declarations, or the compiled
// TypeScript output. Publishing requires an explicit `pnpm run build:release` first —
// this script never rebuilds, so whatever is verified here is exactly what ships.

import { readFileSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const pkgDir = join(dirname(fileURLToPath(import.meta.url)), '..');
const errors = [];

function requireFile(relPath, description) {
  const path = join(pkgDir, relPath);
  if (!existsSync(path)) {
    errors.push(`${relPath} is missing (${description})`);
    return null;
  }
  return path;
}

// 1. The Scala.js bundle must exist and be the fullLinkJS (optimized) output.
//    The sbt link tasks record the link mode in lib/.link-mode alongside the bundle
//    (the marker is rewritten on every link, so it always describes the current bundle).
const bundlePath = requireFile('lib/main.js', 'Scala.js bundle — run `pnpm run build:release`');
const markerPath = requireFile('lib/.link-mode', 'link-mode marker written by the sbt link tasks');
if (bundlePath && markerPath) {
  const linkMode = readFileSync(markerPath, 'utf8').trim();
  if (linkMode !== 'full') {
    errors.push(
      `lib/main.js is the '${linkMode}' (dev) bundle — run \`pnpm run build:release\` (sdkJs/fullLinkJS) before publishing`
    );
  }
}

// 2. The hand-written type declarations must have survived the link
//    (the linker owns lib/ and deletes unknown files; sbt copies them back from types/).
const dtsPath = requireFile('lib/main.d.ts', 'hand-written WvletJS type declarations');
if (dtsPath) {
  const dts = readFileSync(dtsPath, 'utf8');
  if (!dts.includes('WvletJS')) {
    errors.push('lib/main.d.ts does not declare the WvletJS surface');
  }
}
requireFile('lib/README.md', 'lib README');

// 3. The compiled TypeScript wrapper must exist.
requireFile('dist/index.js', 'compiled TypeScript output — run `pnpm run build:release`');
requireFile('dist/index.d.ts', 'TypeScript declarations');

if (errors.length > 0) {
  console.error('Package verification failed:');
  for (const e of errors) {
    console.error(`  - ${e}`);
  }
  process.exit(1);
}
console.log('Package verification passed: optimized bundle, type declarations, and dist/ are present.');
