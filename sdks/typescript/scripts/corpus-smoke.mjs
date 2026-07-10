#!/usr/bin/env node
// Full Wvlet LSP corpus smoke test.
//
// Sweeps EVERY real `.wv` file under spec/{basic,tpch,cdp_*,neg} and calls all
// LSP-facing WvletJS APIs (analyzeDiagnostics, getDocumentSymbols,
// getCompletionItems, getHover, getDefinition) at several derived cursor positions
// per file. Asserts every call returns valid JSON and never throws, records
// per-file timing, and flags any single call slower than SLOW_MS.
//
// Usage:
//   node sdks/typescript/scripts/corpus-smoke.mjs
//   pnpm --filter @wvlet/wvlet run corpus:smoke
//
// Requires the Scala.js bundle at sdks/typescript/lib/main.js to be built first
// (./sbt sdkJs/fastLinkJS). Exits non-zero if any call fails or exceeds SLOW_MS,
// so it can gate a manual/CI check.

import { WvletJS } from '../lib/main.js';
import { listCorpusFiles, readCorpusFile, runFile } from './corpus-lib.mjs';

const SLOW_MS = 2000;

// The compiler logs Typer/Context warnings (via process.stdout.write) while
// analyzing files that reference DuckDB tables/files absent from the JS catalog.
// That noise is expected here, so filter those lines out unless CORPUS_VERBOSE is
// set, keeping the report readable. Non-matching writes (including this script's
// own report) pass through untouched.
if (!process.env.CORPUS_VERBOSE) {
  const noise = /\bwarn \[(Typer|Context)\]/;
  const origWrite = process.stdout.write.bind(process.stdout);
  process.stdout.write = (chunk, ...rest) => {
    if (typeof chunk === 'string' && noise.test(chunk)) return true;
    return origWrite(chunk, ...rest);
  };
}

const files = listCorpusFiles();
const results = [];
const allErrors = [];
const slow = [];

const grandStart = performance.now();
for (const file of files) {
  const content = readCorpusFile(file);
  const before = performance.now();
  const res = runFile(WvletJS, file, content);
  const elapsed = performance.now() - before;
  results.push({ ...res, elapsed });
  allErrors.push(...res.errors);
  if (res.maxCall.ms > SLOW_MS) slow.push(res);
}
const grandMs = performance.now() - grandStart;

// Report ---------------------------------------------------------------------
const perFile = results.map((r) => ({ file: r.file, maxMs: r.maxCall.ms, elapsedMs: r.elapsed }));
perFile.sort((a, b) => b.maxMs - a.maxMs);

console.log('\n=== Wvlet LSP corpus smoke test ===');
console.log(`files:        ${files.length}`);
console.log(`wall time:    ${(grandMs / 1000).toFixed(1)}s`);
console.log(`errors:       ${allErrors.length}`);
console.log(`slow (>${SLOW_MS}ms single call): ${slow.length}`);

console.log('\nSlowest single call per file (top 10):');
for (const r of perFile.slice(0, 10)) {
  console.log(`  ${r.maxMs.toFixed(0).padStart(5)}ms   ${r.file}`);
}

if (allErrors.length > 0) {
  console.log('\nERRORS:');
  for (const e of allErrors.slice(0, 50)) {
    console.log(`  ${e.file}  ${e.api}@${e.pos}  ${e.message}`);
  }
}

if (slow.length > 0) {
  console.log('\nSLOW FILES:');
  for (const r of slow) {
    console.log(`  ${r.file}  slowest ${r.maxCall.ms.toFixed(0)}ms (${r.maxCall.api}@${r.maxCall.pos})`);
  }
}

const ok = allErrors.length === 0 && slow.length === 0;
console.log(`\nresult: ${ok ? 'PASS' : 'FAIL'}`);
process.exit(ok ? 0 : 1);
