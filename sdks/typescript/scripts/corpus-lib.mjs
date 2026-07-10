// Shared helpers for the Wvlet LSP corpus smoke test.
//
// The corpus is the set of real `.wv` files committed under `spec/` in the repo.
// These helpers derive a handful of cursor positions per file and exercise every
// LSP-facing WvletJS API at each of them, so the full sweep (scripts/corpus-smoke.mjs)
// and the CI subset (tests/corpus.test.ts) can share one implementation.
//
// NOTE: the WvletJS bundle is loaded by the caller and passed in, so this module
// stays free of Scala.js coupling and can be imported from both a plain Node script
// and a vitest test.

import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

/** Absolute path to the repository root (three levels up from sdks/typescript/scripts). */
export const REPO_ROOT = fileURLToPath(new URL('../../../', import.meta.url));

/**
 * List the real `.wv` corpus files, sorted for determinism.
 * Covers core functionality (spec/basic), TPC-H (spec/tpch), CDP behavior
 * (spec/cdp_*), and intentionally-failing inputs (spec/neg).
 *
 * Implemented with readdirSync rather than fs.globSync so the harness works on
 * every Node version allowed by this package's `engines` field (>=20.0.0);
 * fs.globSync only exists in newer Node releases.
 */
export function listCorpusFiles() {
  const corpusDirs = readdirSync(REPO_ROOT + 'spec', { withFileTypes: true })
    .filter(
      (e) =>
        e.isDirectory() &&
        (e.name === 'basic' || e.name === 'tpch' || e.name === 'neg' || e.name.startsWith('cdp_'))
    )
    .map((e) => e.name);
  const files = [];
  for (const dir of corpusDirs) {
    for (const name of readdirSync(`${REPO_ROOT}spec/${dir}`)) {
      if (name.endsWith('.wv')) files.push(`spec/${dir}/${name}`);
    }
  }
  return files.sort();
}

/**
 * A small, representative subset of the corpus for CI. One or two files are picked
 * from each area so the vitest run stays fast while still spanning models, TPC-H,
 * multi-statement queries, CDP files, and negative (error) inputs.
 */
export function representativeSubset(files) {
  const pick = (prefix, n) => files.filter((f) => f.startsWith(prefix)).slice(0, n);
  return [
    ...pick('spec/basic/', 8),
    ...pick('spec/tpch/', 3),
    ...pick('spec/cdp_', 2),
    ...pick('spec/neg/', 3),
  ];
}

/** Read a corpus file's contents given a repo-relative path. */
export function readCorpusFile(relPath) {
  return readFileSync(REPO_ROOT + relPath, 'utf8');
}

/**
 * Derive a handful of cursor positions (1-based line/column) to probe for a file:
 * end of the first and last non-empty lines, the middle of the file, right after
 * each `from ` occurrence and on the identifier following it, and EOF.
 */
export function derivePositions(content) {
  const lines = content.split('\n');
  const seen = new Set();
  const out = [];
  const add = (line, column) => {
    if (line < 1 || column < 1) return;
    const key = `${line}:${column}`;
    if (seen.has(key)) return;
    seen.add(key);
    out.push({ line, column });
  };

  const nonEmpty = lines
    .map((text, i) => ({ line: i + 1, text }))
    .filter((e) => e.text.trim().length > 0);
  if (nonEmpty.length > 0) {
    const first = nonEmpty[0];
    const last = nonEmpty[nonEmpty.length - 1];
    const middle = nonEmpty[Math.floor(nonEmpty.length / 2)];
    add(first.line, first.text.length + 1); // end of first non-empty line
    add(last.line, last.text.length + 1); // end of last non-empty line
    add(middle.line, Math.max(1, Math.floor(middle.text.length / 2))); // middle
  }

  lines.forEach((text, i) => {
    const re = /\bfrom\s+/g;
    let m;
    while ((m = re.exec(text)) !== null) {
      const afterFrom = m.index + m[0].length; // 0-based offset just after 'from '
      add(i + 1, afterFrom + 1); // right after `from `
      const ident = text.slice(afterFrom).match(/^[A-Za-z_][\w.]*/);
      if (ident) {
        add(i + 1, afterFrom + Math.floor(ident[0].length / 2) + 1); // mid identifier
      }
    }
  });

  // EOF
  const lastLine = lines[lines.length - 1] ?? '';
  add(lines.length, lastLine.length + 1);
  return out;
}

/**
 * Exercise every LSP-facing WvletJS API against a file and return a result record.
 * Each API call must return valid JSON and must not throw; violations are collected
 * in `errors`. Per-call durations (ms) are recorded, and `maxCall` is the slowest.
 *
 * @param {object} WvletJS the Scala.js module exposing the LSP APIs
 * @param {string} relPath repo-relative file path (used only for reporting)
 * @param {string} content file contents
 */
export function runFile(WvletJS, relPath, content) {
  const errors = [];
  let maxCall = { ms: 0, api: '', pos: '' };

  const call = (api, pos, fn) => {
    const t0 = performance.now();
    let raw;
    let threw = false;
    try {
      raw = fn();
    } catch (e) {
      threw = true;
      errors.push({ file: relPath, api, pos, message: `threw: ${e?.message ?? e}` });
    }
    const ms = performance.now() - t0;
    if (ms > maxCall.ms) maxCall = { ms, api, pos };
    if (!threw) {
      try {
        JSON.parse(raw);
      } catch (e) {
        errors.push({ file: relPath, api, pos, message: `invalid JSON: ${e?.message ?? e}` });
      }
    }
    return ms;
  };

  call('analyzeDiagnostics', '-', () => WvletJS.analyzeDiagnostics(content));
  call('getDocumentSymbols', '-', () => WvletJS.getDocumentSymbols(content));

  for (const { line, column } of derivePositions(content)) {
    const pos = `${line}:${column}`;
    call('getCompletionItems', pos, () => WvletJS.getCompletionItems(content, line, column));
    call('getHover', pos, () => WvletJS.getHover(content, line, column));
    call('getDefinition', pos, () => WvletJS.getDefinition(content, line, column));
  }

  return { file: relPath, errors, maxCall };
}
