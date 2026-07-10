import { describe, it, expect } from 'vitest';
// The LSP APIs are exposed directly on the Scala.js WvletJS module used by the LSP server.
import { WvletJS } from '../lib/main.js';
import {
  listCorpusFiles,
  representativeSubset,
  readCorpusFile,
  runFile,
} from '../scripts/corpus-lib.mjs';

// CI subset of the real `.wv` corpus. The full sweep (all ~150 files) lives in
// scripts/corpus-smoke.mjs; here we exercise a representative subset spanning
// spec/basic (models, types, multi-statement), spec/tpch (large queries),
// spec/cdp_* (CDP behavior), and spec/neg (intentionally-failing inputs) so the
// vitest run stays fast enough for CI.
//
// The contract we assert for every file: every LSP-facing WvletJS API
// (analyzeDiagnostics, getDocumentSymbols, getCompletionItems, getHover,
// getDefinition), called at several derived cursor positions, returns valid JSON
// and never throws. Unresolved-table/file diagnostics are EXPECTED on the JS
// bundle (the catalog is empty), so we do not assert on diagnostic contents here.

// A single LSP call should stay responsive; 2s is a generous ceiling.
const SLOW_MS = 2000;

const subset = representativeSubset(listCorpusFiles());

describe('WvletJS LSP corpus smoke test', () => {
  it('has a non-empty representative subset', () => {
    expect(subset.length).toBeGreaterThan(5);
  });

  it.each(subset)('handles %s without throwing or invalid JSON', (file) => {
    const content = readCorpusFile(file);
    const { errors, maxCall } = runFile(WvletJS, file, content);
    expect(errors, JSON.stringify(errors, null, 2)).toEqual([]);
    expect(
      maxCall.ms,
      `slowest call ${maxCall.api}@${maxCall.pos} took ${maxCall.ms.toFixed(0)}ms`
    ).toBeLessThan(SLOW_MS);
  });
});
