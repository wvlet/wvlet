# VS Code Extension Development

## Important Version Synchronization

When updating VS Code related dependencies, ensure that:
- `engines.vscode` version in package.json matches the `@types/vscode` version
- Both should be updated together to avoid CI build failures

Example:
```json
{
  "engines": {
    "vscode": "^1.103.0"  // Must match @types/vscode version
  },
  "devDependencies": {
    "@types/vscode": "^1.103.0"  // Must match engines.vscode version
  }
}
```

## Build and Test

```bash
# Package the extension
pnpm --filter wvlet run package
```

## LSP Corpus Smoke Test

The language features (diagnostics, document symbols, completion, hover,
go-to-definition) are provided by the Scala.js `WvletJS` bundle in
`sdks/typescript`. A corpus smoke test exercises every LSP API against the real
`.wv` files under `spec/{basic,tpch,cdp_*,neg}` to make sure no input crashes the
providers, every response is valid JSON, and each call stays responsive (< 2s).

```bash
# 1. Build the Scala.js bundle (writes sdks/typescript/lib/main.js).
#    NOTE: fastLinkJS clears sdks/typescript/lib/, which also holds the
#    hand-written main.d.ts and README.md — back them up and restore afterwards,
#    then confirm `git status` shows no deletions.
./sbt sdkJs/fastLinkJS

# 2. Full sweep over all ~150 corpus files (standalone script; exits non-zero on
#    any error or slow call). Set CORPUS_VERBOSE=1 to see the compiler's own
#    Typer/Context warnings, which are otherwise filtered out.
pnpm --filter @wvlet/wvlet run corpus:smoke

# 3. CI subset runs automatically as part of the vitest suite.
pnpm --filter @wvlet/wvlet run test:ci
```

The full sweep lives in `sdks/typescript/scripts/corpus-smoke.mjs`; the CI subset
is `sdks/typescript/tests/corpus.test.ts`. Both share position-derivation and
per-file assertion logic from `sdks/typescript/scripts/corpus-lib.mjs`.

Diagnostics that report unknown tables or missing files for specs that reference
DuckDB tables/files are **expected** in this environment: the Scala.js bundle runs
with an empty catalog, so those sources cannot be resolved.

## CI Requirements

The CI will verify that `@types/vscode` version is compatible with the declared `engines.vscode` version. If there's a mismatch, the build will fail with an error message indicating the version incompatibility.