# Migrate from npm to pnpm

Date: 2026-05-03
Branch: `internal/migrate-npm-to-pnpm-20260503_154007`
Status: in progress

## Goal

Replace npm with pnpm as the package manager for all JavaScript/TypeScript
workspaces in the repository, while keeping user-facing install instructions
(`npm install @wvlet/...`) unchanged so that downstream consumers are not
forced onto pnpm.

## Why pnpm

- Faster, content-addressable installs.
- Strict dependency hoisting catches phantom-dependency bugs that npm hides.
- Better workspace ergonomics (`pnpm --filter` is more predictable than the
  npm `--workspace` flag, especially for nested scripts).
- Lockfile is deterministic across platforms; smaller diffs than npm's
  `package-lock.json` (often 100s of KB of churn).

## Scope

### Workspaces and standalone packages

Before:

| Path                  | npm role              | npm lockfile        |
|-----------------------|-----------------------|---------------------|
| `./` (root)           | npm workspace root    | yes                 |
| `wvlet-ui-main`       | npm workspace member  | no (uses root)      |
| `wvlet-ui-playground` | npm workspace member  | no (uses root)      |
| `vscode-wvlet`        | npm workspace member  | yes (extra, unused) |
| `website`             | standalone            | yes                 |
| `highlightjs-wvlet`   | standalone (publish)  | yes                 |
| `prismjs-wvlet`       | standalone (publish)  | yes                 |
| `sdks/typescript`     | standalone (publish)  | yes                 |

After (final): everything lives in a single pnpm workspace.

| Path                  | pnpm role             |
|-----------------------|-----------------------|
| `./` (root)           | pnpm workspace root   |
| `wvlet-ui-main`       | workspace member      |
| `wvlet-ui-playground` | workspace member      |
| `vscode-wvlet`        | workspace member      |
| `website`             | workspace member      |
| `highlightjs-wvlet`   | workspace member      |
| `prismjs-wvlet`       | workspace member      |
| `sdks/typescript`     | workspace member      |

Why one workspace instead of mirroring the previous standalone layout:
pnpm walks up to find `pnpm-workspace.yaml`, so a "standalone" subdir
inside a workspace root requires `--ignore-workspace` everywhere or per-dir
opt-out files. A single workspace with one root `pnpm-lock.yaml` is the
simpler and more pnpm-idiomatic setup. Each package still publishes
independently from its subdirectory.

### Files to change

1. **`package.json` (root)** — drop `workspaces` array, add `packageManager`
   field, rewrite scripts from `npm run X --workspace=Y` to
   `pnpm --filter Y run X`.
2. **`pnpm-workspace.yaml`** (new) — list workspace globs.
3. **`pnpm-lock.yaml`** (new, in each project) — replace
   `package-lock.json`.
4. **`.npmrc`** (new) — set `auto-install-peers=true` and possibly other
   pnpm-specific settings if needed.
5. **`build.sbt`** — replace the two `Process(List("npm", ...))` calls in the
   `cli/pack` task with `pnpm` invocations.
6. **GitHub Actions workflows** — install pnpm via `pnpm/action-setup`,
   switch to `pnpm install --frozen-lockfile` / `pnpm run`, change
   `setup-node` cache from `npm` to `pnpm` and update
   `cache-dependency-path` to `pnpm-lock.yaml`.
   - `.github/workflows/test.yml` (Scala.js + Playground jobs)
   - `.github/workflows/test-doc.yml`
   - `.github/workflows/website.yml`
   - `.github/workflows/highlightjs.yml`
   - `.github/workflows/prismjs.yml`
   - `.github/workflows/vscode-extension.yml`
   - `.github/workflows/npm-publish.yml`
   - `.github/workflows/typescript-test.yml`
   - `.github/workflows/release.yml` (drops the unused `cache: npm` block;
     no npm steps actually run there)
7. **Internal developer docs** — switch dev commands to pnpm.
   - `CLAUDE.md`, `GEMINI.md`, `AGENTS.md`
   - `vscode-wvlet/README.md`, `vscode-wvlet/BUILD.md`,
     `vscode-wvlet/DEVELOPMENT.md`
   - `sdks/typescript/PUBLISHING.md`
   - `website/docs/development/build.md`,
     `website/docs/development/syntax-highlighting.md`
   - `wvlet-ui-main/README.md`
8. **Dependabot** — `package-ecosystem: "npm"` already auto-detects
   `pnpm-lock.yaml`, so no manifest change is required, but we will verify by
   reading the docs.

### Files left alone

- **End-user install snippets** in publish READMEs:
  - `highlightjs-wvlet/README.md`
  - `prismjs-wvlet/README.md`
  - `sdks/typescript/README.md`
  - `website/docs/usage/install.md`
  - `website/docs/bindings/typescript.md`
  - `website/docs/bindings/index.md`

  These contain `npm install @wvlet/wvlet`-style commands aimed at downstream
  consumers. We keep them on `npm` (the universal default) and optionally add
  a `pnpm add` alternative where it already exists.

## Approach details

### pnpm version pinning

Use `corepack`-aware pinning via `packageManager` in the root `package.json`:

```json
"packageManager": "pnpm@10.7.1"
```

CI workflows enable corepack via `pnpm/action-setup@v4` with `version: 10`,
which honors this field.

### Workspace catalog

`pnpm-workspace.yaml`:

```yaml
packages:
  - 'wvlet-ui-main'
  - 'wvlet-ui-playground'
  - 'vscode-wvlet'
```

We do not add the standalone packages (website, highlightjs-wvlet,
prismjs-wvlet, sdks/typescript) to the workspace because they are published
independently with their own lockfiles and their own CI jobs.

### Lockfile generation

1. Run `pnpm install` at repo root → single `pnpm-lock.yaml` covering all
   7 workspace members.
2. Delete every `package-lock.json` file.

### Risks and mitigations

- **vsce packaging with pnpm**: vsce had trouble with pnpm's symlinked
  `node_modules`. We added `--no-dependencies` to `vsce package`/`vsce
  publish` since esbuild already bundles every JS dep into `out/`.
- **Hoisting differences**: pnpm's strict resolution exposed two phantom
  dependencies that npm's flat hoisting had been silently providing:
  - `prismjs` — required by `website/src/theme/prism-include-languages.ts`
    via `require('prismjs/components/...')`. Added explicitly to
    `website/package.json`.
  - The Docusaurus version pin had to relax from `3.10.0` to `^3.10.0`
    because pnpm picked `3.10.1` for transitive `@docusaurus/*` packages
    and Docusaurus enforces version equality across its own modules.
- **Per-package `pnpm.overrides` are ignored**: pnpm only honors
  `pnpm.overrides` at the workspace root. The `serialize-javascript`,
  `uuid`, `webpackbar`, `dompurify`, and `diff` overrides previously
  scattered in `website/`, `highlightjs-wvlet/`, and root were consolidated
  into the root `package.json`.
- **Unapproved build scripts**: pnpm 10 refuses to run lifecycle scripts
  unless explicitly allow-listed. Listed `esbuild`, `keytar`,
  `@vscode/vsce-sign`, `core-js`, and `unrs-resolver` in
  `pnpm.onlyBuiltDependencies` at the root.
- **Engineer onboarding**: every developer now needs pnpm. Updating
  `CLAUDE.md`, `GEMINI.md`, and `AGENTS.md` is the documented onboarding
  path. The `packageManager` field plus corepack means a fresh clone on a
  recent Node.js (20+) auto-installs the correct pnpm.
- **Dependabot**: behavior should be unchanged once `pnpm-lock.yaml`
  exists. If we see updates fail, we may need to set
  `versioning-strategy: "increase"` or similar tweaks.

## Verification

- `pnpm install` succeeds at root and in each standalone project.
- `pnpm run build-playground` produces `wvlet-ui-playground/dist/`.
- `pnpm run build-ui` produces `wvlet-ui-main/dist/`.
- `pnpm --filter vscode-wvlet run package` builds the .vsix.
- `pnpm test` passes in highlightjs-wvlet and prismjs-wvlet.
- `pnpm run build` succeeds in website (Docusaurus).
- `./sbt "cli/pack"` still works (build.sbt changes wired up correctly).
- CI passes on the PR.

## Rollback

If pnpm causes problems we cannot resolve quickly:

1. Restore deleted `package-lock.json` files from git history.
2. Restore `workspaces` field in root `package.json`.
3. Revert script changes (root `package.json`, `build.sbt`).
4. Revert workflow changes.

The migration is a single PR; revert is a single PR.
