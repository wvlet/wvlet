# AGENTS.md

**Wvlet** is a cross-SQL, flow-style query language for functional data modeling and interactive data exploration. It compiles `.wv` query files into SQL for DuckDB, Trino, Snowflake, and Hive, or runs them on its in-process columnar engine. Stack: Scala 3 (JVM, Scala.js, Scala Native) built with sbt; pnpm workspaces for the UI, docs site, and editor plugins.

[`ARCHITECTURE.md`](ARCHITECTURE.md) is the system map: module map and the compiler, engine, and runner pipelines. [`design/`](design/README.md) holds one page per area with its contracts and gates; [`adr/`](adr/README.md) records decisions. Read the owning design page before changing an area. If a documented contract or gate changes, revise its page in the same PR using [`design/AGENTS.md`](design/AGENTS.md).

## Commands

- `./sbt` opens the sbt shell. `./sbt compile`, `./sbt test`.
- Format: `./sbt scalafmtAll` (CI runs `scalafmtCheck`). `scalafmtOnly` needs an absolute path, one file per call.
- Compile one platform: `./sbt "projectJVM/Test/compile"`, `projectJS/...`, `projectNative/...`.
- Module tests: `./sbt "langJVM/test"`, `"runnerJVM/test"`, `"engineJVM/test"`, `"langJS/test"`. One class: `./sbt "langJVM/testOnly *SqlParserTest"` (keep the quotes so the shell does not glob `*`).
- Spec files: `./sbt "runnerJVM/testOnly *RunnerSpecBasic -- spec:basic:hello.wv"` (wildcards like `query*.wv` work). More runners are in [testing](design/testing.md).
- Debug logs: append `-- -l debug`; SQL generator trace: `-L *GenSQL=trace`.
- CLI: `./sbt cli/packInstall` installs `wv` (`WvletREPLMain`) and `wvlet` (`WvletMain`) to `~/local/bin`. `./sbt cli/pack` (or `cli/packQuick`) builds under `wvlet-cli/target/pack/bin/`. After adding a `@command` method to a CLI command class, run `./sbt cli/clean` once: the `Launcher.of/addModule` macro is not re-expanded incrementally, so the new subcommand fails with "Unknown command" until a clean build.
- Server: `./sbt server/reStart` (stop with `server/reStop`).
- JS: install pnpm with `corepack enable` (the pinned version is `packageManager` in `package.json`), run `pnpm install` once, then `pnpm run ui`, `pnpm run playground`, `pnpm run build-ui`, `pnpm run build-playground`. Docs site: `pnpm --filter website run start` (localhost:3000) and `run build`. Jest packages: `pnpm --filter @wvlet/prismjs-wvlet test`.

## Workflow

- **Before a new task, start from the latest main:** `git switch -c <prefix>/<description>-$(date +"%Y%m%d_%H%M%S") origin/main` (in a worktree, branch from `origin/main`). Prefixes: `feature/`, `fix/`, `doc/`, `internal/`.
- **Commit messages** start with `feature:`, `fix:`, `internal:` (not user-facing), or `doc:`, and say why, not what: `feature: Add XXX to improve user experience`, not `feature: Add XXX class`.
- **PRs:** `gh pr create` following `.github/pull_request_template.md`, with linked issues (`Fixes #123`) and screenshots for UI changes. Fix format, compile, and test failures that CI reports. Read review comments with `gh api repos/wvlet/wvlet/pulls/<n>/comments` and check status with `gh pr checks <n>`. After merging, update the related issues, then switch to main and pull.
- **Never merge a PR or enable auto-merge without the user's explicit approval.** Ask after CI passes and reviews are addressed.
- **Before committing:** compilation passes for src/main, src/test, and Scala.js, and the code is formatted.
- **Plans** go in `plans/YYYY-MM-DD-(topic).md`. **ADRs:** add `adr/YYYY-MM-DD-(topic).md` for a non-obvious decision and list it in [`adr/README.md`](adr/README.md).
- Temporary files go under `target/`, which git ignores.
- Do not commit credentials. Reference environment variables in `profile.yml` as `${VAR_NAME}`.

## Code style

- Scala 3 new syntax; check `SCALA_VERSION`. No Scala 2 support. Scalafmt with a 100-column limit.
- Omit `new` (`StringBuilder()`). Always brace interpolated expressions: `${...}`.
- Document public classes, methods, and objects with Scaladoc.
- Avoid returning `Try[A]`; it forces monadic-style callers.
- Configuration case classes have `withXXX(...)` for every field and `noXXX()` for optional ones, e.g. `withTimeout(t)` sets `Some(t)` and `noTimeout()` sets `None`.
- Errors: throw `WvletLangException` with a `StatusCode`; add a code to `StatusCode` when none fits.
- Platform-specific code goes in `.jvm/`, `.js/`, or `.native/src/main/scala` behind an `XXXCompat` trait; Scala.js code must not use Java-only libraries. See [platforms](design/platforms.md).

## Tests

- Tests extend `wvlet.uni.test.UniTest` (`shouldBe`, `shouldContain`, `shouldBeTheSameInstanceAs`, `intercept[...]`, `shouldMatch`). Importing `wvlet.airspec.AirSpec` in wvlet-lang fails to compile; copy a neighboring test's imports.
- Test files end with `Test.scala` or `Spec.scala`. Name tests in plain English: `"should parse JSON with nested objects"`, not `"testParseJSON"`.
- Avoid mocks; they break when internals change.
- Cover new features and bug fixes. Add `.wv` cases under `spec/` when a behavior is easier to show as a query; SQL specs use `VALUES` instead of tables that do not exist.
- Use `shouldContain "(keyword)"` to check a string fragment.
- Requirements: JDK 17+ (JDK 25+ for Trino connector tests), Node.js 22+, sbt from `project/build.properties`.

## Docs

- New features update `website/docs`.
- When syntax, operators, CLI commands, or options change, update `website/docs/for-ai-agents.md` and the bundled skill in `plugins/wvlet/skills/wvlet/` in the same PR. See [agent docs](design/agent-docs.md).
- Write markdown docs as one line per paragraph; prefer bullets and links over restating.

## Where the details live

Read the matching page before changing these areas.

| Area | Read | Key rule |
| --- | --- | --- |
| Lean models | [`design/formal-verification.md`](design/formal-verification.md) | Changing `ItemStore`/`TaskQueue`/`BatchExecutor.recoverItems`, `TypeInference`, `TyperRules` operators, `ModelOrder`, `SQLBlock.accepts`, `RewriteExpr`, name lookup, or the engine's pushdown, NOT IN, join-key filters, `FactorOr`, or decorrelation: update the Lean model first, regenerate its fixture, run its conformance test |
| Engine | [`design/engine.md`](design/engine.md) | Put every change behind an `Optimization` toggle (listed in `OptimizerOptionsTest`); regenerate golden plans after an intended plan change; raise coverage ratchets, never lower them |
| Stdlib | [`design/stdlib.md`](design/stdlib.md) | After changing `wvlet-stdlib/module/standard/*.wv` or bumping an engine dependency, regenerate the DuckDB and Trino catalogs and the docs page |
| Testing | [`design/testing.md`](design/testing.md) | `TyperCoverageCheck` thresholds only go up; bump the npm CLI's DuckDB bindings with duckdb_jdbc |
| Platforms | [`design/platforms.md`](design/platforms.md) | Shared tests that need DuckDB skip themselves on Native; run `langNative/testOnly *DuckDB*` locally after changing native bindings |
| Connectors | [`design/connectors.md`](design/connectors.md) | Register new connectors through `ConnectorFactoryProvider`; main code never references a connector module's classes |
| Releasing | [`design/releasing.md`](design/releasing.md) | Tag version equals `vscode-wvlet/package.json`; bump `UNI_VERSION` and `SCALA_VERSION` together |
| Bundled skill | [`design/agent-docs.md`](design/agent-docs.md) | Syntax and CLI changes update `for-ai-agents.md` and the skill; `SkillSnippetTest` and `SkillFreshnessTest` gate them |
