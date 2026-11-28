# Releasing

This page owns versioning and the tag-driven release of every artifact. The step-by-step guide for maintainers is [the release guide](../website/docs/development/release.md); `project/release.rb` cuts the tag.

## Versioning

Wvlet uses semi-calendar versioning: `YYYY.(milestone month).(patch)`, e.g. `2026.3.0` is the first release of the 2026 month-3 milestone. The middle segment is the milestone month, not a semver minor; the patch increments within a milestone. The scheme applies to every artifact: Scala artifacts, npm packages, and the VS Code extension.

## Unified release cycle

Pushing a tag `vX.Y.Z` publishes all artifacts, including the npm packages and the VS Code extension. For the extension:

- The tag version must equal `vscode-wvlet/package.json`. Bump it, plus its CHANGELOG, in the release commit, or the publish job fails.
- A suffixed tag (e.g. `v2026.4.0-rc1`) publishes the extension as a Marketplace pre-release; plain tags publish stable.
- Versions already on the Marketplace are skipped, so re-running a tag pipeline is safe.
- Every build smoke-tests the packaged vsix over LSP stdio (`vscode-wvlet/scripts/lsp-smoke.mjs`) before it can be published. Never bypass this gate.
- Out-of-band extension releases run the "VS Code" workflow dispatch with `publish=true`; a patch other than 0 publishes as a pre-release.

The Python wheels (`.github/workflows/python-publish.yml`) package the newest `wvc-lib` artifact from a full-matrix `native.yml` run on main, which runs nightly or on dispatch (see [CI runner budget](testing.md#ci-runner-budget)). If `wvc-lib` or its dependencies changed since the last nightly run, run `gh workflow run native.yml --ref main` and wait for it to pass before pushing the tag.

## JVM CLI packages

Every `v*` tag uploads these packages to the GitHub release (`.github/workflows/release.yml`):

- `wvlet-cli-<version>.tar.gz` (lite) and `wvlet-cli-full-<version>.tar.gz` (with DuckDB and Snowflake): portable, they run on the user's JDK 17+.
- `wvlet-cli-<version>-<platform>.tar.gz` for `macos-arm64`, `macos-x64`, `linux-x64`, `linux-arm64`: the lite package plus a jlink'd Java 25 runtime in `runtime/` (about 95 MB in all). Homebrew installs these, so its formula needs no `openjdk` dependency, whose GUI libraries made up most of the install and upgrade time.
- `.github/scripts/add-jlink-runtime.sh` builds a platform package from the portable one. It runs on each target platform's own runner (the `platform_packages` matrix), so every runtime is smoke-tested where it runs. It holds the runtime's JDK module list.
- The script fails when jdeps finds a jar that needs a JDK module outside that list. The Scala.js job of `test.yml` runs it on every PR, so a new dependency that needs another module fails CI; add the module to the list.
- `build.sbt` (`useBundledRuntime`) patches the sbt-pack launchers to prefer `$PROG_HOME/runtime` over `JAVA_HOME` and `PATH`. The patch sets `JAVACMD` right before the launcher checks it, because sbt-pack's macOS branch sets `JAVACMD` to the first `java` on `PATH` and ignores `JAVA_HOME`. The script's smoke test checks `java.home`, so a launcher that falls back to another JDK fails the build.
- The homebrew job waits for every platform package before opening the formula PR in wvlet/homebrew-wvlet.

## Maven artifacts

Every `v*` tag publishes the JVM library modules to the wvlet Maven repository, `https://repo.wvlet.org/maven2` (Cloudflare R2 bucket `wvlet-maven`, shared with uni and airframe; see uni's `adr/2026-11-18-wvlet-maven-repo.md`). The `publish` job in `.github/workflows/release.yml` stages them with `projectPublish/publishSigned` into `target/sona-staging` and uploads that tree with `.github/scripts/maven_repo.py`.

- Coordinates are `org.wvlet.lang:<module>_3:<version>`, inside the verified `org.wvlet` namespace so a version can later be copied to Maven Central unchanged. Modules: `wvlet-api`, `wvlet-lang`, `wvlet-engine`, `wvlet-client`, `wvlet-runner`, `wvlet-connector`, `wvlet-connector-duckdb`, `wvlet-connector-snowflake`.
- Only the `projectPublish` aggregate in `build.sbt` is published. A new library module joins that list together with every module it depends on in compile scope; apps get `skipPublish`, build-only and test-only projects `noPublish`. A published POM must never depend in compile scope on an unpublished module.
- A version already in the bucket is never overwritten (`maven_repo.py check`); artifacts go up before `maven-metadata.xml`. To redo a broken release, delete its version directories from the bucket first.
- Before changing publish settings or the module list, run the Release workflow by hand with `dry_run` (the default) and check the attached `sona-staging` artifact: module list, POM dependencies, signatures. A dispatch with `dry_run` off is refused unless the ref is a `v*` tag. Re-dispatching an already published tag with `dry_run` off re-runs the CLI release, and its publish job fails on the no-overwrite check, as intended.
- Secrets: `PGP_SECRET` and `PGP_PASSPHRASE` (the dry run needs only these), `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ACCOUNT_ID`; the same as uni's.
- `.github/scripts/maven_repo.py` is a copy of uni's; change both together.
- Maven Central is not published yet; uni's `central_version` dispatch is the model when it is.

## Dependency bumps

- `SCALA_VERSION` must be at least the Scala version uni is built with (3.9 since uni 2026.1.22): TASTy compatibility is one-way, so an older compiler cannot read uni's class files. Bump `UNI_VERSION` (in `build.sbt` and `project/plugin.sbt`) and `SCALA_VERSION` together.
- Bumping duckdb_jdbc also bumps the npm CLI's `@duckdb/node-bindings-*` pins; see [testing](testing.md#ratchets-and-gates). Bumping an engine dependency regenerates the stdlib catalogs; see [stdlib](stdlib.md).
