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

## Dependency bumps

- `SCALA_VERSION` must be at least the Scala version uni is built with (3.9 since uni 2026.1.22): TASTy compatibility is one-way, so an older compiler cannot read uni's class files. Bump `UNI_VERSION` (in `build.sbt` and `project/plugin.sbt`) and `SCALA_VERSION` together.
- Bumping duckdb_jdbc also bumps the npm CLI's `@duckdb/node-bindings-*` pins; see [testing](testing.md#ratchets-and-gates). Bumping an engine dependency regenerates the stdlib catalogs; see [stdlib](stdlib.md).
