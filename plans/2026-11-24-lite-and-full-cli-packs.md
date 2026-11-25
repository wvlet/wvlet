# Lite and full JVM CLI packs

## Goals
- Ship the JVM CLI without the 172 MB of optional JDBC drivers (DuckDB 78 MB, Snowflake 94 MB) by default, and keep a full CLI for users who need DuckDB or Snowflake.
- PR 2 of the optional-connector split ([ADR](../adr/2026-11-23-optional-connector-modules.md)); PR 1 (#2341) added the modules, the ServiceLoader registry, and the install error.

## Background
- `cli.dependsOn(server, connectorDuckDB, connectorSnowflake)` still bundles both modules, so the pack's `lib/` is 228 MB.
- The install error already tells users to get `wvlet-cli-full-<version>.tar.gz`.
- The Homebrew formula in wvlet/homebrew-wvlet downloads `wvlet-cli-<version>.tar.gz`; release.yml only bumps its `VERSION` file.

## Design
- Lane: Standard, pre-approved on 2026-11-23 (design in the work item; merge authorization in the goal's Gates). Packaging is an always-gate area, covered by that approval.
- `cli` (`wvlet-cli`) becomes the lite pack: it depends on `server` only, with the optional connectors in Test scope so its tests keep the DuckDB oracle.
- New sbt project `cliFull` (`wvlet-cli-full`) depends on `cli`, `connectorDuckDB`, and `connectorSnowflake`, and shares the pack settings (launchers, JVM options, web UI assets). Its tarball is `wvlet-cli-full-<version>.tar.gz` with the same `wv` / `wvlet` commands.
- release.yml builds and uploads both tarballs to the same release; CI packages both so a broken full pack fails before a release.
- Docs: the install page gains a lite/full row and the full package; `for-ai-agents.md` and the skill say DuckDB and Snowflake need the full CLI. `cli/packInstall` stays the dev install; `cliFull/packInstall` installs the full CLI.
- Homebrew `wvlet-full` formula is a follow-up in wvlet/homebrew-wvlet (not pushed from here), and must land before the next release so `brew` users can opt in.

## Alternatives and Why Not?
- Connector add-on tarball unpacked into `lib/`: rejected in the ADR (manual version matching, `brew upgrade` replaces `lib/`).
