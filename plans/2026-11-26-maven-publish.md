# Publish wvlet's Scala artifacts to repo.wvlet.org

## Goals

- Java and Scala users can add one resolver and one dependency to use the Wvlet compiler and engine in their own JVM process (the `WvletEmbed` API of #2512), instead of cloning the repo and running `./sbt runnerJVM/publishLocal`.
- Every `v*` tag publishes the library modules, with no manual step, the same way uni does.

## Background

- Nothing publishes wvlet's Scala modules today: `release.yml` only builds the CLI tarballs and updates Homebrew. No wvlet version has ever been on Maven Central or repo.wvlet.org, under `wvlet.lang` or any other groupId.
- uni already publishes every tag to `https://repo.wvlet.org/maven2` (Cloudflare R2 bucket `wvlet-maven`, prefix `maven2/`) and copies a chosen version to Central by hand, because Central caps the `org.wvlet` namespace at about 7 releases and 78 MB a month, shared by uni and airframe (uni `adr/2026-11-18-wvlet-maven-repo.md`). That ADR already says the bucket and its scripts are meant to be shared.
- Central only accepts groupIds under a verified namespace; the wvlet org owns `org.wvlet` (uni is `org.wvlet.uni`, airframe `org.wvlet.airframe`). `wvlet.lang` would map to a `lang.wvlet` domain nobody owns.
- `runner` depends on `lang`, `engine`, `client` (all three on `api`) and, on the JVM, `connector`. DuckDB and Snowflake connectors are optional at runtime and are separate modules.
- Secrets: the wvlet repo has none of the release secrets uni uses. The org-level list could not be read with the current token, so the PR lists what the release job needs.

## Design

Lane: Standard — packaging and release are an always-gate area, and the change adds new published coordinates.

User flow:

- A user reads the JVM bindings page, adds `resolvers += "wvlet" at "https://repo.wvlet.org/maven2/"` (or the Maven `<repository>`), and depends on `org.wvlet.lang:wvlet-runner_3:<version>`. The page links to the repo's `maven-metadata.xml` for the newest version; a docs component that reads it, as uni's site does, is a follow-up.
- A maintainer pushes a `v*` tag as today. The new publish job stages signed artifacts, refuses to overwrite an existing version, uploads them, then rewrites `maven-metadata.xml`. A failed publish turns the job red and does not block the CLI release or Homebrew.
- Before the first real tag, a maintainer runs the Release workflow by hand (`dry_run`, default on). It stages and signs the bundle, runs `maven_repo.py check` and `metadata` against an empty local directory, and attaches `target/sona-staging` to the run as an Actions artifact, with no R2 access. From a tag ref it stages that tag's version; from a branch, a dynver `+N-sha` version.
- A dispatch with `dry_run` off is refused unless the ref is a `v*` tag, so a hand run can only re-publish a real tag (and the no-overwrite check still applies).

Decisions:

- **Modules:** `wvlet-api`, `wvlet-lang`, `wvlet-engine`, `wvlet-client` (gains `name := "wvlet-client"`; it would publish as `client_3` today), `wvlet-runner`, plus the JVM-only `wvlet-connector`, `wvlet-connector-duckdb`, `wvlet-connector-snowflake`. Apps (cli, server, httpServer, ui, uiMain, playground, engineGpu, wvc, wvcLib) get a new `skipPublish` that keeps them importable in IDEs; build-only and test-only projects keep `noPublish`. The job publishes a dedicated `projectPublish` aggregate of exactly the eight modules, so a project missing `noPublish` is never published by accident (`projectJVM` would include server and cli).
- **Platforms:** JVM only (user's choice). Scala.js and Native variants of the cross modules can be added later; they add CI time and nobody depends on them yet.
- **Coordinates:** move the groupId to `org.wvlet.lang`, so a later Central copy needs no rename and no user ever sees two groupIds. Package names stay `wvlet.lang.*`.
- **Versions:** tags as today (`2026.3.0`); `versionScheme := "pvp"` like uni, so `2026.3.x` is treated as one compatible line.
- **POM:** Apache-2.0 license, homepage `https://wvlet.org/wvlet/`, scm, and developers, as Central requires; sources and javadoc jars, with javadoc kept small like uni.
- **Signing:** add sbt-pgp (2.4.0, as uni, checked on wvlet's sbt 2.1.1) and run `publishSigned` into `target/sona-staging` (`publishTo := localStaging`), the same GPG key as uni. The job checks out with full history and tags so dynver yields the tag version.
- **Upload:** reuse uni's `maven_repo.py` (copied into `.github/scripts/`) for the no-overwrite check and metadata rebuild; artifacts first, metadata last with `no-cache`. Concurrent uni/airframe releases need no lock, as in uni's ADR.
- **Maven Central:** later. The shared monthly budget is already the bottleneck for uni; the coordinates are Central-ready, and adding uni's `central_version` dispatch is a follow-up.
- **Secrets:** reuse uni's org-level secrets (user's choice): `PGP_SECRET`, `PGP_PASSPHRASE`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_ACCOUNT_ID`. The dry run needs only the PGP pair.
- **Docs:** `website/docs/bindings/jvm.md` (added by #2512) switches its Installation section to the resolver and coordinates; this PR makes that small edit after #2512 lands, or states the dependency if this one lands first. `design/releasing.md` gains the gate: published modules, never overwrite a version, dry-run before changing publish settings. The release guide mentions the new job.
- **Check:** a local `projectPublish/publish` stages exactly the listed modules with full POMs (run once before the PR, described in the PR).

## Alternatives and Why Not?

- **Keep `wvlet.lang`.** Works on repo.wvlet.org, but Central would refuse it, forcing a groupId change after users adopt it.
- **Publish to Central on every tag.** Would eat the `org.wvlet` budget uni and airframe already exceed.
- **GitHub Packages.** Requires a token even for public reads, so users could not resolve it anonymously.
- **Dry run on every PR.** Signing needs secrets that fork PRs cannot read; a manual dispatch is enough for a path that changes rarely.
