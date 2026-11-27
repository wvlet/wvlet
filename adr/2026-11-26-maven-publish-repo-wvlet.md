# ADR: Publish the JVM library modules to repo.wvlet.org as org.wvlet.lang

Date: 2026-11-26 · PR #2521

## Context

- Nothing published wvlet's Scala modules, so embedding the engine (`WvletEmbed`, #2512) meant cloning the repository and running `publishLocal`.
- uni already publishes every tag to `https://repo.wvlet.org/maven2` (Cloudflare R2) and copies selected versions to Maven Central by hand, because Central caps the shared `org.wvlet` namespace per month (uni `adr/2026-11-18-wvlet-maven-repo.md`).
- The build's groupId was `wvlet.lang`, which maps to no domain the project owns, so Central would refuse it. No version had ever been published under it.

## Decision

- Every `v*` tag publishes the JVM library modules to repo.wvlet.org with the same bucket, scripts, and no-overwrite rule as uni. Maven Central comes later.
- The groupId becomes `org.wvlet.lang`, inside the verified `org.wvlet` namespace, so a later Central copy needs no rename. Package names stay `wvlet.lang.*`.
- Only the `projectPublish` aggregate is published: api, lang, engine, client, runner, and the three connectors, JVM only. A list of what to publish, rather than `noPublish` on everything else, means a new app or test project is never published by accident.
- A manual `dry_run` dispatch stages and signs the bundle and attaches it to the run without R2 access; it is how publish-setting changes are checked before a tag.

## Consequences

- Java and Scala users add the repo.wvlet.org resolver and depend on `org.wvlet.lang:wvlet-runner_3`.
- Adding a library module means adding it, and its compile-scope dependencies, to `publishedProjects` (gate in `design/releasing.md`).
- Scala.js and Scala Native artifacts are not published; adding them is a list change plus CI time.
- `.github/scripts/maven_repo.py` is a copy of uni's and must change with it.
