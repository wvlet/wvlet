# ADR: Bundle a jlink'd Java runtime in per-platform JVM CLI packages

Date: 2026-11-26 · PR #2538

## Context

- The Homebrew formula installed the lite JVM CLI with `depends_on "openjdk@25"`. Homebrew's openjdk is built against about 30 GUI library formulas (cairo, harfbuzz, fontconfig, glib, libx11/xcb, freetype…), which made up most of the time `brew install wvlet` and `brew upgrade` took. The CLI uses none of them.
- The JVM CLI stays the recommended way to run the engine, because its JIT makes the engine fastest. GraalVM native-image would give that up.
- The lite CLI's jars need 14 JDK modules (jdeps, plus two loaded as services). A jlink runtime with them is about 90 MB on disk, and the whole package is 96 MB gzipped.

## Decision

- **Per-platform packages:** each release adds `wvlet-cli-<version>-<platform>.tar.gz` for macos-arm64, macos-x64, linux-x64 and linux-arm64: the portable lite package plus a jlink'd Java 25 runtime in `runtime/`. The portable and full packages don't change.
- **jlink runs on the finished tarball,** on each platform's own GitHub runner (`.github/scripts/add-jlink-runtime.sh`). This avoids an sbt and pnpm build per platform. It also smoke-tests each runtime where it runs, which cross-linking with another platform's jmods would not.
- **The bundled runtime wins** over the user's `JAVA_HOME` and `PATH`. It's the runtime the release tested, and the package no longer depends on which JDK happens to be installed. The launcher patch sets `JAVACMD` and leaves the `PATH` that child processes see unchanged.
- **The module list is checked, not inferred at release time.** The script fails when jdeps errors or reports a JDK module that isn't in the list. The PR workflow runs it, so a dependency that needs a new module fails CI.

## Consequences

- Homebrew can drop the openjdk dependency once a release publishes these packages. That's a follow-up in wvlet/homebrew-wvlet.
- Adding a dependency that uses another JDK module means adding the module to `MODULES` in the script. Modules loaded only through `ServiceLoader` aren't reported by jdeps, so they have to be listed by hand.
- Windows has no platform package and still needs a JDK.
