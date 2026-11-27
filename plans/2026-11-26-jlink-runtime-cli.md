# Bundle a jlink'd Java runtime in the JVM CLI packages

## Goals

- `brew install wvlet/wvlet/wvlet` and `brew upgrade wvlet` download one package (about 96 MB) instead of openjdk@25 plus about 30 formulas it depends on.
- Keep the JVM CLI, since its JIT-compiled engine is the fastest way to run Wvlet.

## Background

- The formula in wvlet/homebrew-wvlet has `depends_on "openjdk@25"`. Homebrew's openjdk is built from source against brew's GUI libraries (cairo, harfbuzz, fontconfig, glib, libx11/xcb, freetype, giflib…). The CLI needs none of them, but every install and every upgrade of those formulas goes through brew.
- jdeps over the lite pack's 43 jars needs these modules: `java.base, java.logging, java.management, java.naming, java.net.http, java.security.jgss, java.sql, java.transaction.xa, java.xml, jdk.unsupported`, plus `java.desktop`, which only the PostgreSQL driver needs, and `jdk.jfr`, which Netty 4.2 needs for its JFR events.
- A jlink runtime with those modules plus `jdk.charsets` and `jdk.zipfs` (loaded as services) is 89 MB on disk; the whole package is 96 MB gzipped. The lite CLI ran a query on it locally (JDK 25, macOS arm64).
- sbt-pack's launcher ignores `JAVA_HOME` on macOS and runs the first `java` on `PATH`. So the launcher has to set `JAVACMD` after that branch, not only `JAVA_HOME`.

## Design

Lane: Standard. This changes packaging, which always needs review, but the user already approved the direction in chat.

- **Packages:** each release adds `wvlet-cli-<version>-<os>-<arch>.tar.gz` for macos-arm64, macos-x64, linux-x64 and linux-arm64. Each one is the portable `wvlet-cli-<version>.tar.gz` plus a `runtime/` folder. The portable package and the full package (`wvlet-cli-full`) stay as they are, with no runtime.
- **Launcher:** when `$PROG_HOME/runtime/bin/java` exists, `wvlet` and `wv` use it ahead of the user's `JAVA_HOME` and `PATH`. The bundled runtime is the tested one, and it doesn't depend on whichever JDK happens to be installed. Without `runtime/`, behaviour is unchanged.
- **Building the runtime:** one script, `.github/scripts/add-jlink-runtime.sh`, takes the portable tarball and the target JDK and writes the platform tarball. The release workflow and local builds both use it. The module list lives in that script.
- **Release workflow:** a matrix job runs after the existing release job, on native runners (macos-15, macos-15-intel, ubuntu-latest, ubuntu-24.04-arm) with Temurin 25. Each runner downloads the portable tarball, runs the script, runs a smoke query, and uploads the result. The homebrew job waits for the matrix.
- **Catching a missing module:** the PR workflow already packs the CLI in its Scala.js job. Add a step there that builds the Linux runtime package and runs a smoke query on it. The script also runs jdeps and fails when a jar needs a JDK module that isn't in the list. A new dependency that needs another module then fails CI instead of failing on a user's machine.
- **Homebrew:** a follow-up PR in wvlet/homebrew-wvlet switches the formula to per-platform URLs and removes `depends_on "openjdk@25"`. It can only land after a release that publishes the new packages.
- **Docs:** `design/releasing.md` gets a section on the CLI packages and the module list; `website/docs/usage/install.md` lists the platform packages and says Homebrew no longer installs OpenJDK.

## Alternatives and Why Not?

- **`depends_on "openjdk"` (unversioned):** it has the same dependency tree, so it only helps people who already have it installed.
- **GraalVM native-image:** it loses the JIT, which is what makes the JVM engine fast.
- **jlink in an sbt task:** each platform would need its own sbt and pnpm build. Running jlink on the finished tarball keeps that build to one runner.
- **Cross-linking all platforms on one runner with each target's jmods:** it saves runners, but the runtime would never be executed on its target platform before release.
