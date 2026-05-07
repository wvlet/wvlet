# Cross-platform `wvlet-lang` core via `wvlet.uni.io`

Date: 2026-05-07

## Context

After the airframe → uni migration finished (final PR #1686), `wvlet-lang` ships as a `crossProject(JVMPlatform, JSPlatform, NativePlatform)` and 523 JS / 1378 Native tests already pass — parser, typer, codegen are platform-clean.

The only platform-specific pieces in `wvlet-lang` today are:

| Compat trait                    | JVM           | JS                              | Native        | Notes                                  |
| ------------------------------- | ------------- | ------------------------------- | ------------- | -------------------------------------- |
| `compiler.IOCompat`             | full impl     | mostly `???` / `false` / `Nil`  | full impl     | JVM/Native ~95% identical              |
| `compiler.WorkEnvCompat`        | full impl     | no-op                           | full impl     | JVM/Native ~95% identical              |
| `model.TreeNodeCompat`          | java.lang.reflect | scalajs.reflect             | scalanative.reflect | fundamentally per-platform     |
| `analyzer.DuckDBSchemaAnalyzer` | DuckDB JDBC   | throws `UnsupportedOperationException` | throws `UnsupportedOperationException` | JDBC binds JVM only |

Plus a JVM-only `catalog.Profile.scala` (config IO).

`uni 2026.1.9` ships `wvlet.uni.io.{IO, IOPath, FileInfo, Gzip, IOWatch, FileSystem}` with three backends:
`FileSystemJvm`, `FileSystemJS` (Node), `FileSystemNative`, plus `BrowserFileSystem` (in-memory, JS) selected automatically by `FileSystemInit` based on `IO.isBrowser` / `IO.isNode`. The JVM `IO` API is functionally a superset of what wvlet's `IOCompat` exposes.

## Direction

Collapse the duplicated `.jvm` / `.native` `IOCompat` and most of `WorkEnvCompat` into a single shared implementation in `wvlet-lang/src/main/scala`, backed by `wvlet.uni.io.IO`. This makes:

- **JS/Node a real platform** for the compiler — `existsFile`, `readAsString`, `listFiles`, `lastUpdatedAt` start working when running under Node, and remain harmless on Browser (uni's `BrowserFileSystem` returns empty/false instead of throwing).
- **Native and JVM share code** — drops ~150 lines of near-duplicate file-system glue.
- **One mental model** for file paths in the compiler (uni's `IOPath`).

## Plan (rough draft, not yet committed)

### Phase 1 — `IOCompat` consolidation

Move `SourceIO`'s file methods into shared `src/main/scala`, implemented via `wvlet.uni.io.IO`:

```scala
// shared replacement
object SourceIO extends LogSupport:
  def readAsString(filePath: String): String = IO.readString(IOPath.parse(filePath))
  def existsFile(p: String): Boolean         = IO.exists(IOPath.parse(p))
  def isDirectory(p: String): Boolean        = IO.isDirectory(IOPath.parse(p))
  def listFiles(p: String): List[String]     =
    IO.list(IOPath.parse(p)).map(_.path).toList
  def lastUpdatedAt(p: String): Long         =
    IO.info(IOPath.parse(p)).lastModified.map(_.toEpochMilli).getOrElse(0L)
  def readGzipAsString(p: String): String    =
    new String(Gzip.decompress(IO.readBytes(IOPath.parse(p))), UTF_8)
```

What stays platform-specific:

- `readAsString(uri: URI)` on JVM — opens `uri.toURL.openStream()`, uni doesn't ship URL fetching. Either drop (only one caller, `URIResource`, which is itself only used by JVM `listResources`), or move it to a JVM-only extension.
- `listResources(path)` JAR scanning — only the dead `CompilationUnit.fromResourcePath` calls this, which has zero callers in the entire repo. **Delete the method outright**.

### Phase 2 — `WorkEnvCompat` consolidation

`hasWvletFiles`, `saveToCache`, `loadCache` collapse into shared code via `IO.list` / `IO.writeString` / `IO.exists`. Only `initLogger` (which uses `LogRotationHandler`) remains JVM-specific because `wvlet.uni.log` doesn't ship a rotation handler on Native/JS — verify by inspecting the uni-log JS/Native jars before committing.

### Phase 3 — Dead-import cleanup

`VirtualFile.scala` and `CompilationUnit.scala` carry unused JVM imports (`java.io.File`, `URLClassLoader`, `JarFile`, `URI`, `URL`, `java.nio.file.{Files,Path}`) — strip them as part of the diff (Scala.js silently tolerates them today because they're unused, but they signal the wrong thing about cross-platform readiness).

### Phase 4 — Browser story (optional, follow-up)

Currently the playground compiler works because it never calls `SourceIO`. After Phase 1, browser-side file calls would route through uni's `BrowserFileSystem` (empty/no-op) rather than throwing. Worth verifying nothing in the playground accidentally starts depending on file I/O once it stops throwing.

## Out of scope

- `TreeNodeCompat` stays as 3 platform-specific files — different reflection runtimes, no uni shim helps.
- `DuckDBSchemaAnalyzer` stays JVM-only — no DuckDB on JS, no plan to add Native bindings to wvlet-lang.
- `wvlet-runner` and `wvlet-labs` stay JVM-only — they pull JDBC, Trino client, etc. The exploration is about `wvlet-lang` only.

## Estimated scope

- ~150 lines removed (duplicated `IOCompat` / `WorkEnvCompat`)
- ~60 lines added in shared `SourceIO` / `WorkEnv`
- 1 PR, low risk: every change is covered by the existing 1378 Native + 523 JS + 2000+ JVM tests.

## Open questions to resolve before committing

1. Does `wvlet.uni.log.LogRotationHandler` exist on JS / Native? (Last confirmed JVM-only.)
2. Does `IO.info(path)` throw or return a sentinel when the path is missing? — affects `lastUpdatedAt` semantics for non-existent files (today JVM returns from `Files.getLastModifiedTime`, which throws).
3. Are there callers that depend on `listResources` returning `URIResource` (jar:) entries? Grep says no, but worth a final sweep before deleting.

## Outcome (after implementation)

- Q1: `LogRotationHandler` is JVM-only. JS/Native `WorkEnvCompat.initLogger` stays a passthrough.
- Q2: Guarded with `IO.exists(p)` first; non-existent paths return `0L` cleanly.
- Q3: Confirmed zero callers — `URIResource`, `FileInResource`, `SourceFile.fromResource`, `CompilationUnit.fromResourcePath`, and `IOCompat.listResources` were all dead and got deleted.

### Shape change forced by uni 2026.1.9 link semantics

The original plan was to put `readAsString` / `existsFile` / etc. directly in shared `src/main/scala`
and have all three platforms call `wvlet.uni.io.IO.*`. This proved unworkable for the browser
playground:

- `IO.readBytes(p)` and `Gzip.decompress(...)` reach `FileSystemJS$` and `NodeZlibModule$`, which
  are declared with `@JSImport("os" | "fs" | "path" | "zlib", ...)`. Scala.js's reachability is
  conservative under `@EnableReflectiveInstantiation` (used by `TreeNodeCompat`), so a single
  reference from `JSONAnalyzer` is enough to pull the Node modules into the link graph.
- With `ModuleKind.NoModule` (langJS Test default) the linker rejects them outright; with
  `ModuleKind.ESModule` (playground) the imports succeed at link time but throw at module
  load time on the browser, because the browser has no built-in `os`/`fs`/`path`/`zlib`.

Resolution: keep `SourceIO`'s file-IO methods on the platform-specific `SourceIOCompat` trait.
JVM and Native carry near-identical bodies (~50 lines each, both backed by `wvlet.uni.io.IO`).
JS keeps the safe-stub bodies (`false` / `Nil` / `0L` / `???` for gzip), matching today's
runtime behavior for the playground. JVM/Native versions also call `FileSystemInit.init()` once
at trait load so consumers never see "FileSystem not initialized."

`WorkEnvCompat` keeps `initLogger` per-platform (LogRotationHandler is JVM-only) but routes its
`hasWvletFiles` / `saveToCache` / `loadCache` through `SourceIO.*` — so they pick up the right
backend automatically.

### What actually landed

- Removed `IOCompat.scala` from `.jvm`, `.js`, `.native` (-233 lines).
- Removed dead `URIResource`, `FileInResource`, `SourceFile.fromResource`,
  `CompilationUnit.fromResourcePath`, and stale `java.io.File` / `URLClassLoader` / `JarFile` /
  `java.nio.file.{Files, Path}` imports from shared code (-92 lines).
- Added `SourceIOCompat.scala` per platform (+~70 lines total): JVM/Native via `uni.io.IO`,
  JS as stubs.
- Folded `hasWvletFiles` / `saveToCache` / `loadCache` into shared `WorkEnv.scala` via
  `SourceIO.*`; only `isScalaJS` and `initLogger` remain in per-platform `WorkEnvCompat`.
- Net: -325 / +33 in old files, +3 small new SourceIOCompat files.

### What remains for a future PR

A real Node-targeted JS consumer (e.g. a Node-based wvlet CLI) is still unsupported because
landing it would require:

1. Setting `ModuleKind.CommonJSModule` or `ESModule` for that consumer's link config (not for
   langJS itself, which must stay browser-friendly).
2. A separate `NodeSourceIO` (or similar) that calls `FileSystemInit.init()` and replaces the
   stubbed JS methods with real `IO.*` calls.

This isn't blocked by uni — uni 2026.1.9 has all the pieces. It's a follow-up that needs a
target consumer to exist first.

## Why bother

- A Node-based wvlet CLI becomes feasible (compile `.wv` files outside the JVM).
- The browser playground stops being the only JS consumer that "just doesn't trigger file I/O" — the JS module becomes useful.
- One fewer compat layer to keep in sync as the compiler evolves.
