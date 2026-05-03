# Plan: Drop airframe deps from `wvlet-lang`

Part of the broader airframe → uni migration tracked in
`plans/2026-05-03-airframe-http-server-to-uni-migration.md`. That plan
is server-side focused; this plan is the language-module slice that has
no server code in it but still listed three airframe artifacts in
`build.sbt`.

## 0. Finding

`wvlet-lang/src` (main + test) contains **zero** `wvlet.airframe.*`
imports — every previously-airframe symbol has already been swapped to
`wvlet.uni.*`. The three airframe declarations in `build.sbt`'s `lang`
block —

```
"org.wvlet.airframe" %% "airframe"        % AIRFRAME_VERSION
"org.wvlet.airframe" %% "airframe-config" % AIRFRAME_VERSION
"org.wvlet.airframe" %% "airframe-ulid"   % AIRFRAME_VERSION
```

— are unused at the source level. They survive only because
`airframe-config` transitively brings in `org.yaml:snakeyaml`, which
`Profile.scala` calls directly:

```
new org.yaml.snakeyaml.Yaml().load(yamlStringEvaluated)
```

## 1. Scope

In scope:
- Remove the three airframe declarations from `lang` in `build.sbt`.
- Move `Profile.scala` from shared sources
  (`wvlet-lang/src/main/scala/wvlet/lang/catalog/Profile.scala`) to
  the JVM-specific source tree
  (`wvlet-lang/.jvm/src/main/scala/wvlet/lang/catalog/Profile.scala`)
  — it was the only file pulling in JVM-only `org.yaml.snakeyaml`,
  and `ProfileTest` was already in `.jvm/src/test`.
- Add `org.yaml:snakeyaml` as a JVM-scoped dep via `.jvmSettings`,
  pinned to `2.5` to match what airframe-config was previously pulling
  (`airframe/build.sbt:28`).
- Add a `SNAKE_YAML_VERSION` constant alongside the other version
  pins in `build.sbt`.

Out of scope:
- Touching the `httpServer`, `server`, `runner`, `cli`, or `client`
  modules — they have their own airframe deps that the umbrella
  migration will address per its phase plan.
- Doing the same JVM-scoping cleanup for `duckdb_jdbc`, which has
  the same shared-libraryDependencies leak. Pre-existing tech debt;
  worth a follow-up but conflating it here would expand scope.

## 2. Why move `Profile.scala`, not just keep snakeyaml shared

Two options were considered for `Profile.scala`:

- **A. Leave `Profile.scala` in shared sources, add snakeyaml to the
  shared `libraryDependencies`.** Minimal-risk, behavior-preserving.
  But it leaves a JVM-only artifact on the langJS/langNative compile
  classpath — exactly what airframe-config had been quietly doing
  via transitive resolution. The repo styleguide
  (`CLAUDE.md` / `GEMINI.md`, `.gemini/styleguide.md`) says
  "Platform specific code needs to be placed in .jvm/src/main/scala…"
  and "In Scala.js code, avoid using Java-specific libraries".
- **B. Move `Profile.scala` to `.jvm/src/main/scala`, scope the
  snakeyaml dep via `.jvmSettings`** (chosen, after Gemini's review
  of the initial PR). Aligns with the style guide. Cheap because
  the lang module already has `.jvm/.js/.native` source trees with
  Compat shims, no JS/Native consumer of `Profile` exists in the
  repo (verified with grep across lang/ui/uiMain/playground/sdkJs),
  and `ProfileTest` was already JVM-only.

The first version of this PR shipped with Option A. Gemini's review
(see PR #1667) flagged it as a style-guide violation; switched to
Option B in the same PR rather than carrying forward the leak.

## 3. Migration steps

1. Edit `build.sbt`:
   - add `val SNAKE_YAML_VERSION = "2.5"` next to the other version
     constants;
   - in the `lang` project's `libraryDependencies`, drop the three
     airframe entries (no replacement — uni already provides what
     `wvlet-lang/src` actually uses);
   - append `.jvmSettings(libraryDependencies += "org.yaml" %
     "snakeyaml" % SNAKE_YAML_VERSION)` to the lang crossProject so
     snakeyaml is JVM-scoped only.
2. `git mv wvlet-lang/src/main/scala/wvlet/lang/catalog/Profile.scala
   wvlet-lang/.jvm/src/main/scala/wvlet/lang/catalog/Profile.scala`.
3. Verify `langJVM/Test/compile`, `langJS/Test/compile`,
   `langNative/Test/compile`.
4. Verify aggregates: `projectJVM/Test/compile`,
   `projectJS/Test/compile`, `projectNative/Test/compile` — to catch
   any downstream module that was relying on lang's transitive
   airframe pull.
5. Run `langJVM/test` to make sure runtime behavior is unchanged
   (Profile YAML reader, etc.).
6. `scalafmtAll` (no source file changes are expected, but run
   anyway).

## 4. Verification (run before opening PR)

Already executed locally on this branch:

- `./sbt langJVM/Test/compile` — green.
- `./sbt langJS/Test/compile` — green.
- `./sbt langNative/Test/compile` — green.
- `./sbt projectJVM/Test/compile` — green (server, runner, cli,
  http-server, spec, client.jvm, testUtil, api.jvm).
- `./sbt projectJS/Test/compile` — green.
- `./sbt projectNative/Test/compile` — green.
- `./sbt langJVM/test` — 1397 tests, 1393 passed, 4 pending
  (pre-existing).

CI on the PR will repeat these and add formatting checks.

## 5. Risks

- **Snakeyaml major-version drift.** Pinned to `2.5` to match
  airframe-config so behavior of `Profile.getProfile` stays
  identical. Any future bump should be deliberate, not implicit
  via airframe-config dropping this dep.
- **Hidden runtime users of airframe in lang.** Mitigated by
  grep — `grep -r "wvlet.airframe" wvlet-lang/` returns zero
  hits in main and test. If something is loaded reflectively
  (FQN string), the JVM tests in step 4 would catch it; they're
  green.
- **JS/Native consumers of Profile.** Profile is JVM-only at
  runtime. langJS/langNative consumers grep clean — none
  reference `Profile.getProfile` from JS/Native source.

## 6. Out-of-scope follow-ups (not in this PR)

- Apply the same JVM-scoping cleanup to `duckdb_jdbc` in the lang
  block — it has the same shared-libraryDependencies leak as
  snakeyaml had pre-fix. `DuckDBSchemaAnalyzer.scala` already lives
  in `.jvm/.js/.native/src` Compat trees, so the file move is even
  more localized; only the dep needs a `.jvmSettings` scoping.
- Drop airframe from the remaining wvlet modules (`api`, `runner`,
  `server`, `cli`, `client`) per the umbrella plan's Phase 2–5.
