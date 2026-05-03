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
- Add `org.yaml:snakeyaml` as a direct dep on `lang` (JVM artifact,
  pinned to `2.5` to match what airframe-config was previously pulling
  — `airframe/build.sbt:28`).
- Add a `SNAKE_YAML_VERSION` constant alongside the other version
  pins in `build.sbt`.

Out of scope:
- Refactoring `Profile.scala` to a `Compat`-style platform split. The
  YAML reader is shared-source today and only ever runs on JVM in
  practice; the JS/Native compile-only classpath has been quietly
  dragging in the snakeyaml jar via airframe-config for the entire
  migration, so this PR preserves that behavior verbatim.
- Touching the `httpServer`, `server`, `runner`, `cli`, or `client`
  modules — they have their own airframe deps that the umbrella
  migration will address per its phase plan.
- Any code change outside `build.sbt`.

## 2. Why snakeyaml stays as a direct dep (not a Compat split)

Two options were considered for `Profile.scala`:

- **A. Add snakeyaml as a direct JVM dep** (chosen).
  Profile.scala stays in shared sources; snakeyaml jar is on the
  compile classpath for langJVM/langJS/langNative as it was before.
  Scala.js/Native linker would fail if any code path actually called
  `Profile.getProfile`, but no JS/Native consumer does — `Profile`
  is read only by JVM-side runner/cli code.
- **B. Move `Profile` to a JVM-only `Compat` impl.**
  Cleaner separation of concerns, but requires (i) restructuring
  `lang` away from `CrossType.Pure`, (ii) inventing a no-op
  JS/Native `Profile` shim, and (iii) auditing every call site for
  cross-platform safety. None of that is required to drop airframe.
  This refactor is a fine future task but conflates two changes.

A is the minimal-risk, behavior-preserving choice. The empirical
evidence is the existing build: langJS has been compiling with the
same shared `Profile.scala` and the same JVM-only snakeyaml jar on
its compile classpath the whole time.

## 3. Migration steps

1. Edit `build.sbt`:
   - add `val SNAKE_YAML_VERSION = "2.5"` next to the other version
     constants;
   - in the `lang` project's `libraryDependencies`, drop the three
     airframe entries and add `"org.yaml" % "snakeyaml" %
     SNAKE_YAML_VERSION` with a comment explaining why it sits next
     to a cross-built crossProject.
2. Verify `langJVM/Test/compile`, `langJS/Test/compile`,
   `langNative/Test/compile`.
3. Verify aggregates: `projectJVM/Test/compile`,
   `projectJS/Test/compile`, `projectNative/Test/compile` — to catch
   any downstream module that was relying on lang's transitive
   airframe pull.
4. Run `langJVM/test` to make sure runtime behavior is unchanged
   (Profile YAML reader, etc.).
5. `scalafmtAll` (no source file changes are expected, but run
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

- Move `Profile.scala` to a JVM Compat trait so JS/Native don't pull
  the snakeyaml jar at all (Option B above).
- Drop airframe from the remaining wvlet modules (`api`, `runner`,
  `server`, `cli`, `client`) per the umbrella plan's Phase 2–5.
