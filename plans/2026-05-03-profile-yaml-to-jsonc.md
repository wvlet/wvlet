# Plan: Switch profile config from YAML to JSONC

Follow-up to #1667 (drop airframe from `wvlet-lang`). That PR landed
`snakeyaml` as a direct JVM-only dep so we could ship the airframe
removal without changing user-facing config behavior. This plan
removes snakeyaml entirely by migrating `Profile.getProfile` from
SnakeYAML-parsed YAML to uni's JSONC (`wvlet.uni.json.JSON.parse`,
which silently accepts line comments, block comments, and trailing
commas — verified with a scratch test on uni 2026.1.8).

## 0. Findings from the probe

- `wvlet.uni.json.JSON.parse(s)` accepts plain JSON, `// line`
  comments, `/* block */` comments, and trailing commas, all
  without any flag. Comments are dropped from the resulting tree.
- `wvlet.uni.weaver.Weaver` exposes `fromJson(String)` and
  `fromJSONValue(JSONValue)` — direct path from JSON text to a
  case class without going through `Map[String, Any]`.

This means the new code path is:

```
fileContent
  |> JSON.parse              (drops comments)
  |> walk JSONValue tree, expand $VAR / ${VAR} only in JSONString leaves
  |> Weaver.of[ProfileConfig].fromJSONValue
  |> _.profiles.find(_.name == name)
```

…replacing the current SnakeYAML → java.util.Map → Scala Map →
Weaver.fromMap pipeline.

> **Design refinement after codex review.** A first pass ran env-var
> interpolation on the raw file text (the same way the YAML version
> did). codex flagged two regressions: (1) `// example: ${TD_API_KEY}`
> in a JSONC comment would now throw, since the interpolator can't
> see that the parser will discard it; (2) injecting env values into
> the raw text without JSON-escaping breaks parse if a value
> contains `"` or `\n`. Both go away by parsing first and substituting
> inside `JSONString` leaves only — the value crosses through the
> JSON model as a Scala string, not as embedded text.

## 1. Scope

In scope:
- Replace SnakeYAML usage in
  `wvlet-lang/.jvm/src/main/scala/wvlet/lang/catalog/Profile.scala`
  with uni's `JSON.parse` + `Weaver.of[ProfileConfig].fromJson`.
- Introduce a private `case class ProfileConfig(profiles:
  Seq[Profile] = Seq.empty)` wrapper to match the file's top-level
  shape `{"profiles": [...]}`.
- Look up the config file in this order:
  1. `~/.wvlet/profiles.json` (primary)
  2. `~/.wvlet/profiles.jsonc` (alternate; same syntax)
- If neither exists but `~/.wvlet/profiles.yml` (or `.yaml`) does:
  log a single WARN with a `yq` one-liner for migration, then
  return `None`. (Don't try to keep parsing YAML — the whole point
  is to drop snakeyaml.)
- Update `ProfileTest` (`.jvm/src/test/.../ProfileTest.scala`) to
  write `profiles.json` fixtures with comments, exercising both
  the JSONC path and the deprecation-warning path.
- Drop `org.yaml % snakeyaml` from `build.sbt` and remove the
  `SNAKE_YAML_VERSION` constant.
- Update user-facing docs:
  `website/docs/usage/snowflake.md`,
  `website/docs/usage/trino.md` — switch fenced examples from
  `yaml` to `jsonc` (or `json`) and rename the file label.
  Blog post at
  `website/blog/2024-12-30-release-2024-9/index.md` is historical;
  leave as-is.

Out of scope:
- Touching the still-present airframe deps in `httpServer`,
  `server`, `runner`, `cli`, `client` modules — covered by the
  umbrella plan.
- Doing the same JVM-scoping cleanup for `duckdb_jdbc` (carried
  over from #1667 follow-ups).
- Auto-converting users' existing `profiles.yml` to `profiles.json`
  on the fly — too clever; the WARN with the `yq` hint puts the
  user in control.

## 2. New file format

Top-level shape stays the same — array of profile records under a
`profiles` key:

```jsonc
{
  // ~/.wvlet/profiles.json
  "profiles": [
    {
      "name": "td-prod",
      "type": "trino",
      "host": "api-presto.treasuredata.com",
      "catalog": "td",
      "schema": "main",
      "user": "${TD_API_KEY}", // env-var interpolation still supported
    },
    {
      "name": "snowflake-dev",
      "type": "snowflake",
      "host": "myacct.snowflakecomputing.com",
      "user": "dev_user",
      "password": "${SF_PWD}"
    }
  ]
}
```

Env-var syntax (`$VAR` and `${VAR}`) carries over from the YAML
implementation untouched — the interpolation runs on the raw file
text *before* JSON parsing, so any string value can use it.

## 3. Migration steps

1. Define `ProfileConfig(profiles: Seq[Profile])` (private to the
   `Profile` companion object).
2. Refactor `Profile.getProfile(profile: String): Option[Profile]`:
   - Resolve config path: try `profiles.json` then
     `profiles.jsonc`. If neither exists, check `profiles.yml` /
     `profiles.yaml` and emit a deprecation WARN if found, then
     return None.
   - Read file as String via `IO.readString(IO.path(...))`.
   - `JSON.parse(...)` (drops JSONC comments).
   - Walk the resulting `JSONValue` tree; in each `JSONString`
     leaf, run env-var interpolation. Skip `JSONObject` keys, comments,
     and non-string leaves.
   - `Weaver.of[ProfileConfig].fromJSONValue(...)` to deserialize.
   - `_.profiles.find(_.name == profile)`.
3. Update `ProfileTest`:
   - Replace YAML test fixtures with JSONC equivalents (include
     a comment + trailing comma to lock in JSONC support).
   - Add a test for the YAML deprecation WARN path.
4. Update `wvlet-spec/.../TDTrinoSpec.scala` skip message to
   reference `profiles.json`.
5. Drop snakeyaml from `build.sbt`:
   - Remove the `.jvmSettings(libraryDependencies += "org.yaml" %
     "snakeyaml" % SNAKE_YAML_VERSION)` line.
   - Remove `val SNAKE_YAML_VERSION = "2.5"` constant.
6. Update website docs (`snowflake.md`, `trino.md`): change file
   label from `~/.wvlet/profiles.yml` to `~/.wvlet/profiles.json`
   and convert the YAML examples to JSONC.
7. Verify `langJVM/Test/compile`, `langJS/Test/compile`,
   `langNative/Test/compile`, `projectJVM/Test/compile`,
   `langJVM/test`, `scalafmtCheckAll`.

## 4. Verification

Per-step sbt checks (same matrix as #1667):
- `./sbt langJVM/Test/compile` — must catch any leftover YAML
  references.
- `./sbt langJS/Test/compile` — sanity check (no JS code touched).
- `./sbt langNative/Test/compile` — sanity check.
- `./sbt projectJVM/Test/compile` — catch downstream consumers
  (cli, server, runner, spec).
- `./sbt langJVM/test` — `ProfileTest` exercises the new JSONC
  path.
- `./sbt scalafmtCheckAll`.

Additional manual checks:
- Hand-author a `~/.wvlet/profiles.json` with a comment and run
  `wvlet ui --profile <name>` to confirm the runtime path works.
- Move the file to `profiles.yml`, re-run, confirm the
  deprecation warning fires and the profile lookup returns None.

## 5. Risks

- **User config breakage.** Hard switch — anyone with
  `~/.wvlet/profiles.yml` will hit "profile not found" until they
  convert. The deprecation WARN with `yq` hint is the mitigation.
  Worth a one-line release note. The primary user is the project
  owner; the radius is small.
- **Env-var interpolation order.** Currently runs before YAML
  parsing — in JSON, the env var must be inside a string literal
  (`"${VAR}"`), not a bare token. If a user wrote
  `port: $PORT` (unquoted) in YAML, that worked; in JSON they
  must write `"port": "${PORT}"` and accept the value as a string
  (Weaver will coerce numerics where appropriate; verify in
  ProfileTest).
- **Weaver behavior on missing `properties` field.** Profile has
  `properties: Map[String, Any] = Map.empty`. Verify Weaver uses
  the default when the JSON omits the key — should "just work"
  with the default value, but lock it in with a test.
- **JSON null handling.** `Weaver.fromJson` on `null` for an
  `Option[String]` field should produce `None`. Lock in with a
  test.

## 6. Out-of-scope follow-ups

- Bring the same JVM-scoping discipline to `duckdb_jdbc` (carried
  over from #1667 follow-ups).
- Drop airframe from the remaining wvlet modules per the umbrella
  migration plan's Phase 2–5.
- Consider supporting `~/.wvlet/profiles.json5` if user demand
  surfaces — JSON5 is a superset of JSONC. uni doesn't ship a
  JSON5 parser today.
