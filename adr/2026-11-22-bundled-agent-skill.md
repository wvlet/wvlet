# ADR: The AI-agent skill ships with the compiler and is verified by it

Date: 2026-11-22 · PR #2251, #2255

## Context

AI coding agents are becoming the main authors of Wvlet queries. They learned Wvlet from a skill kept in a separate `wvlet/skills` repository. It drifted from the compiler. Moving it into this repo and running it through the compiler exposed several wrong examples: `first` used as a column name (a reserved keyword), `decimal(10, 2)` in a `table` declaration, and `save to` inside a flow `stage`. Agents repeat such examples verbatim, so a stale guide costs more than a missing one. uni solves the same problem by keeping its skill in-repo as a Claude Code plugin and compiling its snippets in CI.

## Decision

- The skill lives at `plugins/wvlet/skills/wvlet/` (`SKILL.md` plus `references/`). `.claude-plugin/marketplace.json` lists it as a Claude Code plugin. A PR that changes syntax, operators, or CLI commands updates the skill in the same PR.
- The compiler checks the skill in CI. `SkillSnippetTest` parses every `wvlet` fence, and runs `wvlet run` fences on DuckDB, where their `test` assertions must pass. `SkillFreshnessTest` fails when a `WvletToken` keyword appears in no code example. The exceptions are listed in `notInSkill`, each with a reason. It also checks the reserved-keyword list, which `SkillKeywordGenerator` writes from the parser.
- `plugin.json` has no `version`. Claude Code then tracks commits, so updating the skill needs no version bump. A bump that someone forgets would leave installed copies stale.
- `skillGen` embeds the skill in `wvlet-lang`, and `wvlet skill` serves it on the JVM, npm, and native CLIs (print, `--list`, `--install <dir>`). An agent that has no plugin support, or is offline, reads the guide for the compiler it runs, not the latest docs on the website.

## Consequences

- New syntax fails CI until the skill has a compiling example for it, or an explicit reason to omit it. Prose and inline code in tables are not compiled, so reviewers still check those.
- Examples prefer self-contained `val t(...) = [[...]]` tables with `test` lines, so that they run without a database.
- The CLI binaries grow by about 25 KB of Markdown.
- The `wvlet` plugin in the separate `wvlet/skills` repo is superseded.
