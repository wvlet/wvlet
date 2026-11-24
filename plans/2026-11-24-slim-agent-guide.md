---
title: Slim the agent guide into AGENTS.md, ARCHITECTURE.md, and design/
created: 2026-11-24
---

# Slim the agent guide into AGENTS.md, ARCHITECTURE.md, and design/

## Goals

- Agents load CLAUDE.md on every task, so its 408 lines cost context on every turn. Most of them are area-specific (Lean fixture regeneration, engine gates, stdlib catalogs, release steps) and matter only when touching that area.
- Keep only what every task needs at the top level: commands, workflow rules, conventions, and a pointer table to the area pages.
- Lose no rule: every gate and "when changing X, run Y" instruction stays reachable in one hop from the top-level guide.

## Background

- CLAUDE.md (408 lines) and AGENTS.md (63 lines) overlap and disagree: AGENTS.md still says tests use AirSpec and names `runner/testOnly *BasicSpec`, while CLAUDE.md says UniTest and `runnerJVM/testOnly *RunnerSpecBasic`.
- treasure-work splits this into a short AGENTS.md (no CLAUDE.md), ARCHITECTURE.md as the system map with a reading table, and design/ with one page per subsystem, a README index, and design/AGENTS.md describing how to write those pages.
- design-notes/ holds a single 2025 typer exploration that nothing links to. adr/README.md already says longer design explorations live in plans/.
- formal/README.md already documents each Lean model in detail; the design page only needs the sync rules and a pointer.
- PR #2357 edited one CLAUDE.md line to mention `WVLET_ENGINE_BENCH_AB`.

## Design

- Lane: Express. Docs-only reorganization, no program logic, no always-gate area.
- AGENTS.md becomes the single guide and CLAUDE.md is deleted, as in treasure-work.
- AGENTS.md keeps: overview, commands, workflow (branching, commits, PRs, never merge without approval), code style, testing conventions an agent hits on any change, and a "Where the details live" table with one key rule per area, as treasure-work does.
- ARCHITECTURE.md: module map, the compiler phases, the engine path (binder, runtime plan, lowering, operators), the runner and connectors, and a reading map to design/ and adr/.
- design/ pages: formal-verification, engine, stdlib, testing, platforms (Scala.js/Native, libduckdb loading), connectors, releasing, agent-docs (for-ai-agents page and bundled skill), plus README.md (index) and AGENTS.md (how to write these pages).
- Each "when changing X" rule moves verbatim into its page; the AGENTS.md table row names the trigger so an agent knows to open the page.
- Move design-notes/2025-01-15-new-typer-architecture.md to plans/ and delete design-notes/, so design/ holds only current contracts.
- design/engine.md documents `WVLET_ENGINE_BENCH_AB` as #2357 describes it; the rebase over #2357 drops its CLAUDE.md hunk.

## Alternatives and Why Not?

- Put the details under website/docs/development/: those pages are for human contributors and are published; agent gates fit better next to the code.
