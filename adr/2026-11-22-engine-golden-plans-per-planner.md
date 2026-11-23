# ADR: Golden engine plans per planner while both planners exist

Date: 2026-11-22 · PR #2271

## Context

`EnginePlanGoldenTest` checked the runtime planner and the legacy (recursive) planner against the same files under `spec/engine-plans/`. That forced every runtime-plan rule to reproduce the legacy planner's plans exactly. From M3c on, the runtime plan gets rules the legacy planner doesn't have: PushDownFilters moves filters below projections and into the preserved side of outer joins. With a single set of files, these rules couldn't land.

## Decision

- `spec/engine-plans/<suite>/` holds the runtime planner's plans, which is the default planner.
- `spec/engine-plans/legacy/<suite>/` holds the legacy planner's plans, only for files where they differ.
- The regenerate command (`EnginePlanGolden` main) writes both directories.
- The test checks the legacy planner against its own file when one exists, otherwise against the shared file. It fails on a legacy file that has become identical to the shared one.

## Consequences

- A rule that only the runtime planner has shows its plan changes in the PR diff: the shared file changes, and a legacy file appears with the old plan.
- The legacy planner stays under the golden check until M4 deletes it. The `legacy/` folder then goes away with it.
- When the runtime planner should still match the legacy planner (a rule moved from the planner onto the IR unchanged), a new legacy file in the diff shows the drift.

## Amendment (2026-11-23, M4b)

The legacy planner is deleted. `spec/engine-plans/legacy/` and its handling in `EnginePlanGolden` and `EnginePlanGoldenTest` are gone: `spec/engine-plans/<suite>/` holds the engine's only plans, and the test checks one planner against them.
