# Spill threshold that leaves headroom on small heaps

## Goals

- On a small heap, start spilling while the heap still has room for the memory the engine doesn't track, so spillable queries finish instead of failing with an OOM.
- Keep large heaps as they are: nothing new spills at SF10 with the default heap, and timing is unchanged.

## Background

- #2584 sets the default spill threshold (on the JVM) to half the max heap. The threshold counts only tracked holders (aggregation input, join builds, CTE results). Scans, decode buffers, and per-thread working sets are untracked.
- At SF10 with matched peak RSS, the 5a baseline (note `2026-11-28-duckdb-spill-baseline-at-sf10-on-spark1-2026-11-28`) shows:
  - DS q23 and q67 OOM at `-Xmx1536m` having spilled 0 bytes: untracked memory fills the heap before tracked memory reaches 768 MB.
  - `-Dwvlet.engine.spillThreshold=256MB` at the same heap makes q23 finish in 0.81 s (DuckDB at 2 GB: 1.33 s) and q22 in 0.31 s.
- Untracked memory grows with the number of worker threads, since each thread decodes a row group at a time (gap 1, owned by item 6c), and barely with the heap size. So a fixed fraction of the heap leaves almost no room on a 1.5 GB heap and plenty on a 32 GB one.
- The threshold also sizes the spill path itself: router chunk size, which files are split, and the read-back wave size. A threshold that reflects real headroom keeps read-back in bounds too.

## Design

Lane: Standard by scope (it changes a default), carried out under the goal's approved item. The user authorized auto-merge for this goal.

- Default threshold = max(heap / 8, heap / 2 − allowance), where the allowance is the untracked memory: engine threads × a per-thread allowance (32 MB to start).
  - Examples at 20 threads: 1536m → 192 MB, 2g → 384 MB, 4g → 1.4 GB, 32g → 15.4 GB.
- The per-thread allowance is a system property (`wvlet.engine.spillThreadAllowance`). Item 6c, which bounds scan decode memory, will set it to that bound, so the two items share one number.
- `-Dwvlet.engine.spillThreshold` and `WvletEngine.withSpillThreshold` still override the default. Scala.js and Native keep their behavior (no spill / no default threshold).
- Behind a new `Optimization` toggle, `spill_headroom`. Off restores heap / 2.
- Validation:
  - A unit test of the formula.
  - SF10 A/B at the default heap on spark1: nothing spills either way.
  - The 5a matched-memory runs at 1536m and 2g against the 5a numbers.

## Alternatives and Why Not?

- **React to GC signals** (live heap after each GC through `GarbageCollectorMXBean` notifications, then spill when the post-GC live heap plus tracked growth nears a target). It measures untracked memory instead of estimating it, but it depends on GC timing: after a spill, the headroom looks low until the next GC, and runs aren't reproducible. Kept as a follow-up if the static allowance proves too coarse.
- **A smaller fixed fraction (heap / 4).** It still leaves 384 MB at 1536m, where q23 needs ≤ 256 MB, and spills needlessly early on large heaps.
- **Wait for #2598's calibration.** That fixes how tracked bytes are counted, not the untracked memory; the allowance is still needed after it lands.
