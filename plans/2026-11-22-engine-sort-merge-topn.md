# Engine sorts: parallel merge and bounded top-N

## Goals

- Remove the single-threaded merge that bounds large parallel full sorts (6M rows: 2.3 s → only 1.1–1.3 s).
- Make top-N with a small limit avoid sorting each chunk fully.
- Keep ORDER BY output identical to the sequential stable sort, top-N included.

## Background

- `SortOp` cuts its input into one chunk per thread; each chunk is stable-sorted and cut to the limit as a task.
- `SortOp.merge` then heap-merges the runs on the calling thread in O(n log P); ties go to the earlier run, which keeps the result stable.
- Rows are totally ordered by (sort key, run, position in run), which equals (sort key, input row index) because chunks are contiguous. Both changes rely on that order.
- A parallel session may add a primitive stable sort on `Array[Int]`; the inner sort of `sortRange` stays as is here.

## Design

- Lane: Express — internal performance change with no change in results, no always-gate area. Merge still needs the user's approval (launch prompt).
- **Merge splitters.** On the calling thread, find for each of P output ranks the cut in every run, so that the rows before the cuts are exactly the first k rows of the merge. The search picks a pivot in the run with the widest remaining range and counts rows below it in each run with binary search (earlier runs count ties as below, later runs don't). Cost is O(P² log² n) comparisons, negligible next to the merge.
- **One task per output range.** Each task heap-merges its slice of every run and gathers its output batches, so merge and gather run in one round of tasks; tasks don't wait on tasks.
- **When to split.** Outputs below `wvlet.engine.parallelSortMinRows` keep a single merge on the calling thread, so the existing property (set to 1 by the parallel tests) also exercises the split path.
- **Bounded heap for top-N.** `sortRange` keeps a max-heap of the `limit` smallest rows by (key, row index) when the limit is at most 1/8 of the chunk; a later row enters only if its key is strictly smaller than the heap top, so ties keep input order. The heap is then emptied into sorted order. The sequential path benefits too.
- The 1/8 ratio is a fixed constant rather than a property: it chooses an algorithm, not parallelism, and both branches are tested directly.
- Tests cover the splitter cuts against the sequential merge (ties across runs, empty runs, every rank) and heap top-N against the full sort.

## Alternatives and Why Not?

- Each task searching its own splitters: also valid, but the search is cheap enough that doing it up front keeps tasks simple.
- Partial quickselect for top-N: not stable without extra work; the heap keeps tie order naturally.
