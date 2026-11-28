# Engine: calibrate tracked memory against the live heap

## Goals

- Make each query's tracked memory (`MemoryContext`, `engine_peak_mb`) track what it really holds. The spill threshold (#2584), the per-query limit, and the low-memory killer all read these numbers.
- Close the two SF100 gaps from the baseline note: TPC-DS q67 tracks 53.6 GB against about 25 GB live (an overcount), and TPC-H q9 tracks 3.9 GB against about 20 GB (an undercount, and an OOM at a 12 GB heap).
- Done when tracked peaks are within 1.5x of the live heap over the idle baseline for q9, q13, q18 and q20 at SF10, and the histogram probe is rerun with the results recorded.

## Background

- Measured on spark2 (20 cores) with two probes:
  - live-object class histograms during the query, minus an idle baseline;
  - a scratch per-holder ledger: the reflective deep size of each holder when it is full, against what it reserved.
- **Join builds undercount.** The 12 B/row index estimate misses per-partition hash tables (slots, hashes and key copies) and packed keys.
  - SF100 q9's build of 150M orders rows holds 10 GB and reserves 3.4 GB.
  - At SF10, builds are within 1.35x, because they use flat tables.
- **String columns overcount.**
  - A gathered join output or a set of group keys holds references to the build's (or input's) Strings, but each row is estimated at 48+ bytes.
  - q67's buffered aggregation input estimates i_product_name at 351 MB, and its deep size is 29 MB.
  - The group-key estimate (48 B per string key) inflates q67's groups the same way.
  - Together, q67's PartitionedAgg reserves 30 GB at SF100 against 2.5 GB of output plus a few GB of input.
- **Partial aggregations count nothing.** Morsel parts run under the no-op context, so the 20 concurrent partial tables of q13/q20 (about 7–20 MB each) were untracked. The release-early work (#2580) made it safe to count them per part.
- **In-flight scan and pipeline batches stay uncounted.** On 20 cores, they hold several hundred MB to 1 GB at SF10, independent of the scale factor.

## Design

Lane: Express. Internal accounting fix with a measured diagnosis; no new behaviour or always-gate area.

- **Join builds:** once built, a join build reserves its index's real array sizes (flat or per-partition tables, chains, direct index, packed keys, match flags) beyond the pre-build estimate. The estimate stays as the early reservation, so the limit still fails a build before most of it is allocated.
- **Shared strings:** a string vector marks when its Strings belong to another vector (a gather of plain strings, or an aggregation's group keys). The estimate then counts 8 B per reference instead of the string again.
- **String group keys:** an aggregation sizes each group's string keys from its first input batch: 8 B when the rows reference shared, coded or dictionary strings, and 48 B otherwise, as before.
- **Morsel parts:** each part runs under a child of the query's context, released when the part is drained. Partial tables count while their part runs.
- **Not in this change:**
  - in-flight scan and pipeline batches, which are bounded by parallelism and not by data size (a candidate fixed per-query allowance);
  - DistinctOp and set-op key estimates, whose code sits in Operators.scala at the scalafmt reflow limit.
- **Tests:** unit tests cover the shared-string estimate, the string-key sizing, and a hashed join build's trued-up reservation. The before/after probe numbers on spark2 go in the PR.

## Alternatives and Why Not?

- Count every scan batch: scans are bounded per thread, and per-batch reservations would cost on the hot path. A fixed allowance is cheaper if admission needs it.
- Sample strings for duplicate identities to detect sharing: a 16-value sample can't see sharing across vectors (q67 repeats each item string about 47 times across 4.8M rows).
- Raise the join's per-row constant: the real cost depends on the index kind (flat about 16–24 B, hashed about 40–55 B), so a constant over- or undercounts one of them.
