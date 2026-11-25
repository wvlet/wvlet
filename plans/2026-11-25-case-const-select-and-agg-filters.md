# CASE constant select and sum(CASE) as aggregate FILTER

Work item: engine CASE with constant THEN/ELSE and sum(CASE) as aggregate FILTER (TPC-DS q97, q02, q59, q43). Builds on #2416 (`masked_case_when`).

## Lane

Express (engine optimization loop). Merge authorization: the loop rules allow auto-merge after green required CI, addressed review comments, and an SF10 A/B with no suite-level regression.

## Changes

- `const_case_select`: a `CaseWhen` of one WHEN whose THEN and ELSE are non-NULL constants (after casting to the output type, with a typed layout) picks one of the two values per row from the condition. It skips the branch choice's per-row source index and second copy. Flag `CaseWhen.selectConsts`, set at lowering by `MaskCaseWhen` next to `maskNulls`; `sig` is unchanged, so no golden diff, and the toggle test ignores it.
- `case_agg_filters`: at lowering, `sum/count/min/max/avg(case when c then x [else null] end)` (not DISTINCT, no ORDER BY) becomes `agg(x) filter (where c)` (ANDed with an existing FILTER). These aggregates skip NULL inputs, so the rows the CASE sets to NULL are the rows the FILTER drops. Golden plans change for q02, q43, q59.
- `HashAggOp`'s FILTER path: when every row passes, fold without gathering; when none passes, skip, and don't evaluate the arguments if they weren't evaluated for fusion. Strict equivalences, no toggle.

## Validation

- `CaseWhenTest` (select matches the branch choice for int, long, double, string, bool, decimal), `EngineCaseAggFiltersTest` (flags reach the plan, same rows with both toggles off).
- SF10 A/B on DS q97, q02, q59, q43, q66, q88, q28 and H q12, q14 with each toggle.
- Profile q97's FULL OUTER join and aggregation; record what remains under the item's Follow-ups.

## Risk

- Filtered aggregates don't take `fused_accumulators`' single pass, so `case_agg_filters` can lose on aggregates that fused before (q02's seven decimal sums). The A/B decides whether it stays on.
