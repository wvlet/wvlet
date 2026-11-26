# ADR: Join shared CTE readers on the long key that determines their string join key

Date: 2026-11-26 · PR #2497

## Context

- TPC-DS q04, q11 and q74 join the readers of one shared CTE (`year_total`) on the string `customer_id`, a copy of customer's declared unique `c_customer_id` (#2488). Customer also declares `c_customer_sk` unique, and the body's sales join on it.
- A shared CTE body is bound and optimized once, when it is first read, before the joins of its readers are planned. Its output is fixed by the query, and readers read it by position.

## Decision

- `cte_determining_join_keys` rewrites the bound root plan in `Planner.planRoot`, before the logical passes, not as an optimizer pass. A pass can't add a column to a shared CTE body, which the planner owns. After the rewrite, join ordering, reader statistics and filter pushdown already see the long key.
- An equality of two readers at the same body position becomes `a.long = b.long and a.string is not null` when:
  - the string column copies a single-column declared key of a scan, traced through projections, filters, preserved join inputs, group keys and union branches that copy the same string key of the same table;
  - another single-column declared key of the same scan is a long, and an inner equi-join on it in the body shows it is non-NULL.
- This is sound because two rows of one table with equal long keys are the same row, so their string keys are equal unless NULL. A NULL long key would lose matches that the strings find, which is why it has to be proven non-NULL. Declared keys count NULL as a value, so at most one row has a NULL string key, and the guard drops it, as the string equality did.
- The long key joins the body's group keys instead of being added as an `any_value`. `dependent_group_keys` then groups by the long key and carries the strings, so the body hashes longs too.
- The body gets the key as one more column at the end. Readers bound before the rewrite, and readers bound later, read the columns before it (`SharedCTE.readerSchema`), so `select *` and positional reads don't see it. The re-optimized body must output the bound body's columns in order; otherwise the rewrite isn't done.

## Consequences

- At SF10 (one-JVM A/B, 10 runs, two rounds): q74 0.84–0.89x wall, q11 0.94–0.96x, q04 0.95–0.98x. The control plans (q23, q47) are unchanged.
- Only direct readers inside inner joins are rewritten. Readers under projections, outer or semi joins, and a reader joined to a base table keep the string key.
- A declared NOT NULL or primary key column would prove the long key non-NULL without the body's join, but `TableSource` doesn't declare nullability yet.
