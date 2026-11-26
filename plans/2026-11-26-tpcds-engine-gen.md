# TPC-DS tables on the Wvlet engine without DuckDB

## Goal

Generate the 24 TPC-DS tables on the engine, as [the TPC-H plan](2026-11-25-tpch-engine-connector.md) did for TPC-H, so that `prepareTPCDS` / `tpcdsScaleFactor` work on a `wvlet` connector and `wvlet ui`, tests, and demos get TPC-DS data with no DuckDB or files. The bar is that the spec/tpcds queries return rows wherever they return rows on `dsdgen` data at the same scale factor. Matching `dsdgen`'s values or the official answers is not the goal.

## Non-goals

- A port of `dsdgen`. The engine's benchmarks, golden plans (`spec/engine-plans/`, TPC-DS at SF 0.1), and `EngineTPCDSTest` keep reading `dsdgen` Parquet from DuckDB's tpcds extension and comparing with DuckDB.
- Scale factors above 100 at first. The fact tables are large: 2.9M `store_sales` rows at SF 1, and 11.7M `inventory` rows.

## What the queries need

The 99 spec/tpcds queries filter on exact values (counted from `spec/tpcds/*.wv`). A generator that doesn't produce these values returns empty results, which say nothing about plans:

| Column | Literals the queries use |
| --- | --- |
| `i_category` (23 queries) | Books, Children, Electronics, Home, Jewelry, Men, Music, Sports, Women |
| `i_class` | accessories, birdal, classical, computers, dresses, football, fragrances, maternity, pants, personal, portable, reference, self-help, shirts, stereo, each under its own category |
| `i_color`, `i_units`, `i_size` | 25 colors, 16 units (`Each`, `Oz`, `N/A`, ...), 5 sizes (`petite`, `extra large`, `N/A`, ...) |
| `i_manufact_id`, `i_manager_id` | ids in 1..1000 and 1..100 |
| `ca_state`, `s_state`, `ca_country` | 22 states, `TN`, `United States` |
| `s_county`, `cc_county`, `s_city`, `ca_city` | Williamson County, Bronx County, Orange County, Franklin Parish; Fairview, Midway, Edgewood |
| `cd_*`, `hd_*` | every gender, marital, and education value; `hd_buy_potential` `>10000` and `Unknown`; dep/vehicle counts 0..9 |
| Names from syllables | `s_store_name = 'ese'`, `web_company_name = 'pri'` |
| Dates | `d_year` 1998–2002, every `d_moy`/`d_qoy`, `d_month_seq` 1176..1223, `d_date` ranges in 1998–2002, `d_day_name`, `d_week_seq`, `d_quarter_name` |
| Others | `sm_carrier` DHL and BARIAN, `r_reason_desc = 'reason 28'`, `t_meal_time` breakfast and dinner, `ca_gmt_offset` -5 and -7 |
| Surrogate keys | `ss_store_sk = 4`, `r_reason_sk = 1`, `i_item_sk in (2, 3, 5, ...)`: keys must be dense from 1 |

Joins the queries rely on:
- Returns match their sale on (ticket/order number, item) with the same customer.
- A ticket's lines share a customer, store, date, and time.
- Catalog and web orders ship after they're sold.
- `inventory` is a weekly snapshot of every item in every warehouse.
- Customers buy in more than one channel (`q04`, `q11`, `q74` compare years per customer across channels).

## Design

Same mechanism as TPC-H: `TpcdsGen` + `TpcdsSource` (or a table-parameterized generalization of `TpchSource`), with blocks seeded by (table, block) and generated while scanned.

- **Dimensions fixed by the specification, generated exactly** (done in PR 1; `dsdgen`'s calendar quirks are documented in `TpcdsGen`: 1900 counts as a leap year for weekdays and days of the year, holidays are days 1/186/360/366, `d_qoy` uses a leap year's quarter ends, Friday and Saturday are the weekend, the current year is 2003):
  - `date_dim`: 1900-01-02 to 2100-01-01, `d_date_sk` = Julian day, `d_month_seq` / `d_week_seq` / `d_quarter_seq` counted from 1900.
  - `time_dim`: 86,400 seconds with meal times.
  - `customer_demographics`: the cross product of its domains, 1,920,800 rows.
  - `household_demographics`: 7,200 rows.
  - `income_band`: 20 rows.
  - `reason` and `ship_mode`.

  These need no randomness and make date and demographic filters behave as on `dsdgen`.
- **Value lists:**
  - The queries' literals above, plus enough other values to keep the selectivities near `dsdgen`'s (e.g. ~10 classes per category, ~50 states).
  - Take the domains from the TPC-DS specification and the queries, not by copying `dsdgen`'s `tpcds.dst` distribution file (see [Open questions](#open-questions)).
- **Small dimensions:**
  - Random rows over those lists: `item` with category → class → brand hierarchy and SCD revisions, plus `customer`, `customer_address`, `store`, `warehouse`, `call_center`, `web_site`, `web_page`, `promotion`, `catalog_page`.
  - Dense keys.
  - SCD tables (`item`, `store`, `call_center`, `web_site`, `web_page`) repeat business ids with `rec_start_date` / `rec_end_date` as in the specification.
- **Fact tables, block-streamed:**
  - Each channel generates a block of tickets/orders with their lines, and its returns derived from the same block: `store_sales` + `store_returns`, `catalog_sales` + `catalog_returns`, `web_sales` + `web_returns`. This is the same pairing as `orders` + `lineitem`.
  - About 10% of lines are returned, after the sale date, by the same or a related customer.
  - Sale dates fall in the specification's window (1998-01-02 to 2003-01-02).
  - Prices derive from list price, discount, and quantity with the specification's formulas, so `ss_net_profit`-style columns are consistent.
  - `inventory` is generated per (week, warehouse) block.
- **Row counts:** `dsdgen`'s, checked at SF 0.01–10. Below SF 1 it's `floor(count at SF 1 × sf)`, at least 1. From SF 1 to 10 it's linear between `dsdgen`'s SF 1 and 10 counts, rounded down to a per-table unit (1,000 for `customer`/`customer_address`, 2,000 for `item`, 2 for `web_site`/`web_page`); `catalog_page` and `reason` stay fixed. Above SF 10 `dsdgen`'s counts weren't measured (SF 100 is too large to generate locally), so `TpcdsGen.maxScaleFactor` is 10 until they are.
- **Estimates:** `columnRange` / `partRanges` for date keys and the block-clustered ticket/order numbers; `estimatedDistinct` for keys and the fixed domains.
- **Connector:** `prepareTPCDS` / `tpcdsScaleFactor` on the `wvlet` connector, replacing today's `NOT_IMPLEMENTED` error. `wvlet ui --tpcds` only if asked for.

## Tests

- Per-table row counts at SF 0.1 versus `dsdgen` (DuckDB in Test scope, as `EngineTPCDSTest` does).
- Exact checks on the fixed dimensions: `date_dim` and `time_dim` rows equal `dsdgen`'s for every column the queries read (d_year, d_moy, d_dow, d_day_name, d_month_seq, d_week_seq, d_qoy, d_quarter_name, t_meal_time, ...).
- Referential checks: every fact foreign key hits its dimension, and every return has its sale.
- **Shape gate:** run all 99 spec/tpcds queries on generated SF 0.1 data. Each query that returns rows on `dsdgen` SF 0.1 must return rows here, except for a listed set with reasons. That list may only shrink.
- Scala.js run of the small dimensions, so the playground could offer TPC-DS later.

## PR order

1. Fixed dimensions (`date_dim`, `time_dim`, `customer_demographics`, `household_demographics`, `income_band`) equal to `dsdgen` in every column except `d_last_dom`. `TpchSource` and `TpcdsSource` share `GeneratedSource`.
2. The other dimensions with the value lists, including `reason` and `ship_mode`: `dsdgen`'s schemas, row counts, null shares, and SCD layout (3 business ids per 6 rows, half the rows current), with the query literals present at SF 1.
3. Fact tables and returns, block-streamed, with the referential checks. Per-order columns follow `dsdgen`: a store ticket keeps its customer, demographics, address, store, date, and time; a catalog order its bill and ship customer with their demographics and addresses, call center, and warehouse; a web order its bill customer, page, and warehouse, while each line ships to its own customer. A preview of the shape gate at SF 0.1 matched 82 of the 85 queries that return rows on `dsdgen` data (q04, q23, and q29 return 1 row there and none here); q37 and q64 return rows only here.
4. `prepareTPCDS` on the `wvlet` connector, the shape gate as a runner, and docs (`usage/wvlet-engine.md`, `for-ai-agents.md`, skill, `design/connectors.md`).

## Effort and risk

- About 3–4 times the TPC-H work. TPC-DS has 24 tables instead of 8, many value lists, and returns and SCD rows to keep consistent.
- The shape gate is the main risk. Some queries chain several selective filters (category × class × state × year), and their results depend on how the value lists are weighted. Expect a few rounds of tuning weights against the gate.

## Open questions

- **License:** `dsdgen`'s distribution file (`tpcds.dst`) falls under the TPC EULA. Taking the domains from the specification document and the query literals avoids copying it, but the lists will be shorter than `dsdgen`'s. Confirm that this is acceptable, or decide whether the EULA allows bundling the distributions.
- Is the playground a target? It would need the Scala.js path to stay fast at SF 0.01, which the fact tables make harder than TPC-H.
