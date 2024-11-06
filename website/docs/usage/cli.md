---
sidebar_label: CLI (wvlet compile)
---

# Generate SQL Queries

`wvlet compile` commands generate SQL queries from the given input Wvlet query.

```bash
$ wvlet compile "from lineitem limit 10"
-- wvlet version=2024.9.10, src=01JC1T63TKWNRQ1BF57P3AW7Z2.wv:1
select * from lineitem
limit 10
```

## Compile SQL from .wv file

It's also possible to compile a query from a file:

```sql title="query.wv"
execute sql"call dbgen(sf=0.01)"

from lineitem
group by l_returnflag
agg _.count as cnt
```

```bash
$ wvlet compile -f query.wv
-- wvlet version=2024.9.10, src=query.wv:1
call dbgen(sf=0.01)
;
-- wvlet version=2024.9.10, src=query.wv:5
select l_returnflag as l_returnflag, count(*) as cnt
from lineitem
group by l_returnflag
;
```

If you use DuckDB, you can run the generated SQL through pipe:

```bash
$ wvlet compile -f query.wv | duckdb
┌─────────┐
│ Success │
│ boolean │
├─────────┤
│ 0 rows  │
└─────────┘
┌──────────────┬───────┐
│ l_returnflag │  cnt  │
│   varchar    │ int64 │
├──────────────┼───────┤
│ A            │ 14876 │
│ N            │ 30397 │
│ R            │ 14902 │
└──────────────┴───────┘
```

