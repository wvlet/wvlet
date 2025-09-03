
# AsOf Join

:::warning
Note: AsOf join is supported only in DuckDB, Snowflake, but not yet in Trino [trinodb/trino#21759](https://github.com/trinodb/trino/issues/21759), Hive, etc.
:::

AsOf join is useful when you want to join two tables by looking at the most recent value from the specific time (_as of this time_) for each record.

For example, if you want to know the share holding value at a specific date, you can join the holding table with the stock table by looking at the most recent stock price at the holding date:
```wvlet
val holding(symbol, date, shares) = [
  ['AAPL', '2024-11-07', 1.0],
  ['AAPL', '2024-11-08', 2.0],
  ['AAPL', '2024-11-09', 3.0],
  ['AAPL', '2024-11-10', 4.0],
]

val stock(symbol, date, price) = [
  ['AAPL', '2024-11-07', 10],
  ['AAPL', '2024-11-08', 50],
  ['AAPL', '2024-11-09', 100],
]

from holding
asof join stock
  on stock.symbol = holding.symbol
  and stock.date <= holding.date
add stock.price * holding.shares as holding_value
select symbol, date, shares, price, holding_value
```

This query looks up the most recent stock price from the stock table, and compute the holding_value at each date:
```
┌────────┬────────────┬──────────────┬───────┬───────────────┐
│ symbol │    date    │    shares    │ price │ holding_value │
│ string │   string   │ decimal(2,1) │  int  │ decimal(12,1) │
├────────┼────────────┼──────────────┼───────┼───────────────┤
│ AAPL   │ 2024-11-07 │          1.0 │    10 │          10.0 │
│ AAPL   │ 2024-11-08 │          2.0 │    50 │         100.0 │
│ AAPL   │ 2024-11-09 │          3.0 │   100 │         300.0 │
│ AAPL   │ 2024-11-10 │          4.0 │   100 │         400.0 │
├────────┴────────────┴──────────────┴───────┴───────────────┤
│ 4 rows                                                     │
└────────────────────────────────────────────────────────────┘
```

