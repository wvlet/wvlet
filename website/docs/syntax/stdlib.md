# Standard Library Functions

Wvlet ships with a standard library of functions that you call with dot syntax on column values, e.g. `name.upper` or `price.round(2)`. Functions compile to the SQL of the target database engine: when engines differ (e.g. DuckDB and Trino), Wvlet picks the right SQL for the engine you are compiling for, so the same query works across engines.

```wvlet
from orders
select
  customer_name.upper as customer,
  order_date.year as order_year,
  amount.round(2) as amount
```

Functions can be chained:

```wvlet
from logs
select message.trim.lower.replace('error', 'warning') as normalized
```

## Type Conversions

Available on all values:

| Function | Description |
|----------|-------------|
| `x.to_string` | Cast to string |
| `x.to_int` / `x.to_long` | Cast to a 64-bit integer |
| `x.to_float` / `x.to_double` | Cast to a 64-bit floating point number |
| `x.to_boolean` | Cast to boolean |
| `x.to_date` | Cast to date |
| `x.to_timestamp` | Cast to timestamp |

## Null Handling

Available on all values:

| Function | Description |
|----------|-------------|
| `x.is_null` | True if the value is null |
| `x.is_not_null` | True if the value is not null |
| `x.or_else(default)` | Return `default` if the value is null (SQL `coalesce`) |
| `x.null_if(v)` | Return null if the value equals `v` (SQL `nullif`) |

## String Functions

| Function | Description |
|----------|-------------|
| `s.length` | Number of characters |
| `s.upper` / `s.lower` | Change case |
| `s.trim` / `s.ltrim` / `s.rtrim` | Remove surrounding whitespace |
| `s.reverse` | Reverse the characters |
| `s.concat(other)` | Concatenate strings (`\|\|`) |
| `s.substring(start)` | Substring from a 1-origin position |
| `s.substring(start, length)` | Substring of the given length |
| `s.replace(search, replacement)` | Replace all occurrences |
| `s.lpad(length, pad)` / `s.rpad(length, pad)` | Pad to the given length |
| `s.strpos(substr)` | 1-origin position of a substring (0 if absent) |
| `s.contains(substr)` | True if the string contains the substring |
| `s.starts_with(prefix)` / `s.ends_with(suffix)` | Prefix/suffix test |
| `s.like(pattern)` | SQL LIKE match |
| `s.regexp_like(pattern)` | True if the regex matches |
| `s.regexp_extract(pattern)` | First substring matching the regex |
| `s.regexp_extract(pattern, group)` | Regex capture group |
| `s.regexp_replace(pattern, replacement)` | Replace every regex match |
| `s.split(separator)` | Split into an `array[string]` |
| `s.levenshtein(other)` | Edit distance |
| `s.md5` / `s.sha256` | Hex-encoded digest |
| `s.json_extract(path)` | Extract a JSON value with a JSONPath (e.g. `'$.a.b'`) |
| `s.json_extract_string(path)` | Extract a JSON value as a plain string |

## Math Functions

Available on numeric values (int, long, float, double, decimal):

| Function | Description |
|----------|-------------|
| `x.abs` | Absolute value |
| `x.ceil` / `x.floor` | Round up / down |
| `x.round(digits)` | Round to the given number of digits |
| `x.truncate` | Drop the fractional part |
| `x.sqrt` / `x.cbrt` | Square/cube root |
| `x.exp` / `x.ln` / `x.log10` / `x.log2` | Exponential and logarithms |
| `x.power(exponent)` | Raise to a power |
| `x.mod(divisor)` | Modulo |
| `x.sign` | -1, 0, or 1 |
| `x.between(low, high)` | Range test |
| `x.in(v1, v2, ...)` / `x.not_in(...)` | Set membership |

On float/double values: `x.is_nan`, `x.is_finite`, `x.is_infinite`, and trigonometric functions (`sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `degrees`, `radians`).

On integer values: `x.from_unixtime` interprets the number as unix epoch seconds and returns a timestamp.

## Date and Timestamp Functions

On `date` and `timestamp` values:

| Function | Description |
|----------|-------------|
| `d.year` / `d.month` / `d.day` | Calendar fields |
| `d.quarter` / `d.week` | Quarter and ISO week of year |
| `d.day_of_week` | ISO day of week (1 = Monday, 7 = Sunday) |
| `d.day_of_year` | Day of year |
| `d.truncate_to(unit)` | Truncate to `'year'`, `'month'`, `'day'`, ... |
| `d.add_days(n)` / `d.add_months(n)` / `d.add_years(n)` | Date arithmetic |
| `d.diff_days(other)` / `d.diff_months(other)` / `d.diff_years(other)` | Difference in the given unit |
| `d.format(pattern)` | Format with a `'%Y-%m-%d'`-style pattern |
| `d.last_day` | Last day of the month |
| `d.extract(field)` | Extract an arbitrary field |

Additionally on timestamps: `hour`, `minute`, `second`, `add_seconds(n)`, `add_minutes(n)`, `add_hours(n)`, `diff_seconds(other)`, `diff_minutes(other)`, `diff_hours(other)`, `to_unixtime` (epoch seconds), and `to_date`.

```wvlet
from events
where event_time.between('2024-01-01'.to_timestamp, '2024-12-31'.to_timestamp)
select
  event_time.truncate_to('month') as month,
  event_time.format('%Y-%m-%d') as day
```

## Array Functions

On array values (e.g. from `split`, array literals, or `array_agg`):

| Function | Description |
|----------|-------------|
| `a.size` / `a.length` | Number of elements |
| `a.get(index)` | Element at a 1-origin index (same as `a[index]`) |
| `a.contains(elem)` | True if the array contains the element |
| `a.index_of(elem)` | 1-origin position of an element (0 if absent) |
| `a.sort` | Sort ascending |
| `a.reverse` | Reverse the order |
| `a.distinct` | Remove duplicates |
| `a.concat(other)` | Concatenate arrays |
| `a.flatten` | Flatten an array of arrays |
| `a.mk_string(separator)` | Join elements into a string |

## Map Functions

On map values (e.g. `map {"a": 1, "b": 2}`):

| Function | Description |
|----------|-------------|
| `m.size` | Number of entries |
| `m.keys` / `m.values` | Keys or values as an array |
| `m.contains_key(key)` | True if the key is present |
| `m.get(key)` | Value for the key, or null |

## Aggregation Functions

After `group by`, a column reference represents the group's values, and these
aggregation functions apply (see also [Aggregation](index.md#group-by)):

| Function | Description |
|----------|-------------|
| `c.count` / `c.count_distinct` | Count rows / distinct values |
| `c.count_if(cond)` | Count rows matching a condition |
| `c.count_approx_distinct` | Fast approximate distinct count |
| `c.min` / `c.max` / `c.sum` / `c.avg` | Basic aggregates |
| `c.min_by(expr)` / `c.max_by(expr)` | Value at the row minimizing/maximizing `expr` |
| `c.arbitrary` | Any value of the group |
| `c.to_array` | Collect values into an array |
| `c.string_agg(separator)` | Concatenate strings with a separator |
| `c.bool_and` / `c.bool_or` | Boolean aggregates |
| `c.median` | Median value |
| `c.stddev` / `c.variance` | Sample standard deviation / variance |
| `c.stddev_pop` / `c.stddev_samp` / `c.var_pop` / `c.var_samp` | Population/sample variants |
| `c.approx_quantile(pos)` | Approximate quantile (e.g. `0.95`) |

```wvlet
from orders
group by customer_id
agg
  _.count as order_count,
  amount.sum as total,
  amount.approx_quantile(0.95) as p95,
  product.string_agg(',') as products
```

## Engine-Specific Functions

Functions above compile to each target engine's SQL automatically. You can also define your own functions, including engine-specific variants, selected by the compile target:

```wvlet
-- Selected when compiling for DuckDB
def bit_count(x: long) in duckdb: int = sql"bit_count(${x})"
-- Selected when compiling for Trino
def bit_count(x: long) in trino: int = sql"bitwise_bit_count(${x})"
```

All DuckDB and Trino engine functions are bundled with the standard library, so calls like `bit_count(x)` type-check offline out of the box and compile to the SQL of the engine you target. For other databases, or engine-specific UDFs, import the engine's function catalog with [`wvlet catalog import`](../usage/catalog-import.md).
