---
sidebar_label: Stdlib Reference
---

# Standard Library Reference

<!-- Generated from wvlet-stdlib/module/standard/*.wv. DO NOT EDIT.
     Regenerate with: ./sbt "langJVM/Test/runMain wvlet.lang.compiler.codegen.StdLibDocGenerator" -->

Complete listing of the functions defined in the Wvlet standard library, generated from
its sources. Call these with dot syntax on a value of the listed type (e.g. `name.upper`,
`price.round(2)`). See [Standard Library Functions](stdlib.md) for a guided tour with
examples.

The **Engines** column lists the engines a definition is specialized for; **all** means a
single dialect-neutral definition serves every supported engine (DuckDB, Trino, Hive,
Snowflake, and BigQuery). Engine-specific SQL is selected automatically for the compile
target.

## Any Values

Available on values of every type.

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `to_string` | string | all |  |
| `to_int` | int | all |  |
| `to_long` | long | all |  |
| `to_float` | float | all |  |
| `to_double` | double | all |  |
| `to_boolean` | boolean | all |  |
| `to_date` | date | all | Cast to the SQL date type |
| `to_timestamp` | timestamp | all | Cast to the SQL timestamp type |
| `is_null` | boolean | all | True if this value is null |
| `is_not_null` | boolean | all |  |
| `or_else(other: any)` | any | all | Return the other value if this value is null |
| `null_if(v: any)` | any | all | Return null if this value equals the given value |
| `type_of` | string | all | Name of the runtime type of this value |
| `to_json` | json | duckdb, trino |  |

## Numeric Values

Shared by all numeric types (int, long, float, real, double, decimal).

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `round(decimal: int)` | double | all | Round to the given number of decimal places |
| `sqrt` | double | all |  |
| `cbrt` | double | all |  |
| `exp` | double | all |  |
| `ln` | double | all |  |
| `log10` | double | all |  |
| `log2` | double | all |  |
| `power(exponent: double)` | double | all |  |
| `sign` | int | all |  |
| `from_unixtime` | timestamp | duckdb, trino, snowflake, bigquery | Interpret this value as unix epoch seconds (in UTC), keeping the fraction to the microsecond (to the millisecond on Trino, whose from_unixtime(double) is a timestamp(3)) |
| `sinh` | double | all | Hyperbolic sine, cosine, and tangent |
| `cosh` | double | all |  |
| `tanh` | double | all |  |
| `asinh` | double | all | Inverse hyperbolic sine, cosine (NaN below 1), and tangent (NaN outside -1..1, infinite at -1 and 1) |
| `acosh` | double | duckdb, trino, snowflake, bigquery |  |
| `atanh` | double | duckdb, trino, snowflake, bigquery |  |
| `cot` | double | all | Cotangent (1 / tan) |
| `round_half_even` | double | duckdb, hive | Round to an integer, or to the given number of decimal places, rounding ties to the even neighbor (banker's rounding) |
| `round_half_even(decimal: int)` | double | duckdb, hive |  |
| `in(v: any*)` | boolean | all |  |
| `not_in(v: any*)` | boolean | all |  |

## Int Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `to_int` | int | all |  |
| `to_long` | long | all |  |
| `to_float` | float | all |  |
| `to_double` | double | all |  |
| `to_boolean` | boolean | all |  |
| `to_string` | string | all |  |
| `or_else(other: int)` | int | all |  |
| `abs` | int | all |  |
| `ceil` | int | all |  |
| `floor` | int | all |  |
| `mod(divisor: int)` | int | all |  |
| `gcd(other: long)` | long | duckdb | Greatest common divisor and least common multiple (non-negative; 0 when both are 0) |
| `lcm(other: long)` | long | duckdb |  |
| `factorial` | long | duckdb | n! for n in 0..20 (an error for negative n or when the result exceeds a long) |
| `from_unixtime` | timestamp | duckdb, trino, hive, snowflake, bigquery | Interpret this value as a unix epoch second (in UTC) |
| `between(l: int, r: int)` | boolean | all |  |

## Long Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `to_int` | int | all |  |
| `to_long` | long | all |  |
| `to_float` | float | all |  |
| `to_double` | double | all |  |
| `to_boolean` | boolean | all |  |
| `to_string` | string | all |  |
| `or_else(other: long)` | long | all |  |
| `abs` | long | all |  |
| `ceil` | long | all |  |
| `floor` | long | all |  |
| `mod(divisor: long)` | long | all |  |
| `gcd(other: long)` | long | duckdb | Greatest common divisor and least common multiple (non-negative; 0 when both are 0) |
| `lcm(other: long)` | long | duckdb |  |
| `factorial` | long | duckdb | n! for n in 0..20 (an error for negative n or when the result exceeds a long) |
| `from_unixtime` | timestamp | duckdb, trino, hive, snowflake, bigquery | Interpret this value as a unix epoch second (in UTC) |
| `between(l: long, r: long)` | boolean | all |  |
| `within(duration: string)` | boolean | td_trino |  |
| `td_time_string(format: string)` | string | td_trino, td_hive |  |
| `td_interval(duration: string)` | boolean | td_hive |  |

## Float Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `to_int` | int | all | Truncate toward zero (2.5 -> 2, -2.5 -> -2); use round to round |
| `to_long` | long | all | Truncate toward zero (2.5 -> 2, -2.5 -> -2); use round to round |
| `to_float` | float | all |  |
| `to_double` | double | all |  |
| `to_boolean` | boolean | all | Non-zero is true, NaN included |
| `to_string` | string | all |  |
| `or_else(other: float)` | float | all |  |
| `abs` | float | all |  |
| `ceil` | float | all |  |
| `floor` | float | all |  |
| `is_nan` | boolean | duckdb, trino, bigquery |  |
| `is_finite` | boolean | duckdb, trino, bigquery |  |
| `is_infinite` | boolean | duckdb, trino, bigquery |  |
| `between(l: float, r: float)` | boolean | all |  |

## Real Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `to_int` | int | all |  |
| `to_long` | long | all |  |
| `to_float` | float | all |  |
| `to_double` | double | all |  |
| `to_boolean` | boolean | all |  |
| `to_string` | string | all |  |
| `or_else(other: real)` | real | all |  |
| `abs` | real | all |  |
| `ceil` | real | all |  |
| `floor` | real | all |  |
| `between(l: real, r: real)` | boolean | all |  |

## Double Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `to_int` | int | all | Truncate toward zero (2.5 -> 2, -2.5 -> -2); use round to round |
| `to_long` | long | all | Truncate toward zero (2.5 -> 2, -2.5 -> -2); use round to round |
| `to_float` | float | all |  |
| `to_double` | double | all |  |
| `to_boolean` | boolean | all | Non-zero is true, NaN included |
| `to_string` | string | all |  |
| `or_else(other: double)` | double | all |  |
| `abs` | double | all |  |
| `ceil` | double | all |  |
| `floor` | double | all |  |
| `degrees` | double | all |  |
| `radians` | double | all |  |
| `sin` | double | all |  |
| `cos` | double | all |  |
| `tan` | double | all |  |
| `asin` | double | all |  |
| `acos` | double | all |  |
| `atan` | double | all |  |
| `truncate` | double | duckdb, trino, hive, snowflake, bigquery | Drop the fractional part of this value |
| `is_nan` | boolean | duckdb, trino, bigquery |  |
| `is_finite` | boolean | duckdb, trino, bigquery |  |
| `is_infinite` | boolean | duckdb, trino, bigquery |  |
| `between(l: double, r: double)` | boolean | all |  |

## Decimal Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `to_int` | int | all | Truncate toward zero (2.5 -> 2, -2.5 -> -2); use round to round |
| `to_long` | long | all | Truncate toward zero (2.5 -> 2, -2.5 -> -2); use round to round |
| `to_float` | float | all |  |
| `to_double` | double | all |  |
| `to_boolean` | boolean | all | Non-zero is true, NaN included |
| `to_string` | string | all |  |
| `or_else(other: decimal)` | decimal | all |  |
| `abs` | decimal | all |  |
| `ceil` | decimal | all |  |
| `floor` | decimal | all |  |
| `mod(divisor: decimal)` | decimal | all |  |
| `truncate` | decimal | duckdb, trino, hive, snowflake, bigquery | Drop the fractional part of this value |
| `between(l: decimal, r: decimal)` | boolean | all |  |

## Boolean Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `to_int` | int | all |  |
| `to_long` | long | all |  |
| `to_string` | string | all |  |
| `or_else(other: boolean)` | boolean | all | if the value is null, return the given default value |

## String Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `to_int` | int | all |  |
| `to_long` | long | all |  |
| `to_float` | float | all |  |
| `to_double` | double | all |  |
| `to_boolean` | boolean | all |  |
| `to_date` | date | all |  |
| `to_timestamp` | timestamp | all |  |
| `or_else(other: string)` | string | all | if the string is null, return the default value |
| `length` | int | all | Number of characters in the string |
| `upper` | string | all |  |
| `lower` | string | all |  |
| `trim` | string | all |  |
| `ltrim` | string | all |  |
| `rtrim` | string | all |  |
| `reverse` | string | all |  |
| `concat(other: string)` | string | all | Concatenate the given string after this string |
| `replace(search: string, replacement: string)` | string | all | Replace all occurrences of the search string with the replacement string |
| `lpad(length: int, pad: string)` | string | all | Pad the string to the given length by prepending (lpad) or appending (rpad) the pad string |
| `rpad(length: int, pad: string)` | string | all |  |
| `strpos(substr: string)` | int | all | 1-origin position of the first occurrence of the substring (0 if not found) |
| `like(pattern: string)` | boolean | all |  |
| `substring(start: int)` | string | all | Substring from the 1-origin start position (to the end, or of the given length) |
| `substring(start: int, length: int)` | string | all |  |
| `regexp_extract(pattern: string)` | string | all | Extract the first substring matching the regular expression (the whole match, or the given capture group). NULL when the pattern does not match; an empty match is '' |
| `regexp_extract(pattern: string, group_index: int)` | string | all |  |
| `starts_with(prefix: string)` | boolean | all |  |
| `translate(source: string, target: string)` | string | all | Replace each character of `source` with the character at the same position in `target`; characters of `source` with no counterpart in `target` are removed |
| `in(v: any*)` | boolean | all |  |
| `not_in(expr: any*)` | boolean | all |  |
| `regexp_like(pattern: string)` | boolean | duckdb, trino, hive, snowflake, bigquery | Snowflake's REGEXP_LIKE matches the entire string, so use REGEXP_INSTR for the partial-match semantics of the other engines |
| `regexp_replace(pattern: string, replacement: string)` | string | duckdb, trino, hive, snowflake, bigquery | Replace every substring matching the regular expression with the replacement |
| `contains(substr: string)` | boolean | duckdb, trino, hive, snowflake, bigquery |  |
| `ends_with(suffix: string)` | boolean | duckdb, trino, hive, snowflake, bigquery |  |
| `split(separator: string)` | array[string] | duckdb, trino, hive, snowflake, bigquery | Split the string by the separator into an array of strings |
| `md5` | string | duckdb, trino, hive, snowflake, bigquery | Hex-encoded digest strings |
| `sha256` | string | duckdb, trino, hive, snowflake, bigquery |  |
| `sha1` | string | duckdb, trino, hive, snowflake, bigquery |  |
| `to_base64` | string | duckdb, trino, hive, snowflake, bigquery | Base64 and lowercase hex encodings of the string's UTF-8 bytes, and their inverses |
| `from_base64` | string | duckdb, trino, hive, snowflake, bigquery |  |
| `to_hex` | string | duckdb, trino, hive, snowflake, bigquery |  |
| `from_hex` | string | duckdb, trino, hive, snowflake, bigquery |  |
| `octet_length` | int | duckdb, trino, hive, snowflake, bigquery | Number of bytes in the UTF-8 encoding of the string |
| `normalize` | string | duckdb, trino, bigquery | Unicode NFC normalization |
| `regexp_extract_all(pattern: string)` | array[string] | duckdb, trino, snowflake | All matches of the regular expression (the whole match, or the given capture group) |
| `regexp_extract_all(pattern: string, group_index: int)` | array[string] | duckdb, trino, snowflake |  |
| `regexp_split(pattern: string)` | array[string] | duckdb, trino | Split the string around matches of the regular expression, keeping empty strings |
| `url_encode` | string | duckdb, trino | Percent-encode the UTF-8 bytes except A-Z a-z 0-9 - _ . ~ (a space becomes %20), and decode it |
| `url_decode` | string | duckdb, trino |  |
| `hamming_distance(other: string)` | int | duckdb, trino | Number of positions with different characters; the strings must have the same length |
| `jaro_winkler_similarity(other: string)` | double | duckdb | Jaro-Winkler similarity from 0 (different) to 1 (equal); case-sensitive |
| `levenshtein(other: string)` | int | duckdb, trino, hive, snowflake, bigquery | Edit distance between two strings |
| `json_extract(path: string)` | json | duckdb, trino, hive, snowflake, bigquery | Extract a JSON value from a string holding JSON text |
| `json_extract_string(path: string)` | string | duckdb, trino, hive, snowflake, bigquery |  |
| `to_timestamp(format: string)` | timestamp | duckdb, trino, bigquery | Parse the string as a timestamp with strftime codes (e.g. '%Y-%m-%d %H:%M:%S'); the whole string must match. try_to_timestamp returns NULL where to_timestamp fails |
| `try_to_timestamp(format: string)` | timestamp | duckdb, trino, bigquery |  |
| `format(a: any)` | string | duckdb, trino, hive, bigquery | printf-style formatting with this string as the format: each %s takes the next argument's text and %% is a literal %, e.g. '%s:%s'.format(a, b). Pass strings (to_string others); other specifiers are engine-specific. A NULL argument gives NULL. Up to 5 arguments |
| `format(a: any, b: any)` | string | duckdb, trino, hive, bigquery |  |
| `format(a: any, b: any, c: any)` | string | duckdb, trino, hive, bigquery |  |
| `format(a: any, b: any, c: any, d: any)` | string | duckdb, trino, hive, bigquery |  |
| `format(a: any, b: any, c: any, d: any, e: any)` | string | duckdb, trino, hive, bigquery |  |

## Date Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `extract(field: string)` | int | all |  |
| `year` | int | all |  |
| `month` | int | all |  |
| `day` | int | all |  |
| `quarter` | int | all |  |
| `week` | int | all |  |
| `or_else(other: date)` | date | all |  |
| `truncate_to(unit: string)` | date | all | Truncate to the given unit ('year', 'quarter', 'month', 'week', 'day') |
| `diff_days(other: date)` | long | all | Difference between this and the other date, in the given unit |
| `diff_months(other: date)` | long | all |  |
| `diff_years(other: date)` | long | all |  |
| `between(l: date, r: date)` | boolean | all |  |
| `day_of_week` | int | duckdb, trino, hive, snowflake, bigquery | ISO day of week (1 = Monday, 7 = Sunday) and day of year |
| `day_of_year` | int | duckdb, trino, hive, snowflake, bigquery |  |
| `add_days(n: int)` | date | duckdb, trino, hive, snowflake, bigquery |  |
| `add_months(n: int)` | date | duckdb, trino, hive, snowflake, bigquery |  |
| `add_years(n: int)` | date | duckdb, trino, hive, snowflake, bigquery |  |
| `format(pattern: string)` | string | duckdb, trino, hive, snowflake, bigquery | Format the date with a strftime-style pattern (e.g. '%Y-%m-%d') |
| `last_day` | date | duckdb, trino, hive, snowflake, bigquery | The last day of the month of this date |
| `day_name` | string | duckdb, trino, hive, bigquery | Full English day and month names ('Monday', 'January') |
| `month_name` | string | duckdb, trino, hive, bigquery |  |
| `iso_year` | int | duckdb, trino, snowflake, bigquery | ISO-8601 week-numbering year: the year of the Thursday of this date's ISO week |

## Timestamp Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `extract(field: string)` | int | all |  |
| `year` | int | all |  |
| `month` | int | all |  |
| `day` | int | all |  |
| `hour` | int | all |  |
| `minute` | int | all |  |
| `second` | int | all |  |
| `quarter` | int | all |  |
| `week` | int | all |  |
| `or_else(other: timestamp)` | timestamp | all |  |
| `to_date` | date | all |  |
| `truncate_to(unit: string)` | timestamp | all | Truncate to the given unit ('year', 'month', 'day', 'hour', 'minute', 'second') |
| `diff_seconds(other: timestamp)` | long | all | Difference between this and the other timestamp, in the given unit |
| `diff_minutes(other: timestamp)` | long | all |  |
| `diff_hours(other: timestamp)` | long | all |  |
| `diff_days(other: timestamp)` | long | all |  |
| `between(l: timestamp, r: timestamp)` | boolean | all |  |
| `day_of_week` | int | duckdb, trino, hive, snowflake, bigquery | ISO day of week (1 = Monday, 7 = Sunday) and day of year |
| `day_of_year` | int | duckdb, trino, hive, snowflake, bigquery |  |
| `add_seconds(n: int)` | timestamp | duckdb, trino, hive, snowflake, bigquery | Timestamp arithmetic via unix epoch seconds (Hive has no timestamp interval functions) |
| `add_minutes(n: int)` | timestamp | duckdb, trino, hive, snowflake, bigquery |  |
| `add_hours(n: int)` | timestamp | duckdb, trino, hive, snowflake, bigquery |  |
| `add_days(n: int)` | timestamp | duckdb, trino, hive, snowflake, bigquery |  |
| `add_months(n: int)` | timestamp | duckdb, trino, snowflake |  |
| `add_years(n: int)` | timestamp | duckdb, trino, snowflake |  |
| `format(pattern: string)` | string | duckdb, trino, hive, snowflake, bigquery | Format the timestamp with a strftime-style pattern (e.g. '%Y-%m-%d %H:%M:%S') |
| `to_unixtime` | long | duckdb, trino, hive, snowflake, bigquery | Unix epoch seconds of this timestamp, floored to a whole second |
| `to_string` | string | duckdb, trino, hive, snowflake, bigquery | Render as 'YYYY-MM-DD HH:MM:SS'. Overridden per engine so the rendering is identical |
| `day_name` | string | duckdb, trino, hive, bigquery | Full English day and month names ('Monday', 'January') |
| `month_name` | string | duckdb, trino, hive, bigquery |  |
| `iso_year` | int | duckdb, trino, snowflake, bigquery | ISO-8601 week-numbering year: the year of the Thursday of this timestamp's ISO week |
| `to_time_zone(zone: string)` | timestamp | duckdb, trino, hive, snowflake, bigquery | This UTC wall-clock time as wall-clock time in the zone (e.g. 'Asia/Tokyo'), and the inverse |
| `from_time_zone(zone: string)` | timestamp | duckdb, trino, hive, snowflake, bigquery |  |

## JSON Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `json_extract(path: string)` | json | all | Extract the JSON value at the JSONPath expression (e.g. '$.store.book') |
| `json_extract_string(path: string)` | string | duckdb, trino | Extract the value at the JSONPath expression as a plain string |

## Null Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `to_int` | int | all |  |
| `to_long` | long | all |  |
| `to_float` | float | all |  |
| `to_double` | double | all |  |
| `to_boolean` | boolean | all |  |
| `to_date` | date | all |  |
| `to_timestamp` | timestamp | all |  |
| `to_string` | string | all |  |

## Array Values

A column reference after `group by` also has an array type, so aggregation functions apply to it with the same syntax.

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `length` | int | duckdb, trino, hive, snowflake, bigquery |  |
| `size` | int | duckdb, trino, hive, snowflake, bigquery |  |
| `get(index: int)` | A | all |  |
| `count` | int | all |  |
| `count_distinct` | int | all |  |
| `count_if(cond: boolean)` | int | all |  |
| `count_approx_distinct` | int | trino, duckdb, snowflake, bigquery | Fast and memory-efficient approximate counting of distinct elements |
| `arbitrary` | A | all |  |
| `any` | A | all |  |
| `min` | A | all |  |
| `max` | A | all |  |
| `min_by(expr: sql)` | A | all |  |
| `max_by(expr: sql)` | A | all |  |
| `map_agg(value: sql)` | map[A,any] | duckdb, trino | Map each grouped key (this column) to the value of the same row, e.g. key.map_agg(value). Rows with a NULL key are skipped; NULL values are kept; a group with no key gives NULL. Keys must be unique: a duplicate is an error on DuckDB and the engine, while Trino keeps one value. Use a column as the key: DuckDB's variant repeats the key expression |
| `to_array` | array[A] | all |  |
| `to_array(order_by: sql)` | array[A] | duckdb, trino, snowflake, bigquery | Collect the values in ascending order of the given key, e.g. item.to_array(ts). Rows with equal keys may come in any order |
| `bool_and` | boolean | all | Aggregate boolean values of a grouped column |
| `bool_or` | boolean | all |  |
| `string_agg(separator: string)` | string | duckdb, trino, hive, snowflake, bigquery | Concatenate grouped string values with the separator |
| `exclude(arr: sql)` | array[A] | duckdb, trino |  |
| `exists` | boolean | all |  |
| `not_exists` | boolean | all |  |
| `contains(elem: A)` | boolean | all | True if the array contains the given element |
| `mk_string` | string | duckdb, trino, hive, snowflake, bigquery | Concatenate the array elements into a string without a separator |
| `mk_string(separator: string)` | string | duckdb, trino, hive, snowflake, bigquery | Concatenate the array elements into a string with the separator |
| `distinct` | array[A] | all | Remove duplicate elements |
| `flatten` | array[A] | all | Flatten an array of arrays into a single array |
| `sort` | array[A] | duckdb, trino, hive, snowflake |  |
| `reverse` | array[A] | duckdb, trino, snowflake, bigquery |  |
| `index_of(elem: A)` | int | duckdb, trino, snowflake | 1-origin position of the first occurrence of the element (0 if not found) |
| `concat(other: any)` | array[A] | duckdb, trino, snowflake, bigquery |  |
| `transform(f: sql)` | array[any] | duckdb, trino | Apply the lambda to each element |
| `filter(f: sql)` | array[A] | duckdb, trino | Keep the elements for which the lambda returns true |
| `reduce(f: sql)` | A | duckdb, trino | Fold the elements from the first one with a two-argument lambda (acc, x) -> ..., e.g. [1, 2, 3].reduce((a, b) -> a + b) = 6. An empty array is an error |
| `lag` | A | all | Value of the column at a preceding row of the window |
| `lag(offset: int)` | A | all |  |
| `lag(offset: int, default: any)` | A | all |  |
| `lead` | A | all | Value of the column at a following row of the window |
| `lead(offset: int)` | A | all |  |
| `lead(offset: int, default: any)` | A | all |  |
| `first_value` | A | all | First value of the window |
| `last_value` | A | all | Last value of the window |
| `nth_value(n: int)` | A | all | Value at the n-th row (1-origin) of the window |
| `sum` | A | all |  |
| `avg` | A | all |  |
| `median` | A | all |  |
| `variance` | A | all |  |
| `stddev` | A | all |  |
| `stddev_pop` | A | all |  |
| `stddev_samp` | A | all |  |
| `var_pop` | A | all |  |
| `var_samp` | A | all |  |
| `corr(x: sql)` | double | all | Correlation and covariance of (this, x) pairs, skipping pairs with a NULL |
| `covar_pop(x: sql)` | double | all |  |
| `covar_samp(x: sql)` | double | all |  |
| `regr_slope(x: sql)` | double | duckdb, trino, hive, snowflake | Least-squares line of this (dependent) over x (independent), skipping pairs with a NULL |
| `regr_intercept(x: sql)` | double | duckdb, trino, hive, snowflake |  |
| `regr_r2(x: sql)` | double | duckdb, hive, snowflake | Coefficient of determination of the line, and the number of non-NULL pairs |
| `regr_count(x: sql)` | long | duckdb, hive, snowflake |  |
| `kurtosis` | double | duckdb, trino, snowflake | Sample excess kurtosis (NULL for fewer than 4 values) and sample skewness (fewer than 3) |
| `skewness` | double | duckdb, trino, snowflake | Trino's skewness is the population form: apply the sample correction |
| `bit_and` | A | duckdb, trino, snowflake, bigquery | Bitwise AND / OR / XOR of the integer values |
| `bit_or` | A | duckdb, trino, snowflake, bigquery |  |
| `bit_xor` | A | duckdb, trino, snowflake, bigquery |  |
| `approx_quantile(pos: double)` | A | trino, duckdb, hive, snowflake, bigquery |  |

## Map Values

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `size` | int | all | Number of entries in the map |
| `keys` | array[K] | all |  |
| `values` | array[V] | all |  |
| `contains_key(key: K)` | boolean | all | True if the map contains the given key |
| `get(key: K)` | V | duckdb, trino, hive, snowflake | The value for the given key, or null if the key is absent |

## Top-Level Functions

Called without a receiver value (window functions require an `over(...)` clause).

| Function | Returns | Engines | Description |
|----------|---------|---------|-------------|
| `range(start: long, stop: long)` | array[long] | duckdb, trino | Integers from start (inclusive) to stop (exclusive), e.g. range(1, 4) = [1, 2, 3]. An empty array when stop \<= start. Trino limits a range to 10000 elements |
| `range(start: long, stop: long, step: long)` | array[long] | duckdb, trino | Integers from start (inclusive) to stop (exclusive) in increments of step, e.g. range(0, 10, 3) = [0, 3, 6, 9] and range(3, 0, -1) = [3, 2, 1]. A step of 0 is an error on DuckDB and yields an empty array on Trino |
| `ulid_string` | string | all | Generate a new ULID |
| `uuid` | string | duckdb, trino, snowflake, bigquery | uuid(): a random version-4 UUID as a lowercase string, e.g. 'f47ac10b-58cc-4372-a567-0e02b2c3d479'. A new value for every row |
| `current_date` | date | duckdb, trino, snowflake, bigquery | current_date / current_timestamp: today's date and the current time, in UTC (stdlib timestamps are UTC wall-clock times). Written without parentheses |
| `current_timestamp` | timestamp | duckdb, trino, snowflake, bigquery |  |
| `row_number` | long | all | Sequential row number within the window (1, 2, 3, ...) |
| `rank` | long | all | Rank with gaps after ties (1, 1, 3, ...) |
| `dense_rank` | long | all | Rank without gaps after ties (1, 1, 2, ...) |
| `percent_rank` | double | all | Relative rank in [0, 1]: (rank - 1) / (rows - 1) |
| `cume_dist` | double | all | Cumulative distribution in (0, 1]: rows preceding or peer / rows |
| `ntile(n: int)` | long | all | Bucket number when the window is divided into n groups |
