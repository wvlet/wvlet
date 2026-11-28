---
sidebar_label: Stdlib Reference
---

# Standard Library Reference

<!-- Generated from wvlet-stdlib/module/standard/*.wv. DO NOT EDIT.
     Regenerate with: ./sbt "engineJVM/Test/runMain wvlet.lang.engine.StdLibDocGenerator" -->

Complete listing of the functions defined in the Wvlet standard library, generated from
its sources and grouped by purpose. Call a function with dot syntax on a value of its
receiver type (`string.upper` is called as `name.upper`, `numeric.round(decimal: int)` as
`price.round(2)`). See [Standard Library Functions](stdlib.md) for a guided tour.

A function defined on several types shows its receiver as `T` and lists the types
(`T.abs: T` with T: int, long, ...). Example results are computed by running each example
on the [Wvlet engine](../engines/wvlet-engine.md); try them in the
[Playground](https://wvlet.org/wvlet/playground/). A function marked with engines is
specialized for those engines only; the others have a single dialect-neutral definition
that serves every supported engine (DuckDB, Trino, Hive, Snowflake, and BigQuery).

| Category | Functions |
|----------|-----------|
| [Text](#text) | 27 |
| [Pattern Matching](#pattern-matching) | 10 |
| [Numeric](#numeric) | 37 |
| [Date and Time](#date-time) | 39 |
| [Type Conversion](#conversion) | 10 |
| [Comparison and Null Handling](#comparison) | 7 |
| [Aggregate](#aggregate) | 23 |
| [Statistical Aggregate](#statistical-aggregate) | 15 |
| [Window](#window) | 15 |
| [Array](#array) | 24 |
| [Map](#map) | 5 |
| [JSON](#json) | 3 |
| [Hashing, Encoding, and IDs](#encoding) | 11 |

## Text {#text}

Functions of string values.

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `string.length: int` | Number of characters in the string | `'Wvlet'.length` | `5` |
| `string.upper: string` |  | `'Wvlet'.upper` | `'WVLET'` |
| `string.lower: string` |  | `'Wvlet'.lower` | `'wvlet'` |
| `string.trim: string` |  | `'  Wvlet  '.trim` | `'Wvlet'` |
| `string.ltrim: string` |  | `'  Wvlet'.ltrim` | `'Wvlet'` |
| `string.rtrim: string` |  | `'Wvlet  '.rtrim` | `'Wvlet'` |
| `string.reverse: string` |  | `'Wvlet'.reverse` | `'telvW'` |
| `string.concat(other: string): string` | Concatenate the given string after this string | `'Wv'.concat('let')` | `'Wvlet'` |
| `string.replace(search: string, replacement: string): string` | Replace all occurrences of the search string with the replacement string | `'a-b-c'.replace('-', '/')` | `'a/b/c'` |
| `string.lpad(length: int, pad: string): string` | Pad the string to the given length by prepending (lpad) or appending (rpad) the pad string | `'42'.lpad(5, '0')` | `'00042'` |
| `string.rpad(length: int, pad: string): string` |  | `'ab'.rpad(5, '.')` | `'ab...'` |
| `string.strpos(substr: string): int` | 1-origin position of the first occurrence of the substring (0 if not found) | `'Wvlet'.strpos('let')` | `3` |
| `string.substring(start: int): string` | Substring from the 1-origin start position (to the end, or of the given length) | `'Wvlet'.substring(3)` | `'let'` |
| `string.substring(start: int, length: int): string` | Substring from the 1-origin start position (to the end, or of the given length) | `'Wvlet'.substring(2, 3)` | `'vle'` |
| `string.translate(source: string, target: string): string` | Replace each character of `source` with the character at the same position in `target`; characters of `source` with no counterpart in `target` are removed | `'2024-03-15'.translate('-', '/')` | `'2024/03/15'` |
| `string.contains(substr: string): boolean` | Engines: duckdb, trino, hive, snowflake, bigquery. | `'Wvlet'.contains('vle')` | `true` |
| `string.split(separator: string): array[string]` | Split the string by the separator into an array of strings Engines: duckdb, trino, hive, snowflake, bigquery. | `'a,b,c'.split(',')` | `['a', 'b', 'c']` |
| `string.octet_length: int` | Number of bytes in the UTF-8 encoding of the string Engines: duckdb, trino, hive, snowflake, bigquery. | `'é'.octet_length` | `2` |
| `string.normalize: string` | Unicode NFC normalization Engines: duckdb, trino, bigquery. | `'Wvlet'.normalize` | `'Wvlet'` |
| `string.hamming_distance(other: string): int` | Number of positions with different characters; the strings must have the same length Engines: duckdb, trino. | `'karolin'.hamming_distance('kathrin')` | `3` |
| `string.jaro_winkler_similarity(other: string): double` | Jaro-Winkler similarity from 0 (different) to 1 (equal); case-sensitive Engines: duckdb. | `'martha'.jaro_winkler_similarity('marhta')` | `0.961111` |
| `string.levenshtein(other: string): int` | Edit distance between two strings Engines: duckdb, trino, hive, snowflake, bigquery. | `'kitten'.levenshtein('sitting')` | `3` |
| `string.format(a: any): string` | printf-style formatting with this string as the format: each %s takes the next argument's text and %% is a literal %, e.g. '%s:%s'.format(a, b). Pass strings (to_string others); other specifiers are engine-specific. A NULL argument gives NULL. Up to 5 arguments Engines: duckdb, trino, hive, bigquery. | `'Hello, %s!'.format('Wvlet')` | `'Hello, Wvlet!'` |
| `string.format(a: any, b: any): string` | printf-style formatting with this string as the format: each %s takes the next argument's text and %% is a literal %, e.g. '%s:%s'.format(a, b). Pass strings (to_string others); other specifiers are engine-specific. A NULL argument gives NULL. Up to 5 arguments Engines: duckdb, trino, hive, bigquery. | `'%s-%s'.format('a', 'b')` | `'a-b'` |
| `string.format(a: any, b: any, c: any): string` | printf-style formatting with this string as the format: each %s takes the next argument's text and %% is a literal %, e.g. '%s:%s'.format(a, b). Pass strings (to_string others); other specifiers are engine-specific. A NULL argument gives NULL. Up to 5 arguments Engines: duckdb, trino, hive, bigquery. | `'%s/%s/%s'.format('a', 'b', 'c')` | `'a/b/c'` |
| `string.format(a: any, b: any, c: any, d: any): string` | printf-style formatting with this string as the format: each %s takes the next argument's text and %% is a literal %, e.g. '%s:%s'.format(a, b). Pass strings (to_string others); other specifiers are engine-specific. A NULL argument gives NULL. Up to 5 arguments Engines: duckdb, trino, hive, bigquery. | `'%s %s %s %s'.format('a', 'b', 'c', 'd')` | `'a b c d'` |
| `string.format(a: any, b: any, c: any, d: any, e: any): string` | printf-style formatting with this string as the format: each %s takes the next argument's text and %% is a literal %, e.g. '%s:%s'.format(a, b). Pass strings (to_string others); other specifiers are engine-specific. A NULL argument gives NULL. Up to 5 arguments Engines: duckdb, trino, hive, bigquery. | `'%s%s%s%s%s'.format('W', 'v', 'l', 'e', 't')` | `'Wvlet'` |

## Pattern Matching {#pattern-matching}

Match strings against `like` patterns, prefixes, and regular expressions.

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `string.like(pattern: string): boolean` |  | `'Wvlet'.like('Wv%')` | `true` |
| `string.regexp_extract(pattern: string): string` | Extract the first substring matching the regular expression (the whole match, or the given capture group). NULL when the pattern does not match; an empty match is '' | `from [['order-123']] as t(s) select s.regexp_extract('[0-9]+')` | `'123'` |
| `string.regexp_extract(pattern: string, group_index: int): string` | Extract the first substring matching the regular expression (the whole match, or the given capture group). NULL when the pattern does not match; an empty match is '' | `from [['key=value']] as t(s) select s.regexp_extract('([a-z]+)=([a-z]+)', 2)` | `'value'` |
| `string.starts_with(prefix: string): boolean` |  | `'Wvlet'.starts_with('Wv')` | `true` |
| `string.regexp_like(pattern: string): boolean` | Snowflake's REGEXP_LIKE matches the entire string, so use REGEXP_INSTR for the partial-match semantics of the other engines Engines: duckdb, trino, hive, snowflake, bigquery. | `'abc123'.regexp_like('^[a-z]+[0-9]+$')` | `true` |
| `string.regexp_replace(pattern: string, replacement: string): string` | Replace every substring matching the regular expression with the replacement Engines: duckdb, trino, hive, snowflake, bigquery. | `'a1b22c333'.regexp_replace('[0-9]+', '#')` | `'a#b#c#'` |
| `string.ends_with(suffix: string): boolean` | Engines: duckdb, trino, hive, snowflake, bigquery. | `'Wvlet'.ends_with('let')` | `true` |
| `string.regexp_extract_all(pattern: string): array[string]` | All matches of the regular expression (the whole match, or the given capture group) Engines: duckdb, trino, snowflake. | `'a1b22c333'.regexp_extract_all('[0-9]+')` | `['1', '22', '333']` |
| `string.regexp_extract_all(pattern: string, group_index: int): array[string]` | All matches of the regular expression (the whole match, or the given capture group) Engines: duckdb, trino, snowflake. | `'a=1, b=2'.regexp_extract_all('([a-z])=([0-9])', 1)` | `['a', 'b']` |
| `string.regexp_split(pattern: string): array[string]` | Split the string around matches of the regular expression, keeping empty strings Engines: duckdb, trino. | `'a1b22c'.regexp_split('[0-9]+')` | `['a', 'b', 'c']` |

## Numeric {#numeric}

Arithmetic, rounding, and math functions of numeric values (int, long, float, double, decimal).

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `numeric.round(decimal: int): double` | Round to the given number of decimal places | `3.14159.round(2)` | `3.14` |
| `numeric.sqrt: double` |  | `16.sqrt` | `4.0` |
| `numeric.cbrt: double` |  | `27.cbrt` | `3.0` |
| `numeric.exp: double` |  | `1.exp` | `2.718282` |
| `numeric.ln: double` |  | `1.ln` | `0.0` |
| `numeric.log10: double` |  | `1000.log10` | `3.0` |
| `numeric.log2: double` |  | `8.log2` | `3.0` |
| `numeric.power(exponent: double): double` |  | `2.power(10)` | `1024.0` |
| `numeric.sign: int` |  | `(-5).sign` | `-1` |
| `numeric.sinh: double` | Hyperbolic sine, cosine, and tangent | `1.0.sinh` | `1.175201` |
| `numeric.cosh: double` |  | `1.0.cosh` | `1.543081` |
| `numeric.tanh: double` |  | `1.0.tanh` | `0.761594` |
| `numeric.asinh: double` | Inverse hyperbolic sine, cosine (NaN below 1), and tangent (NaN outside -1..1, infinite at -1 and 1) | `1.0.asinh` | `0.881374` |
| `numeric.acosh: double` | Engines: duckdb, trino, snowflake, bigquery. | `2.0.acosh` | `1.316958` |
| `numeric.atanh: double` | Engines: duckdb, trino, snowflake, bigquery. | `0.5.atanh` | `0.549306` |
| `numeric.cot: double` | Cotangent (1 / tan) | `1.0.cot` | `0.642093` |
| `numeric.round_half_even: double` | Round to an integer, or to the given number of decimal places, rounding ties to the even neighbor (banker's rounding) Engines: duckdb, hive. | `2.5.round_half_even` | `2` |
| `numeric.round_half_even(decimal: int): double` | Round to an integer, or to the given number of decimal places, rounding ties to the even neighbor (banker's rounding) Engines: duckdb, hive. | `2.345.round_half_even(2)` | `2.34` |
| `T.abs: T`<br/>T: int, long, float, real, double, decimal |  | `from [[-5]] as t(x) select x.abs`<br/>`(-3.5).abs` | `5`<br/>`3.5` |
| `T.ceil: T`<br/>T: int, long, float, real, double, decimal |  | `3.2.ceil` | `4` |
| `T.floor: T`<br/>T: int, long, float, real, double, decimal |  | `3.8.floor` | `3` |
| `T.mod(divisor: T): T`<br/>T: int, long, decimal |  | `17.mod(5)`<br/>`10.5.mod(3)` | `2`<br/>`1.5` |
| `int.gcd(other: long): long` | Greatest common divisor and least common multiple (non-negative; 0 when both are 0) Engines: int: duckdb; long: duckdb. | `12.gcd(18)` | `6` |
| `int.lcm(other: long): long` | Engines: int: duckdb; long: duckdb. | `4.lcm(6)` | `12` |
| `int.factorial: long` | n! for n in 0..20 (an error for negative n or when the result exceeds a long) Engines: int: duckdb; long: duckdb. | `5.factorial` | `120` |
| `T.is_nan: boolean`<br/>T: float, double | Engines: float: duckdb, trino, bigquery; double: duckdb, trino, bigquery. | `0.0.to_double.is_nan` | `false` |
| `T.is_finite: boolean`<br/>T: float, double | Engines: float: duckdb, trino, bigquery; double: duckdb, trino, bigquery. | `1.0.to_double.is_finite` | `true` |
| `T.is_infinite: boolean`<br/>T: float, double | Engines: float: duckdb, trino, bigquery; double: duckdb, trino, bigquery. | `1.0.to_double.is_infinite` | `false` |
| `double.degrees: double` |  | `3.141592653589793.to_double.degrees` | `180.0` |
| `double.radians: double` |  | `180.0.to_double.radians` | `3.141593` |
| `double.sin: double` |  | `0.0.to_double.sin` | `0.0` |
| `double.cos: double` |  | `0.0.to_double.cos` | `1.0` |
| `double.tan: double` |  | `0.0.to_double.tan` | `0.0` |
| `double.asin: double` |  | `1.0.to_double.asin` | `1.570796` |
| `double.acos: double` |  | `1.0.to_double.acos` | `0.0` |
| `double.atan: double` |  | `1.0.to_double.atan` | `0.785398` |
| `T.truncate: T`<br/>T: double, decimal | Drop the fractional part of this value Engines: double: duckdb, trino, hive, snowflake, bigquery; decimal: duckdb, trino, hive, snowflake, bigquery. | `(-3.7).truncate` | `-3` |

## Date and Time {#date-time}

Extract parts of dates and timestamps, shift and truncate them, and format them as text.

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `T.from_unixtime: timestamp`<br/>T: numeric, int, long | Interpret this value as unix epoch seconds (in UTC), keeping the fraction to the microsecond (to the millisecond on Trino, whose from_unixtime(double) is a timestamp(3)) Engines: numeric: duckdb, trino, snowflake, bigquery; int: duckdb, trino, hive, snowflake, bigquery; long: duckdb, trino, hive, snowflake, bigquery. | `1710498645.from_unixtime` | `2024-03-15 10:30:45` |
| `long.within(duration: string): boolean` | Engines: td_trino. |  |  |
| `long.td_time_string(format: string): string` | Engines: td_trino, td_hive. |  |  |
| `long.td_interval(duration: string): boolean` | Engines: td_hive. |  |  |
| `string.try_to_timestamp(format: string): timestamp` | Engines: duckdb, trino, bigquery. | `'2024/03/15'.try_to_timestamp('%Y/%m/%d')` | `2024-03-15 00:00:00` |
| `T.extract(field: string): int`<br/>T: date, timestamp |  | `'2024-03-15'.to_date.extract('dow')`<br/>`'2024-03-15 10:30:45'.to_timestamp.extract('hour')` | `5`<br/>`10` |
| `T.year: int`<br/>T: date, timestamp |  | `'2024-03-15'.to_date.year`<br/>`'2024-03-15 10:30:45'.to_timestamp.year` | `2024`<br/>`2024` |
| `T.month: int`<br/>T: date, timestamp |  | `'2024-03-15'.to_date.month` | `3` |
| `T.day: int`<br/>T: date, timestamp |  | `'2024-03-15'.to_date.day` | `15` |
| `T.quarter: int`<br/>T: date, timestamp |  | `'2024-03-15'.to_date.quarter` | `1` |
| `T.week: int`<br/>T: date, timestamp |  | `'2024-03-15'.to_date.week` | `11` |
| `T.truncate_to(unit: string): T`<br/>T: date, timestamp | Truncate to the given unit ('year', 'quarter', 'month', 'week', 'day') | `'2024-03-15'.to_date.truncate_to('month')`<br/>`'2024-03-15 10:30:45'.to_timestamp.truncate_to('hour')` | `2024-03-01`<br/>`2024-03-15 10:00:00` |
| `T.diff_days(other: T): long`<br/>T: date, timestamp | Difference between this and the other date, in the given unit | `'2024-03-15'.to_date.diff_days('2024-03-25'.to_date)`<br/>`'2024-03-15 10:00:00'.to_timestamp.diff_days('2024-03-18 09:00:00'.to_timestamp)` | `10`<br/>`3` |
| `date.diff_months(other: date): long` |  | `'2024-03-15'.to_date.diff_months('2024-06-15'.to_date)` | `3` |
| `date.diff_years(other: date): long` |  | `'2020-03-15'.to_date.diff_years('2024-03-15'.to_date)` | `4` |
| `T.day_of_week: int`<br/>T: date, timestamp | ISO day of week (1 = Monday, 7 = Sunday) and day of year Engines: date: duckdb, trino, hive, snowflake, bigquery; timestamp: duckdb, trino, hive, snowflake, bigquery. | `'2024-03-15'.to_date.day_of_week` | `5` |
| `T.day_of_year: int`<br/>T: date, timestamp | Engines: date: duckdb, trino, hive, snowflake, bigquery; timestamp: duckdb, trino, hive, snowflake, bigquery. | `'2024-03-15'.to_date.day_of_year` | `75` |
| `T.add_days(n: int): T`<br/>T: date, timestamp | Engines: date: duckdb, trino, hive, snowflake, bigquery; timestamp: duckdb, trino, hive, snowflake, bigquery. | `'2024-03-15'.to_date.add_days(20)`<br/>`'2024-03-15 10:30:45'.to_timestamp.add_days(1)` | `2024-04-04`<br/>`2024-03-16 10:30:45` |
| `T.add_months(n: int): T`<br/>T: date, timestamp | Engines: date: duckdb, trino, hive, snowflake, bigquery; timestamp: duckdb, trino, snowflake. | `'2024-01-31'.to_date.add_months(1)`<br/>`'2024-03-15 10:30:45'.to_timestamp.add_months(1)` | `2024-02-29`<br/>`2024-04-15 10:30:45` |
| `T.add_years(n: int): T`<br/>T: date, timestamp | Engines: date: duckdb, trino, hive, snowflake, bigquery; timestamp: duckdb, trino, snowflake. | `'2024-02-29'.to_date.add_years(1)`<br/>`'2024-03-15 10:30:45'.to_timestamp.add_years(1)` | `2025-02-28`<br/>`2025-03-15 10:30:45` |
| `T.format(pattern: string): string`<br/>T: date, timestamp | Format the date with a strftime-style pattern (e.g. '%Y-%m-%d') Engines: date: duckdb, trino, hive, snowflake, bigquery; timestamp: duckdb, trino, hive, snowflake, bigquery. | `'2024-03-15'.to_date.format('%Y/%m/%d')`<br/>`'2024-03-15 10:30:45'.to_timestamp.format('%Y-%m-%d %H:%M')` | `'2024/03/15'`<br/>`'2024-03-15 10:30'` |
| `date.last_day: date` | The last day of the month of this date Engines: duckdb, trino, hive, snowflake, bigquery. | `'2024-02-10'.to_date.last_day` | `2024-02-29` |
| `T.day_name: string`<br/>T: date, timestamp | Full English day and month names ('Monday', 'January') Engines: date: duckdb, trino, hive, bigquery; timestamp: duckdb, trino, hive, bigquery. | `'2024-03-15'.to_date.day_name`<br/>`'2024-03-15 10:30:45'.to_timestamp.day_name` | `'Friday'`<br/>`'Friday'` |
| `T.month_name: string`<br/>T: date, timestamp | Engines: date: duckdb, trino, hive, bigquery; timestamp: duckdb, trino, hive, bigquery. | `'2024-03-15'.to_date.month_name` | `'March'` |
| `T.iso_year: int`<br/>T: date, timestamp | ISO-8601 week-numbering year: the year of the Thursday of this date's ISO week Engines: date: duckdb, trino, snowflake, bigquery; timestamp: duckdb, trino, snowflake, bigquery. | `'2024-12-30'.to_date.iso_year` | `2025` |
| `timestamp.hour: int` |  | `'2024-03-15 10:30:45'.to_timestamp.hour` | `10` |
| `timestamp.minute: int` |  | `'2024-03-15 10:30:45'.to_timestamp.minute` | `30` |
| `timestamp.second: int` |  | `'2024-03-15 10:30:45'.to_timestamp.second` | `45` |
| `timestamp.diff_seconds(other: timestamp): long` | Difference between this and the other timestamp, in the given unit | `'2024-03-15 10:30:00'.to_timestamp.diff_seconds('2024-03-15 10:31:30'.to_timestamp)` | `90` |
| `timestamp.diff_minutes(other: timestamp): long` |  | `'2024-03-15 10:30:00'.to_timestamp.diff_minutes('2024-03-15 12:00:00'.to_timestamp)` | `90` |
| `timestamp.diff_hours(other: timestamp): long` |  | `'2024-03-15 10:30:45'.to_timestamp.diff_hours('2024-03-15 13:30:45'.to_timestamp)` | `3` |
| `timestamp.add_seconds(n: int): timestamp` | Timestamp arithmetic via unix epoch seconds (Hive has no timestamp interval functions) Engines: duckdb, trino, hive, snowflake, bigquery. | `'2024-03-15 10:30:45'.to_timestamp.add_seconds(30)` | `2024-03-15 10:31:15` |
| `timestamp.add_minutes(n: int): timestamp` | Engines: duckdb, trino, hive, snowflake, bigquery. | `'2024-03-15 10:30:45'.to_timestamp.add_minutes(45)` | `2024-03-15 11:15:45` |
| `timestamp.add_hours(n: int): timestamp` | Engines: duckdb, trino, hive, snowflake, bigquery. | `'2024-03-15 10:30:45'.to_timestamp.add_hours(3)` | `2024-03-15 13:30:45` |
| `timestamp.to_unixtime: long` | Unix epoch seconds of this timestamp, floored to a whole second Engines: duckdb, trino, hive, snowflake, bigquery. | `'2024-03-15 10:30:45'.to_timestamp.to_unixtime` | `1710498645` |
| `timestamp.to_time_zone(zone: string): timestamp` | This UTC wall-clock time as wall-clock time in the zone (e.g. 'Asia/Tokyo'), and the inverse Engines: duckdb, trino, hive, snowflake, bigquery. | `'2024-03-15 10:00:00'.to_timestamp.to_time_zone('Asia/Tokyo')` | `2024-03-15 19:00:00` |
| `timestamp.from_time_zone(zone: string): timestamp` | Engines: duckdb, trino, hive, snowflake, bigquery. | `'2024-03-15 19:00:00'.to_timestamp.from_time_zone('Asia/Tokyo')` | `2024-03-15 10:00:00` |
| `current_date: date` | current_date / current_timestamp: today's date and the current time, in UTC (stdlib timestamps are UTC wall-clock times). Written without parentheses Engines: duckdb, trino, snowflake, bigquery. |  |  |
| `current_timestamp: timestamp` | Engines: duckdb, trino, snowflake, bigquery. |  |  |

## Type Conversion {#conversion}

Cast a value to another type.

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `T.to_string: string`<br/>T: any, int, long, float, real, double, decimal, boolean, timestamp, null | Hive rejects `varchar` without a length in casts; its string type is `string` Engines: timestamp: duckdb, trino, hive, snowflake, bigquery. | `42.to_string`<br/>`3.5.to_string`<br/>`true.to_string`<br/>`'2024-03-15 10:30:45'.to_timestamp.to_string` | `'42'`<br/>`'3.5'`<br/>`'true'`<br/>`'2024-03-15 10:30:45'` |
| `any.to_int: int` | Truncate toward zero (2.5 -> 2, -2.5 -> -2); use round to round | `'42'.to_int`<br/>`3.7.to_int`<br/>`true.to_int` | `42`<br/>`3`<br/>`1` |
| `any.to_long: long` | Truncate toward zero (2.5 -> 2, -2.5 -> -2); use round to round | `'42'.to_long` | `42` |
| `any.to_float: float` |  | `'1.5'.to_float` | `1.5` |
| `any.to_double: double` |  | `42.to_double`<br/>`'3.14'.to_double` | `42.0`<br/>`3.14` |
| `T.to_boolean: boolean`<br/>T: any, int, long, float, real, double, decimal, string, null | Non-zero is true, NaN included | `'true'.to_boolean` | `true` |
| `T.to_date: date`<br/>T: any, string, timestamp, null | Cast to the SQL date type | `'2024-03-15'.to_date`<br/>`'2024-03-15 10:30:45'.to_timestamp.to_date` | `2024-03-15`<br/>`2024-03-15` |
| `T.to_timestamp: timestamp`<br/>T: any, string, null | Cast to the SQL timestamp type | `'2024-03-15 10:30:00'.to_timestamp` | `2024-03-15 10:30:00` |
| `any.type_of: string` | Name of the runtime type of this value | `'Wvlet'.type_of` | `'string'` |
| `string.to_timestamp(format: string): timestamp` | Parse the string as a timestamp with strftime codes (e.g. '%Y-%m-%d %H:%M:%S'); the whole string must match. try_to_timestamp returns NULL where to_timestamp fails Engines: duckdb, trino, bigquery. | `'2024/03/15 10:30'.to_timestamp('%Y/%m/%d %H:%M')` | `2024-03-15 10:30:00` |

## Comparison and Null Handling {#comparison}

Range and set membership tests, and functions that handle null values.

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `any.is_null: boolean` | True if this value is null | `from [[1], [null]] as t(x) select x.is_null` | `false, true` |
| `any.is_not_null: boolean` |  | `from [[1], [null]] as t(x) select x.is_not_null` | `true, false` |
| `T.or_else(other: T): T`<br/>T: any, int, long, float, real, double, decimal, boolean, string, date, timestamp | Return the other value if this value is null | `from [[1], [null]] as t(x) select x.or_else(0)`<br/>`from [['a'], [null]] as t(s) select s.or_else('-')` | `1, 0`<br/>`'a', '-'` |
| `any.null_if(v: any): any` | Return null if this value equals the given value | `from [[0], [5]] as t(x) select x.null_if(0)` | `null, 5` |
| `T.in(v: any*): boolean`<br/>T: numeric, string |  | `2.in(1, 2, 3)`<br/>`'b'.in('a', 'b', 'c')` | `true`<br/>`true` |
| `numeric.not_in(v: any*): boolean` |  | `5.not_in(1, 2, 3)`<br/>`'d'.not_in('a', 'b', 'c')` | `true`<br/>`true` |
| `T.between(l: T, r: T): boolean`<br/>T: int, long, float, real, double, decimal, date, timestamp |  | `5.between(1, 10)`<br/>`'2024-03-15'.to_date.between('2024-01-01'.to_date, '2024-12-31'.to_date)` | `true`<br/>`true` |

## Aggregate {#aggregate}

Aggregate the values of a column after `group by`, or of the whole table with `agg`.

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `array.count: int` |  | `from [[1], [2], [2]] as t(x) agg x.count` | `3` |
| `array.count_distinct: int` |  | `from [[1], [2], [2]] as t(x) agg x.count_distinct` | `2` |
| `array.count_if(cond: boolean): int` |  | `from [[1], [2], [3]] as t(x) agg x.count_if(x > 1)` | `2` |
| `array.count_approx_distinct: int` | Fast and memory-efficient approximate counting of distinct elements Engines: trino, duckdb, snowflake, bigquery. | `from [[1], [2], [2]] as t(x) agg x.count_approx_distinct` | `2` |
| `array.arbitrary: A` |  | `from [[7], [7]] as t(x) agg x.arbitrary` | `7` |
| `array.any: A` |  | `from [[7], [7]] as t(x) agg x.any` | `7` |
| `array.min: A` |  | `from [[3], [1], [2]] as t(x) agg x.min` | `1` |
| `array.max: A` |  | `from [[3], [1], [2]] as t(x) agg x.max` | `3` |
| `array.min_by(expr: sql): A` |  | `from [['a', 3], ['b', 1]] as t(name, score) agg name.min_by(score)` | `'b'` |
| `array.max_by(expr: sql): A` |  | `from [['a', 3], ['b', 1]] as t(name, score) agg name.max_by(score)` | `'a'` |
| `array.map_agg(value: sql): map[A,any]` | Map each grouped key (this column) to the value of the same row, e.g. key.map_agg(value). Rows with a NULL key are skipped; NULL values are kept; a group with no key gives NULL. Keys must be unique: a duplicate is an error on DuckDB and the engine, while Trino keeps one value. Use a column as the key: DuckDB's variant repeats the key expression Engines: duckdb, trino. | `from [['a', 1], ['b', 2]] as t(k, v) agg k.map_agg(v)` | `{'a': 1, 'b': 2}` |
| `array.to_array: array[A]` | Collect the values in ascending order of the given key, e.g. item.to_array(ts). Rows with equal keys may come in any order | `from [['a', 1], ['a', 2], ['b', 3]] as t(g, x) group by g agg x.to_array order by g` | `('a', [1, 2]), ('b', [3])` |
| `array.to_array(order_by: sql): array[A]` | Collect the values in ascending order of the given key, e.g. item.to_array(ts). Rows with equal keys may come in any order Engines: duckdb, trino, snowflake, bigquery. | `from [['b', 2], ['a', 1]] as t(s, k) agg s.to_array(k)` | `['a', 'b']` |
| `array.bool_and: boolean` | Aggregate boolean values of a grouped column | `from [[true], [false]] as t(b) agg b.bool_and` | `false` |
| `array.bool_or: boolean` |  | `from [[true], [false]] as t(b) agg b.bool_or` | `true` |
| `array.string_agg(separator: string): string` | Concatenate grouped string values with the separator Engines: duckdb, trino, hive, snowflake, bigquery. | `from [['a'], ['b']] as t(s) agg s.string_agg(',')` | `'a,b'` |
| `array.sum: A` |  | `from [[1], [2], [3]] as t(x) agg x.sum` | `6` |
| `array.avg: A` |  | `from [[1], [2], [3], [4]] as t(x) agg x.avg` | `2.5` |
| `array.median: A` |  | `from [[1], [2], [10]] as t(x) agg x.median` | `2.0` |
| `array.bit_and: A` | Bitwise AND / OR / XOR of the integer values Engines: duckdb, trino, snowflake, bigquery. | `from [[12], [10]] as t(x) agg x.bit_and` | `8` |
| `array.bit_or: A` | Engines: duckdb, trino, snowflake, bigquery. | `from [[12], [10]] as t(x) agg x.bit_or` | `14` |
| `array.bit_xor: A` | Engines: duckdb, trino, snowflake, bigquery. | `from [[12], [10]] as t(x) agg x.bit_xor` | `6` |
| `array.approx_quantile(pos: double): A` | Engines: trino, duckdb, hive, snowflake, bigquery. | `from [[1], [2], [3], [4], [5]] as t(x) agg x.approx_quantile(0.5)` | `3` |

## Statistical Aggregate {#statistical-aggregate}

Variance, correlation, and regression of a column's values, used like other aggregates.

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `array.variance: A` |  | `from [[1], [2], [3], [4]] as t(x) agg x.variance` | `1.666667` |
| `array.stddev: A` |  | `from [[1], [2], [3], [4]] as t(x) agg x.stddev` | `1.290994` |
| `array.stddev_pop: A` |  | `from [[1], [2], [3], [4]] as t(x) agg x.stddev_pop` | `1.118034` |
| `array.stddev_samp: A` |  | `from [[1], [2], [3], [4]] as t(x) agg x.stddev_samp` | `1.290994` |
| `array.var_pop: A` |  | `from [[1], [2], [3], [4]] as t(x) agg x.var_pop` | `1.25` |
| `array.var_samp: A` |  | `from [[1], [2], [3], [4]] as t(x) agg x.var_samp` | `1.666667` |
| `array.corr(x: sql): double` | Correlation and covariance of (this, x) pairs, skipping pairs with a NULL | `from [[1, 2], [2, 4], [3, 7]] as t(x, y) agg y.corr(x)` | `0.993399` |
| `array.covar_pop(x: sql): double` |  | `from [[1, 2], [2, 4], [3, 7]] as t(x, y) agg y.covar_pop(x)` | `1.666667` |
| `array.covar_samp(x: sql): double` |  | `from [[1, 2], [2, 4], [3, 7]] as t(x, y) agg y.covar_samp(x)` | `2.5` |
| `array.regr_slope(x: sql): double` | Least-squares line of this (dependent) over x (independent), skipping pairs with a NULL Engines: duckdb, trino, hive, snowflake. | `from [[1, 2], [2, 4], [3, 6]] as t(x, y) agg y.regr_slope(x)` | `2.0` |
| `array.regr_intercept(x: sql): double` | Engines: duckdb, trino, hive, snowflake. | `from [[1, 3], [2, 5], [3, 7]] as t(x, y) agg y.regr_intercept(x)` | `1.0` |
| `array.regr_r2(x: sql): double` | Coefficient of determination of the line, and the number of non-NULL pairs Engines: duckdb, hive, snowflake. | `from [[1, 2], [2, 4], [3, 7]] as t(x, y) agg y.regr_r2(x)` | `0.986842` |
| `array.regr_count(x: sql): long` | Engines: duckdb, hive, snowflake. | `from [[1, 2], [2, null], [3, 7]] as t(x, y) agg y.regr_count(x)` | `2` |
| `array.kurtosis: double` | Sample excess kurtosis (NULL for fewer than 4 values) and sample skewness (fewer than 3) Engines: duckdb, trino, snowflake. | `from [[1], [2], [3], [10]] as t(x) agg x.kurtosis` | `3.228` |
| `array.skewness: double` | Trino's skewness is the population form: apply the sample correction Engines: duckdb, trino, snowflake. | `from [[1], [2], [3], [10]] as t(x) agg x.skewness` | `1.763633` |

## Window {#window}

Ranking and offset functions, used with an `over(...)` clause.

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `array.lag: A` | Value of the column at a preceding row of the window | `from [[10], [20], [30]] as t(x) select x.lag over (order by x)` | `null, 10, 20` |
| `array.lag(offset: int): A` | Value of the column at a preceding row of the window | `from [[10], [20], [30]] as t(x) select x.lag(2) over (order by x)` | `null, null, 10` |
| `array.lag(offset: int, default: any): A` | Value of the column at a preceding row of the window | `from [[10], [20], [30]] as t(x) select x.lag(1, 0) over (order by x)` | `0, 10, 20` |
| `array.lead: A` | Value of the column at a following row of the window | `from [[10], [20], [30]] as t(x) select x.lead over (order by x)` | `20, 30, null` |
| `array.lead(offset: int): A` | Value of the column at a following row of the window | `from [[10], [20], [30]] as t(x) select x.lead(2) over (order by x)` | `30, null, null` |
| `array.lead(offset: int, default: any): A` | Value of the column at a following row of the window | `from [[10], [20], [30]] as t(x) select x.lead(1, 0) over (order by x)` | `20, 30, 0` |
| `array.first_value: A` | First value of the window | `from [[10], [20], [30]] as t(x) select x.first_value over (order by x)` | `10, 10, 10` |
| `array.last_value: A` | Last value of the window | `from [[10], [20], [30]] as t(x) select x.last_value over (order by x rows [,])` | `30, 30, 30` |
| `array.nth_value(n: int): A` | Value at the n-th row (1-origin) of the window | `from [[10], [20], [30]] as t(x) select x.nth_value(2) over (order by x)` | `null, 20, 20` |
| `row_number: long` | Sequential row number within the window (1, 2, 3, ...) | `from [['a'], ['b'], ['c']] as t(x) select row_number() over (order by x)` | `1, 2, 3` |
| `rank: long` | Rank with gaps after ties (1, 1, 3, ...) | `from [[10], [10], [20]] as t(x) select rank() over (order by x)` | `1, 1, 3` |
| `dense_rank: long` | Rank without gaps after ties (1, 1, 2, ...) | `from [[10], [10], [20]] as t(x) select dense_rank() over (order by x)` | `1, 1, 2` |
| `percent_rank: double` | Relative rank in [0, 1]: (rank - 1) / (rows - 1) | `from [[10], [20], [30]] as t(x) select percent_rank() over (order by x)` | `0.0, 0.5, 1.0` |
| `cume_dist: double` | Cumulative distribution in (0, 1]: rows preceding or peer / rows | `from [[10], [20], [20], [30]] as t(x) select cume_dist() over (order by x)` | `0.25, 0.75, 0.75, 1.0` |
| `ntile(n: int): long` | Bucket number when the window is divided into n groups | `from [[1], [2], [3], [4]] as t(x) select ntile(2) over (order by x)` | `1, 1, 2, 2` |

## Array {#array}

Functions of array values.

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `array.length: int` | Engines: duckdb, trino, hive, snowflake, bigquery. | `[1, 2, 3].length` | `3` |
| `array.size: int` | Engines: duckdb, trino, hive, snowflake, bigquery. | `[1, 2, 3].size` | `3` |
| `array.get(index: int): A` | Hive and Snowflake arrays are 0-indexed, so shift the 1-origin index | `['a', 'b', 'c'].get(2)` | `'b'` |
| `array.exclude(arr: sql): array[A]` | Engines: duckdb, trino. | `[1, 2, 3, 4].exclude([2, 4])` | `[1, 3]` |
| `array.exists: boolean` | True if the lambda is true for some element, e.g. [1, 5].exists(x -> x > 3). False for an empty array. When no element gives true and some give NULL, the result is NULL, as SQL `or` |  |  |
| `array.not_exists: boolean` |  |  |  |
| `array.contains(elem: A): boolean` | True if the array contains the given element | `[1, 2, 3].contains(2)` | `true` |
| `array.mk_string: string` | Concatenate the array elements into a string without a separator Engines: duckdb, trino, hive, snowflake, bigquery. | `['a', 'b', 'c'].mk_string` | `'abc'` |
| `array.mk_string(separator: string): string` | Concatenate the array elements into a string with the separator Engines: duckdb, trino, hive, snowflake, bigquery. | `['a', 'b', 'c'].mk_string(', ')` | `'a, b, c'` |
| `array.distinct: array[A]` | Remove duplicate elements | `[3, 1, 3, 2].distinct.sort` | `[1, 2, 3]` |
| `array.flatten: array[A]` | Flatten an array of arrays into a single array |  |  |
| `array.sort: array[A]` | Engines: duckdb, trino, hive, snowflake. | `[3, 1, 2].sort` | `[1, 2, 3]` |
| `array.reverse: array[A]` | Engines: duckdb, trino, snowflake, bigquery. | `[1, 2, 3].reverse` | `[3, 2, 1]` |
| `array.index_of(elem: A): int` | 1-origin position of the first occurrence of the element (0 if not found) Engines: duckdb, trino, snowflake. | `['a', 'b', 'c'].index_of('b')` | `2` |
| `array.concat(other: any): array[A]` | Engines: duckdb, trino, snowflake, bigquery. | `[1, 2].concat([3, 4])` | `[1, 2, 3, 4]` |
| `array.transform(f: sql): array[any]` | Apply the lambda to each element Engines: duckdb, trino. | `[1, 2, 3].transform(x -> x * 10)` | `[10, 20, 30]` |
| `array.filter(f: sql): array[A]` | Keep the elements for which the lambda returns true Engines: duckdb, trino. | `[1, 2, 3, 4].filter(x -> x % 2 = 0)` | `[2, 4]` |
| `array.reduce(f: sql): A` | Fold the elements from the first one with a two-argument lambda (acc, x) -> ..., e.g. [1, 2, 3].reduce((a, b) -> a + b) = 6. An empty array is an error Engines: duckdb, trino. | `[1, 2, 3, 4].reduce((a, b) -> a + b)` | `10` |
| `array.reduce(init: any, f: sql): any` | Fold the elements from an initial value with (acc, x) -> ..., e.g. [1, 2, 3].reduce(10, (acc, x) -> acc + x) = 16. An empty array gives the initial value Engines: duckdb, trino. | `[1, 2, 3].reduce(100, (acc, x) -> acc + x)` | `106` |
| `array.exists(f: sql): boolean` | True if the lambda is true for some element, e.g. [1, 5].exists(x -> x > 3). False for an empty array. When no element gives true and some give NULL, the result is NULL, as SQL `or` Engines: duckdb, trino. | `[1, 2, 3].exists(x -> x > 2)` | `true` |
| `array.forall(f: sql): boolean` | True if the lambda is true for every element, e.g. [1, 5].forall(x -> x > 0). True for an empty array. When no element gives false and some give NULL, the result is NULL, as SQL `and` Engines: duckdb, trino. | `[1, 2, 3].forall(x -> x > 0)` | `true` |
| `array.zip_with(other: any, f: sql): array[any]` | Combine the elements at the same position with a two-argument lambda, e.g. [1, 2].zip_with([10, 20], (x, y) -> x + y) = [11, 22]. The shorter array is padded with NULL, and a NULL array gives NULL Engines: duckdb, trino, wvlet. | `[1, 2].zip_with([10, 20], (a, b) -> a + b)` | `[11, 22]` |
| `range(start: long, stop: long): array[long]` | Integers from start (inclusive) to stop (exclusive), e.g. range(1, 4) = [1, 2, 3]. An empty array when stop \<= start. Trino limits a range to 10000 elements Engines: duckdb, trino. | `range(1, 5)` | `[1, 2, 3, 4]` |
| `range(start: long, stop: long, step: long): array[long]` | Integers from start (inclusive) to stop (exclusive) in increments of step, e.g. range(0, 10, 3) = [0, 3, 6, 9] and range(3, 0, -1) = [3, 2, 1]. A step of 0 is an error on DuckDB and yields an empty array on Trino Engines: duckdb, trino. | `range(0, 10, 3)` | `[0, 3, 6, 9]` |

## Map {#map}

Functions of map values.

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `map.size: int` | Number of entries in the map | `map {"a": 1, "b": 2}.size` | `2` |
| `map.keys: array[K]` |  | `map {"a": 1, "b": 2}.keys` | `['a', 'b']` |
| `map.values: array[V]` |  | `map {"a": 1, "b": 2}.values` | `[1, 2]` |
| `map.contains_key(key: K): boolean` | True if the map contains the given key | `map {"a": 1, "b": 2}.contains_key("a")` | `true` |
| `map.get(key: K): V` | The value for the given key, or null if the key is absent Engines: duckdb, trino, hive, snowflake. | `map {"a": 1, "b": 2}.get("b")` | `2` |

## JSON {#json}

Read values from JSON text and convert values to JSON.

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `any.to_json: json` | Engines: duckdb, trino. |  |  |
| `string.json_extract(path: string): json` | Extract the JSON value at the JSONPath expression (e.g. '$.store.book') Engines: string: duckdb, trino, hive, snowflake, bigquery. | `from [['{"a": {"b": 1}}']] as t(j) select j.json_extract('$.a')`<br/>`from [['{"a": [1, 2]}']] as t(j) select j.json_extract('$.a').json_extract('$[1]')` | `'{ "b": 1 }'`<br/>`'2'` |
| `string.json_extract_string(path: string): string` | Extract the value at the JSONPath expression as a plain string Engines: string: duckdb, trino, hive, snowflake, bigquery; json: duckdb, trino. | `'{"name": "Wvlet"}'.json_extract_string('$.name')`<br/>`from [['{"a": {"b": "x"}}']] as t(j) select j.json_extract('$.a').json_extract_string('$.b')` | `'Wvlet'`<br/>`'x'` |

## Hashing, Encoding, and IDs {#encoding}

Hash and encode values, and generate unique identifiers.

| Function | Description | Example | Result |
|----------|-------------|---------|--------|
| `string.md5: string` | Hex-encoded digest strings Engines: duckdb, trino, hive, snowflake, bigquery. | `'Wvlet'.md5` | `'d2fccb3acd1e05c874b152cf8a812444'` |
| `string.sha256: string` | Engines: duckdb, trino, hive, snowflake, bigquery. | `'Wvlet'.sha256` | `'46430475c14b6d6b2ee9dcca961da154ddb5b6315521039de8edd3a5bc4a6597'` |
| `string.sha1: string` | Engines: duckdb, trino, hive, snowflake, bigquery. | `'Wvlet'.sha1` | `'a4eb2f34de10d4c6cc097c5346412bb1973921b0'` |
| `string.to_base64: string` | Base64 and lowercase hex encodings of the string's UTF-8 bytes, and their inverses Engines: duckdb, trino, hive, snowflake, bigquery. | `'Wvlet'.to_base64` | `'V3ZsZXQ='` |
| `string.from_base64: string` | Engines: duckdb, trino, hive, snowflake, bigquery. | `'V3ZsZXQ='.from_base64` | `'Wvlet'` |
| `string.to_hex: string` | Engines: duckdb, trino, hive, snowflake, bigquery. | `'Wvlet'.to_hex` | `'57766c6574'` |
| `string.from_hex: string` | Engines: duckdb, trino, hive, snowflake, bigquery. | `'57766C6574'.from_hex` | `'Wvlet'` |
| `string.url_encode: string` | Percent-encode the UTF-8 bytes except A-Z a-z 0-9 - _ . ~ (a space becomes %20), and decode it Engines: duckdb, trino. | `'a b&c'.url_encode` | `'a%20b%26c'` |
| `string.url_decode: string` | Engines: duckdb, trino. | `'a%20b%26c'.url_decode` | `'a b&c'` |
| `ulid_string: string` | Generate a new ULID |  |  |
| `uuid: string` | uuid(): a random version-4 UUID as a lowercase string, e.g. 'f47ac10b-58cc-4372-a567-0e02b2c3d479'. A new value for every row Engines: duckdb, trino, snowflake, bigquery. |  |  |
