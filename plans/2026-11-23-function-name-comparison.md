# Cross-engine function name comparison (for Wvlet stdlib naming)

Researched 2026-11-23. Engines: DuckDB (1.2+), Trino (~480), PostgreSQL (18), Spark SQL (4.x), Snowflake, BigQuery (GoogleSQL).
"—" = no built-in (an extension or a workaround expression may exist). "(?)" = not verified; check before relying on it.
Items checked by web search during this research are marked [v]. Everything else comes from prior knowledge of the official docs.

Existing Wvlet stdlib style (wvlet-stdlib/module/standard/*.wv): snake_case method style on the receiver
(`s.upper`, `x.round(2)`, `s.regexp_extract(p)`, `s.levenshtein(t)`, `s.md5`, `s.sha256`), `to_*` for casts
(`to_int`, `to_long`, `to_double`, `to_boolean`, `to_json`, `to_timestamp`), `diff_*` / `add_*` for date math
(`diff_days`, `add_months`), `is_*` predicates (`is_nan`, `is_null`), `max_by` / `min_by`, `count_if`, `bool_and`.

## Aggregates

| Capability | ANSI | DuckDB | Trino | PostgreSQL | Spark | Snowflake | BigQuery | Notes |
|---|---|---|---|---|---|---|---|---|
| correlation | CORR(y,x) | corr | corr | corr | corr | CORR | CORR | Same everywhere |
| covariance pop / samp | COVAR_POP / COVAR_SAMP | covar_pop / covar_samp | covar_pop / covar_samp | covar_pop / covar_samp | covar_pop / covar_samp | COVAR_POP / COVAR_SAMP | COVAR_POP / COVAR_SAMP | Same everywhere |
| regression slope / intercept | REGR_SLOPE(y,x) / REGR_INTERCEPT | regr_slope / regr_intercept | regr_slope / regr_intercept [v] | regr_slope / regr_intercept | regr_slope / regr_intercept | REGR_SLOPE / REGR_INTERCEPT | — | The dependent variable y comes first everywhere |
| regression r2 / count | REGR_R2 / REGR_COUNT (also REGR_AVGX, AVGY, SXX, SYY, SXY) | regr_r2 / regr_count (+ the full ANSI set) [v] | — (only slope/intercept) | full ANSI set | regr_r2 / regr_count + the set (3.3+) | full ANSI set | — | Trino and BigQuery lack them |
| kurtosis | — | kurtosis (sample, excess), kurtosis_pop [v] | kurtosis | — | kurtosis | KURTOSIS (sample excess) [v] | — | Spark's kurtosis is the population excess form (no bias correction) (?); the values differ from DuckDB/Snowflake on small n |
| skewness | — | skewness | skewness | — | skewness | **SKEW** [v] | — | Snowflake is the outlier |
| entropy | — | entropy(x) (base 2, over value frequencies) [v] | entropy(c) (input is **counts**, log2) | — | — | — | — | Trino takes a bucket count per row, not raw values. Same name, different input |
| median absolute deviation | — | mad [v] | — | — | — | — | — | DuckDB only |
| standard error of mean | — | sem [v] | — | — | — | — | — | DuckDB only; equals stddev_samp/sqrt(count) |
| bitwise and/or/xor agg | — | bit_and / bit_or / bit_xor | bitwise_and_agg / bitwise_or_agg / bitwise_xor_agg [v] | bit_and / bit_or / bit_xor (xor since 14) | bit_and / bit_or / bit_xor | BITAND_AGG / BITOR_AGG / BITXOR_AGG [v] | BIT_AND / BIT_OR / BIT_XOR | bit_* in 4 engines |
| value at max/min of other col | — | arg_max / arg_min (aliases max_by / min_by) | max_by / min_by | — | max_by / min_by | MAX_BY / MIN_BY [v] | MAX_BY / MIN_BY [v] | max_by(x, y) in 5 engines; Wvlet already has it |
| Kahan sum | — | fsum (aliases kahan_sum, sumkahan) [v] | — | — | — | — | — | DuckDB only. The result can depend on input order |

## Strings

| Capability | ANSI | DuckDB | Trino | PostgreSQL | Spark | Snowflake | BigQuery | Notes |
|---|---|---|---|---|---|---|---|---|
| translate chars | TRANSLATE (using a transliteration name; a different feature) | translate(s, from, to) | translate [v] | translate | translate | TRANSLATE | TRANSLATE [v] | Same in all 6. Characters with no match in `to` are deleted |
| overlay | OVERLAY(s PLACING r FROM n [FOR m]) | — (?) | — | overlay (ANSI syntax) | overlay (ANSI syntax) | INSERT(s, pos, len, r) | — | 1-based position |
| printf / format | — | printf (C % style), format (fmt `{}` style) | format (Java Formatter %) | format (%s, %I, %L only) | format_string / printf | — | FORMAT (% style) | `format` means different things: DuckDB `{}`, the others %. Wvlet already has `format` |
| byte length | OCTET_LENGTH | octet_length (blob), strlen (varchar) | — (length(to_utf8(s))) | octet_length | octet_length | OCTET_LENGTH [v] | BYTE_LENGTH [v] | octet_length in 4 engines |
| bit length | BIT_LENGTH | bit_length | — | bit_length | bit_length | BIT_LENGTH [v] | — | |
| strip accents | — | strip_accents | — | — (unaccent extension) | — | — | — | DuckDB only; elsewhere use normalize(NFD) + regexp_replace('\p{M}') |
| unicode normalize | NORMALIZE(s [, NFC]) | nfc_normalize (NFC only) | normalize(s [, form]) [v] | normalize(s [, form]) (13+) | — (?) | — | NORMALIZE(s [, mode]) [v] (+ NORMALIZE_AND_CASEFOLD) | `normalize` is ANSI, in 3 engines. NFC is the default |
| code point of first char | — | unicode / ord / ascii | codepoint (input must be **exactly one** char) [v] | ascii (code point under UTF8) | ascii | UNICODE / ASCII | UNICODE [v] / ASCII | `unicode` in 3 engines. Trino fails on longer strings |
| regex extract all | — | regexp_extract_all(s, p [, group]) | regexp_extract_all(s, p [, group]) | regexp_matches(s, p, 'g') (set-returning) | regexp_extract_all(s, p [, idx]) [v] | REGEXP_SUBSTR_ALL | REGEXP_EXTRACT_ALL [v] | Default group: DuckDB/Trino 0 (whole match); **Spark 1** [v]; BigQuery 1 if the pattern has a capture group |
| regex full match | (SIMILAR TO is full-match) | regexp_full_match | — (regexp_like is partial) | — (~ is partial) | — (rlike is partial) | REGEXP_LIKE / RLIKE (implicitly anchored: **full match**) | — (REGEXP_CONTAINS is partial) | Trap: Snowflake REGEXP_LIKE is full-match, but Trino/DuckDB regexp_like is a partial search |
| regex split to array | — | regexp_split_to_array / string_split_regex | regexp_split | regexp_split_to_array | split (the delimiter is **always a regex**) | — (SPLIT is literal) | — (SPLIT is literal) | Trap: Spark `split` is a regex split; elsewhere `split` is literal |
| url encode / decode | — | url_encode / url_decode (1.2+) [v] | url_encode / url_decode | — | url_encode / url_decode (3.4+) [v] | — (?) | — | Spark uses form encoding (space becomes `+`) [v], as does Trino (its docs: space as `+`) (?); DuckDB uses `%20` [v] |
| Damerau-Levenshtein | — | damerau_levenshtein | — | — | — | — | — | DuckDB only. Plain Levenshtein: levenshtein (DuckDB, Spark, PG fuzzystrmatch), levenshtein_distance (Trino), EDITDISTANCE (Snowflake), EDIT_DISTANCE (BigQuery) |
| Jaro / Jaro-Winkler | — | jaro_similarity / jaro_winkler_similarity (0..1) | — | — | — | JAROWINKLER_SIMILARITY (**integer 0..100, case-insensitive**) [v] | — | Scale trap |
| Hamming distance | — | hamming (alias mismatches) | hamming_distance [v] | — | — | — | — | Both require equal lengths |
| Jaccard similarity | — | jaccard (over character sets) | — | — | — | — (APPROXIMATE_JACCARD_INDEX is a MinHash aggregate) | — | DuckDB only |

## Encoding / hashing

| Capability | ANSI | DuckDB | Trino | PostgreSQL | Spark | Snowflake | BigQuery | Notes |
|---|---|---|---|---|---|---|---|---|
| base64 encode / decode | — | to_base64 (alias base64) / from_base64 | to_base64 / from_base64 (varbinary) | encode(b,'base64') / decode(s,'base64') | base64 / unbase64 | BASE64_ENCODE / BASE64_DECODE_STRING, BASE64_DECODE_BINARY | TO_BASE64 / FROM_BASE64 (BYTES) | to_/from_base64 in 4 engines. The input is binary in Trino/BigQuery (a string needs to_utf8 / CAST AS BYTES first) |
| hex encode / decode | — | hex (alias to_hex) / unhex (alias from_hex) | to_hex / from_hex (varbinary; **uppercase**) | encode(b,'hex') / decode; to_hex(int) | hex / unhex (uppercase) | HEX_ENCODE / HEX_DECODE_STRING, HEX_DECODE_BINARY | TO_HEX (lowercase) / FROM_HEX | Case differs: DuckDB/Trino/Spark/Snowflake uppercase, BigQuery/PG lowercase. DuckDB hex(int) gives the hex of the number, not of its bytes |
| sha1 | — | sha1 (hex string) | sha1 (varbinary) | — (pgcrypto digest) | sha1 / sha (hex) | SHA1 (hex), SHA1_BINARY | SHA1 (BYTES) | Return type differs: hex string or binary |
| sha256 | — | sha256 (hex string) | sha256 (varbinary) | sha256 (bytea, 11+) | sha2(s, 256) (hex) | SHA2(s, 256) (hex), SHA2_BINARY | SHA256 (BYTES) | Trino/PG/BigQuery need to_hex(...) to match DuckDB. Spark/Snowflake use sha2 with a bit-length argument |
| md5 | — | md5 (hex), md5_number (HUGEINT) | md5 (varbinary) | md5 (hex text) | md5 (hex) | MD5 (hex), MD5_NUMBER_LOWER64 / MD5_NUMBER_UPPER64 | MD5 (BYTES) | md5 as a number: only DuckDB (128-bit) and Snowflake (64-bit halves) |
| uuid generation | — | uuid / gen_random_uuid (v4); uuidv7 (1.3+) (?) | uuid | gen_random_uuid (13+), uuidv4 / uuidv7 (18) | uuid | UUID_STRING | GENERATE_UUID (STRING) | `uuid()` in 3 engines. The return type is a UUID type in DuckDB/Trino/PG and a string in the others |

## Math

| Capability | ANSI | DuckDB | Trino | PostgreSQL | Spark | Snowflake | BigQuery | Notes |
|---|---|---|---|---|---|---|---|---|
| gcd / lcm | — | gcd / lcm (aliases greatest_common_divisor / least_common_multiple) | — [v] | gcd / lcm (13+) | — (?) | — (?) | — | |
| factorial | — | factorial (also `!` postfix) | — | factorial (`!` removed in 14) | factorial (0..20, NULL outside) | FACTORIAL | — | |
| gamma / lgamma | — | gamma / lgamma | — | gamma / lgamma (18+) [v] | — | — | — | |
| fmod (float remainder) | MOD (exact numerics only) | fmod; also % and mod | mod (works on double) | — (mod is numeric/int only) | mod / % ; pmod (non-negative) | MOD | — (MOD is INT64/NUMERIC only) | The sign follows the dividend everywhere except Spark pmod |
| round half even | — | round_even (alias roundbankers) | — (round is half away from zero) | round(double) = half-even (via rint); round(numeric) = half away | bround | ROUND(x, s, 'HALF_TO_EVEN') | ROUND(x, s, 'ROUND_HALF_EVEN') (NUMERIC/BIGNUMERIC only) | Plain round: half away from zero on DuckDB/Trino/Spark/Snowflake/BigQuery. PG depends on the type |
| signbit | — | signbit | — | — | — | — | — | DuckDB only; tells -0.0 from 0.0 |
| nextafter | — | nextafter | — | — | — | — | — | DuckDB only |
| sinh/cosh/tanh | — | sinh / cosh / tanh | sinh / cosh / tanh [v] | sinh / cosh / tanh (12+) | sinh / cosh / tanh | SINH / COSH / TANH | SINH / COSH / TANH | Same in all 6 |
| asinh/acosh/atanh | — | asinh / acosh / atanh | — [v] | asinh / acosh / atanh | asinh / acosh / atanh | ASINH / ACOSH / ATANH | ASINH / ACOSH / ATANH | Missing only in Trino |
| cot | — | cot | — | cot | cot | COT | COT | |

## Date / time

| Capability | ANSI | DuckDB | Trino | PostgreSQL | Spark | Snowflake | BigQuery | Notes |
|---|---|---|---|---|---|---|---|---|
| day name | — | dayname (full, "Monday") | date_format(t,'%W') / format_datetime(t,'EEEE') | to_char(t,'Day') (**blank-padded**; use 'FMDay') | dayname (**"Mon"**, 4.0+) [v] | DAYNAME (**"Mon"**) | FORMAT_DATE('%A', d) | Full name vs abbreviation differs |
| month name | — | monthname (full) | date_format(t,'%M') | to_char(t,'Month') (padded) | monthname (**"Feb"**, 4.0+) [v] | MONTHNAME ("Jan") | FORMAT_DATE('%B', d) | Same trap as day name |
| day of week | EXTRACT(DOW) is not ANSI | dayofweek / dow: **0=Sun..6**; isodow: 1=Mon..7 | day_of_week / dow: **ISO 1=Mon..7** | extract(dow) 0=Sun; extract(isodow) 1=Mon | dayofweek **1=Sun..7**; weekday 0=Mon..6 | DAYOFWEEK (depends on WEEK_START; default 0 gives 0=Sun..6); DAYOFWEEKISO 1=Mon..7 | EXTRACT(DAYOFWEEK) **1=Sun..7** | Four numbering schemes. Wvlet `day_of_week` must choose one (ISO 1=Mon..7 is the only portable one: isodow / day_of_week / DAYOFWEEKISO) |
| ISO year | — | isoyear | year_of_week / yow | extract(isoyear) | extract(YEAROFWEEK) | YEAROFWEEKISO | EXTRACT(ISOYEAR) | |
| year-week | — | yearweek (YYYYWW integer, ISO) | — | — | — | — | — | DuckDB only |
| decade / century / millennium | — | decade / century / millennium (also via extract) | — | extract(decade / century / millennium) | — | — | — | Century 21 starts in 2001 (PG and DuckDB) |
| time bucket | — | time_bucket(interval, ts [, origin]) | — (date_trunc only) | date_bin(stride, ts, origin) (14+) | — (window() for streaming only) | TIME_SLICE(ts, n, 'unit' [, 'START'/'END']) | TIMESTAMP_BUCKET / DATE_BUCKET (?) | Default origins differ (DuckDB 2000-01-03 for day/week units, PG requires an origin) |
| parse timestamp with format | — | strptime(s, fmt) (strftime **%**) | date_parse(s, fmt) (MySQL %), parse_datetime(s, fmt) (**Joda**) | to_timestamp(s, 'YYYY-MM-DD HH24:MI:SS') (Oracle-style) | to_timestamp(s, fmt) (**Java** DateTimeFormatter) | TO_TIMESTAMP(s, 'YYYY-MM-DD HH24:MI:SS') (Snowflake / Oracle-style) | PARSE_TIMESTAMP(**fmt, s**) (%) | Three format dialects (%, Java/Joda, Oracle). BigQuery puts the format first |
| ...null on failure | — | try_strptime | try(date_parse(...)) | — | try_to_timestamp (3.4+) [v] | TRY_TO_TIMESTAMP | SAFE.PARSE_TIMESTAMP | The `try_` prefix in 3 engines |
| make time | — | make_time(h, m, s_double) | — | make_time(h, m, s) | make_time (4.1+) [v] | TIME_FROM_PARTS(h, m, s [, ns]) | TIME(h, m, s) | |
| timestamp from epoch millis / nanos | — | epoch_ms(ms), make_timestamp(micros), make_timestamp_ns(ns), to_timestamp(sec) | from_unixtime(sec), from_unixtime_nanos(ns); no millis function | to_timestamp(sec double) | timestamp_millis / timestamp_micros / timestamp_seconds | TO_TIMESTAMP(n, scale) (scale 3 = ms, 9 = ns) | TIMESTAMP_MILLIS / TIMESTAMP_MICROS / TIMESTAMP_SECONDS | Wvlet already has `from_unixtime` (seconds) |
| current date / timestamp | CURRENT_DATE / CURRENT_TIMESTAMP | same + now() | same + now() | same + now() | same + now() | same + SYSDATE | CURRENT_DATE() / CURRENT_TIMESTAMP() | Result type: TIMESTAMPTZ (DuckDB/Trino/PG), LTZ (Snowflake), UTC TIMESTAMP (BigQuery) |
| interval constructors | INTERVAL 'n' DAY literals | to_days / to_hours / to_minutes / to_seconds / to_months / to_years / to_weeks / to_milliseconds | — (`n * INTERVAL '1' DAY`; parse_duration) | make_interval(years =>, days =>, ...) | make_interval(y, m, w, d, h, mi, s), make_dt_interval, make_ym_interval | — (literals; DATEADD) | MAKE_INTERVAL(year =>, ...) | DuckDB's `to_days` clashes with the Wvlet `to_*` cast convention (it builds an INTERVAL; it does not convert to days) |
| age | — | age(ts1, ts2) to INTERVAL | — | age(ts1, ts2) (symbolic y-m-d) | — | — | — | |
| date diff by unit | — | date_diff / datediff(part, start, end) = **boundaries crossed**; date_sub(part, start, end) = **complete units** [v] | date_diff(unit, ts1, ts2) = complete units (like DuckDB date_sub) | — (extract / age) | datediff(**end, start**) days only; timestampdiff / datediff(unit, start, end) (3.3+) | DATEDIFF(unit, start, end) = boundaries crossed | DATE_DIFF(**end, start**, unit) = boundaries [v]; TIMESTAMP_DIFF(end, start, unit) = complete units [v] | Big trap: same name `date_diff`, different semantics in DuckDB and Trino. DuckDB `date_sub` is a diff, not a subtraction. Spark 2-arg and BigQuery put end first |

## Casts

| Capability | ANSI | DuckDB | Trino | PostgreSQL | Spark | Snowflake | BigQuery | Notes |
|---|---|---|---|---|---|---|---|---|
| CAST(2.5 double AS BIGINT) | implementation-defined | **2** (half-even, nearbyint); DECIMAL to int is half away (3) [v] | **3** (half-up/away from zero, DoubleMath.roundToLong HALF_UP) [v] | **2** (float8 to int8 uses rint: half-even); numeric to int is 3 | **2** (truncates, ANSI mode too) [v] | **3** (half away) [v] | **3** (half away; documented) | Three behaviors. A literal `2.5` is DECIMAL in DuckDB/Trino, so test with `2.5::DOUBLE` / `2.5E0` |
| numeric to boolean cast | allowed by SQL:2023? (?) | yes (0 = false) | yes (int and double) | int4 only (no bigint/float to bool) | yes | yes (TO_BOOLEAN; 0 = false) | INT64 only (no FLOAT64) | PG and BigQuery are partial |

## JSON

| Capability | ANSI | DuckDB | Trino | PostgreSQL | Spark | Snowflake | BigQuery | Notes |
|---|---|---|---|---|---|---|---|---|
| build object | JSON_OBJECT(KEY k VALUE v) (SQL:2016) | json_object(k, v, ...) | json_object(KEY k VALUE v) (ANSI syntax) | json_build_object(k, v, ...); JSON_OBJECT (16+, ANSI) | — (to_json(named_struct(...))) | OBJECT_CONSTRUCT (drops NULL values) | JSON_OBJECT(k, v, ...) | DuckDB/BigQuery take positional pairs; Trino/PG use the ANSI syntax |
| build array | JSON_ARRAY | json_array | json_array | json_build_array; JSON_ARRAY (16+) | — (to_json(array(...))) | ARRAY_CONSTRUCT | JSON_ARRAY | |
| object keys | — | json_keys | — (CAST to map(varchar,json) + map_keys) | json_object_keys (set-returning) | json_object_keys (3.1+) | OBJECT_KEYS | JSON_KEYS (recursive by default, dotted paths) [v] | BigQuery returns nested paths |
| is valid | `IS JSON` predicate | json_valid | try(json_parse(s)) IS NOT NULL [v]; an ANSI IS JSON predicate may exist (?) | `IS JSON` (16+) | — | CHECK_JSON (returns **NULL when valid**, else an error message) | — (SAFE.PARSE_JSON IS NOT NULL) | Snowflake's inverted result is a trap |
| type of value | JSON path `.type()` | json_type | json_value(j, 'lax $.type()') [v] | json_typeof / jsonb_typeof | — | TYPEOF(variant) | JSON_TYPE | Wvlet already has `type_of` (on SQL values) |
| contains | — | json_contains | json_array_contains (arrays only) | `@>` (jsonb) | — | ARRAY_CONTAINS (variant) | — (?) | |
| merge patch (RFC 7396) | — | json_merge_patch | — | — (jsonb `||` is a shallow concat) | — | — | — (?) | DuckDB only |
| value to json | JSON_SERIALIZE / JSON() | to_json | CAST(x AS JSON), json_format | to_json / to_jsonb | to_json (struct/array/map) | TO_JSON (variant to string); TO_VARIANT | TO_JSON / TO_JSON_STRING | `to_json` in 5 engines; Wvlet already has it |
| parse into typed value | JSON_VALUE ... RETURNING | json_transform / from_json(j, structure) | CAST(json AS ROW/ARRAY/MAP); json_parse for text | json_populate_record / jsonb_to_record | from_json(s, schema) | PARSE_JSON + `::type` | PARSE_JSON + INT64(j) / LAX_INT64 / ... | `from_json` in DuckDB and Spark |

## Arrays / lists

| Capability | ANSI | DuckDB | Trino | PostgreSQL | Spark | Snowflake | BigQuery | Notes |
|---|---|---|---|---|---|---|---|---|
| has any | — | list_has_any / array_has_any (`&&`) | arrays_overlap | `&&` | arrays_overlap | ARRAYS_OVERLAP | — | |
| has all | — | list_has_all / array_has_all (`@>`) | — (cardinality(array_except(b, a)) = 0) | `@>` | — | ARRAY_CONTAINS is single-element only (?) | — | |
| intersect | — | list_intersect / array_intersect | array_intersect | — | array_intersect | ARRAY_INTERSECTION (**multiset**: keeps duplicates) | — | array_intersect in 3 engines; dedup semantics differ |
| distinct count | — | list_unique / array_unique (**returns a count**) | cardinality(array_distinct(a)) | — | size(array_distinct(a)) | ARRAY_SIZE(ARRAY_DISTINCT(a)) | — | Trap: DuckDB `array_unique` returns a count, but "unique" elsewhere usually means dedupe. `array_distinct` (dedupe) is shared by DuckDB, Trino, Spark, Snowflake |
| zip | — | list_zip (list of unnamed structs) | zip (array of rows; pads with NULL) | — (unnest(a, b)) | arrays_zip (struct fields named after the inputs) | — | — | |
| map with a lambda | — | list_transform / array_transform / apply | transform | — | transform | TRANSFORM | — (unnest + array subquery) | `transform` in 3 engines; DuckDB's lambda may take an index as a second parameter |
| filter with a lambda | — | list_filter / array_filter / filter | filter | — | filter | FILTER | — | `filter` everywhere with lambdas |
| any / every element matches | — | — (fold the lambda results: list_reduce(list_transform(l, f), lambda a, b: a or b, false)) | any_match / all_match / none_match | — (`x = ANY(arr)` for equality only) | exists / forall | — (FILTER + ARRAY_SIZE, or REDUCE) | — (EXISTS over UNNEST) | `exists` / `forall` in Spark and Scala; ClickHouse arrayExists / arrayAll; Trino and Spark return NULL when no element decides and one gives NULL |
| fold with a lambda | — | list_reduce(l, f [, init]) (first element when no init; empty list is an error) | reduce(a, init, f, finish) | — | aggregate / reduce(a, init, f [, finish]) | REDUCE(a, init, f) | — | `reduce` in 4 sources; only DuckDB has a no-initial form |
| filter by mask | — | list_where(values, mask) | — | — | — | — | — | DuckDB only |
| select by indexes | — | list_select(values, indexes) (1-based) | — | — | — | — | — | DuckDB only |
| resize | — | list_resize / array_resize(l, n [, fill]) | — | — | — | — | — | DuckDB only |
| sort indexes (grade up) | — | list_grade_up / grade_up (1-based) | — | — | — | — | — | DuckDB only |
| cosine similarity / distance / inner product | — | list_cosine_similarity / list_cosine_distance / list_inner_product (alias list_dot_product); array_* for fixed-size ARRAY | cosine_similarity / cosine_distance / dot_product / euclidean_distance (array(double), also maps) [v] | — (pgvector `<=>`, `<#>`) | — | VECTOR_COSINE_SIMILARITY / VECTOR_INNER_PRODUCT / VECTOR_L2_DISTANCE (VECTOR type) | COSINE_DISTANCE / EUCLIDEAN_DISTANCE [v]; no scalar dot product | cosine_similarity / cosine_distance are close to shared (DuckDB with a list_ prefix) |

## Maps

| Capability | ANSI | DuckDB | Trino | PostgreSQL | Spark | Snowflake | BigQuery | Notes |
|---|---|---|---|---|---|---|---|---|
| entries | — | map_entries | map_entries | — (hstore each / jsonb_each) | map_entries | — (structured MAP: MAP_KEYS etc., no entries) (?) | — (no MAP type) | map_entries in 3 engines |
| concat | — | map_concat | map_concat | hstore `||` | map_concat | MAP_CAT | — | Later keys win (Spark errors on duplicate keys unless mapKeyDedupPolicy=LAST_WIN) |
| from entries | — | map_from_entries | map_from_entries | — | map_from_entries | — | — | |
| extract value | — | map_extract_value (1.2+) / element_at / m[k]; map_extract returns a **list** | element_at(m, k) (NULL if missing); m[k] **errors** if missing | hstore `->` | element_at / m[k] (ANSI mode errors) | m[k] / GET | — | Missing-key behavior differs |

## Structs / rows

| Capability | ANSI | DuckDB | Trino | PostgreSQL | Spark | Snowflake | BigQuery | Notes |
|---|---|---|---|---|---|---|---|---|
| pack / build | ROW(...) | struct_pack(k := v), {'k': v}, row(...) | CAST(ROW(...) AS ROW(k T)), row(...) | ROW(...) (anonymous record) | named_struct('k', v), struct(v AS k) | OBJECT_CONSTRUCT (semi-structured) | STRUCT(v AS k) | |
| extract field | s.k | struct_extract(s, 'k' or index), s.k | s.k, s[i] (1-based) | (s).k | s.k | s:k / GET(s, 'k') | s.k | Dot syntax is universal. Wvlet should keep `.` and add no function |
| keys | — | struct_keys [v] | — | — | — | OBJECT_KEYS | — | |
| values | — | struct_values (returns an unnamed struct) [v] | — | — | — | — | — | |
| insert field | — | struct_insert(s, k := v) (add); struct_update (overwrite) [v] | — | — | — (Dataset API withField only) | OBJECT_INSERT(o, k, v [, update]) | — | |

## Observations

**Names shared by 4 or more engines (strong candidates for Wvlet names):**
- Aggregates: `corr`, `covar_pop`, `covar_samp` (6); `regr_slope`, `regr_intercept` (5); `regr_r2`, `regr_count` (4: DuckDB, PG, Spark, Snowflake); `max_by` / `min_by` (5); `bit_and` / `bit_or` / `bit_xor` (4: DuckDB, PG, Spark, BigQuery); `kurtosis` (4); `skewness` (3 + Snowflake SKEW).
- Strings: `translate` (6); `octet_length` (4) and `bit_length` (4); `regexp_extract_all` (4); `normalize` (ANSI; Trino, PG, BigQuery; DuckDB uses nfc_normalize).
- Math: `sinh` / `cosh` / `tanh` (6); `asinh` / `acosh` / `atanh` and `cot` (5, all but Trino); `factorial` (4).
- Encoding: `md5` (6, but the return type differs); `sha1` (5); `to_base64` / `from_base64` (DuckDB, Trino, BigQuery; Spark base64, Snowflake BASE64_ENCODE); `to_hex` / `from_hex` (DuckDB alias, Trino, BigQuery).
- Date: `current_date` / `current_timestamp` / `now` (all); `make_time` (DuckDB, PG, Spark 4.1).
- JSON: `to_json` (5); `json_object` / `json_array` (ANSI; DuckDB, Trino, PG 16, BigQuery).
- Arrays / maps: `array_distinct`, `map_concat`, `map_entries`, `map_from_entries` (Spark/Trino/DuckDB core).
- Methods that fit the existing stdlib with no conflict: `x.corr(y)`-style aggregates, `s.translate(a, b)`, `s.normalize`, `x.sinh`, `s.sha1`, `s.to_base64`, `s.regexp_extract_all(p)`.

**Where engines conflict:**
- `date_diff`: DuckDB counts boundaries crossed (as do Snowflake DATEDIFF and BigQuery DATE_DIFF); Trino and BigQuery TIMESTAMP_DIFF count complete units (as does DuckDB `date_sub`). Argument order: (unit, start, end) in DuckDB, Trino, Snowflake, and Spark 3-arg; (end, start[, unit]) in BigQuery and Spark 2-arg datediff. Wvlet's existing `diff_days` etc. should state which semantics they use.
- `format`: DuckDB `{}` fmt style vs % (printf) style in Trino, PG, and BigQuery.
- `skewness` vs Snowflake `SKEW`; `bit_*` vs Trino `bitwise_*_agg` vs Snowflake `BIT*_AGG`.
- Day of week: 0=Sun (DuckDB, PG, Snowflake default), 1=Sun (Spark, BigQuery), 1=Mon ISO (Trino). Only the ISO variant has a name in most engines (isodow / day_of_week / DAYOFWEEKISO / weekday+1).
- Day and month names: full in DuckDB, Trino, PG, and BigQuery; 3-letter in Snowflake and Spark.
- Timestamp parsing formats: strftime `%` (DuckDB, BigQuery, Trino date_parse), Java/Joda (Spark, Trino parse_datetime), Oracle-style (PG, Snowflake). The parse argument order is also reversed in BigQuery.
- `entropy`: DuckDB takes raw values; Trino takes counts.
- Hash return types: hex string (DuckDB, Spark, Snowflake, PG md5) vs binary (Trino, BigQuery, PG sha256). Hex case also differs.

**Semantic traps:**
- Double-to-bigint cast of 2.5: DuckDB 2 (half-even), PG 2 (rint), Spark 2 (truncate), Trino/Snowflake/BigQuery 3 (half away). Decimal literals behave differently from doubles in DuckDB and Trino. An engine-neutral `to_long` on a double needs an explicit round/trunc to be portable.
- Plain `round`: half away from zero everywhere except PG on double (half-even). Banker's rounding has a different name in each engine: round_even, bround, ROUND(..., 'HALF_TO_EVEN'), ROUND(..., 'ROUND_HALF_EVEN').
- Regex: Snowflake REGEXP_LIKE/RLIKE match the whole string; the others search. Spark `split` is a regex split. Spark regexp_extract_all defaults to group 1; DuckDB/Trino default to group 0.
- DuckDB `array_unique` / `list_unique` returns a count, not a deduplicated list.
- DuckDB `date_sub` is a difference function, not date minus interval. DuckDB `to_days(n)` builds an INTERVAL, which clashes with Wvlet's `to_*` = cast convention.
- Snowflake CHECK_JSON returns NULL for valid input. Snowflake JAROWINKLER_SIMILARITY returns an integer 0..100 and ignores case. Trino `codepoint` rejects strings longer than one character.
- Trino `m[k]` errors on a missing key; element_at returns NULL. DuckDB `map_extract` returns a list.
- Snowflake OBJECT_CONSTRUCT drops NULL-valued keys.
- Items marked (?) were not verified. Before adopting a name based on those engines, check: Spark kurtosis bias, Spark gcd, Spark normalize, Snowflake url encode, Trino IS JSON, BigQuery TIMESTAMP_BUCKET/JSON contains/merge, DuckDB overlay and uuidv7.

## Doc references (one per engine)
- ANSI: ISO/IEC 9075 (not public); see PostgreSQL's SQL conformance appendix https://www.postgresql.org/docs/current/features.html
- DuckDB: https://duckdb.org/docs/current/sql/functions/overview (with .../functions/numeric, .../functions/date, .../data_types/numeric for casts)
- Trino: https://trino.io/docs/current/functions.html (math: https://trino.io/docs/current/functions/math.html)
- PostgreSQL: https://www.postgresql.org/docs/18/functions.html
- Spark SQL: https://spark.apache.org/docs/latest/api/sql/
- Snowflake: https://docs.snowflake.com/en/sql-reference/functions-all
- BigQuery: https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-all
