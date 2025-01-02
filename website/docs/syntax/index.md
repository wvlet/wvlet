import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Query Syntax

Wvlet is a query language designed to be more human-readable and easier to write than SQL. If you familiar to SQL, you will find it easy to learn Wvlet syntax due to the many similarities. If you know about [DataFrame in Python](https://pandas.pydata.org/docs/user_guide), it will help you understand the Wvlet query language, as chaining relational operators in flow-style is similar to using the DataFrame API.

### Documentation Overview

- [Quick Start](./quick-start.md)
- [Relational Operators](#relational-operators)
- [Expressions](#expressions)
- [Examples](#examples)
- [Data Models](./data-models.md)

### Typical Flow of a Wvlet Query

Wvlet queries start with the
`from` keyword, and you can chain multiple [relational operators](#relational-operators) to process the input data and generate output data. Here's a typical flow:

```sql
from ...         -- Scan the input data
where ...        -- Apply filtering conditions
where ...        -- [optional] Apply more filtering conditions
add   ... as ... -- Add new columns
transform ...    -- Transform a subset of columns
group by ...     -- Group rows by the given columns
agg ...          -- Add group aggregation expressions, e.g., _.count, _.sum 
where ...        -- Apply filtering conditions for groups (HAVING clause in SQL)
exclude ...      -- Remove columns from the output
shift ...        -- Shift the column position to the left
select ...       -- Select columns to output
order by ...     -- Sort the rows by the given columns
limit ...        -- Limit the number of rows to output
```

Unlike SQL, whose queries always must follow the `SELECT ... FROM ... WHERE ... GROUP BY ... ORDER BY ... LIMIT ...` structure, Wvlet uses the __flow-style syntax
__ to match the order of data processing order as much as possible, facilitating more intuitive query writing. A query should have a `from` statement to read the data, but
`select` is not mandatory in Wvlet.

[//]: # (Some operators like `add`, `transform`, `agg`, `exclude`, `shift`, etc. are not available in the standard SQL, but these new operators will reduce your query text size and make the query more readable and easier to compose. Eventually, these operators will be translated to the equivalent SQL syntax.)

## Relational Operators

In Wvlet, you need to use __lower-case__ keywords for SQL-like operators. Below is a list of relational operators for manipulating table-format data (relation):

### Catalog Operators

Wvlet provides operators to list the available catalogs, schemas (databases), tables, and models in the current environment. The hierarchy of the catalog, schema, and table is as follows:

- catalog: A collection of schemas (databases)
- schema (database, datasets): A collection of tables
- table: A collection of rows (records)

| Operator                                                       | Output                                                       |
|----------------------------------------------------------------|--------------------------------------------------------------|
| [__show__ tables (__in__ `catalog`?(.`schema`)?](#show-tables) | Return a list of all tables in the current schema (database) |
| [__show__ schemas (__in__ `catalog`)?](#show-schemas)          | Return a list of all schemas (databases) in the catalog      |
| [__show__ catalogs](#show-catalogs)                            | Return a list of all catalogs                                |
| __show__ models                                                | Return a list of all models                                  |
| __describe__ (relation)                                        | Return the schema of the specified table or query            |

### Query Operators

Wvlet provides various relational operators to process input data and generate output data in the table format.

| Operator                                                                                       | Output                                                                                                                                        |
|------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------| 
| [__from__ `expr`](#from)                                                                       | Rows from the given source table, model, value, or file                                                                                       |
| [__where__ `cond`](#where)                                                                     | Rows that satisfy the given condition. Multiple `where` clauses can be used in the same query                                                 |
| [__add__ `expr` (__as__ `alias`)?, ...](#add)                                                  | Same rows with new columns                                                                                                                    |
| [__select__ `expr`, ...](#select)                                                              | Rows with the given expressions. `select *` is allowed                                                                                        |
| __select distinct__ `expr`,...                                                                 | Rows with distinct values of the given expressions                                                                                            |
| __select__ `alias` __=__ `expr`, ...                                                           | Rows with the given expressions with aliases                                                                                                  |
| __select__ `expr` __as__ `alias`, ...                                                          | Rows with the given expressions with aliases                                                                                                  |
| __select as__ `alias`                                                                          | Add an alias to the query to reference it from another query                                                                                  |
| [__rename__ `column` __as__ `alias`, ...](#rename)                                                        | Same rows with the given columns renamed                                                                                                      | 
| [__exclude__ `column`, ...](#exclude)                                                                      | Same rows except the given columns                                                                                                            |
| [__transform__ `expr` __as__ `name`, ...](#transform)                                                        | Same rows with added or updated columns                                                                                                       |  
| [__shift__ (to left)? `column`, ...](#shift)                                                             | Same rows with selected column moved to the left                                                                                              |
| [__shift to right__ `column`, ...](#shift)                                                               | Same rows with selected column moved to the right                                                                                             |
| __group by__ `column`, ...                                                                     | Grouped rows by the given columns. Grouping keys can be referenced as `select _1, _2, ...`  in the subsequent operator                        |
| [__agg__ `agg_expr`, ...](#agg)                                                                | Rows with the grouping keys in `group by` clause and aggregated values                                                                        |
| [__order by__ `expr` (__asc__ \| __desc__)?, ...](#order-by)                                                | Rows sorted by the given expression. 1-origin column indexes can be used like `1`, `2`, ...                                                   |
| [__limit__ `n`](#limit)                                                                                  | Rows up to the given number                                                                                                                   |
| [__asof__](asof-join.md)? (__left__ \| __right__ \| __cross__)? __join__ `table` __on__ `cond` | Joined rows with the given condition                                                                                                          |
| [__concat__ \{ `query` \}](#concat)                                                                       | Concatenated rows with the given subquery result. Same as `UNION ALL` in SQL                                                                  |
| [__dedup__](#dedup)                                                                                      | Rows with duplicated rows removed. Equivalent to `select distinct *`                                                                          | 
| ([__intersect__](#intersect) \| [__except__](#except)) __all__? ...                                                     | Rows with the intersection (difference) of the given sources. By default set semantics is used. If `all` is given, bag semantics will be used |
| [__pivot on__ `pivot_column` (__in__ (`v1`, `v2`, ...) )?](#pivot)                                       | Rows whose column values in the pivot column are expanded as columns                                                                          |
| __call__ `func(args, ...)`                                                                     | Rows processed by the given table function                                                                                                    |
| __sample__ `method`? (`size` __rows__? \| __%__)                                               | Randomly sampled rows. Sampling method can be reservoir (default), system, or bernoulli                                                       | 
| [__unnest__(`array expr`)](unnest.md)                                                          | Expand an array into rows                                                                                                                     | 
| [__test__ `(test_expr)`](test-syntax.md)                                                       | Test the query result, evaluated only in the test-run mode                                                                                    |
| __debug__ \{ `(query)` \}                                                                      | Pass-through the input results (no-op), but run a debug query against the input rows                                                          |
| __describe__                                                                                   | Return the schema of the input relation (column_name, column_type)                                                                            |

### Update Operators

Wvlet provides update operators to save the query result as a new table or file, append the query result to the target table, or delete input rows from the source table.

| Operator                                            | Description                                                                                          |
|-----------------------------------------------------|------------------------------------------------------------------------------------------------------|
| __save to__ `table_name`                            | Save the query result as a new table with the given name                                             |
| __save to__ `table_name` __with__ p1:v1, p2:v2, ... | Save the query result as a new table with the given name and options                                 |
| __save to__ `'file name'`                           | Save the query result as a file with the given name                                                  |
| __append to__ `table_name`                          | Append the query result to the target table. If the target table doesn't exist, it creates a new one |
| __append to__ `'file_name'`                         | Append the query result to the target file. It will create a new file if the file doesn't exist      |
| __delete__                                          | Delete input rows from the source table                                                              |

### Raw SQL Commands

| Operator             | Description                                                |
|----------------------|------------------------------------------------------------|
| __execute__ sql"..." | Execute a raw SQL statement in the context engine          |
| __from__ sql"..."    | Execute a raw SQL statement and read the result as a table |

## Expressions

One of the major difference from traditional SQL is that wvlet uses single or double quoted strings for representing string values and back-quoted strings for referencing column or table names, which might contain space or special characters.

| Operator                                              | Description                                                                                                                                              |
|:------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| 'single quote'                                      | String literal for representing string values, file names, etc.                                                                                          |
| "double quote"                                      | Same as single quote strings                                                                                                                             |
| """triple quote string"""                           | Multi-line strings                                                                                                                                       |
| \`(back quote)\`                                      | Column or table name, which requires quotations                                                                                                          |
| __sql__"`sql expr`"                                       | SQL expression used for inline expansion                                                                                                                 |
| __sql__" ... $\{`expr`\} ..."                             | Interpolated SQL expression with embedded expressions                                                                                                    |
| __s__"... $\{expr\} ..."                                  | Interpolated strings with expressions                                                                                                                    |
| __s__\`... $\{expr\} ...\`                                | Interpolated backquote strings with expressions                                                                                                          |
| [`expr`, ...]                                         | Array value                                                                                                                                              |
| [[`expr`, ...], ...]                                  | Array of arrays for representing table records                                                                                                           |
| \{`key`\: `value`, ...\}                              | Struct (row) value                                                                                                                                       |
| `_`                                                   | Underscore refers to the previous input                                                                                                                  |
| `agg_func(expr)` over (partition by ... order by ...) | [Window functions](window.md) for computing aggregate values computed from the entire query result. This follows similar window function syntax with SQL |
| `_1`, `_2`, ...                                       | Refers to 1-origin grouping keys in the preceding `group by` clause                                                                                      |
| `1`, `2`, ...                                         | Refers to 1-origin column index for `order by` clause                                                                                                    |

### Variable Definition

You can define a variable using `val` keyword:

```sql
-- Define a new variable
val name = 'wvlet'

-- Variable is evaluated once before running the query
select s"Hello ${x}!" as msg
-- Returns [['Hello wvlet!']]
```

### Conditional Expressions

| Operator                        | Description                                                                      |
|---------------------------------|----------------------------------------------------------------------------------|
| `expr` __and__ `expr`               | Logical AND                                                                      |
| `expr` __or__  `expr`               | Logical OR                                                                       |
| __not__ `expr`                      | Logical NOT                                                                      |
| !`expr`                         | Logical NOT                                                                      |
| `expr` __is__ `expr`                | Equality check                                                                   |
| `expr` __=__ `expr`                 | Equality check                                                                   |
| `expr` __is not__ `expr`            | Inequality check                                                                 |
| `expr` __!=__ `expr`                | Inequality check                                                                 |
| `expr` __is null__                  | True if the expression is null                                                   |
| `expr` __= null__                   | True if the expression is null                                                   |
| `expr` __is not null__              | True if the expression is not null                                               |
| `expr` __!= null__                  | True if the expression is not null                                              |
| `expr` __in__ (`v1`, `v2`, ...)     | True if the expression value is in the given list                                |
| `expr` __in__ \{ from ... \}            | True if the expression value is in the given list provided by a sub query        |
| `expr` __not in__ (`v1`, `v2`, ...) | True if the expression is not in the given list                                  |
| `expr` __between__ `v1` __and__ `v2`    | True if the expression value is between v1 and v2, i.e., v1 &le; (value) &le; v2 |
| `expr` __like__ `pattern`           | True if the expression matches the given pattern, e.g., , `'abc%'`               |

#### If Expression

`if ... then .. else` expression can be used to switch the output value based on the condition:

```sql
from lineitem
select
  if l_returnflag = 'A' then 1 else 0
  as return_code
```

The `if` expression can be nested as follows:

```sql
from lineitem
select
  if l_returnflag = 'A' then 1
  else if l_returnflag = 'R' then 2
  else 0
  as return_code
```

#### Case Expression

To switch the output value based on the input value, you can use the `case` expression:

```sql
from lineitem
select
  case l_returnflag
    when 'A' then 1
    when 'R' then 2
    else 0
  as return_code
```

You can also use the `case` expression with conditional expressions for clarity:

```sql
from lineitem
select 
  case 
    when l_returnflag = 'A' then 1
    when l_returnflag = 'R' then 2
    else 0
  as return_code 
```

:::tip
Unlike SQL, Wvlet doesn't require `end` at the end of case expressions.
:::

### String Expressions

| Operator        | Description             |
|-----------------|-------------------------|
| `expr` __+__ `expr` | Concatenate two strings |

### Array Expressions

You can construct array values with `[e1, e2, ...]` syntax:

```sql
select ['a', 'b', 'c'] as arr
```

Arrays can be accessed with index (1-origin):

```sql
select ['a', 'b', 'c'] as arr
select arr[1] as first_element
```

### Map Expressions

You can construct map values with
`map {k1: v1, k2: v2, ...}` syntax. Unlike struct expressions, keys (k1, k2, ...) needs to be the same type values, and values (v1, v2, ...) also need to be the same type values:

```sql
select map {'a': 1, 'b': 2} as m
```

### Struct/Row Expressions

Struct (row) expressions are used to represent key-value pairs. You can access the values by name:

```sql
select {'i': 3, 's': 'str'} as obj
select 
  -- Name based access
  obj.i, obj.s,
  -- Lookup by name 
  obj['i'], obj['s']
```

Note that key names in a struct do not require quotes for plain key names:

```sql
select {k1:1, k2:'v1'} as obj
select obj.k1, obj.k2
```

### Lambda Expression

To manipulate array values, you can use lambda expressions:

```sql
select list_transform([4, 5, 6], x -> x + 1)
```

This query applies the lambda function `x -> x + 1` to each element of the input array:

```
┌────────────┐
│    arr     │
│ array(int) │
├────────────┤
│ [5, 6, 7]  │
├────────────┤
│ 1 rows     │
└────────────┘
```

To pass multiple arguments to the lambda function, use the following syntax:

```sql
select list_reduce([4, 5, 6], (a, b) -> a + b) as sum;

┌─────┐
│ sum │
│ int │
├─────┤
│  15 │
├─────┤
│ 1 … │
└─────┘
```

Lambda functions can be nested as follows:

```sql
select
  list_transform(
    [1, 2, 3],
    x -> list_reduce([4, 5, 6], (a, b) -> a + b) + x
  ) as arr;

┌──────────────┐
│     arr      │
│  array(int)  │
├──────────────┤
│ [16, 17, 18] │
├──────────────┤
│ 1 rows       │
└──────────────┘
```

## Examples

### show tables

To find a target table to query in the current database (schema), you can use the `show tables` query:

```sql
show tables
```

The output of `show tables` can be followed by the `where` clause to filter the table names:

```sql
show tables
where name like 'n%';

┌────────┐
│  name  │
│ string │
├────────┤
│ nation │
├────────┤
│ 1 rows │
└────────┘
```

### show schemas

To find a target database (schema) to query in the current catalog, you can use the `show schemas` query:

```sql
show schemas;

┌─────────┬────────────────────┐
│ catalog │        name        │
│ string  │       string       │
├─────────┼────────────────────┤
│ memory  │ information_schema │
│ system  │ information_schema │
│ temp    │ information_schema │
│ memory  │ main               │
│ system  │ main               │
│ temp    │ main               │
│ memory  │ pg_catalog         │
│ system  │ pg_catalog         │
│ temp    │ pg_catalog         │
├─────────┴────────────────────┤
│ 9 rows                       │
└──────────────────────────────┘
```

These schema names can be used to specify the target schema in the `show tables` command:

```sql
show tables in main 
where name like 'p%';

┌──────────┐
│   name   │
│  string  │
├──────────┤
│ part     │
│ partsupp │
├──────────┤
│ 2 rows   │
└──────────┘
```

### show catalogs

List all available catalogs:

```sql
show catalogs;

┌────────┐
│  name  │
│ string │
├────────┤
│ memory │
│ system │
│ temp   │
├────────┤
│ 3 rows │
└────────┘
```

### from

Read rows from a given table:

```sql
from nation
```

In DuckDB backend, you can read data from local Parquet or JSON files:

```sql
from 'sample.parquet' 
```

Or from files on the Web:

```sql
from 'https://(path to your data)/sample.parquet'
```

Reading data from an URL will also work for S3 Presigned URLs.

#### Raw SQL statements

In case you need to use a raw SQL statement, you can use the `sql` string interpolation:

```sql
from sql"select * from nation"
```

:::warning
Although raw SQL statements are convenient, Wvlet needs to issue a real query to the target DBMS to inspect the data types of the query result,
which might slowdown the query or cause errors in the subsequent relational operators.
:::

### where

You can apply a filter condition to the input rows:

```sql
from nation
where n_regionkey = 1
```

Unlike SQL, applying multiple `where` clauses is allowed in Wvlet:

```sql
from nation
where n_regionkey = 1
where n_name like 'U%'
```

This is useful even when you need to generate Wvlet queries programatically as you only need to append new condition lines to the query. In SQL, you need to parse the text inside WHERE clause and concatenate condition with AND expression.

### add

Add a new column to the input. This operator is helpful to reduce the typing effort as you don't need to enumerate all the columns in the `select` clause:

```sql
from nation
select n_name
add n_name.substring(1, 3) as prefix
limit 5;

┌───────────┬────────┐
│  n_name   │ prefix │
│  string   │ string │
├───────────┼────────┤
│ ALGERIA   │ ALG    │
│ ARGENTINA │ ARG    │
│ BRAZIL    │ BRA    │
│ CANADA    │ CAN    │
│ EGYPT     │ EGY    │
├───────────┴────────┤
│ 5 rows             │
└────────────────────┘
```

### select

Output only the selected columns from the input:

```sql
from nation
select n_name, n_regionkey
```

As in SQL, arbitrary expressions can be used in select statements. Note that, however, in Wvlet, you usually don't need to use select statement
as the other column-at-a-time operators, like `add`, `transform`, `exclude`, `shift`, etc. can be used for manipulating column values.

### rename

The `rename` operator changes the column names in the input rows. This is useful when you want to rename columns in the output.

```sql
from nation
rename n_name as nation_name
```

### exclude

The `exclude` operator removes specified columns from the input rows. This is useful when you want to drop certain columns from the output.

```sql
from nation
exclude n_regionkey
```


### transform

The `transform` operator allows you to add or update columns in the input rows. This operator is useful for applying transformations to existing columns without needing to list all columns in the select clause.

```sql
from nation
transform n_name.uppercase() as upper_name
limit 5
```


### shift

The `shift` operator changes the position of specified columns in the input rows. You can move columns to the left or right.

Shift to the left (default):
```sql
from nation
shift n_name
```

Shift to the right:
```sql
from nation
shift to right n_comment
```


### group by

Create a list of grouped rows by the given column:

```sql
from nation
group by n_regionkey
```

In SQL, you need to specify some aggregation operators like `count`, `sum`, `avg`, etc. to generate the output.
In Wvlet, the default aggegation operator `arbitrary (any)` is used to generate the output from `group by` operator. If you want to
use other aggregation operators, use the `agg` operator.

### agg

Add an aggregation expression to the grouped rows:

```sql
from nation
group by n_regionkey
agg _.count as count
```

`_` denotes a list of rows in a group and can follow SQL aggregation expressions with dot notation.
For example, `_.sum`, `_.avg`, `_.max`, `_.min`, `_.max_by(sort_col)`, `_.min_by(sort_col)`, etc. can be used in agg statement.

### limit

Limit the number of output rows:

```sql
from nation
limit 10
```

### order by

Sort the output rows by the given column:

```sql
from nation
order by n_name
limit 5
```

The `order by` statement can follow `asc`, `desc` ordering specifier as in SQL.


### concat

The concat operator concatenates rows from multiple subqueries. This is similar to the UNION ALL operator in SQL.

```sql
from nation
where n_regionkey = 0
-- Append rows from another query
concat {
  from nation
  where n_regionkey = 1
}
```

The ordering of rows are not guaranteed in the `concat` operator. If you need to sort the output, use the `order by` operator after the `concat` operator. 

### dedup

The `dedup` operator removes duplicated rows from the input rows. This is equivalent to `select distinct *` in SQL.

```sql
from nation
dedup
```

### intersect

The `intersect` operator returns the intersection of the input rows from multiple subqueries. By default, set semantics are used, but you can use bag semantics by specifying `all`.

```sql
from nation
intersect all {
  from nation
  where n_regionkey = 1
}
```

### except

The `except` operator returns the difference of the input rows from multiple subqueries. By default, set semantics are used, but you can use bag semantics by specifying `all`.

```sql
from nation
except {
  from nation
  where n_regionkey = 1
}
```

### pivot

The `pivot` operator expands the column values as horizontal columns. In `pivot`, you need to specify grouping columns (`group by`) and aggregation expressions (`agg`) in the subsequent operators.

Example:
```sql
from orders
pivot on o_orderpriority
group by o_orderstatus
agg _.count;

┌───────────────┬──────────┬────────┬──────────┬─────────────────┬───────┐
│ o_orderstatus │ 1-URGENT │ 2-HIGH │ 3-MEDIUM │ 4-NOT SPECIFIED │ 5-LOW │
│    string     │   long   │  long  │   long   │      long       │ long  │
├───────────────┼──────────┼────────┼──────────┼─────────────────┼───────┤
│ F             │     1468 │   1483 │     1445 │            1465 │  1443 │
│ O             │     1488 │   1506 │     1421 │            1482 │  1436 │
│ P             │       64 │     76 │       75 │              77 │    71 │
├───────────────┴──────────┴────────┴──────────┴─────────────────┴───────┤
│ 3 rows                                                                 │
└────────────────────────────────────────────────────────────────────────┘
```

To specify the column values to expand, use `in` clause:
```sql
from orders
pivot on o_orderpriority in ('1-URGENT', '2-HIGH')
group by o_orderstatus
agg _.count;

┌───────────────┬──────────┬────────┐
│ o_orderstatus │ 1-URGENT │ 2-HIGH │
│    string     │   long   │  long  │
├───────────────┼──────────┼────────┤
│ F             │     1468 │   1483 │
│ O             │     1488 │   1506 │
│ P             │       64 │     76 │
├───────────────┴──────────┴────────┤
│ 3 rows                            │
└───────────────────────────────────┘
```
