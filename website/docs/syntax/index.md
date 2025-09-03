import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Query Syntax Reference

Wvlet is a query language designed to be more human-readable and easier to write than SQL. If you are familiar to SQL, you will find it easy to learn Wvlet syntax due to the many similarities. If you know about [DataFrame in Python](https://pandas.pydata.org/docs/user_guide), it will help you understand the Wvlet query language, as chaining relational operators in flow-style is similar to using the DataFrame API.

:::tip Getting Started
New to Wvlet? Check out the [Quick Start](./quick-start.md) tutorial for a hands-on introduction.
:::

## Quick Navigation

- [Operator Quick Reference](#operator-quick-reference)
- [Data Source Operators](#data-source-operators)
- [Filtering & Selection](#filtering--selection)
- [Column Operations](#column-operations)
- [Aggregation & Grouping](#aggregation--grouping)
- [Joining & Combining Data](#joining--combining-data)
- [Sorting & Transformation](#sorting--transformation)
- [Advanced Operations](#advanced-operations)
- [Expressions](#expressions)
- [Common Query Patterns](#common-query-patterns)

## Operator Quick Reference

### Core Operators at a Glance

| Operator | Description | SQL Equivalent | Example |
|----------|-------------|----------------|----------|
| **Data Sources** | | | |
| `from` | Read data from table/file | `SELECT * FROM` | `from customers` |
| `with` | Define reusable subquery | `WITH ... AS` | `with t as { from users }` |
| `show tables` | List available tables | `SHOW TABLES` | `show tables` |
| **Filtering & Selection** | | | |
| `where` | Filter rows | `WHERE` | `where age > 21` |
| `select` | Choose columns | `SELECT` | `select name, age` |
| `limit` | Limit row count | `LIMIT` | `limit 10` |
| `sample` | Random sampling | `TABLESAMPLE` | `sample 1000` |
| **Column Operations** | | | |
| `add` | Add new columns | `SELECT *, expr AS` | `add price * 0.9 as discounted` |
| `exclude` | Remove columns | - | `exclude password` |
| `rename` | Rename columns | `AS` | `rename user_id as id` |
| `shift` | Reorder columns | - | `shift name, email` |
| **Aggregation** | | | |
| `group by` | Group rows | `GROUP BY` | `group by category` |
| `agg` | Add aggregations | `SELECT agg_func()` | `agg _.count, revenue.sum` |
| `pivot` | Pivot table | `PIVOT` | `pivot on month` |
| **Joining Data** | | | |
| `join` | Inner join | `JOIN` | `join orders on id = user_id` |
| `left join` | Left outer join | `LEFT JOIN` | `left join orders on ...` |
| `concat` | Union all | `UNION ALL` | `concat { from archive }` |
| **Transformation** | | | |
| `order by` | Sort rows | `ORDER BY` | `order by created_at desc` |
| `dedup` | Remove duplicates | `DISTINCT` | `dedup` |
| `unnest` | Expand arrays | `UNNEST` | `unnest(tags)` |

### Typical Flow of a Wvlet Query

Wvlet queries start with the
`from` keyword, and you can chain multiple relational operators to process the input data and generate output data. Here's a typical flow:

```wvlet
from ...         -- Scan the input data
where ...        -- Apply filtering conditions
where ...        -- [optional] Apply more filtering conditions
add   ... as ... -- Add new columns
group by ...     -- Group rows by the given columns
agg ...          -- Add group aggregation expressions, e.g., _.count, _.sum 
where ...        -- Apply filtering conditions for groups (HAVING clause in SQL)
exclude ...      -- Remove columns from the output
shift ...        -- Shift the column position to the left
select ...       -- Select columns to output
order by ...     -- Sort the rows by the given columns
limit ...        -- Limit the number of rows to output
```

Unlike SQL, whose queries always must follow the `SELECT ... FROM ... WHERE ... GROUP BY ... ORDER BY ... LIMIT ...` structure, Wvlet uses the flow-style syntax to match the order of data processing order as much as possible, facilitating more intuitive query writing. A query should have a `from` statement to read the data, but
`select` is not mandatory in Wvlet.

## Relational Operators

In Wvlet, you need to use __lower-case__ keywords for SQL-like operators. Operators are organized by their primary function to help you find what you need quickly.

## Data Source Operators

These operators help you access and explore data sources in your environment.

### Catalog Discovery

| Operator | Description | SQL Equivalent |
|----------|-------------|----------------|
| [__show__ tables](#show-tables) | List all tables in current schema | `SHOW TABLES` |
| [__show__ tables __in__ `schema`](#show-tables) | List tables in specific schema | `SHOW TABLES IN schema` |
| [__show__ schemas](#show-schemas) | List all schemas in catalog | `SHOW SCHEMAS` |
| [__show__ schemas __in__ `catalog`](#show-schemas) | List schemas in specific catalog | `SHOW SCHEMAS IN catalog` |
| [__show__ catalogs](#show-catalogs) | List all available catalogs | `SHOW CATALOGS` |
| __show__ models | List all available models | - |
| __describe__ `table` | Show table/query schema | `DESCRIBE table` |

### Reading Data

| Operator | Description | SQL Equivalent |
|----------|-------------|----------------|
| [__from__ `table`](#from) | Read from table | `SELECT * FROM table` |
| [__from__ `'file.parquet'`](#from) | Read from file | - |
| [__from__ `[[...]]`](#from) | Read from inline values | `VALUES (...)` |
| [__with__ `alias` __as__ { `query` }](#with) | Define reusable subquery | `WITH alias AS (query)` |
| __from__ sql"..." | Execute raw SQL | - |

## Filtering & Selection

These operators help you filter rows and select specific columns from your data.

| Operator | Description | SQL Equivalent |
|----------|-------------|----------------|
| [__where__ `condition`](#where) | Filter rows by condition | `WHERE condition` |
| [__select__ `col1`, `col2`](#select) | Select specific columns | `SELECT col1, col2` |
| __select__ * | Select all columns | `SELECT *` |
| __select distinct__ `cols` | Select unique values | `SELECT DISTINCT cols` |
| [__limit__ `n`](#limit) | Limit to n rows | `LIMIT n` |
| [__sample__ `n`](#sample) | Random sample of n rows | `TABLESAMPLE` |
| [__sample__ `n%`](#sample) | Random sample percentage | `TABLESAMPLE n PERCENT` |
| __exists__ { `subquery` } | Check if subquery has results | `EXISTS (subquery)` |

## Column Operations

These operators help you manipulate columns without changing the number of rows.

| Operator | Description | SQL Equivalent |
|----------|-------------|----------------|
| [__add__ `expr` __as__ `name`](#add) | Add column to the right | `SELECT *, expr AS name` |
| [__prepend__ `expr` __as__ `name`](#prepend) | Add column to the left | - |
| [__exclude__ `col1`, `col2`](#exclude) | Remove specific columns | - |
| [__rename__ `old` __as__ `new`](#rename) | Rename columns | `old AS new` |
| [__shift__ `col1`, `col2`](#shift) | Move columns to left | - |
| __shift to right__ `cols` | Move columns to right | - |

## Aggregation & Grouping

These operators help you aggregate data and perform group operations.

| Operator | Description | SQL Equivalent |
|----------|-------------|----------------|
| [__group by__ `cols`](#group-by) | Group rows by columns | `GROUP BY cols` |
| [__agg__ `expr1`, `expr2`](#agg) | Add aggregation expressions | `SELECT agg_func(...)` |
| [__count__](#count) | Count all rows | `SELECT COUNT(*)` |
| [__pivot on__ `col`](#pivot) | Pivot table on column values | `PIVOT` |
| [__unpivot__ `val` __for__ `col` __in__ (...)](#unpivot) | Unpivot columns to rows | `UNPIVOT` |

### Common Aggregation Functions

When using `agg` after `group by`, you can use these aggregation functions:

- `_.count` - Count rows in group
- `column.sum` - Sum of column values
- `column.avg` - Average of column values
- `column.min` / `column.max` - Min/max values
- `_.count_distinct(column)` - Count distinct values
- `_.array_agg(column)` - Collect values into array

## Joining & Combining Data

These operators help you combine data from multiple sources.

| Operator | Description | SQL Equivalent |
|----------|-------------|----------------|
| [__join__ `table` __on__ `condition`](#join) | Inner join | `JOIN table ON condition` |
| [__left join__ `table` __on__ `condition`](#join) | Left outer join | `LEFT JOIN table ON condition` |
| __right join__ `table` __on__ `condition` | Right outer join | `RIGHT JOIN table ON condition` |
| __cross join__ `table` | Cartesian product | `CROSS JOIN table` |
| [__asof join__](asof-join.md) | Time-based join | - |
| [__concat__ { `query` }](#concat) | Union all results | `UNION ALL` |
| [__intersect__ { `query` }](#intersect) | Set intersection | `INTERSECT` |
| [__except__ { `query` }](#except) | Set difference | `EXCEPT` |

## Sorting & Transformation

These operators help you sort and transform your data.

| Operator | Description | SQL Equivalent |
|----------|-------------|----------------|
| [__order by__ `col` __desc__](#order-by) | Sort descending | `ORDER BY col DESC` |
| [__order by__ `col` __asc__](#order-by) | Sort ascending | `ORDER BY col ASC` |
| [__dedup__](#dedup) | Remove duplicate rows | `SELECT DISTINCT *` |
| [__unnest__(`array_col`)](unnest.md) | Expand array to rows | `UNNEST(array_col)` |

## Advanced Operations

These operators provide advanced functionality for testing and debugging.

| Operator | Description | Use Case |
|----------|-------------|----------|
| [__test__ `condition`](test-syntax.md) | Assert test conditions | Testing queries |
| __debug__ { `query` } | Debug intermediate results | Debugging |
| __\|__ `function(args)` | Apply table function | Custom transformations |

## Update Operations

These operators allow you to save or modify data.

| Operator | Description | Notes |
|----------|-------------|-------|
| __save to__ `table` | Create new table | Overwrites if exists |
| __save to__ `'file.parquet'` | Save to file | Supports various formats |
| __append to__ `table` | Append to existing table | Creates if not exists |
| __delete__ | Delete matching rows | From source table |

## Inspection Commands

These commands help you understand query structure and data.

| Operator | Description | Use Case |
|----------|-------------|----------|
| __describe__ `query` | Show query schema | Understanding output columns |
| __explain__ `query` | Show logical plan | Query optimization |
| __explain__ sql"..." | Explain raw SQL | SQL debugging |

## Expressions

One of the major difference from traditional SQL is that wvlet uses single or double quoted strings for representing string values and back-quoted strings for referencing column or table names, which might contain space or special characters.

| Operator                                              | Description                                                                                                                                              |
|:------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| 'single quote'                                        | String literal for representing string values, file names, etc.                                                                                          |
| "double quote"                                        | Same as single quote strings                                                                                                                             |
| """triple quote string"""                             | Multi-line strings                                                                                                                                       |
| \`(back quote)\`                                      | Column or table name, which requires quotations                                                                                                          |
| __sql__"`sql expr`"                                   | SQL expression used for inline expansion                                                                                                                 |
| __sql__" ... $\{`expr`\} ..."                         | Interpolated SQL expression with embedded expressions                                                                                                    |
| __s__"... $\{expr\} ..."                              | Interpolated strings with expressions                                                                                                                    |
| __s__\`... $\{expr\} ...\`                            | Interpolated backquote strings with expressions                                                                                                          |
| [`expr`, ...]                                         | Array value                                                                                                                                              |
| [[`expr`, ...], ...]                                  | Array of arrays for representing table records                                                                                                           |
| \{`key`\: `value`, ...\}                              | Struct (row) value                                                                                                                                       |
| `_`                                                   | Underscore refers to the previous input                                                                                                                  |
| `agg_func(expr)` over (partition by ... order by ...) | [Window functions](window.md) for computing aggregate values computed from the entire query result. This follows similar window function syntax with SQL |
| `_1`, `_2`, ...                                       | Refers to 1-origin grouping keys in the preceding `group by` clause                                                                                      |
| `1`, `2`, ...                                         | Refers to 1-origin column index for `order by` clause                                                                                                    |
| `expr`:`type`                                         | Cast the value to the target type. Equivalent to cast(`expr` as `type`) in SQL                                                                           |
| '2025-01-01':date                                     | Cast the string to a date value                                                                                                                          |
| '1 year':interval                                     | Cast the string to an interval of SQL                                                                                                                    |
### Variable Definition

You can define a variable using `val` keyword:

```wvlet
-- Define a new variable
val name = 'wvlet'

-- Variable is evaluated once before running the query
select s"Hello ${x}!" as msg
-- Returns [['Hello wvlet!']]
```

#### Table Variable Constants

You can also define table constants using the `val` keyword with column names and data:

```wvlet
-- Basic table value constant
val products(id, name, price) = [
  [1, "Laptop", 999.99],
  [2, "Mouse", 29.99],
  [3, "Keyboard", 79.99],
]

from products
where _.price > 50
-- Returns [[1, "Laptop", 999.99], [3, "Keyboard", 79.99]]
```

You can specify type annotations for table columns:

```wvlet
-- With type annotations
val users(id:int, name:string, active:boolean) = [
  [1, "Alice", true],
  [2, "Bob", false],
]

from users
where active = true
-- Returns [[1, "Alice", true]]
```

This syntax allows you to define inline data tables that can be referenced in your queries, which is particularly useful for testing, small lookup tables, or providing sample data.

### Conditional Expressions

| Operator                             | Description                                                                      |
|--------------------------------------|----------------------------------------------------------------------------------|
| `expr` __and__ `expr`                | Logical AND                                                                      |
| `expr` __or__  `expr`                | Logical OR                                                                       |
| __not__ `expr`                       | Logical NOT                                                                      |
| !`expr`                              | Logical NOT                                                                      |
| `expr` __is__ `expr`                 | Equality check                                                                   |
| `expr` __=__ `expr`                  | Equality check                                                                   |
| `expr` __is not__ `expr`             | Inequality check                                                                 |
| `expr` __!=__ `expr`                 | Inequality check                                                                 |
| `expr` __is null__                   | True if the expression is null                                                   |
| `expr` __= null__                    | True if the expression is null                                                   |
| `expr` __is not null__               | True if the expression is not null                                               |
| `expr` __!= null__                   | True if the expression is not null                                               |
| `expr` __in__ (`v1`, `v2`, ...)      | True if the expression value is in the given list                                |
| `expr` __in__ \{ from ... \}         | True if the expression value is in the given list provided by a sub query        |
| `expr` __not in__ (`v1`, `v2`, ...)  | True if the expression is not in the given list                                  |
| `expr` __between__ `v1` __and__ `v2` | True if the expression value is between v1 and v2, i.e., v1 &le; (value) &le; v2 |
| `expr` __like__ `pattern`            | True if the expression matches the given pattern, e.g., , `'abc%'`               |
| __exists__ \{ from ... \}            | True if the subquery returns any rows                                            |
| __not exists__ \{ from ... \}        | True if the subquery returns no rows                                             |

#### If Expression

`if ... then .. else` expression can be used to switch the output value based on the condition:

```wvlet
from lineitem
select
  if l_returnflag = 'A' then 1 else 0
  as return_code
```

The `if` expression can be nested as follows:

```wvlet
from lineitem
select
  if l_returnflag = 'A' then 1
  else if l_returnflag = 'R' then 2
  else 0
  as return_code
```

#### Case Expression

To switch the output value based on the input value, you can use the `case` expression:

```wvlet
from lineitem
select
  case l_returnflag
    when 'A' then 1
    when 'R' then 2
    else 0
  as return_code
```

You can also use the `case` expression with conditional expressions for clarity:

```wvlet
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

```wvlet
select ['a', 'b', 'c'] as arr
```

Arrays can be accessed with index (1-origin):

```wvlet
select ['a', 'b', 'c'] as arr
select arr[1] as first_element
```

### Map Expressions

You can construct map values with
`map {k1: v1, k2: v2, ...}` syntax. Unlike struct expressions, keys (k1, k2, ...) needs to be the same type values, and values (v1, v2, ...) also need to be the same type values:

```wvlet
select map {'a': 1, 'b': 2} as m
```

### Struct/Row Expressions

Struct (row) expressions are used to represent key-value pairs. You can access the values by name:

```wvlet
select {'i': 3, 's': 'str'} as obj
select 
  -- Name based access
  obj.i, obj.s,
  -- Lookup by name 
  obj['i'], obj['s']
```

Note that key names in a struct do not require quotes for plain key names:

```wvlet
select {k1:1, k2:'v1'} as obj
select obj.k1, obj.k2
```

### Lambda Expression

To manipulate array values, you can use lambda expressions:

```wvlet
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

```wvlet
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

```wvlet
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

```wvlet
show tables
```

The output of `show tables` can be followed by the `where` clause to filter the table names:

```wvlet
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

```wvlet
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

```wvlet
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

```wvlet
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

```wvlet
from nation
```

In DuckDB backend, you can read data from local Parquet or JSON files:

```wvlet
from 'sample.parquet' 
```

Or from files on the Web:

```wvlet
from 'https://(path to your data)/sample.parquet'
```

Reading data from an URL will also work for S3 Presigned URLs.

#### Raw SQL statements

In case you need to use a raw SQL statement, you can use the `sql` string interpolation:

```wvlet
from sql"select * from nation"
```

:::warning
Although raw SQL statements are convenient, Wvlet needs to issue a real query to the target DBMS to inspect the data types of the query result,
which might slowdown the query or cause errors in the subsequent relational operators.
:::

### where

You can apply a filter condition to the input rows:

```wvlet
from nation
where n_regionkey = 1
```

Unlike SQL, applying multiple `where` clauses is allowed in Wvlet:

```wvlet
from nation
where n_regionkey = 1
where n_name like 'U%'
```

This is useful even when you need to generate Wvlet queries programatically as you only need to append new condition lines to the query. In SQL, you need to parse the text inside WHERE clause and concatenate condition with AND expression.

### count 

Wvlet has a shorthand notation for counting the number of input rows:

```wvlet
from nation
count
```

This is equilvalen to:
```wvlet
from nation
select _.count
```

Or more SQL-like expression can be used too:
```wvlet
from nation
select count(*)
```

For clarity, you can use pipe `|` before the count operator:
```wvlet
from nation
select n_name, n_regionkey,
-- pipe (|) is required after trailing comma
| count
```

### add

Add a new column to the input. This operator is helpful to reduce the typing effort as you don't need to enumerate all the columns in the `select` clause:

```wvlet
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

### prepend

Prepend a new column to the left side of the input:

```wvlet
from nation
select n_name
prepend n_name.substring(1, 3) as prefix
limit 5;

┌────────┬───────────┐
│ prefix │  n_name   │
│ string │  string   │
├────────┼───────────┤
│ ALG    │ ALGERIA   │
│ ARG    │ ARGENTINA │
│ BRA    │ BRAZIL    │
│ CAN    │ CANADA    │
│ EGY    │ EGYPT     │
├────────┴───────────┤
│ 5 rows             │
└────────────────────┘
```

### select

Output only the selected columns from the input:

```wvlet
from nation
select n_name, n_regionkey
```

As in SQL, arbitrary expressions can be used in select statements. Note that, however, in Wvlet, you usually don't need to use select statement
as the other column-at-a-time operators, like `add`, `exclude`, `shift`, etc. can be used for manipulating column values.

### rename

The `rename` operator changes the column names in the input rows. This is useful when you want to rename columns in the output.

```wvlet
from nation
rename n_name as nation_name
```

### exclude

The `exclude` operator removes specified columns from the input rows. This is useful when you want to drop certain columns from the output.

```wvlet
from nation
exclude n_regionkey
```

### shift

The `shift` operator changes the position of specified columns in the input rows. You can move columns to the left or right.

Shift to the left (default):
```wvlet
from nation
shift n_name
```

Shift to the right:
```wvlet
from nation
shift to right n_comment
```


### group by

The `group by` operator groups rows by one or more columns, creating groups of rows that share the same values in the specified columns.

#### Basic Grouping

```wvlet
val data(id, category, value) = [
  [1, "A", 100],
  [2, "B", 200],
  [1, "A", 150],
  [2, "B", 50],
  [3, "C", 300]
]

from data
group by id, category;

┌─────┬──────────┬───────┐
│ id  │ category │ value │
│ int │  string  │  int  │
├─────┼──────────┼───────┤
│   1 │ A        │   100 │
│   2 │ B        │   200 │
│   3 │ C        │   300 │
├─────┴──────────┴───────┤
│ 3 rows                 │
└────────────────────────┘
```

:::note
In SQL, you need to specify aggregation operators like `count`, `sum`, `avg`, etc. to generate output from `GROUP BY`.
In Wvlet, the default aggregation operator `arbitrary (any)` is used, which returns an arbitrary value from each group.
:::

#### Referencing Grouping Keys

After grouping, you can reference grouping keys by their column names directly:

```wvlet
val data(id, category, value) = [
  [1, "A", 100],
  [2, "B", 200],
  [1, "A", 150],
  [2, "B", 50]
]

from data
group by id, category
select id, category;

┌─────┬──────────┐
│ id  │ category │
│ int │  string  │
├─────┼──────────┤
│   1 │ A        │
│   2 │ B        │
├─────┴──────────┤
│ 2 rows         │
└────────────────┘
```

:::tip
While you can also reference grouping keys using `_1`, `_2`, etc. in the order they appear in the `group by` clause, it's recommended to use column names for better readability and maintainability.
:::

### agg

The `agg` operator adds aggregation expressions. It is typically used after a `group by` clause to aggregate data within groups, but can also be used without `group by` to aggregate all rows in the input.

#### Basic Aggregations

```wvlet
val data(id, category, value) = [
  [1, "A", 100],
  [2, "B", 200],
  [1, "A", 150],
  [2, "B", 50],
  [3, "C", 300]
]

from data
group by category
agg 
  _.count as item_count,
  value.sum as total_value,
  value.avg as avg_value;

┌──────────┬────────────┬─────────────┬───────────────┐
│ category │ item_count │ total_value │  avg_value    │
│  string  │    long    │     int     │ decimal(17,4) │
├──────────┼────────────┼─────────────┼───────────────┤
│ A        │          2 │         250 │      125.0000 │
│ B        │          2 │         250 │      125.0000 │
│ C        │          1 │         300 │      300.0000 │
├──────────┴────────────┴─────────────┴───────────────┤
│ 3 rows                                              │
└─────────────────────────────────────────────────────┘
```

#### Available Aggregation Functions

The underscore `_` represents the group of rows and supports various aggregation functions:

```wvlet
val sales(store_id, amount, product) = [
  [1, 10, "A"],
  [2, 20, "B"],
  [1, 30, "C"],
  [2, 40, "D"],
  [1, 50, "E"]
]

from sales
group by store_id
agg 
  _.count as transaction_count,
  amount.sum as total_sales,
  amount.avg as avg_sale,
  amount.min as min_sale,
  amount.max as max_sale,
  _.count_distinct(product) as unique_products,
  _.max_by(product, amount) as best_product,
  _.min_by(product, amount) as worst_product;

┌──────────┬───────────────────┬─────────────┬───────────────┬──────────┬──────────┬─────────────────┬──────────────┬───────────────┐
│ store_id │ transaction_count │ total_sales │   avg_sale    │ min_sale │ max_sale │ unique_products │ best_product │ worst_product │
│   int    │       long        │     int     │ decimal(17,4) │   int    │   int    │      long       │    string    │    string     │
├──────────┼───────────────────┼─────────────┼───────────────┼──────────┼──────────┼─────────────────┼──────────────┼───────────────┤
│        1 │                 3 │          90 │       30.0000 │       10 │       50 │               3 │ E            │ A             │
│        2 │                 2 │          60 │       30.0000 │       20 │       40 │               2 │ D            │ B             │
├──────────┴───────────────────┴─────────────┴───────────────┴──────────┴──────────┴─────────────────┴──────────────┴───────────────┤
│ 2 rows                                                                                                                            │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

#### Filtering Groups (HAVING clause)

You can filter groups after aggregation using a `where` clause after `agg`:

```wvlet
val orders(id, category, amount) = [
  [1, "Electronics", 1000],
  [2, "Books", 50],
  [3, "Electronics", 2000],
  [4, "Books", 75],
  [5, "Toys", 500],
  [6, "Electronics", 1500]
]

from orders
group by category
agg 
  _.count as order_count,
  amount.sum as total_amount
where total_amount > 1000
order by total_amount desc;

┌─────────────┬─────────────┬──────────────┐
│  category   │ order_count │ total_amount │
│   string    │    long     │     int      │
├─────────────┼─────────────┼──────────────┤
│ Electronics │           3 │         4500 │
├─────────────┴─────────────┴──────────────┤
│ 1 rows                                   │
└──────────────────────────────────────────┘
```

#### Multiple Grouping with Complex Aggregations

```wvlet
val sales(date, store, category, amount) = [
  ["2024-01-01", "A", "Electronics", 100],
  ["2024-01-01", "B", "Books", 50],
  ["2024-01-02", "A", "Electronics", 200],
  ["2024-01-02", "A", "Books", 75],
  ["2024-01-03", "B", "Electronics", 150]
]

from sales
group by date, store
agg 
  _.count as transactions,
  amount.sum as daily_total,
  _.array_agg(category) as categories_sold,
  _.array_agg(amount) as amounts;

┌────────────┬───────┬──────────────┬─────────────┬───────────────────────┬───────────────┐
│    date    │ store │ transactions │ daily_total │   categories_sold     │    amounts    │
│   string   │ string│     long     │     int     │     array(string)     │   array(int)  │
├────────────┼───────┼──────────────┼─────────────┼───────────────────────┼───────────────┤
│ 2024-01-01 │ A     │            1 │         100 │ [Electronics]         │ [100]         │
│ 2024-01-01 │ B     │            1 │          50 │ [Books]               │ [50]          │
│ 2024-01-02 │ A     │            2 │         275 │ [Electronics, Books]  │ [200, 75]     │
│ 2024-01-03 │ B     │            1 │         150 │ [Electronics]         │ [150]         │
├────────────┴───────┴──────────────┴─────────────┴───────────────────────┴───────────────┤
│ 4 rows                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

#### Using Aggregations Without group by

You can use aggregations directly without `group by` to aggregate all rows:

```wvlet
val data(id, value) = [
  [1, 100],
  [2, 200],
  [3, 300],
  [4, 400],
  [5, 500]
]

from data
agg 
  _.count as total_rows,
  value.sum as sum_all,
  value.avg as average;

┌────────────┬─────────┬───────────────┐
│ total_rows │ sum_all │    average    │
│    long    │   int   │ decimal(17,4) │
├────────────┼─────────┼───────────────┤
│          5 │    1500 │      300.0000 │
├────────────┴─────────┴───────────────┤
│ 1 rows                               │
└──────────────────────────────────────┘
```

#### Practical Example: Sales Analysis

Here's a practical example analyzing sales data:

```wvlet
val sales(month, region, product, quantity, unit_price) = [
  ["2024-01", "North", "Laptop", 5, 1200],
  ["2024-01", "North", "Phone", 10, 800],
  ["2024-01", "South", "Laptop", 3, 1200],
  ["2024-01", "South", "Tablet", 7, 600],
  ["2024-02", "North", "Laptop", 8, 1200],
  ["2024-02", "North", "Tablet", 5, 600],
  ["2024-02", "South", "Phone", 12, 800],
  ["2024-02", "South", "Laptop", 4, 1200]
]

from sales
group by month, region
agg 
  _.count as num_orders,
  quantity.sum as total_units_sold,
  (quantity * unit_price).sum as total_revenue,
  _.count_distinct(product) as unique_products
where total_revenue > 10000
order by month, total_revenue desc;

┌─────────┬────────┬────────────┬──────────────────┬───────────────┬─────────────────┐
│  month  │ region │ num_orders │ total_units_sold │ total_revenue │ unique_products │
│ string  │ string │    long    │       int        │      int      │      long       │
├─────────┼────────┼────────────┼──────────────────┼───────────────┼─────────────────┤
│ 2024-01 │ North  │          2 │               15 │         14000 │               2 │
│ 2024-02 │ South  │          2 │               16 │         14400 │               2 │
│ 2024-02 │ North  │          2 │               13 │         12600 │               2 │
├─────────┴────────┴────────────┴──────────────────┴───────────────┴─────────────────┤
│ 3 rows                                                                             │
└────────────────────────────────────────────────────────────────────────────────────┘
```

:::tip
The `_` underscore in aggregations represents the group of rows. You can use it with various aggregation functions like `_.count`, `_.sum`, `_.avg`, `_.max`, `_.min`, `_.count_distinct()`, `_.array_agg()`, `_.max_by()`, `_.min_by()`, etc.
:::

:::note
The `where` clause after `agg` is equivalent to SQL's `HAVING` clause - it filters groups based on aggregated values, not individual rows.
:::

### limit

Limit the number of output rows:

```wvlet
from nation
limit 10
```

### order by

Sort the output rows by the given column:

```wvlet
from nation
order by n_name
limit 5
```

The `order by` statement can follow `asc`, `desc` ordering specifier as in SQL.
You can add `nulls first` or `nulls last` to specify the order of null values.

### join

The `join` operator combines rows from two or more tables based on a related column between them. Wvlet supports various types of joins similar to SQL, but with a more intuitive flow-style syntax.

#### Basic Join (Inner Join)

The basic `join` returns rows when there is a match in both tables:

```wvlet
val fruit(id, name, price) = [
  [1, "apple", 50],
  [2, "banana", 10],
  [3, "cherry", 70]
]

val fruit_order(order_id, fruit_id, qty) = [
  ["o1", 1, 10],
  ["o2", 2, 5]
]

from fruit
join fruit_order
on fruit.id = fruit_order.fruit_id
select fruit.name, fruit_order.qty, fruit.price * fruit_order.qty as total_price;

┌────────┬─────┬─────────────┐
│  name  │ qty │ total_price │
│ string │ int │     int     │
├────────┼─────┼─────────────┤
│ apple  │  10 │         500 │
│ banana │   5 │          50 │
├────────┴─────┴─────────────┤
│ 2 rows                     │
└────────────────────────────┘
```

#### Left Join

A `left join` returns all rows from the left table, and matched rows from the right table. If no match, NULL values are returned for right table columns:

```wvlet
val users(id, name) = [
  [1, "Alice"],
  [2, "Bob"],
  [3, "Charlie"]
]

val orders(user_id, order_id) = [
  [1, "order1"],
  [1, "order2"],
  [3, "order3"]
]

from users
left join orders
on users.id = orders.user_id
select users.name, orders.order_id;

┌─────────┬──────────┐
│  name   │ order_id │
│ string  │  string  │
├─────────┼──────────┤
│ Alice   │ order1   │
│ Alice   │ order2   │
│ Bob     │ null     │
│ Charlie │ order3   │
├─────────┴──────────┤
│ 4 rows             │
└────────────────────┘
```

#### Right Join

A `right join` returns all rows from the right table, and matched rows from the left table:

```wvlet
val orders(user_id, order_id) = [
  [1, "order1"],
  [1, "order2"],
  [3, "order3"],
  [4, "order4"]
]

val users(id, name) = [
  [1, "Alice"],
  [2, "Bob"],
  [3, "Charlie"]
]

from orders
right join users
on orders.user_id = users.id
select users.name, orders.order_id;

┌─────────┬──────────┐
│  name   │ order_id │
│ string  │  string  │
├─────────┼──────────┤
│ Alice   │ order1   │
│ Alice   │ order2   │
│ Bob     │ null     │
│ Charlie │ order3   │
├─────────┴──────────┤
│ 4 rows             │
└────────────────────┘
```

#### Cross Join

A `cross join` produces the Cartesian product of two tables, combining each row from the first table with every row from the second table:

```wvlet
val t1(id, val) = [
  [1, "A"],
  [2, "B"]
]

val t2(letter) = [
  ["X"],
  ["Y"],
  ["Z"]
]

from t1
cross join t2
select t1.val, t2.letter;

┌────────┬────────┐
│  val   │ letter │
│ string │ string │
├────────┼────────┤
│ A      │ X      │
│ A      │ Y      │
│ A      │ Z      │
│ B      │ X      │
│ B      │ Y      │
│ B      │ Z      │
├────────┴────────┤
│ 6 rows          │
└─────────────────┘
```

#### Join with Multiple Conditions

You can specify multiple conditions in the `on` clause using `and`/`or` operators:

```wvlet
val t1(id, code, value) = [
  [1, "A", 100],
  [2, "B", 200],
  [3, "C", 300]
]

val t2(id, code, qty) = [
  [1, "A", 10],
  [2, "B", 20],
  [3, "D", 30]
]

from t1
join t2
on t1.id = t2.id and t1.code = t2.code
select t1.id, t1.code, t1.value, t2.qty;

┌─────┬────────┬───────┬─────┐
│ id  │  code  │ value │ qty │
│ int │ string │  int  │ int │
├─────┼────────┼───────┼─────┤
│   1 │ A      │   100 │  10 │
│   2 │ B      │   200 │  20 │
├─────┴────────┴───────┴─────┤
│ 2 rows                     │
└────────────────────────────┘
```

#### Combining Joins with Other Operators

Joins can be seamlessly combined with other Wvlet operators in a flow-style query:

```wvlet
val categories(id, name, budget) = [
  [1, "Electronics", 1000],
  [2, "Books", 500],
  [3, "Clothing", 750]
]

val products(category_id, name, price) = [
  [1, "Laptop", 800],
  [1, "Phone", 600],
  [2, "Novel", 20],
  [4, "Unknown", 100]
]

from categories
left join products
on categories.id = products.category_id
where products.price != null
group by categories.name
agg 
  _.count as product_count,
  products.price.sum as total_price
order by product_count desc;

┌─────────────┬───────────────┬─────────────┐
│    name     │ product_count │ total_price │
│   string    │     long      │     int     │
├─────────────┼───────────────┼─────────────┤
│ Electronics │             2 │        1400 │
│ Books       │             1 │          20 │
├─────────────┴───────────────┴─────────────┤
│ 2 rows                                    │
└───────────────────────────────────────────┘
```

#### Multiple Joins

You can chain multiple join operations to combine data from several tables:

```wvlet
val users(id, name) = [
  [1, "Alice"],
  [2, "Bob"],
  [3, "Charlie"]
]

val accounts(user_id, account_id) = [
  [1, 101],
  [2, 102],
  [3, 103]
]

val balances(account_id, amount) = [
  [101, 1000],
  [102, 2000],
  [103, 1500]
]

from users
join accounts
on users.id = accounts.user_id
join balances
on accounts.account_id = balances.account_id
select users.name, accounts.account_id, balances.amount;

┌─────────┬────────────┬────────┐
│  name   │ account_id │ amount │
│ string  │    int     │  int   │
├─────────┼────────────┼────────┤
│ Alice   │        101 │   1000 │
│ Bob     │        102 │   2000 │
│ Charlie │        103 │   1500 │
├─────────┴────────────┴────────┤
│ 3 rows                        │
└───────────────────────────────┘
```

#### Self Join

A table can be joined with itself (self join) by using aliases. The `with` statement helps avoid duplicating the data:

```wvlet
val employees(id, name, manager_id) = [
  [1, "Alice", 2],
  [2, "Bob", 3],
  [3, "Charlie", null]
]

from employees as emp
left join employees as mgr
  on emp.manager_id = mgr.id
select emp.name as employee, mgr.name as manager;

┌──────────┬─────────┐
│ employee │ manager │
│  string  │ string  │
├──────────┼─────────┤
│ Alice    │ Bob     │
│ Bob      │ Charlie │
│ Charlie  │ null    │
├──────────┴─────────┤
│ 3 rows             │
└────────────────────┘
```

:::tip
Unlike SQL where you write `SELECT ... FROM table1 JOIN table2`, Wvlet's flow-style syntax starts with `from` and then adds `join` as a subsequent operation. This makes it easier to build queries incrementally and matches the logical flow of data processing.
:::

:::note
For time-based joins, Wvlet also supports [AsOf Join](asof-join.md), which is useful for joining tables based on the most recent value at a specific time.
:::

### concat

The concat operator concatenates rows from multiple subqueries. This is similar to the UNION ALL operator in SQL.

```wvlet
val t1(id, val) = [
  [1, "A"],
  [2, "B"]
]

val t2(id, val) = [
  [3, "C"],
  [4, "D"]
]

from t1
concat {
  from t2
}
order by id;

┌─────┬────────┐
│ id  │  val   │
│ int │ string │
├─────┼────────┤
│   1 │ A      │
│   2 │ B      │
│   3 │ C      │
│   4 │ D      │
├─────┴────────┤
│ 4 rows       │
└──────────────┘
```

The ordering of rows is not guaranteed in the `concat` operator. If you need to sort the output, use the `order by` operator after the `concat` operator.

You can concatenate multiple subqueries:

```wvlet
from nation
where n_regionkey = 0
concat {
  from nation
  where n_regionkey = 1
}
concat {
  from nation
  where n_regionkey = 2
}
select n_name, n_regionkey
order by n_regionkey, n_name
```

### dedup

The `dedup` operator removes duplicated rows from the input rows. This is equivalent to `select distinct *` in SQL.

```wvlet
val data(id, val) = [
  [1, "A"],
  [2, "B"],
  [1, "A"],
  [3, "C"],
  [2, "B"]
]

from data
dedup;

┌─────┬────────┐
│ id  │  val   │
│ int │ string │
├─────┼────────┤
│   1 │ A      │
│   2 │ B      │
│   3 │ C      │
├─────┴────────┤
│ 3 rows       │
└──────────────┘
```

### intersect

The `intersect` operator returns the intersection of the input rows from multiple subqueries. By default, set semantics are used (duplicates removed).

```wvlet
val t1(id, val) = [
  [1, "A"],
  [2, "B"],
  [3, "C"],
  [4, "D"]
]

val t2(id, val) = [
  [2, "B"],
  [3, "C"],
  [5, "E"]
]

from t1
intersect {
  from t2
};

┌─────┬────────┐
│ id  │  val   │
│ int │ string │
├─────┼────────┤
│   2 │ B      │
│   3 │ C      │
├─────┴────────┤
│ 2 rows       │
└──────────────┘
```

With `all` keyword, bag semantics are used (duplicates preserved):

```wvlet
val t1(id, val) = [
  [1, "A"],
  [2, "B"],
  [2, "B"],
  [3, "C"]
]

val t2(id, val) = [
  [2, "B"],
  [2, "B"],
  [3, "C"],
  [3, "C"]
]

from t1
intersect all {
  from t2
};

┌─────┬────────┐
│ id  │  val   │
│ int │ string │
├─────┼────────┤
│   2 │ B      │
│   2 │ B      │
│   3 │ C      │
├─────┴────────┤
│ 3 rows       │
└──────────────┘
```

### except

The `except` operator returns the difference of the input rows from multiple subqueries (rows in the first query but not in the second). By default, set semantics are used.

```wvlet
val t1(id, val) = [
  [1, "A"],
  [2, "B"],
  [3, "C"],
  [4, "D"]
]

val t2(id, val) = [
  [2, "B"],
  [4, "D"]
]

from t1
except {
  from t2
};

┌─────┬────────┐
│ id  │  val   │
│ int │ string │
├─────┼────────┤
│   1 │ A      │
│   3 │ C      │
├─────┴────────┤
│ 2 rows       │
└──────────────┘
```

With `all` keyword, bag semantics are used:

```wvlet
val t1(id, val) = [
  [1, "A"],
  [2, "B"],
  [2, "B"],
  [3, "C"]
]

val t2(id, val) = [
  [2, "B"]
]

from t1
except all {
  from t2
};

┌─────┬────────┐
│ id  │  val   │
│ int │ string │
├─────┼────────┤
│   1 │ A      │
│   2 │ B      │
│   3 │ C      │
├─────┴────────┤
│ 3 rows       │
└──────────────┘
```

### pivot

The `pivot` operator expands the column values as horizontal columns. In `pivot`, you need to specify grouping columns (`group by`) and aggregation expressions (`agg`) in the subsequent operators.

Example:
```wvlet
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
```wvlet
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

### unpivot

The `unpivot` operator transforms multiple columns into rows. This is useful when you need to transform wide table into a long table. Currently, unpivot is available only in DuckDB backend.

Example:
```wvlet
val sales(id, dept, jan, feb, mar, apr, may, jun) = [
  [1, 'electronics', 1, 2, 3, 4, 5, 6],
  [2, 'clothes', 10, 20, 30, 40, 50, 60],
  [3, 'cars', 100, 200, 300, 400, 500, 600]
]

from sales
unpivot
  sales for month in (jan, feb, mar, apr, may, jun)

┌─────┬─────────────┬────────┬───────┐
│ id  │    dept     │ month  │ sales │
│ int │   string    │ string │  int  │
├─────┼─────────────┼────────┼───────┤
│   1 │ electronics │ jan    │     1 │
│   1 │ electronics │ feb    │     2 │
│   1 │ electronics │ mar    │     3 │
│   1 │ electronics │ apr    │     4 │
│   1 │ electronics │ may    │     5 │
│   1 │ electronics │ jun    │     6 │
│   2 │ clothes     │ jan    │    10 │
│   2 │ clothes     │ feb    │    20 │
│   2 │ clothes     │ mar    │    30 │
│   2 │ clothes     │ apr    │    40 │
│   2 │ clothes     │ may    │    50 │
│   2 │ clothes     │ jun    │    60 │
│   3 │ cars        │ jan    │   100 │
│   3 │ cars        │ feb    │   200 │
│   3 │ cars        │ mar    │   300 │
│   3 │ cars        │ apr    │   400 │
│   3 │ cars        │ may    │   500 │
│   3 │ cars        │ jun    │   600 │
├─────┴─────────────┴────────┴───────┤
│ 18 rows                            │
└────────────────────────────────────┘

```

### sample

The `sample` operator randomly samples rows from the input data. You can specify either a fixed number of rows or a percentage.

Sample a fixed number of rows:
```wvlet
val numbers(n) = [
  [1], [2], [3], [4], [5],
  [6], [7], [8], [9], [10]
]

from numbers
sample 5;

┌─────┐
│  n  │
│ int │
├─────┤
│   2 │
│   4 │
│   7 │
│   8 │
│  10 │
├─────┤
│ 5 … │
└─────┘
```

Sample a percentage of rows:
```wvlet
val numbers(n) = [
  [1], [2], [3], [4], [5],
  [6], [7], [8], [9], [10]
]

from numbers
sample 30%;

┌─────┐
│  n  │
│ int │
├─────┤
│   3 │
│   6 │
│   9 │
├─────┤
│ 3 … │
└─────┘
```

You can specify the sampling method - `reservoir` (default), `system`, or `bernoulli`:

```wvlet
-- Reservoir sampling (default) - good for getting exact sample size
from large_table
sample reservoir (1000)

-- System sampling - faster but approximate, samples blocks of data
from large_table  
sample system (10%)

-- Bernoulli sampling - samples individual rows with given probability
from large_table
sample bernoulli (10%)
```

:::tip
Reservoir sampling guarantees the exact number of rows in the output (if available), while system and bernoulli sampling may return approximately the requested percentage of rows.
:::

### with

To define a local subquery definition, use the `with` expression:

```wvlet
with t1 as {
  from nation
  where n_regionkey = 1
}
from t1
```

You can define multiple subqueries: 
```wvlet
with t1 as {
  from nation
  where n_regionkey = 1
}
with t2 as {
  from t1
  limit 10
}
from t2
```

:::tip
The `with` expression is similar to the common-table expression (CTE) in SQL, but for each subquery, you need to use a separate `with` clause. This is for convenience of adding multiple sub queries in separate line blocks without concatenating them with commas (,).
:::

## Common Query Patterns

### Filter → Group → Aggregate

A common pattern for analyzing data:

```wvlet
from sales
where date >= '2024-01-01':date
group by product_category
agg 
  _.count as num_sales,
  revenue.sum as total_revenue
order by total_revenue desc
limit 10
```

### Join → Filter → Select

Combining data from multiple tables:

```wvlet
from customers
join {
  from orders
  where status = 'completed'
}
on customers.id = orders.customer_id
select 
  customers.name,
  orders.total,
  orders.created_at
order by orders.created_at desc
```

### Pivot → Aggregate

Creating cross-tab reports:

```wvlet
from sales_data
pivot on month
group by product
agg revenue.sum
```

### Multiple CTEs → Complex Analysis

Using CTEs for complex queries:

```wvlet
with active_users as {
  from users
  where last_login >= current_date - '30 days':interval
}
with user_orders as {
  from orders
  where created_at >= current_date - '30 days':interval
}
from active_users
left join user_orders
  on active_users.id = user_orders.user_id
group by active_users.segment
agg 
  _.count as user_count,
  user_orders.total.sum as revenue
```

This reference guide provides a comprehensive overview of Wvlet's query syntax. For hands-on examples and tutorials, check out the [Quick Start](./quick-start.md) guide.
