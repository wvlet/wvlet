import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Query Syntax

Wvlet is a query language designed to be more human-readable and easier to write than SQL. If you are familiar to SQL, you will find it easy to learn Wvlet syntax due to the many similarities. If you know about [DataFrame in Python](https://pandas.pydata.org/docs/user_guide), it will help you understand the Wvlet query language, as chaining relational operators in flow-style is similar to using the DataFrame API.

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

[//]: # (Some operators like `add`, `agg`, `exclude`, `shift`, etc. are not available in the standard SQL, but these new operators will reduce your query text size and make the query more readable and easier to compose. Eventually, these operators will be translated to the equivalent SQL syntax.)

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
| [__count__](#count)                                                                            | Count the number of input rows                                                                                                                |
| [__add__ `expr` (__as__ `alias`)?, ...](#add)                                                  | Same rows with new columns added to the right                                                                                                 |
| [__prepend__ `expr` (__as__ `alias`)?, ...](#prepend)                                          | Same rows with new columns prepended to the left                                                                                              |
| [__select__ `expr`, ...](#select)                                                              | Rows with the given expressions. `select *` is allowed                                                                                        |
| __select distinct__ `expr`,...                                                                 | Rows with distinct values of the given expressions                                                                                            |
| __select__ `alias` __=__ `expr`, ...                                                           | Rows with the given expressions with aliases                                                                                                  |
| __select__ `expr` __as__ `alias`, ...                                                          | Rows with the given expressions with aliases                                                                                                  |
| __select as__ `alias`                                                                          | Add an alias to the query to reference it from another query                                                                                  |
| [__rename__ `column` __as__ `alias`, ...](#rename)                                             | Same rows with the given columns renamed                                                                                                      |
| [__exclude__ `column`, ...](#exclude)                                                          | Same rows except the given columns                                                                                                            |
| [__shift__ (to left)? `column`, ...](#shift)                                                   | Same rows with selected column moved to the left                                                                                              |
| [__shift to right__ `column`, ...](#shift)                                                     | Same rows with selected column moved to the right                                                                                             |
| __group by__ `column`, ...                                                                     | Grouped rows by the given columns. Grouping keys can be referenced as `select _1, _2, ...`  in the subsequent operator                        |
| [__agg__ `agg_expr`, ...](#agg)                                                                | Rows with the grouping keys in `group by` clause and aggregated values                                                                        |
| [__order by__ `expr` (__asc__ \| __desc__)?, ...](#order-by)                                   | Rows sorted by the given expression. 1-origin column indexes can be used like `1`, `2`, ...                                                   |
| [__limit__ `n`](#limit)                                                                        | Rows up to the given number                                                                                                                   |
| [__asof__](asof-join.md)? (__left__ \| __right__ \| __cross__)? __join__ `table` __on__ `cond` | Joined rows with the given condition                                                                                                          |
| [__concat__ \{ `query` \}](#concat)                                                            | Concatenated rows with the given subquery result. Same as `UNION ALL` in SQL                                                                  |
| [__dedup__](#dedup)                                                                            | Rows with duplicated rows removed. Equivalent to `select distinct *`                                                                          |
| ([__intersect__](#intersect) \| [__except__](#except)) __all__? ...                            | Rows with the intersection (difference) of the given sources. By default set semantics is used. If `all` is given, bag semantics will be used |
| [__pivot on__ `pivot_column` (__in__ (`v1`, `v2`, ...) )?](#pivot)                             | Rows whose column values in the pivot column are expanded as columns                                                                          |
| [__unpivot__ `new_column` __for__ `unpivot_column` __in__ (`v1`, `v2`, ...) ](#unpivot)        | Rows whose columns are transformed into single column values                                                                                  |
| __\|__ `func(args, ...)`                                                                       | Rows processed by the given table function (pipe operator)                                                                                    |
| [__with__ `alias` __as__ \{ `(query)` \}](#with)                                               | Define a local query alias, which is the same with a common-table expression (CTE) in SQL                                                     |
| __sample__ `method`? (`(rows)` \| `(percentage)%`)                                             | Randomly sampled rows. Sampling method can be reservoir (default), system, or bernoulli                                                       |
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

### Relation Inspection Commands

| Operator                  | Description                                    |
|---------------------------|------------------------------------------------|
| __describe__ `query`      | Display the schema of the input query result   |
| __explain__ `query`       | Display the LogicalPlan of the query           |
| __explain__ sql"""...""" | Display a LogicalPlan of the raw SQL statement |

### Raw SQL Commands

| Operator              | Description                                                |
|-----------------------|------------------------------------------------------------|
| __execute__ sql"..."  | Execute a raw SQL statement in the context engine          |
| __from__ sql"..."     | Execute a raw SQL statement and read the result as a table |


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

```sql
-- Define a new variable
val name = 'wvlet'

-- Variable is evaluated once before running the query
select s"Hello ${x}!" as msg
-- Returns [['Hello wvlet!']]
```

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

### count 

Wvlet has a shorthand notation for counting the number of input rows:

```sql
from nation
count
```

This is equilvalen to:
```sql
from nation
select _.count
```

Or more SQL-like expression can be used too:
```sql
from nation
select count(*)
```

For clarity, you can use pipe `|` before the count operator:
```sql
from nation
select n_name, n_regionkey,
-- pipe (|) is required after trailing comma
| count
```

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

### prepend

Prepend a new column to the left side of the input:

```sql
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

```sql
from nation
select n_name, n_regionkey
```

As in SQL, arbitrary expressions can be used in select statements. Note that, however, in Wvlet, you usually don't need to use select statement
as the other column-at-a-time operators, like `add`, `exclude`, `shift`, etc. can be used for manipulating column values.

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

The `group by` operator groups rows by one or more columns, creating groups of rows that share the same values in the specified columns.

#### Basic Grouping

```sql
from [[1, "A", 100], [2, "B", 200], [1, "A", 150], [2, "B", 50], [3, "C", 300]]
  as data(id, category, value)
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

After grouping, you can reference grouping keys using `_1`, `_2`, etc. in the order they appear in the `group by` clause:

```sql
from [[1, "A", 100], [2, "B", 200], [1, "A", 150], [2, "B", 50]]
  as data(id, category, value)
group by id, category
select _1 as id, _2 as category;

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

### agg

The `agg` operator adds aggregation expressions to grouped rows. It must follow a `group by` clause.

#### Basic Aggregations

```sql
from [[1, "A", 100], [2, "B", 200], [1, "A", 150], [2, "B", 50], [3, "C", 300]]
  as data(id, category, value)
group by category
agg 
  _.count as item_count,
  value.sum as total_value,
  value.avg as avg_value;

┌──────────┬────────────┬─────────────┬──────────────┐
│ category │ item_count │ total_value │  avg_value   │
│  string  │    long    │     int     │ decimal(17,4) │
├──────────┼────────────┼─────────────┼──────────────┤
│ A        │          2 │         250 │     125.0000 │
│ B        │          2 │         250 │     125.0000 │
│ C        │          1 │         300 │     300.0000 │
├──────────┴────────────┴─────────────┴──────────────┤
│ 3 rows                                             │
└────────────────────────────────────────────────────┘
```

#### Available Aggregation Functions

The underscore `_` represents the group of rows and supports various aggregation functions:

```sql
from [[1, 10, "A"], [2, 20, "B"], [1, 30, "C"], [2, 40, "D"], [1, 50, "E"]]
  as sales(store_id, amount, product)
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

┌──────────┬───────────────────┬─────────────┬──────────────┬──────────┬──────────┬─────────────────┬──────────────┬───────────────┐
│ store_id │ transaction_count │ total_sales │   avg_sale   │ min_sale │ max_sale │ unique_products │ best_product │ worst_product │
│   int    │       long        │     int     │ decimal(17,4) │   int    │   int    │      long       │    string    │    string     │
├──────────┼───────────────────┼─────────────┼──────────────┼──────────┼──────────┼─────────────────┼──────────────┼───────────────┤
│        1 │                 3 │          90 │      30.0000 │       10 │       50 │               3 │ E            │ A             │
│        2 │                 2 │          60 │      30.0000 │       20 │       40 │               2 │ D            │ B             │
├──────────┴───────────────────┴─────────────┴──────────────┴──────────┴──────────┴─────────────────┴──────────────┴───────────────┤
│ 2 rows                                                                                                                             │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

#### Filtering Groups (HAVING clause)

You can filter groups after aggregation using a `where` clause after `agg`:

```sql
from [[1, "Electronics", 1000], [2, "Books", 50], [3, "Electronics", 2000], 
      [4, "Books", 75], [5, "Toys", 500], [6, "Electronics", 1500]]
  as orders(id, category, amount)
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
│ 1 rows                                    │
└───────────────────────────────────────────┘
```

#### Multiple Grouping with Complex Aggregations

```sql
from [["2024-01-01", "A", "Electronics", 100],
      ["2024-01-01", "B", "Books", 50],
      ["2024-01-02", "A", "Electronics", 200],
      ["2024-01-02", "A", "Books", 75],
      ["2024-01-03", "B", "Electronics", 150]]
  as sales(date, store, category, amount)
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
│ 4 rows                                                                                    │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

#### Using Aggregations Without group by

You can use aggregations directly without `group by` to aggregate all rows:

```sql
from [[1, 100], [2, 200], [3, 300], [4, 400], [5, 500]]
  as data(id, value)
agg 
  _.count as total_rows,
  value.sum as sum_all,
  value.avg as average;

┌────────────┬─────────┬──────────────┐
│ total_rows │ sum_all │   average    │
│    long    │   int   │ decimal(17,4) │
├────────────┼─────────┼──────────────┤
│          5 │    1500 │     300.0000 │
├────────────┴─────────┴──────────────┤
│ 1 rows                               │
└──────────────────────────────────────┘
```

#### Practical Example: Sales Analysis

Here's a practical example analyzing sales data:

```sql
from [["2024-01", "North", "Laptop", 5, 1200],
      ["2024-01", "North", "Phone", 10, 800],
      ["2024-01", "South", "Laptop", 3, 1200],
      ["2024-01", "South", "Tablet", 7, 600],
      ["2024-02", "North", "Laptop", 8, 1200],
      ["2024-02", "North", "Tablet", 5, 600],
      ["2024-02", "South", "Phone", 12, 800],
      ["2024-02", "South", "Laptop", 4, 1200]]
  as sales(month, region, product, quantity, unit_price)
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
│ 2024-02 │ North  │          2 │               13 │         12600 │               2 │
│ 2024-02 │ South  │          2 │               16 │         14400 │               2 │
├─────────┴────────┴────────────┴──────────────────┴───────────────┴─────────────────┤
│ 3 rows                                                                              │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

:::tip
The `_` underscore in aggregations represents the group of rows. You can use it with various aggregation functions like `_.count`, `_.sum`, `_.avg`, `_.max`, `_.min`, `_.count_distinct()`, `_.array_agg()`, `_.max_by()`, `_.min_by()`, etc.
:::

:::note
The `where` clause after `agg` is equivalent to SQL's `HAVING` clause - it filters groups based on aggregated values, not individual rows.
:::

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
You can add `nulls first` or `nulls last` to specify the order of null values.

### join

The `join` operator combines rows from two or more tables based on a related column between them. Wvlet supports various types of joins similar to SQL, but with a more intuitive flow-style syntax.

#### Basic Join (Inner Join)

The basic `join` returns rows when there is a match in both tables:

```sql
from
  [[1, "apple", 50], [2, "banana", 10], [3, "cherry", 70]]
  as fruit(id, name, price)
join {
  from [["o1", 1, 10], ["o2", 2, 5]]
  as fruit_order(order_id, fruit_id, qty)
}
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

```sql
from [[1, "Alice"], [2, "Bob"], [3, "Charlie"]] as users(id, name)
left join {
  from [[1, "order1"], [1, "order2"], [3, "order3"]]
  as orders(user_id, order_id)
}
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

```sql
from [[1, "order1"], [1, "order2"], [3, "order3"], [4, "order4"]]
  as orders(user_id, order_id)
right join {
  from [[1, "Alice"], [2, "Bob"], [3, "Charlie"]]
  as users(id, name)
}
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

```sql
from [[1, "A"], [2, "B"]] as t1(id, val)
cross join {
  from [["X"], ["Y"], ["Z"]] as t2(letter)
}
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
│ 6 rows         │
└────────────────┘
```

#### Join with Multiple Conditions

You can specify multiple conditions in the `on` clause using `and`/`or` operators:

```sql
from [[1, "A", 100], [2, "B", 200], [3, "C", 300]] as t1(id, code, value)
join {
  from [[1, "A", 10], [2, "B", 20], [3, "D", 30]] as t2(id, code, qty)
}
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

```sql
from [[1, "Electronics", 1000], [2, "Books", 500], [3, "Clothing", 750]]
  as categories(id, name, budget)
left join {
  from [[1, "Laptop", 800], [1, "Phone", 600], [2, "Novel", 20], [4, "Unknown", 100]]
  as products(category_id, name, price)
}
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
│ 2 rows                                     │
└────────────────────────────────────────────┘
```

#### Multiple Joins

You can chain multiple join operations to combine data from several tables:

```sql
from [[1, "Alice"], [2, "Bob"], [3, "Charlie"]] as users(id, name)
join {
  from [[1, 101], [2, 102], [3, 103]] as accounts(user_id, account_id)
}
on users.id = accounts.user_id
join {
  from [[101, 1000], [102, 2000], [103, 1500]] as balances(account_id, amount)
}
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

```sql
with employees as {
  from [[1, "Alice", 2], [2, "Bob", 3], [3, "Charlie", null]]
    as data(id, name, manager_id)
}
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

```sql
from [[1, "A"], [2, "B"]] as t1(id, val)
concat {
  from [[3, "C"], [4, "D"]] as t2(id, val)
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

```sql
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

```sql
from [[1, "A"], [2, "B"], [1, "A"], [3, "C"], [2, "B"]]
  as data(id, val)
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

```sql
from [[1, "A"], [2, "B"], [3, "C"], [4, "D"]] as t1(id, val)
intersect {
  from [[2, "B"], [3, "C"], [5, "E"]] as t2(id, val)
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

```sql
from [[1, "A"], [2, "B"], [2, "B"], [3, "C"]] as t1(id, val)
intersect all {
  from [[2, "B"], [2, "B"], [3, "C"], [3, "C"]] as t2(id, val)
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

```sql
from [[1, "A"], [2, "B"], [3, "C"], [4, "D"]] as t1(id, val)
except {
  from [[2, "B"], [4, "D"]] as t2(id, val)
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

```sql
from [[1, "A"], [2, "B"], [2, "B"], [3, "C"]] as t1(id, val)
except all {
  from [[2, "B"]] as t2(id, val)
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

### unpivot

The `unpivot` operator transforms multiple columns into rows. This is useful when you need to transform wide table into a long table. Currently, unpivot is available only in DuckDB backend.

Example:
```sql
from [
 [1, 'electronics', 1, 2, 3, 4, 5, 6],
 [2, 'clothes', 10, 20, 30, 40, 50, 60],
 [3, 'cars', 100, 200, 300, 400, 500, 600]
] as sales(id, dept, jan, feb, mar, apr, may, jun)
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
```sql
from [[1], [2], [3], [4], [5], [6], [7], [8], [9], [10]] as numbers(n)
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
```sql
from [[1], [2], [3], [4], [5], [6], [7], [8], [9], [10]] as numbers(n)
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

```sql
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

```sql
with t1 as {
  from nation
  where n_regionkey = 1
}
from t1
```

You can define multiple subqueries: 
```sql
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

