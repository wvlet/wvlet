import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Query Syntax

## References

- [Quick Start](./quick-start.md)
- [Expressions](./expressions.md)
- [Metadata Functions](./metadata-functions.md)
- [Data Models](./data-models.md)

## Introduction

Wvlet is a query language designed to be more human-readable and easier to write than SQL. If you already familiar to SQL, you will find it's easy to learn the syntax of wvlet as there are a lot of similarities between wvlet and SQL. If you know about [DataFrame in Python](https://pandas.pydata.org/docs/user_guide), it will help you understand the wvlet query language as chaining relational operators in the flow-style is quite similar to using DataFrame API.

Wvlet queries start with
`from` keyword, and you can chain multiple [relational operators](#relational-operators) to process the input data and generate the output data. The following is a typical flow of chaining operators in a wvlet query:

```sql
from ...         -- Scan the input data
where ...        -- Apply filtering conditions
where ...        -- [optional] Apply more filtering conditions
add   ... as ... -- Add new columns
transform ...    -- Transform a subset of columns
group by ...     -- Grouping rows by the given columns
agg ...          -- Add group aggregation expressions, e.g., _.count, _.sum 
where ...        -- Apply filtering conditions for groups (e.g., HAVING clause in SQL)
exclude ...      -- Remove columns from the output
shift ...        -- Shift the column position to the left
select ...       -- Select columns to output
order by ...     -- Sort the rows by the given columns
limit ...        -- Limit the number of rows to output
```

Unlike SQL, whose queries always must follow the `SELECT ... FROM ... WHERE ... GROUP BY ... ORDER BY ... LIMIT ...` structure, Wvlet uses the __flow-style syntax__ to match the syntax order with the data processing order as much as possible to facilitate more intuitive query writing. A query should have a `from` statement to read the data, but `select` is not mandatory in Wvlet.

Some operators like `add`, `transform`, `agg`, `exclude`, `shift`, etc. are not available in the standard SQL, but these new operators will reduce your query text size and make the query more readable and easier to compose. Eventually, these operators will be translated to the equivalent SQL syntax.

## Relational Operators

In wvlet, you need to use __lower-case__ keywords for SQL-like operators. The following is a list of relational operators in wvlet for manipulating table-format data (i.e.,
_relation_):

### Catalog Operators

Wvlet provides operators to list the available catalogs, schemas (databases), tables, and models in the current environment. The hierarchy of the catalog, schema, and table is as follows:

- catalog: A collection of schemas (databases)
- schema (e.g., database, datasets): A collection of tables
- table: A collection of rows (records)

| Operator                                                       | Output                                                       |
|----------------------------------------------------------------|--------------------------------------------------------------|
| [__show__ tables (__in__ `catalog`?(.`schema`)?](#show-tables) | Return a list of all tables in the current schema (database) |
| [__show__ schemas (__in__ `catalog`)?](#show-schemas)          | Return a list of all schemas (databases) in the catalog      |
| [__show__ catalogs](#show-catalogs)                            | Return a list of all catalogs                                |
| __show__ models                                                | Return a list of all models                                  |
| __describe__ (relation)                                        | Return the schema of the specified table or query            |

### Query Operators

Wvlet provides various relational operators to process the input data and generate the output data in the table format.

| Operator                                                                                    | Output                                                                                                                                                                                                                                                  |
|---------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| [__from__ `expr`](#from)                                                                    | Rows from the given source table, model, value, or file.                                                                                                                                                                                                |
| [__where__ `cond`](#where)                                                                  | Rows that satisfy the given condition. `where` can be used multiple times in the same query like `from ... where ... where ...`                                                                                                                         |
| [__add__ `expr` (__as__ `alias`)?, ...](#add)                                               | Same rows with new columns.                                                                                                                                                                                                                             |
| [__select__ `expr`, ...](#select)                                                           | Rows with the given expressions. `select *` is allowed here.                                                                                                                                                                                            |
| __select distinct__ `expr`,...                                                              | Rows with distinct values of the given expressions.                                                                                                                                                                                                     |
| __select__ `alias` = `expr`, ...                                                            | Rows with the given expressions with aliases.                                                                                                                                                                                                           |
| __select__ `expr` as `alias`, ...                                                           | Rows with the given expressions with aliases.                                                                                                                                                                                                           |
| __select as__ `alias`                                                                       | Add an alias to the query result to reference it from another query.                                                                                                                                                                                    |
| __rename__ `column` __as__ `alias`, ...                                                     | Same rows with the given columns renamed                                                                                                                                                                                                                | 
| __exclude__ `column`, ...                                                                   | Same rows except the given columns.                                                                                                                                                                                                                     |
| __transform__ `column` = `expr`, ...                                                        | Same rows with added or updated columns.                                                                                                                                                                                                                |  
| __group by__ `column`, ...                                                                  | Grouped rows by the given columns. Grouping keys can be referenced as `select _1, _2, ...`  in the subsequent operator. `group by` returns `_1, _2, ..., arbitrary(col1), arbitrary(col2), ...` if there is no subsequent agg or select operator.       |
| [__agg__ `agg_expr`, ...](#agg)                                                             | Rows with the grouping keys in `group by` clause and aggregated values.  This is is a shorthand notation for `select _1, _2, ..., (agg_expr), ...`. In aggr_expr, dot-notation like `_.count`, `(column).sum` can be used for aggregating grouped rows. |
| __order by__ `expr` (asc \| desc)?, ...                                                     | Rows sorted by the given expression. 1-origin column indexes can be used like `1`, `2`,                                                                                                                                                                 |
| __limit__ `n`                                                                               | Rows up to the given number                                                                                                                                                                                                                             |
| [asof](asof-join.md)? (left \| right \| cross)? __join__ `table` __on__ `cond`              | Joined rows with the given condition. `cond` can be just common column names between joined tables (e.g., `using` in SQL)                                                                                                                               |
| __concat__ ( `query` )                                                                      | Concatenated rows from multiple sources. Same with UNION ALL in SQL                                                                                                                                                                                     |
| __dedup__                                                                                   | Rows with duplicated rows removed. Equivalent to `select distinct *`                                                                                                                                                                                    | 
| (__intersect__ \| __except__) all? ...                                                      | Rows with the intersection (difference) of the given sources. By default it uses set semantics. If all is given, bag semantics will be used.                                                                                                            |
| __pivot on__ `pivot_column` (__in__ (`v1`, `v2`, ...) )?                                    | Rows whose column values in the pivot column are expanded as columns.                                                                                                                                                                                   |
| __pivot on__ `pivot_column`<br/> (__group by__ `grouping columns`)?<br/> __agg__ `agg_expr` | Pivoted rows with the given grouping columns and aggregated values.                                                                                                                                                                                     |
| __call__ `func(args, ...)`                                                                  | Rows processed by the given table function                                                                                                                                                                                                              |
| __shift__ (to left)? `column`, ...                                                          | Same rows with selected column moved to the left                                                                                                                                                                                                        |
| __shift to right__ `column`, ...                                                            | Same rows with selected column moved to the right                                                                                                                                                                                                       |
| __sample__ `method`? (`size` rows? \| %)                                                    | Randomly sampled rows. Sampling method can be reservoir (default), system, or bernoulli                                                                                                                                                                 | 
| [__unnest__(`array expr`)](unnest.md)                                                       | Expand an array into rows.                                                                                                                                                                                                                              | 
| [__test__ `(test_expr)`](test-syntax.md)                                                    | Test the query result, which will be evaluated only in the test-run mode.                                                                                                                                                                               |
| __debug__ \{ `(query)` \}                                                                   | Pass-through the input results (no-op), but run a debug query against the input rows.                                                                                                                                                                   |

### Update Operators

Wvlet provides update operators to save the query result as a new table or file, append the query result to the target table, or delete input rows from the source table.

| Operator                                            | Description                                                                                           |
|-----------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| __save to__ `table_name`                            | Save the query result as a new table with the given name                                              |
| __save to__ `table_name` __with__ p1:v1, p2:v2, ... | Save the query result as a new table with the given name and options                                  |
| __save to__ `'file name'`                           | Save the query result as a file with the given name                                                   |
| __append to__ `table_name`                          | Append the query result to the target table. If the target table doesn't exist, it creates a new one. |
| __append to__ `'file_name'`                         | Append the query result to the target file. It will create a new file if the file doesn't exist.      |
| __delete__                                          | Delete input rows from the source table.                                                              |

### Relation Inspection Operators

| Operator     | Output                                                             |
|--------------|--------------------------------------------------------------------|
| __describe__ | Return the schema of the input relation (column_name, column_type) |


### Commands

| Operator             | Description                                       |
|----------------------|---------------------------------------------------|
| __execute__ sql"..." | Execute a raw SQL statement in the context engine |



## Examples

### show tables

To find a target table to query in the current database (schema), you can use the `show tables` query:

```sql
show tables
```

The output of `show tables` can be followed by the `where` clause to filter the table names:

<Tabs>
<TabItem value="Query" default>
```sql
show tables
where name like 'n%'
```
</TabItem>
<TabItem value="Result">
```sql
┌────────┐
│  name  │
│ string │
├────────┤
│ nation │
├────────┤
│ 1 rows │
└────────┘
```
</TabItem>
</Tabs>

### show schemas

To find a target database (schema) to query in the current catalog, you can use the `show schemas` query:

<Tabs>
<TabItem value="Query" default>
```sql
show schemas
```
</TabItem>
<TabItem value="Result">
```sql
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
</TabItem>
</Tabs>

These schema names can be used to specify the target schema in the `show tables` command:

<Tabs>
<TabItem value="Query" default>
```sql
show tables in main 
where name like 'p%'
```
</TabItem>  
<TabItem value="Result">
```sql
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
</TabItem>  
</Tabs>

### show catalogs

List all available catalogs:

<Tabs>
<TabItem value="Query" default>
```sql
show catalogs
```
</TabItem>
<TabItem value="Result">
```sql
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
</TabItem>
</Tabs>

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

<Tabs>
<TabItem value="Query" default>
```sql
from nation
select n_name
add n_name.substring(1, 3) as prefix
limit 5
```
</TabItem>
<TabItem value="Result">
```sql
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
</TabItem>
</Tabs>

### select

Output only the selected columns from the input:

```sql
from nation
select n_name, n_regionkey
```
As in SQL, arbitrary expressions can be used in select statements. Note that, however, in Wvlet, you usually don't need to use select statement 
as the other column-at-a-time operators, like `add`, `transform`, `exclude`, `shift`, etc. can be used for manipulating column values. 

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
For example, `_.sum`, `_.avg`, `_.max`, `_.min`, `_.max_by`, `_.min_by`, etc. can be used in agg statement.

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

