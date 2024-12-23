# Relational Operators

In wvlet, you need to use __lower-case__ keywords for SQL-like operators. The following is a list of relational operators in wvlet for manipulating table-format data (i.e.,
_relation_):

| Operator                                                                                    | Output                                                                                                                                                                                                                                                  |
|---------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| __from__ `expr`                                                                             | Rows from the given source table, model, value, or file.                                                                                                                                                                                                |
| __select__ `expr`, ...                                                                      | Rows with the given expressions. `select *` is allowed here.                                                                                                                                                                                            |
| __select distinct__ `expr`,...                                                              | Rows with distinct values of the given expressions.                                                                                                                                                                                                     |
| __select__ `alias` = `expr`, ...                                                            | Rows with the given expressions with aliases.                                                                                                                                                                                                           |
| __select__ `expr` as `alias`, ...                                                           | Rows with the given expressions with aliases.                                                                                                                                                                                                           |
| __select as__ `alias`                                                                       | Add an alias to the query result to reference it from another query.                                                                                                                                                                                    |
| __add__ `expr` (__as__ `alias`)?, ...                                                       | Same rows with new columns.                                                                                                                                                                                                                             |
| __rename__ `column` __as__ `alias`, ...                                                     | Same rows with the given columns renamed                                                                                                                                                                                                                | 
| __exclude__ `column`, ...                                                                   | Same rows except the given columns.                                                                                                                                                                                                                     |
| __where__ `cond`                                                                            | Rows that satisfy the given condition. `where` can be used multiple times in the same query like `from ... where ... where ...`                                                                                                                         |
| __transform__ `column` = `expr`, ...                                                        | Same rows with added or updated columns.                                                                                                                                                                                                                |  
| __group by__ `column`, ...                                                                  | Grouped rows by the given columns. Grouping keys can be referenced as `select _1, _2, ...`  in the subsequent operator. `group by` returns `_1, _2, ..., arbitrary(col1), arbitrary(col2), ...` if there is no subsequent agg or select operator.       |
| __agg__ `agg_expr`, ...                                                                     | Rows with the grouping keys in `group by` clause and aggregated values.  This is is a shorthand notation for `select _1, _2, ..., (agg_expr), ...`. In aggr_expr, dot-notation like `_.count`, `(column).sum` can be used for aggregating grouped rows. |
| __order by__ `expr` (asc \| desc)?, ...                                                     | Rows sorted by the given expression. 1-origin column indexes can be used like `1`, `2`,                                                                                                                                                                 |
| __limit__ `n`                                                                               | Rows up to the given number                                                                                                                                                                                                                             |
| [asof](asof-join.md)? (left \| right \| cross)? __join__ `table` __on__ `cond`                                    | Joined rows with the given condition. `cond` can be just common column names between joined tables (e.g., `using` in SQL)                                                                                                                               |
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
| __debug__ \{ `(query)` \}                                                                     | Pass-through the input results (no-op), but run a debug query against the input rows.                                                                                                                                                  |

## Update Statements

| Operator                                            | Description                                                                                           |
|-----------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| __save to__ `table_name`                            | Save the query result as a new table with the given name                                              |
| __save to__ `table_name` __with__ p1:v1, p2:v2, ... | Save the query result as a new table with the given name and options                                  |
| __save to__ `'file name'`                           | Save the query result as a file with the given name                                                   |
| __append to__ `table_name`                          | Append the query result to the target table. If the target table doesn't exist, it creates a new one. |
| __append to__ `'file_name'`                         | Append the query result to the target file. It will create a new file if the file doesn't exist.      |
| __delete__                                          | Delete input rows from the source table.                                                              |

## Catalog Inspection Operators

| Operator     | Output                                                             |
|--------------|--------------------------------------------------------------------|
| __show__ tables (in `catalog`?.`schema`)? | Return a list of all tables in the current schema (database)   |
| __show__ schemas (in `catalog`)?| Return a list of all schemas (databases) in the catalog        |
| __show__ catalogs | Return a list of all catalogs                                 |
| __show__ models  | Return a list of all models |
| __describe__ (relation) | Return the schema of the specified table or query |


## Relation Inspection Operators

| Operator     | Output                                                             |
|--------------|--------------------------------------------------------------------|
| __describe__ | Return the schema of the input relation (column_name, column_type) |


## Commands

| Operator             | Description                                       |
|----------------------|---------------------------------------------------|
| __execute__ sql"..." | Execute a raw SQL statement in the context engine |

