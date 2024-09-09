---
sidebar_label: 'Syntax'
sidebar_position: 2
---

# Query Syntax

## Relational Operators

wvlet favors lower-case keywords for SQL-like operators. The following is a list of relational operators in wvlet for manipulating table-format data (i.e., relation):

| Operator | Output |
| --- | --- | 
| from `expr` | Rows from the given source table, model, value, or file.  |
| select `expr`, ... | Rows with the given expressions. `select *` is allowed here. |
| select distinct `expr`,... | Rows with distinct values of the given expressions. |
| select `alias` = `expr`, ... | Rows with the given expressions with aliases. |
| select `expr` as `alias`, ... | Rows with the given expressions with aliases. |
| add `expr` (as `alias`)?, ... | Same rows with new columns. |
| exclude `column`, ... | Same rows except the given columns. |
| where `cond` | Rows that satisfy the given condition. `where` can be used multiple times in the same query like `from ... where ... where ...` |
| transform `column` = `expr`, ... | Same rows with added or updated columns. |  
| group by `column`, ... | Grouped rows by the given columns. Grouping keys can be referenced as `select _1, _2, ...`  in the subsequent operator. `group by` returns `_1, _2, ..., arbitrary(col1), arbitrary(col2), ...` if there is no subsequent agg or select operator. |
| agg `agg_expr`, ... | Rows with the grouping keys in `group by` clause and aggregated values.  This is is a shorthand notation for `select _1, _2, ..., (agg_expr), ...`. In aggr_expr, dot-notation like `_.count`, `(column).sum` can be used for aggregating grouped rows.|
| order by `expr` (asc \| desc)?, ... | Rows sorted by the given expression. 1-origin column indexes can be used like `1`, `2`, |
| limit `n` | Rows up to the given number |
| (left \| right \| cross)? join `table` on `cond` | Joined rows with the given condition. `cond` can be just common column names between joined tables (e.g., `using` in SQL) |
| concat ... | Concatenated rows from multiple sources. Same with UNION ALL in SQL |
| dedup | Rows with duplicated rows removed. Equivalent to `select distinct *` | 
| (intersect \| except) all? ... | Rows with the intersection (difference) of the given sources. By default it uses set semantics. If all is given, bag semantics will be used. |
| pivot on `pivot_column` (in (`v1`, `v2`, ...) )? | Rows whose column values in the pivot column are expanded as columns. |
| pivot on `pivot_column`<br/> (group by `grouping columns`)?<br/> agg `agg_expr` |  Pivoted rows with the given grouping columns and aggregated values.|
| pipe `func(args, ...)` | Rows processed by the given table function |
| shift (to left)? `column`, ... | Same rows with selected column moved to the left |
| shift to right `column`, ... | Same rows with selected column moved to the right |
| sample `method`? (`size` rows? \| %) | Randomly sampled rows. Sampling method can be reservoir (default), system, or bernoulli |   

## Relation Inspection Operators

| Operator | Output                                                             |
| --- |--------------------------------------------------------------------|
| describe | Return the schema of the input relation (column_name, column_type) |
