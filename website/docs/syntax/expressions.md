# Expressions

One of the major difference from tradtional SQL is that wvlet uses single or double quoted strings for representing string values, and back-quoted strings for referencing column or table names, which might contain space or special characters.

| Operator                                              | Description | 
|-------------------------------------------------------| --- |
| '(single quote)'                                      | String literal for representing string values, file names, etc. |
| "(double quote)"                                      | Same as single quote strings | 
| """(triple quote string)"""                           | Multi-line strings |
| \`(back quote)\`                                      | Column or table name, which requires quotations |
| sql"`sql expr`"                                       | SQL expression used for inline expansion |
| sql" ... $\{`expr`\} ..."                             | Interpolated SQL expression with embedded expressions |
| [`expr`, ...]                                         | Array value |
| [[`expr`, ...], ...]                                  | Array of arrays for representing table records |
| `_`                                                   | underscore refers to the previous input | 
| `agg_func(expr)` over (partition by ... order by ...) | Window functions for computing aggregate values computed from the entire query result. This follows the same window function syntax with SQL |
| `_1`, `_2`, ...                                       | Refers to grouping keys in the preceding `group by` clause |


## Conditional Expressions

| Operator | Description                                                                       |
| --- |-----------------------------------------------------------------------------------|
| `expr` and `expr` | Logical AND                                                                       |
| `expr` or  `expr` | Logical OR                                                                        |
| not `expr` | Logical NOT                                                                       |
| !`expr` | Logical NOT                                                                       |
| `expr` is `expr` | equality check                                                                    |
| `expr` = `expr` | equality check                                                                    |
| `expr` is not `expr` | inequality check                                                                  |
| `expr` != `expr` | inequality check                                                                  |
| `expr` is null | True if the expression is null                                                    |
| `expr` = null | True if the expression is null                                                    |
| `expr` is not null | True if the expression is not null                                                |
| `expr` != null | True if the expression is not null.                                               |
| `expr` in (`v1`, `v2`, ...) | True if the expression value is in the given list                                 |
| `expr` in (from ...) | True if the expression value is in the given list provided by a sub query         |
| `expr` not in (`v1`, `v2`, ...) | True if the expression is not in the given list                                   |
| `expr` between `v1` and `v2` | True if the expression value is between v1 and v2, i.e., v1 &le; (value) &le; v2 |
| `expr` like `pattern` | True if the expression matches the given pattern, e.g., , `'abc%'`                |

