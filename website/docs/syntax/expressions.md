# Expressions

One of the major difference from traditional SQL is that wvlet uses single or double quoted strings for representing string values, and back-quoted strings for referencing column or table names, which might contain space or special characters.

| Operator                                              | Description                                                                                                                                  |
|:------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|
| '(single quote)'                                      | String literal for representing string values, file names, etc.                                                                              |
| "(double quote)"                                      | Same as single quote strings                                                                                                                 |
| """(triple quote string)"""                           | Multi-line strings                                                                                                                           |
| \`(back quote)\`                                      | Column or table name, which requires quotations                                                                                              |
| sql"`sql expr`"                                       | SQL expression used for inline expansion                                                                                                     |
| sql" ... $\{`expr`\} ..."                             | Interpolated SQL expression with embedded expressions                                                                                        |
| s"... $\{expr\} ..."                                  | Interpolated strings with expressions                                                                                                        |
| s\`... $\{expr\} ...\`                                | Interpolated backquote strings with expressions                                                                                              |
| [`expr`, ...]                                         | Array value                                                                                                                                  |
| [[`expr`, ...], ...]                                  | Array of arrays for representing table records                                                                                               |
| \{'`key`'\: `value`, ...\}                                 | Struct (row) value                                                                                                                         |
| `_`                                                   | underscore refers to the previous input                                                                                                      |
| `agg_func(expr)` over (partition by ... order by ...) | [Window functions](window.md) for computing aggregate values computed from the entire query result. This follows similar window function syntax with SQL |
| `_1`, `_2`, ...                                       | Refers to 1-origin grouping keys in the preceding `group by` clause                                                                          |
| `1`, `2`, ...                                         | Refers to 1-origin column index for `order by` clause                                                                                        |

## Variable Definition

You can define a variable using `val` keyword:

```sql
-- Define a new variable
val name = 'wvlet'

-- Variable is evaluated once before running the query
select s"Hello ${x}!" as msg
-- Returns [['Hello wvlet!']]
```

## Conditional Expressions

| Operator                        | Description                                                                        |
|---------------------------------|------------------------------------------------------------------------------------|
| `expr` and `expr`               | Logical AND                                                                        |
| `expr` or  `expr`               | Logical OR                                                                         |
| not `expr`                      | Logical NOT                                                                        |
| !`expr`                         | Logical NOT                                                                        |
| `expr` is `expr`                | equality check                                                                     |
| `expr` = `expr`                 | equality check                                                                     |
| `expr` is not `expr`            | inequality check                                                                   |
| `expr` != `expr`                | inequality check                                                                   |
| `expr` is null                  | True if the expression is null                                                     |
| `expr` = null                   | True if the expression is null                                                     |
| `expr` is not null              | True if the expression is not null                                                 |
| `expr` != null                  | True if the expression is not null.                                                |
| `expr` in (`v1`, `v2`, ...)     | True if the expression value is in the given list                                  |
| `expr` in (from ...)            | True if the expression value is in the given list provided by a sub query          |
| `expr` not in (`v1`, `v2`, ...) | True if the expression is not in the given list                                    |
| `expr` between `v1` and `v2`    | True if the expression value is between v1 and v2, i.e., v1 &le; (value) &le; v2   |
| `expr` like `pattern`           | True if the expression matches the given pattern, e.g., , `'abc%'`                 |

### If Expression

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


### Case Expression

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

:::warning
Unlike SQL, Wvlet doesn't require `end` at the end of case expressions. 
:::

## String Expressions

| Operator        | Description             |
|-----------------|-------------------------|
| `expr` + `expr` | Concatenate two strings |


## Array Expressions

You can construct array values with `[e1, e2, ...]` syntax:

```sql
select ['a', 'b', 'c'] as arr
```

Arrays can be accessed with index (1-origin):
```sql
select ['a', 'b', 'c'] as arr
select arr[1] as first_element
```

## Map Expressions

You can construct map values with `map {k1: v1, k2: v2, ...}` syntax. Unlike struct expressions, keys (k1, k2, ...) needs to be the same type values, and values (v1, v2, ...) also need to be the same type values:

```sql
select map {'a': 1, 'b': 2} as m
```

## Struct/Row Expressions

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

## Lambda Expression

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
select list_reduce([4, 5, 6], (a, b) -> a + b) as sum
```

```
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
  ) as arr
```

```
┌──────────────┐
│     arr      │
│  array(int)  │
├──────────────┤
│ [16, 17, 18] │
├──────────────┤
│ 1 rows       │
└──────────────┘
```
