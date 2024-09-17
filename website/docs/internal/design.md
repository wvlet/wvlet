
# Design Philosophy

Wvlet query language is designed to meet the following principles:

## Core

The design of syntax should follow a typing order of left-to-right, top-to-bottom as much as possible to enable seamless typing so as not to distract the user from the flow of data processing. 

The language also needs to incorporate the best practices of software engineering in order to make the query reusable (modularity) and composable for building more complex queries and data processing pipelines.

 Each [relational operator](../syntax/relational-operators.md) processes the input table data and returns a new table data, but the returned data is not limited to a simple table format. It can be a nested objects with additional metadata to enrich the query results.

## Syntax Design Choices

### Use Lower Cases for Keywords

All keywords in Wvlet must be lower cases to reduce typing efforts and maintain the syntax consistency. In SQL, using both upper-cases or lower-cases like select or SELECT is allowed, but this makes the query format inconsistent between users or even in a single query. This also adds unnecessary complication to the parser implementation for managing upper-case keywords.  

### Consistent String Quotations

Use `'...'` (single quotes) and `"..."` (double quotes) for the convenience of writing string literals, and use `` `...` `` (back quotes) for describing column or table names, which might contain special characters or spaces.

### Break Down SELECT

The SELECT statement in SQL is a quite complex operator, which can do multiple operations at the same time, including aggregation, adding, removing, or renaming columns, annotating columns with aliases, changing column orders, etc. Wvlet breaks down this functionality into different operators `agg`, `add`, `exclude`, `transform`, `shift`, etc. With these new operators, users don't need to enumerate all columns in the SELECT statement, which makes the query more readable and easier to maintain.


```sql
SELECT 
  sum(c1),
  -- Rename c2 with an alias
  c2 as c2_new,
  -- skip c3 for exclusion
  -- Add a new computed column
  c4 + c5 as c101,
  -- Shift c6 and c7 to the end 
  c8, 
  ...
  ...,
  c100,
  c6,
  c7,
FROM tbl  
```

In Wvlet, you can write the same query as follows:
```sql
from tbl
-- Add a simple aggregation 
add c1.sum
-- Rename c2 with an alias
transform c2 as c2_new
-- Exclude c3
exclude c3 
-- Add a new computed column
add c4 + c5 as c101
-- Shift c6 and c7 to the end
shift to right c6, c7
```

As tables of log data can have hundreds of columns, it is not practical to enumerate all columns in the SELECT statement. By breaking down the SELECT statement into multiple operators, users can focus on the data processing logic rather than the column enumeration.

### Use Less Contexts (Parentheses, Brackets, etc.)

Use less parenthesis and brackets to make the query more readable and easier to compose, not only for humans but also for query generators and LLMs. LLM is good at guessing the next query statements from the previous text, but it's poor to understand deeply nested query context, which is not always represented at the text level. For example, in query languages like Wvlet or SQL, detailed table schemas and its changes inside query statements are not always present in the query texts, so LLM might produce query texts using wrong schema due to hallucination.

Also, Wvlet queries need to be script-friendly. In SQL, to properly generate WHERE clause in SQL queries, you need to understand the context of SELECT ... WHERE statements. For example, adding multiple WHERE clauses is not allowed in SQL:
```sql
select * from tbl
where a = 1 
-- ERROR: Adding where is not allowed.
where b = 'Y'
```

So, the query generator needs to find WHERE clause to add more conditions:
```sql
select * from tbl
-- Conditions need to be concatenated with AND
where a = 1 AND b = 'Y' 
```

In Wvlet, writing multiple WHERE statement is totally acceptable:
```sql
from tbl
where a = 1 
-- You can add more conditions in separate lines
where b = 'Y'
```
You can just add additional relational operators to refine the query without tracking the previous context.


### No Significant Indentation

To describe nested queries, we may be able to use parenthesized subquery `(...)` or significant indentations like Python or Scala 3. Although indentation-based syntaxes improve the readability, writing and parsing queries becomes harder for IDE or edictors (e.g., parser, linter, query generators, etc.) 

### Support Dot Notation

The modern programming languages support dot-notation for method chaining. SQL, which was designed around 1970s, however, does not support this feature, and always requires to use global function calls like this example:

```sql
cast(round(abs(sum(c1)), 1) as varchar)
```

In Wvlet, this can be written with dot-chain notation:
```sql
c1.sum.abs.round(1).to_string
```

Chaining functions like this also helps editors or IDEs to complement function names and arguments. Recently, DuckDB also supports [dot operator syntaxes](https://duckdb.org/docs/sql/functions/overview) for chaining function calls.

### Require Schema Only When Necessary

SQL enforces designing concrete table schema at all query stages. Even in subqueries, SQL requires full column names and fields with a fixed order. This requirement, however, introduces too early materialization of the column schema, which makes the query less flexible for schema changes based on the user requirement. 

Wvlet requires schema only when it is necessary, such as when writing the query result to a file or a database table. This allows users to focus on the data processing logic rather than the schema design. 

## Minor Design Choices

- For expressions within string interpolation, used `${...}` syntax, instead of `{...}` because we often want to include JSON data inside a string. Using `{...}` conflicts with JSON object notation.
  - Although adding JSON string as the first-class syntax was an option, we didn't pursue this direction because of the complexity of managing JSON tokens in the same grammar. Also, JSON is not always the best choice for describing table data and floating-point values.    
- Unlike ZetaSQL, which uses pipe operator `|>` to mix regular SQL and [pipe syntax](https://github.com/google/zetasql/blob/master/docs/pipe-syntax.md), Wvlet is a brand-new query language, isolated from SQL. So we don't need any such pipe operator for separating relational operators.
- For debug operator, using significant indent for the subsequent debug expressions was considered, but we decided to use `|` (pipe) operator. Unlike SQL, which uses `||` for concatenating strings, wvlet uses `+` operator for string concatenation, so we can use `|` for listing debug expressions. This pipe syntax also works in one-liner query.
- For `group by` operator, the default aggregation function for each column is `arbitrary` (`any`), which returns an arbitrary value from the grouped rows. This is because `arbitrary` is the most light-weight aggregation operator, which doesn't require reading all column values. When other type of aggregation is necessary, we can use `agg` operator to specify the aggregation function explicitly.
