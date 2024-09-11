
# Design Philosophy

Wvlet query language is designed to meet the following principles:

- A query starts with `from` keyword, followed by a table name or relational operators to represent the data source.
- Each relational operator processes the input table data and returns a new table data.
- All keywords need to be lower cases, to reduce typing efforts.
- Use `'...'` (single quotes) and `"..."` (double quotes) for string literals, and use `` `...` `` (back quotes) for column or table names, which might contain special characters or spaces.
- Use less parenthesis and brackets to make the query more readable and easier to compose, not only for humans but also for LLMs or query generators.
- Incorporate the best practices of software engineering, to make the query reusable (modularity) and composable for building more complex queries and data processing pipelines.


## Minor Design Choices

- For expressions within string interpolation, used `${...}` syntax, instead of `{...}` because we often want to include JSON data inside a string. Using `{...}` conflicts with JSON object notation.
  - Although adding JSON string as the first-class syntax was an option, we didn't pursue this direction because of the complexity of managing JSON tokens in the same grammar. Also, JSON is not always the best choice for describing table data and floating-point values.    
- Unlike ZetaSQL, which uses pipe operator `|>` to mix regular SQL and [pipe syntax](https://github.com/google/zetasql/blob/master/docs/pipe-syntax.md), wvlet is a brand-new query language, isolated from SQL. So we don't need any such pipe operator for separating relational operators.


