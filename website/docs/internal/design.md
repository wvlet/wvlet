
# Design Philosophy

Wvlet query language is designed to meet the following principles:

## Core

The design of syntax should follow a typing order of left-to-right, top-to-bottom as much as possible to enable seamless typing so as not to distract the user from the flow of data processing. 

The language also needs to incorporate the best practices of software engineering in order to make the query reusable (modularity) and composable for building more complex queries and data processing pipelines.

 Each [relational operator](../syntax/relational-operators.md) processes the input table data and returns a new table data, but the returned data is not limited to a simple table format. It can be a nested objects with additional metadata to enrich the query results.

## Syntax Design Choices


- All keywords must be lower cases, to reduce typing efforts and syntax consistency. In SQL, describing SELECT or select is allowed, but this makes the query format inconsistent between users or even in a single query. This also addes unnecessary complication to the parser implementation. 
- Use `'...'` (single quotes) and `"..."` (double quotes) for the convenience of writing string literals, and use `` `...` `` (back quotes) for describing column or table names, which might contain special characters or spaces.
- Use less parenthesis and brackets to make the query more readable and easier to compose, not only for humans but also for LLMs or query generators.
- Avoid using significant indentation like Python or Scala 3. Although it improves readability, it makes it harder to parse the query in one line.
- Function should be able to call with dot-notation like `func(args)`, `func(args).func2(args)`, etc. This helps editors or IDEs to complement function names and arguments.


## Minor Design Choices

- For expressions within string interpolation, used `${...}` syntax, instead of `{...}` because we often want to include JSON data inside a string. Using `{...}` conflicts with JSON object notation.
  - Although adding JSON string as the first-class syntax was an option, we didn't pursue this direction because of the complexity of managing JSON tokens in the same grammar. Also, JSON is not always the best choice for describing table data and floating-point values.    
- Unlike ZetaSQL, which uses pipe operator `|>` to mix regular SQL and [pipe syntax](https://github.com/google/zetasql/blob/master/docs/pipe-syntax.md), Wvlet is a brand-new query language, isolated from SQL. So we don't need any such pipe operator for separating relational operators.
- For debug operator, using significant indent for the subsequent debug expressions was considered, but we decided to use `|` (pipe) operator. Unlike SQL, which uses `||` for concatenating strings, wvlet uses `+` operator for string concatenation, so we can use `|` for listing debug expressions. This pipe syntax also works in one-liner query.
