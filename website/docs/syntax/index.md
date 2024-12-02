# Query Syntax

## References

- [Quick Start](./quick-start.md)
- [Relational Operators](./relational-operators.md)
- [Expressions](./expressions.md)
- [Metadata Functions](./metadata-functions.md)
- [Data Models](./data-models.md)

## Introduction

Wvlet is a query language designed to be more human-readable and easier to write than SQL. If you already familiar to SQL, you will find it's easy to learn the syntax of wvlet as there are a lot of similarities between wvlet and SQL. Even if you are new to SQL, _no worries_! You can start learning wvlet from scratch. If you know about [DataFrame in Python](https://pandas.pydata.org/docs/user_guide), it will help you understand the wvlet query language as chaining relational operators in the flow-style is quite similar to using DataFrame API.

Wvlet queries start with `from` keyword, and you can chain multiple [relational operators](./relational-operators.md) to process the input data and generate the output data. The following is a typical flow of chaining operators in a wvlet query:

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

Unlike SQL, whose queries always must follow the `SELECT ... FROM ... WHERE ... GROUP BY ... ORDER BY ... LIMIT ...` structure, wvlet uses the __flow-style syntax__ to match the syntax order with the data processing order as much as possible to facilitate more intuitive query writing.

Some operators like `add`, `transform`, `agg`, `exclude`, `shift`, etc. are not available in the standard SQL, but these new operators have been added for reducing the amount of code and making the query more readable and easier to compose. Eventually, these operators will be translated into the equivalent SQL syntax.


To start learning Wvlet, see the [Quick Start](./quick-start.md) guide.
