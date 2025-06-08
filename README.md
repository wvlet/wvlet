![wvlet](logos/wvlet-banner-300.png)

Wvlet, pronounced as weave-let, is a new cross-SQL flow-style query language for functional data modeling and interactive data exploration. Wvlet works with various types of SQL-based database engines, including [DuckDB](https://duckdb.org/), [Trino](https://trino.io/), [Hive](https://hive.apache.org/), etc.

- [Documentation](https://wvlet.org/wvlet/)
- [Milestones](https://github.com/wvlet/wvlet/milestones)
- [Project Roadmap](https://github.com/orgs/wvlet/projects/2)

![wvlet-architecture](website/docs/img/wvlet-architecture.svg)

![demo](website/static/img/demo.gif)

## Why Wvlet?

Wvlet queries (saved as .wv files) provide a natural way to describe data processing pipelines, which will eventually be compiled into a sequence of SQL queries. While SQL is a powerful language for processing data, its syntax often does not match the semantic order of data processing. Let's see the following example: The syntactic order of SQL's SELECT ... statements mismatches with the actual data flow inside the SQL engines (cited from _[A Critique of Modern SQL And A Proposal Towards A Simple and Expressive Query Language (CIDR '24)](https://www.cidrdb.org/cidr2024/papers/p48-neumann.pdf)_):

![semantic-order](website/docs/img/sql-semantic-order.png)

For overcoming this shortcoming of SQL, Wvlet starts from a table scan statement `from ...`, and the result can be streamlined to the next processing operators like `where`, `group by`, `select`, etc., as if passing table values through [a pipe](https://en.wikipedia.org/wiki/Pipeline_(Unix)) to the next operator: 

```sql
from (table)
where (filtering condition)
...
where (more filtering condition can be added)
group by (grouping keys, ...)
where (group condition can be added just with where)
select (columns to output)
order by (ordering columns...)
limit (limiting the number of output rows)
```

With this flow style, you can describe data processing pipelines in a natural order to create complex queries. You can also add operators for testing or debugging data in the middle of the query. This flow syntax is gaining traction and has been adopted in Google's SQL to simplify writing SQL queries. For more details, see _[SQL Has Problems. We Can Fix Them: Pipe Syntax In SQL (VLDB 2024)](https://research.google/pubs/sql-has-problems-we-can-fix-them-pipe-syntax-in-sql/)_.

## Key Features

- **Flow-style syntax**: Write queries in the natural order of data processing
- **Multi-database support**: Works with DuckDB, Trino, Hive, and more
- **Static catalog support**: Compile queries offline without database connections
- **Interactive REPL**: Explore data interactively with auto-completion
- **Type-safe queries**: Catch errors at compile time with schema validation
- **Modular queries**: Organize and reuse queries as functions

### Static Catalog Support

Wvlet can export database catalog metadata (schemas, tables, columns) to JSON files and use them for offline query compilation. This enables:

- **CI/CD Integration**: Validate queries in pull requests without database access
- **Faster Development**: Compile queries instantly without network latency
- **Version Control**: Track schema changes alongside your queries

```bash
# Export catalog metadata
wv catalog import --name mydb

# Compile queries offline
wv compile query.wv --use-static-catalog --catalog mydb
```

See [Catalog Management](https://wvlet.org/wvlet/docs/usage/catalog-management) for more details.

## Contributors

Many thanks to everyone who has contributed to our progress:

[![Contributors](https://contrib.rocks/image?repo=wvlet/wvlet)](https://github.com/wvlet/wvlet/graphs/contributors)
