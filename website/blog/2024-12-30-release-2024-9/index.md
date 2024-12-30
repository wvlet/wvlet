---
slug: release-2024-9
title: 'Redesigning 50-Year-Old SQL for Modern Data Analytics'
authors: [xerial]
tags: [tech, release]
---

We are excited to announce the release of Wvlet version [2024.9](https://github.com/wvlet/wvlet/milestone/11), an open-source flow-style query language designed to help users to write efficient queries for SQL engines. You can try Wvlet directly in your web browser at [Wvlet Playground](https://wvlet.org/wvlet/playground/). The source code is available on [GitHub](https://github.com/wvlet/wvlet).

<!-- truncate -->


## Why Wvlet?

At Treasure Data, we process over 3 million SQL queries daily. Managing this volume of queries and helping users (or LLM) write efficient queries presents several challenges.

The first challenge was that the syntactic order of SQL does not match the actual data flow. This discrepancy makes debugging complex and deeply nested queries difficult, even for SQL experts. _[A Critique of Modern SQL And A Proposal Towards A Simple and Expressive Query Language (CIDR '24)](https://www.cidrdb.org/cidr2024/papers/p48-neumann.pdf)_ highlights this issue intuitively:

<center>
![semantic-order](./sql-semantic-order.png)
</center>

Another challenge is that the SQL standard (e.g., SQL-92) covers only a small area and lacks essential software engineering features for managing multiple queries, including:
- No built-in support for reusing and generating queries.
- No extension point for multi-query optimization, such as incremental processing and pipeline execution like dbt.
- No built-in debugging or testing capabilities.

These challenges have arisen because SQL, designed in the 1970s, is now widely used for data analytics beyond its original scope. Wvlet is designed to address these challenges by modernizing [50-year-old SQL](https://dl.acm.org/doi/10.1145/3649887) to be more intuitive and functional, incorporating the best practices of software engineering.


## What's the current state of Wvlet?

Though still in early development, Wvlet already enables users to write and run queries against DuckDB, Trino through either a command line client (`wv`) or the Web-based UI (`wvlet ui`).  

### Interactive Editor (wv)

If you are using Mac, you can easily install the [interative shell (wv)](/docs/usage/repl) with the Homebrew command: `brew install wvlet/wvlet/wvlet`. 

The `wv` interactive editor (REPL) supports various shortcut keys, allowing you to check the schema (ctrl-j, ctrl-d), test the sub query (ctrl-j ctrl-t), or run the query (ctrl-j, ctrl-r) even in the middle of the query.

![wvlet shell](/img/demo.gif)

For using Trino SQL engine, you need to configure `~/.wvlet/profiles.yml` file to [specify the target Trino server address](/docs/usage/trino).  


### Wvlet Playground 

Wvlet is written in Scala 3, which can be compiled to JavaScript using Scala.js. This allows us to run Wvlet in the browser. Here is a demo for running Wvlet queries, compiled into SQL, and executing them on [the WebAssembly version of DuckDB](https://duckdb.org/2021/10/29/duckdb-wasm.html). No installation is required.


- [Wvlet Playground](https://wvlet.org/wvlet/playground/)

![wvlet playground](./playground-screenshot-1280x640.png)


Wvlet also provides a standalone [WebUI](/docs/usage/ui) to start a local web server to run Wvlet queries in your browser.

### Flow-Style Query Syntax

Wvlet has redesigned SQL in various ways to match the syntax with the natural data flow by introducing flow-style relational operators (e.g., `add`, `agg`, `concat`, `sample`, etc.), and column-at-a-time operators (e.g., `rename`, `exclude`, `shift`) for reducing the burden of enumerating columns.

We have also added `update`, `test`, `debug` syntaxes for convenience. The functionality of Wvlet is [tested using Wvlet queries with test expressions](https://github.com/wvlet/wvlet/tree/main/spec/basic). 

For more details on the query syntax, refer to the following [presentation slides](https://speakerdeck.com/xerial/wvlet-a-new-flow-style-query-language-for-functional-data-modeling-and-interactive-data-analysis-trino-summit-2024) from [Trino Summit 2024](https://trino.io/blog/2024/12/18/trino-summit-2024-quick-recap.html):

<iframe class="speakerdeck-iframe" frameborder="0" src="https://speakerdeck.com/player/4148a46ee4f24fb0816d1207439cbd33?slide=10" title="Wvlet: A New Flow-Style Query Language For Functional Data Modeling and Interactive Data Analysis - Trino Summit 2024" allowfullscreen="true" style={{width: '100%', height: 'auto', aspectRatio: 1.777}} ></iframe>

and in the following documents:
- [Wvlet - Quick Start](/docs/syntax/quick-start)
- [Query Syntax](/docs/syntax/)


### Functional Data Modeling

Queries written in Wvlet are reusable and composable, making it easier to manage complex queries. Once you write your query in .wv files, you can call or reuse them in other queries.

- [Data Models](/docs/syntax/data-models)


### Wvlet SDKs

We plan to add SDKs for various programming languages to help users convert Wvlet queries into SQL. Wvlet compiler, written in Scala 3, can be compiled into native LLVM code, which can be integrated with various programming languages, including Python, Rust, Ruby, C/C++ etc. In 2024.9 version, we have created an early version of Python SDKs: 

- [Wvlet SDKs](/docs/bindings)

Thanks to contributors from the community, we are getting closer to support multiple programming languages. For example, [an extension to use Wvlet using DuckDB](https://github.com/quackscience/duckdb-extension-wvlet), has been created. After the build pipeline is stabilized, we will release the official SDKs for various programming languages in PyPI, Maven, and other package repositories.


## What's Next?

We plan to release milestone versions approximately every 3 months, following the format `(year).(milestone month).(patch)`. The next milestone version will be [2025.1](https://github.com/wvlet/wvlet/milestone/12). You can find our project roadmap and features under active development on the [Wvlet Roadmap](https://github.com/orgs/wvlet/projects/2).

The next 2025.1 milestone will focus on functional data modeling features, including:
- Advanced query optimization with cuscading updates and materialization of Wvlet data models, similar to DBT, featuring incremental processing and query fusion.
- Support for importing Wvlet queries from GitHub repositories. 
- Enhance the type system with improved dot-syntax support for complex expressions.
- Support for more SQL dialects through context-specific query inlining.

Join our discussions in the [Discord channel](https://discord.com/invite/vJBXRfEeNQ). We welcom your thoughts, feedback, and feature requests. 

