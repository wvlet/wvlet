---
sidebar_label: Embedding (planned)
description: "Running the Wvlet engine inside your own application, the way apps embed DuckDB or SQLite, is planned. Today the SDKs compile Wvlet to SQL."
---

# Embedding the Wvlet Engine

:::info Planned
Embedding the engine in your own application is planned and not available yet. This page will describe it once the APIs ship.
:::

The engine already runs on the JVM, in the native CLI, in Node.js, and in the browser (the [Playground](../usage/playground.md)). The plan is to let applications open an engine, register files or tables, run Wvlet queries, and read the results in-process, the way apps embed DuckDB or SQLite.

Today, the [Python and TypeScript SDKs](../bindings/index.md) compile Wvlet queries to SQL; you run that SQL on DuckDB or a warehouse. To run queries on the engine now, use the [CLI](../usage/install.md).
