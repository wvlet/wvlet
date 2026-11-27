# Design references

Area contracts and the gates that guard them. Start with [`ARCHITECTURE.md`](../ARCHITECTURE.md) for the module map and pipelines, and use this index to find the area you need. See [Writing design docs](AGENTS.md) when editing these pages. The language design principles are on the [Design Philosophy](https://wvlet.org/wvlet/docs/development/design/) page; these pages hold area contracts, and [ADRs](../adr/README.md) hold individual decisions.

| Reference | Owns |
| --- | --- |
| [Formal verification](formal-verification.md) | Lean models, conformance fixtures, which code change regenerates which fixture |
| [Engine](engine.md) | Runtime IR, optimizer toggles, conformance against DuckDB, golden plans, coverage ratchets, benchmarks |
| [Stdlib](stdlib.md) | Standard functions, per-engine catalogs, freshness gates |
| [Testing](testing.md) | Spec-driven tests, typer coverage ratchet, npm DuckDB pins, test-run pitfalls |
| [Platforms](platforms.md) | JVM / Scala.js / Scala Native code split, native builds, libduckdb loading |
| [Embedding](embedding.md) | The in-process `WvletEmbed` API and the result contract of the language bindings |
| [Connectors](connectors.md) | Connector modules, registration, test dependencies |
| [Releasing](releasing.md) | Versioning, the tag-driven release, VS Code extension publishing, Scala and uni bumps |
| [Agent docs](agent-docs.md) | `for-ai-agents.md` and the bundled skill, with their snippet and freshness tests |
