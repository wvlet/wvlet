---
id: compiler-internals
title: "Wvlet Architecture: Compiler Internals"
sidebar_label: Compiler Internals
sidebar_position: 1
---

This document is a single‑page, high‑level walkthrough of Wvlet’s compiler and internal representation. It is intentionally practical and links to concrete code locations so new contributors can orient quickly. Wvlet is still evolving; details here may change as phases are added or rearranged.

**Audience:** contributors building or debugging the language, compiler phases, and codegen.

**What this is not:** a user tutorial or syntax reference (see the main docs for those).

## Quick Map

At a glance, a compilation unit flows through these stages:

1) Parse source → LogicalPlan + Expressions
2) Label symbols (unique IDs) for definitions and queries
3) Resolve types (DataType and RelationType) and rewrite unresolved nodes
4) Optimize/normalize via tree rewrites (ongoing)
5) Plan execution (tests/debug/save/query tasks)
6) Generate text (pretty‑print Wvlet; SQL dialects in future work)

![Architecture](../img/wvlet-architecture.svg)

Key packages and files (links go to GitHub, labels show file names only):
- **Parsing**: [WvletParser.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/parser/WvletParser.scala), [SqlParser.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/parser/SqlParser.scala)
  - **Tokens**: [WvletToken.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/parser/WvletToken.scala), [SqlToken.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/parser/SqlToken.scala)
  - **Scanners**: [WvletScanner.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/parser/WvletScanner.scala), [SqlScanner.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/parser/SqlScanner.scala), [Scanner.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/parser/Scanner.scala)
- **Trees (expr)**: [Expression.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/model/expr/Expression.scala), [exprs.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/model/expr/exprs.scala), [Attribute.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/model/expr/Attribute.scala)
- **Trees (plan)**: [LogicalPlan.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/model/plan/LogicalPlan.scala), [relation.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/model/plan/relation.scala), [plan.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/model/plan/plan.scala), [ddl.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/model/plan/ddl.scala), [sqlPlan.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/model/plan/sqlPlan.scala), [execution.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/model/plan/execution.scala)
- **Types**: [DataType.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/model/DataType.scala), [TreeNode.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/model/TreeNode.scala)
- **Symbols**: [Symbol.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/Symbol.scala), [Symbolnfo.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/Symbolnfo.scala), [SymbolLabeler.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/analyzer/SymbolLabeler.scala)
- **Type resolution**: [TypeResolver.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/analyzer/TypeResolver.scala)
- **Rewriting infra**: [RewriteRule.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/RewriteRule.scala)
- **Execution planning**: [ExecutionPlanner.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/planner/ExecutionPlanner.scala)
- **Pretty printing**: [WvletGenerator.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/codegen/WvletGenerator.scala), [LogicalPlanPrinter.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/codegen/LogicalPlanPrinter.scala)

Useful commands:
- **Compile**: `./sbt compile`
- **Tests**: `./sbt test` (or per‑module, e.g., `./sbt langJVM/test`)
- **Format**: `./sbt scalafmtAll`

## CompilationUnit

The compiler processes one source file per CompilationUnit and records all intermediate artifacts for that file.

- **Type**: [CompilationUnit.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/CompilationUnit.scala)
- **Holds**:
  - `unresolvedPlan`: parsed tree before symbol/type resolution.
  - `resolvedPlan`: fully typed `LogicalPlan` after resolver passes.
  - `executionPlan`: tasks produced by `ExecutionPlanner`.
  - `modelDependencies`: DAG of model‑to‑model references.
  - `knownSymbols`: symbols discovered/entered during analysis.
  - `lastError`, `finishedPhases`, `lastCompiledAt` (book‑keeping for incremental builds).
- **Source**: `sourceFile` (tracks file path, content, timestamps) and helpers like `text(span)` and `toSourceLocation` for diagnostics.
- **Lifecycle**:
  - Created from files or strings via `CompilationUnit.fromPath|fromFile|fromWvletString|fromSqlString`.
  - Standard library and presets load as units with `isPreset = true`.
  - `reload()` clears phase markers and errors when the file changes; `needsRecompile` compares timestamps.
- **Context integration**: Every `Context` carries a `compilationUnit`; phases log and look up symbols using it (e.g., `ctx.compilationUnit.enter(sym)`, error locations via `sourceLocationAt(span)`).

## Trees: LogicalPlan and Expressions

The compiler uses two primary node families:
- **Expression** (values, names, operators): see `model/expr/*.scala`. Each expression exposes `def dataType: DataType`, often computed structurally from its children.
- **LogicalPlan** (relations/statements): see `model/plan/*.scala`. Every plan exposes `def relationType: RelationType` (a subtype of `DataType`) describing its output schema.

Common patterns:
- **Immutability**: Trees are immutable products; transformation utilities (`transform`, `transformUp`, `transformExpressions`, etc.) rebuild nodes when children change.
- **Metadata preservation**: `SyntaxTreeNode.copyInstance` preserves metadata (e.g., attached symbol) on rewritten copies.
- **Schema computation**: Plan nodes compute `relationType` functionally from inputs. Leaf/source nodes may carry a `schema: RelationType` (e.g., scans/files/values) used to compute `relationType`.

Terminology:
- `DataType` describes scalar and compound types (e.g., `int`, `varchar(10)`, `array[t]`).
- `RelationType` extends `DataType` to represent table‑like schemas. Variants include `SchemaType`, `ProjectedType`, `AggregationType`, `ConcatType`, `RelationTypeList`, `UnresolvedRelationType`, `EmptyRelationType`.
- `NamedType(name, dataType)` pairs a field name with a `DataType` and is used to enumerate columns.

## Symbols and SymbolInfo

Symbols provide stable identifiers for definitions and queries across phases.

- **Symbol**: unique ID + back‑pointer to the owning tree (set during labeling and updated on copies).
- **SymbolInfo**: phase‑mutable info attached to a symbol:
  - `symbolType`: `Package | Import | ModelDef | TypeDef | MethodDef | ValDef | Relation | Query | Expression`
  - `tpe` / `dataType`: the resolved `Type` (`DataType` for most symbols)
  - scope/owner: used for name resolution

**Where symbols appear today**
- Definitions: `PackageDef`, `Import`, `TypeDef`, `TopLevelFunctionDef`, `ModelDef`, `ValDef`, and the top‑level `Query` get symbols in `analyzer/SymbolLabeler.scala`.
- Other nodes may not have symbols yet; they rely on structural `relationType`/`dataType` until we broaden symbol coverage.

**Invariants to keep in mind**
- Trees are immutable; “type updates” happen by mutating `SymbolInfo` or by emitting a rewritten tree with new fields (e.g., a `FileScan` with a concrete `schema`).
- When a node has a symbol and also a stable schema (e.g., `ModelDef`, `TableScan`, `Values`), the symbol’s `dataType` should match the node’s `relationType`.

## Compilation Pipeline (Phases)

### 1) Parsing → Trees
`WvletParser` (for `.wv`) and `SqlParser` (for `.sql`) turn source into `LogicalPlan`/`Expression` trees. Both parsers are driven by scanners that produce a stream of token records.

**Lexer/Scanner basics**
- Token enums: `WvletToken` and `SqlToken` classify keywords, identifiers, operators, literals, quotes, and control tokens (comments, whitespace, EOF). They also encode reserved vs non‑reserved keywords.
- Scanners: `WvletScanner` and `SqlScanner` extend a shared `ScannerBase`, which yields `TokenData` via `nextToken()`; parsers inspect tokens with `lookAhead()` and `consume(...)`.
- Comments/doc: scanners surface `COMMENT` and `DOC_COMMENT` tokens; the parser attaches them to nodes so printers/SQL can preserve them.
- Strings/interpolation: `WvletScanner` handles triple‑quoted and back‑quoted interpolation (`STRING_INTERPOLATION_PREFIX`, `BACKQUOTE_INTERPOLATION_PREFIX`) and emits `STRING_PART` tokens; `SqlScanner` recognizes double‑quoted identifiers and string literals.

A few special cases:
- `ValDef`: the parser sets `dataType` from explicit annotations, table column syntax (`val t(a,b) = [[...]]`), or the expression’s type. This is later reflected into the symbol.
- Table/file references are parsed as `TableRef`/`FileRef` with `UnresolvedRelationType`.

### 2) Symbol Labeling
`analyzer/SymbolLabeler.scala` assigns symbols and seeds `SymbolInfo`:
- `ModelDef`: `ModelSymbolInfo` seeded with `givenRelationType.getOrElse(child.relationType)`.
- `ValDef`: `ValSymbolInfo` seeded from the parsed `v.dataType` and expression.
- Top‑level queries/relations get `QuerySymbol`/`RelationAliasSymbolInfo` as appropriate.

This phase also establishes scopes for packages, types, and methods so later passes can resolve names.

### 3) Type Resolution and Tree Rewrites
`analyzer/TypeResolver.scala` resolves types and replaces unresolved leaves with concrete scans/values:
- `TableRef` → `TableScan` via catalog lookup or known type definitions.
- `FileRef` → `FileScan` by inspecting JSON/Parquet to obtain a `schema`.
- `ModelScan` binds to the referenced `ModelDef`’s `relationType`.
- `ValDef` table value constants: when referenced as a table, the resolver detects `SchemaType` on the `ValDef` and rewrites to a `Values` relation with that schema.

**Where types live today**
- Expressions compute `def dataType` on demand.
- Plans compute `def relationType`. Leaf/source plans also carry `schema: RelationType` in constructors.
- Definitions store type on their `SymbolInfo` (authoritative for `ModelDef`, seeded for `ValDef`).

Ongoing work: moving toward a symbol‑centric typing model (see “Typing Model” below and issue #1175).

### 4) Normalization / Optimizations (ongoing)
Tree rewrites are expressed via `RewriteRule` and applied with the `transform*` APIs on plans/exprs. Typical examples:
- Pruning debug/test nodes from execution paths
- Pushing projections/filters (future work)

### 5) Execution Planning
`planner/ExecutionPlanner.scala` turns a `LogicalPlan` into an `ExecutionPlan` DAG used by runners:
- Removes `TestRelation`/`Debug` from evaluated queries while generating separate tasks for them.
- Emits tasks such as `ExecuteQuery`, `ExecuteSave`, `ExecuteValDef`.

### 6) Code Generation / Pretty Printing
`codegen/WvletGenerator.scala` and `LogicalPlanPrinter.scala` render plans/expressions back to Wvlet syntax for `explain` and logs. For SQL output and layout details, see:
- [GenSQL: SQL Generation](#gensql-sql-generation) — end‑to‑end SQL statement generation and task handling.
- [SQL Formatting (Wadler Doc)](#sql-formatting-wadler-doc) — how the pretty printer builds compact/expanded SQL.

## Typing Model

Types appear in three places today:
- **Expression nodes**: expose `def dataType: DataType` (structural).
- **Plan nodes**: expose `def relationType: RelationType` (structural/derived); leaf sources carry a `schema`.
- **Definitions’ symbols**: `symbol.symbolInfo.dataType` (phase‑mutable denotation).

Short‑term plan (tracked in [#1175](https://github.com/wvlet/wvlet/issues/1175)):
- **Authoritative definitions**: Treat `SymbolInfo.dataType` as authoritative for definitions (`ModelDef`, `ValDef`).
- **Seed source relations**: For leaf sources (e.g., `TableScan`, `FileScan`, `Values`), keep `schema` on the node, and seed `symbol.dataType` from it so downstream code can read a uniform view via symbols.
- **Functional operators**: Keep intermediate operators purely functional: compute `relationType` from children; do not introduce new type fields.

## Traversal and Transformation

**Common utilities you’ll use while writing rules**
- `LogicalPlan.transform` / `transformUp` / `transformOnce` for plan rewrites
- `LogicalPlan.transformExpressions` to operate on embedded expressions
- `Expression.transformExpression` and the `traverse*` helpers for inspection

**Important details**
- Always return the original node if nothing changes (structural sharing reduces churn).
- When replacing a node with a new instance, the symbol (if any) is preserved and its `tree` back‑pointer is updated by `copyInstance`.

## Catalog and External Schemas

Resolving `TableRef` and `FileRef` requires external metadata:
- **Catalog lookup**: provides `SchemaType` for known tables.
- **File inspection**: JSON/Parquet analyzers infer `RelationType` for files.

The resolver rewrites leaves to `TableScan`/`FileScan` with an explicit `schema`, from which `relationType` is derived (or projected if `columns` are specified).

## Debugging and Developer Ergonomics

**Printing**
- `plan.pp` prints a readable tree with nested indentation.
- `WvletGenerator` renders a Wvlet program; useful in `explain` outputs.

**Logging**
- Many phases respect debug flags; append `-- -l debug` to test invocations to see detailed logs, e.g.:
  - `./sbt "runner/testOnly *BasicSpec -- -l debug"`

**Targeted testing**
- Parser/typing focused tests live under `wvlet-lang/src/test/scala/...` and can be run per class with `testOnly`.

## Invariants and Guidelines (WIP)

- **Avoid duplication**: Trees are immutable; types should not be stored twice when avoidable.
- **Consistency**: If a plan node has both a symbol and an intrinsic schema, keep them consistent (`symbol.dataType == relationType`).
- **Single source of truth**: Prefer computing types from children; use `SymbolInfo` as the single source of truth for definitions and named entities.
- **Locality**: Keep rewrites local and explicit; avoid hidden global state.

## Roadmap and Open Questions

- Symbol‑centric typing (definitions first, then source relations). See [#1175](https://github.com/wvlet/wvlet/issues/1175).
- Expand optimization passes (projection/filter pushdown, join key normalization).
- SQL dialect backends.

If you’re about to change phase ordering, add a short ADR (Architecture Decision Record) and link it here.

## GenSQL: SQL Generation

GenSQL converts a resolved `LogicalPlan` (and its `ExecutionPlan` tasks) into executable SQL text for a target SQL engine. It orchestrates statement generation, handles dialect differences, and preserves useful metadata in header comments.

- **Entrypoint**: [GenSQL.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/codegen/GenSQL.scala)
- **SQL printer**: [SqlGenerator.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/codegen/SqlGenerator.scala)
- **Dialects and flags**: [DBType.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/DBType.scala), `SQLDialect` enums
- **Formatting**: [CodeFormatter.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/codegen/CodeFormatter.scala)

**What GenSQL does**
- **Plans tasks**: Uses `ExecutionPlanner.plan(...)` to obtain an `ExecutionPlan`, then walks tasks:
  - `ExecuteQuery(Relation)`: generates a single SELECT using `generateSQLFromRelation`.
  - `ExecuteSave(Save, ...)`: emits DDL/DML strings via `generateSaveSQL` (CTAS, INSERT INTO, COPY, DELETE). Honors dialect flags such as `supportCreateOrReplace`, `supportCreateTableWithOption`, `supportSaveAsFile`.
  - `ExecuteValDef`: evaluates native expressions (e.g., `NativeExpression`) and updates the `ValDef` symbol.
  - `ExecuteCommand(ExecuteExpr)`: prints an expression to SQL via the shared `SqlGenerator`.
  - **Concatenates** multiple statements with `;` and prepends a header comment containing version and source location.

**Generating a SELECT**
- `generateSQLFromRelation(relation)` performs:
  1) **Expand**: Inline `ModelScan` bodies and function arguments; evaluate backquoted identifier interpolations; re-run `TypeResolver` on the inlined tree.
  2) **Print**: Hand the expanded `Relation` to `SqlGenerator.print` with a `CodeFormatterConfig(sqlDBType = ctx.dbType)`.
  3) **Wrap**: Optionally add a header comment with compiler version and source position.

**Expansion details (inline models and locals)**
- **Model binding**: Looks up the referenced `ModelDef` and creates a child `Context` where model parameters are bound as `ValSymbolInfo` with their argument expressions.
- **Rewrite & evaluate**: Rewrites the model body with those bindings and evaluates backquote‑interpolated identifiers and simple native expressions.
- **Re‑resolve types**: Resolves the result again through `TypeResolver` to ensure types and schemas are consistent post‑inlining.

**SqlGenerator (SELECT/DDL printer)**
- **Purpose**: Works on `LogicalPlan` and `Expression` to produce a pretty‑printed SQL `Doc` using the Wadler‑inspired formatter.
- **Merges operators into SELECT** (`SQLBlock`):
  - Projection/aggregation → SELECT list (with `distinct` when present).
  - `Filter` before `GroupBy` → HAVING; otherwise → WHERE.
  - `OrderBy`, `Limit`, `Offset` are positioned correctly and compacted when possible.
  - `WithQuery` emits `WITH ... AS (...)` CTEs before the main query.
  - Set operations (`UNION`, `INTERSECT`, `EXCEPT`) and pivots are lowered into supported SQL shapes.
  - DDL/updates like `CreateTableAs` and `InsertInto` are printed for engines that support them; VALUES handling respects `requireParenForValues`.

**Dialect handling**
- The current dialect (`ctx.dbType`) influences both printing and DDL choices:
  - `supportCreateOrReplace`, `supportCreateTableWithOption`, `supportSaveAsFile`, `supportDescribeSubQuery` change emitted statements.
  - Expression syntax variations (e.g., array/map constructors, row/struct support) are toggled by `SQLDialect` flags in [DBType.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/DBType.scala).

**Outputs**
- **Single SELECT**: one SQL block with a header.
- **Multiple tasks**: concatenated statements separated by semicolons; a trailing semicolon is added for multi‑statement outputs.

**Notes and TODOs**
- **Show models** currently prints via `SqlGenerator` by introspecting known symbols; it may be moved out of SQL generation later.
- **Dialect coverage**: Some database dialects aren’t fully covered yet; adding support typically requires small DBType/SQLDialect flag updates plus minor printer branches.

## SQL Formatting (Wadler Doc)

Wvlet uses a Wadler‑style pretty printer to format SQL predictably. The generator builds a tree of small layout tokens (Docs), then flattens a Group to one line if it fits the configured width; otherwise it expands with newlines and indentation.

- **Doc algebra**: Text, NewLine, MaybeNewLine, WhiteSpaceOrNewLine (wsOrNL), LineBreak, HList (`+`), VList (`/`), Nest, Block, Group. See [CodeFormatter.scala](https://github.com/wvlet/wvlet/blob/main/wvlet-lang/src/main/scala/wvlet/lang/compiler/codegen/CodeFormatter.scala).
- **Groups**: Try a single‑line layout by flattening soft breaks; if the flattened length `<= maxLineWidth`, keep it; else render with real breaks and indentation.
- **Soft breaks**: `wsOrNL` becomes a space when flat, a newline when expanded; `MaybeNewLine` may elide entirely when flat. Use `LineBreak` for hard, non‑flattenable breaks.
- **Helpers**:
  - `cl(a, b, c)`: comma list with `,` + `wsOrNL` as a separator (compact when it fits; one item per line when not).
  - `wl(x, y, z)`: join with spaces; good for keywords and short fragments.
  - `group(...)`, `nest(...)`, `paren(...)`, `indentedParen(...)`: wrap subqueries/blocks so they compact or expand consistently.

Example (SELECT list)
```sql
-- Code pattern in generator
group(wl("select", cl(selectItems.map(expr))))

-- Compact (fits width)
select a, b, c

-- Expanded (too long; breaks at cl separator)
select
  a,
  b,
  c
```

Example (FROM with subquery)
```sql
-- Code pattern in generator
wl("from", indentedParen(query(child)))

-- Compact
from (select a from t)

-- Expanded
from (
  select a
  from t
)
```

Tuning knobs live in `CodeFormatterConfig` (indent width, max line width, dialect for printing expressions). `SqlGenerator` uses these building blocks so that adding new operators mostly means placing soft breaks (`wsOrNL`) at legal SQL boundaries and wrapping groups appropriately.

## Glossary

- DataType: type of an expression or column (e.g., `int`, `varchar(20)`, `array[int]`).
- RelationType: schema of a relation (table‑like value) with named fields.
- NamedType: a `(name, DataType)` pair describing a field.
- Symbol: stable identifier for a tree; anchors phase‑mutable `SymbolInfo`.
- Denotation (`SymbolInfo`): the symbol’s type and other metadata at a phase.

---

Questions or proposals? Open an issue and tag “compiler” or send a PR updating this page alongside your change.
