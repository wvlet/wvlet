# New Typer Architecture with Type Field on SyntaxTreeNode

**Date**: 2025-01-15
**Author**: Taro L. Saito (with Claude Code assistance)
**Related Issue**: [#392](https://github.com/wvlet/wvlet/issues/392) - Redesign TypeResolver with bottom-up traversal

## Summary

This document describes the design for a new typer implementation that replaces the current pattern-matching based TypeResolver with a more efficient bottom-up traversal approach. The key innovation is adding a `tpe: Type` field to all SyntaxTreeNodes while keeping Symbols only for named entities, following modern compiler design patterns similar to Scala 3.

## Current Problems

1. **Multiple Tree Traversals**: Current TypeResolver uses ~17 separate RewriteRules, each triggering a full tree traversal
2. **Type Information Ambiguity**: Two paths to get types - `LogicalPlan → Symbol → DataType` vs `LogicalPlan.relationType`
3. **Mixed Concerns**: Typing and tree rewriting happen simultaneously
4. **Performance Issues**: O(n*m) complexity where n=tree size, m=number of rules
5. **Fragile Rule Ordering**: Rules must be applied in specific order with some applied multiple times

## Design Principles

1. **Symbols for Identity**: Symbols remain only for named entities (ModelDef, TypeDef, ValDef, functions)
2. **Type Field for All Nodes**: Every SyntaxTreeNode gets a `tpe: Type` field for type information
3. **Bottom-Up Single Pass**: One traversal that types from leaves to root
4. **PartialFunction Composition**: Use Scala's pattern matching instead of visitor pattern
5. **Immutable Tree Structure**: AST structure unchanged, only type information added

## Core Architecture

### 1. Enhanced SyntaxTreeNode

```scala
trait SyntaxTreeNode extends TreeNode with Product with LogSupport:
  // Symbol remains only for named entities
  private var _symbol: Symbol = Symbol.NoSymbol

  // NEW: Every node has a type field
  private var _tpe: Type = NoType

  // Existing comment fields
  private var _comment: List[TokenData[?]] = Nil
  private var _postComment: List[TokenData[?]] = Nil

  // Type accessors
  def tpe: Type = _tpe
  def tpe_=(t: Type): Unit = _tpe = t

  // Symbol accessors (unchanged - only for named entities)
  def symbol: Symbol = _symbol
  def symbol_=(s: Symbol): Unit = _symbol = s

  // Helper to check if node is typed
  def isTyped: Boolean = _tpe != NoType && _tpe != UnknownType

  // Copy metadata when transforming nodes
  override def copyMetadataFrom(t: SyntaxTreeNode): Unit =
    _symbol = t._symbol
    _tpe = t._tpe  // Copy type information
    _comment = t._comment
    _postComment = t._postComment
```

### 2. Extended Type Hierarchy

```scala
sealed trait Type:
  def isResolved: Boolean
  def isError: Boolean = false

// Singleton for untyped nodes
case object NoType extends Type:
  def isResolved = false

// Existing DataType becomes part of Type hierarchy
sealed trait DataType extends Type:
  // Existing DataType implementation remains

// Additional type constructs
case class ErrorType(msg: String) extends Type:
  def isResolved = false
  def isError = true

case object UnitType extends Type:
  def isResolved = true

// For type variables during inference
case class TypeVar(id: Int) extends Type:
  def isResolved = false
```

### 3. Typer Implementation

```scala
object Typer extends Phase("typer"):

  override def run(unit: CompilationUnit, context: Context): CompilationUnit =
    given TyperContext = TyperContext.from(context, unit)
    val typed = typePlan(unit.unresolvedPlan)
    unit.copy(typedPlan = typed)

  private def typePlan(plan: LogicalPlan)(using ctx: TyperContext): LogicalPlan =
    // Bottom-up: type children first
    val typedChildren = plan.children.map(typePlan)
    val withTypedChildren = plan.mapChildren(_ => typedChildren)

    // Type current node using composable rules
    typeNode(withTypedChildren)

  private def typeNode(plan: LogicalPlan)(using ctx: TyperContext): LogicalPlan =
    val typed = (statementRules orElse relationRules orElse expressionRules)
      .applyOrElse(plan, identity[LogicalPlan])

    if typed.tpe == NoType then
      typed.tpe = inferType(typed)

    typed
```

### 4. Composable Typing Rules

```scala
object TyperRules:

  // Expression typing rules
  val expressionRules: PartialFunction[LogicalPlan, LogicalPlan] =
    literalRule orElse
    identifierRule orElse
    binaryOpRule orElse
    functionApplyRule

  // Relation typing rules
  val relationRules: PartialFunction[LogicalPlan, LogicalPlan] =
    tableScanRule orElse
    projectRule orElse
    filterRule orElse
    joinRule orElse
    groupByRule

  // Statement typing rules
  val statementRules: PartialFunction[LogicalPlan, LogicalPlan] =
    packageDefRule orElse
    modelDefRule orElse
    typeDefRule orElse
    queryRule
```

## Package Structure

```
wvlet-lang/src/main/scala/wvlet/lang/compiler/typer/
├── Typer.scala                   # Main typer phase
├── TyperContext.scala            # Typing context with given/using
├── TyperRules.scala              # Composable PartialFunction rules
├── rules/
│   ├── ExpressionRules.scala    # Expression typing rules
│   ├── RelationRules.scala      # Relation typing rules
│   ├── StatementRules.scala     # Statement typing rules
│   └── PackageRules.scala       # Package-level rules
├── TypeInference.scala          # Type inference and unification
└── TyperError.scala             # Error handling
```

## Migration Strategy

### Phase 1: Add tpe field alongside relationType
- Keep `relationType` method as compatibility layer
- Add `tpe` field to all nodes
- Both old and new typers can coexist

### Phase 2: Gradual Code Migration
- Replace `relationType` calls with `tpe` access
- Update SqlGenerator and transformations
- Run parallel validation

### Phase 3: Remove Legacy Code
- Remove `relationType` methods
- Remove old TypeResolver
- Clean up compatibility layers

## Example: Typed Tree

```scala
// Query: from users where age > 18 select name

// After typing - tpe field is set on all nodes
Filter(
  tpe = SchemaType("users", List(
    NamedType("id", IntType),
    NamedType("name", StringType),
    NamedType("age", IntType)
  )),
  child = TableScan(
    tpe = SchemaType("users", ...),
    "users"
  ),
  condition = BinaryOp(
    tpe = BooleanType,
    ">",
    left = Identifier(tpe = IntType, "age"),
    right = IntLiteral(tpe = IntType, 18)
  )
)
```

## Benefits

1. **Performance**: Single-pass O(n) instead of O(n*m) traversals
2. **Clarity**: Clean separation between identity (Symbol) and types (tpe)
3. **Consistency**: Uniform type access via `node.tpe` for all nodes
4. **Maintainability**: Composable rules using PartialFunctions
5. **Debugging**: Easy to identify typed/untyped nodes
6. **Compatibility**: Can coexist with old TypeResolver during migration

## Implementation Timeline

- **Week 1**: Foundation - Create typer package, TyperContext, basic infrastructure
- **Week 2**: Expression Typing - Implement expression typing rules
- **Week 3**: Relation Typing - Type relations with proper propagation
- **Week 4**: Statement Typing - Handle statements in correct order
- **Week 5-6**: Integration & Testing - Parallel validation, performance testing

## Success Metrics

1. All existing tests pass with new typer
2. 2x+ performance improvement in typing phase
3. Reduced code complexity (fewer LOC, cleaner architecture)
4. Better error messages with context
5. Support for incremental typing in REPL

## References

- [Scala 3 Compiler Architecture](https://dotty.epfl.ch/docs/contributing/architecture/index.html)
- [A Critique of Modern SQL (CIDR '24)](https://www.cidrdb.org/cidr2024/papers/p48-neumann.pdf)
- [SQL Has Problems. We Can Fix Them: Pipe Syntax In SQL (VLDB 2024)](https://research.google/pubs/sql-has-problems-we-can-fix-them-pipe-syntax-in-sql/)