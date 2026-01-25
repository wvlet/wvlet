# Typer Integration: Phase 1-2 Implementation Plan

## Background

Issue #392 tracks the Type redesign to replace the old `TypeResolver` (multi-pass rewrite rules) with the new `Typer` (single-pass bottom-up traversal). The new Typer package exists but is incomplete:

- **Current state**: Typer.run() returns unit UNCHANGED (doesn't store typed plan)
- **Gap**: TypeResolver has ~17 rewrite rules; Typer has ~7 typing rules
- **Integration ready**: Compiler.withNewTyper() exists to switch between them

## Scope

This PR focuses on two phases:
1. **Phase 1**: Make Typer functional by storing results in CompilationUnit
2. **Phase 2**: Add validation infrastructure to compare TypeResolver vs Typer

## Phase 1: Store Typed Plan in CompilationUnit

### Problem
`Typer.run()` (line 61) has a TODO and returns `unit` unchanged:
```scala
// TODO: Store typed plan in CompilationUnit once we have a field for it
unit
```

### Solution
Store the typed plan in `unit.resolvedPlan` like TypeResolver does:
```scala
unit.resolvedPlan = typed
unit
```

### File Change
- `wvlet-lang/src/main/scala/wvlet/lang/compiler/typer/Typer.scala` (line ~59-61)

## Phase 2: Add Validation Infrastructure

### 2.1 TyperValidationTest

Create a test that compares TypeResolver and Typer outputs on the same input.

**File**: `wvlet-lang/src/test/scala/wvlet/lang/compiler/typer/TyperValidationTest.scala`

**Purpose**:
- Compile same source with both TypeResolver and Typer
- Compare resulting plan structures
- Report and document type differences
- Track which expression types work correctly

### 2.2 Gap Documentation

Document the current gap between TypeResolver and Typer rules.

**High Priority Rules to Migrate**:
| Rule | Complexity | Notes |
|------|------------|-------|
| resolveTableRef | High | Table/model reference resolution |
| resolveRelation | Medium | Expression typing in relations |
| resolveModelDef | Medium | Model definition typing |
| resolveSelectItem | Low | Projected column typing |

**Already Implemented in Typer**:
- literalRules, identifierRules, binaryOpRules
- castRules, caseExprRules, dotRefRules, functionApplyRules

## Implementation Steps

1. **Modify Typer.run()** to store typed plan
2. **Add test for plan storage** in TyperTest.scala
3. **Create TyperValidationTest.scala** with comparison logic
4. **Run validation tests** to document current gaps
5. **Update TyperTest** with additional coverage for validated cases

## Verification

```bash
# Run typer tests
./sbt "langJVM/testOnly *TyperTest"

# Run validation tests
./sbt "langJVM/testOnly *TyperValidationTest"

# Verify full test suite still passes
./sbt "langJVM/test"
```

## Expected Outcome

- Typer becomes functional in the compiler pipeline
- Validation tests document which cases work and which don't
- Clear documentation of remaining gaps for future work
- No change to default behavior (TypeResolver remains default)

## Implementation Notes

### Key Insight: Plan Storage
The fix was minimal - just storing `typed` in `unit.resolvedPlan`. The Typer already returns
properly typed plans through its `typePlan` method.

### Test Style for AirSpec
- Use `shouldNotBe` instead of `should not be` (Scala 3 compatibility)
- Use `(condition) shouldBe true` instead of `should be >= value`
- WorkEnv is a case class, not an object with factory methods

### Wvlet Syntax
- Wvlet uses `if...then...else` instead of SQL's `CASE...WHEN...END`
- Be careful when writing test cases with Wvlet-specific syntax

## Remaining Challenges (Out of Scope)

These are identified but not addressed in this PR:
1. Table reference resolution (requires catalog integration)
2. Local file scan schema inference
3. Model definition expansion
4. Aggregation context handling
5. Package scope management (Issue #93)

## PR Status

PR #1524: All CI checks passing
