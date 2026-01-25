# Data Flow Syntax for Wvlet

## Overview

This plan adds a data flow/workflow syntax to Wvlet to enable expressing CDP (Customer Data Platform) journey-style semantics. The implementation extends Wvlet's existing architecture to support connections between LogicalPlans with static type checking.

**Scope**: Design-only PR (AST nodes, parser, type checking). Execution support will be added in follow-up PRs.

**Keywords**: `flow` for top-level construct, `stage` for individual steps.

## Goals

1. Enable describing connections between LogicalPlans (queries) with support for data flows including branching, merging, and jumps
2. Enable static type checking of workflow correctness
3. Maintain consistency with existing Wvlet syntax patterns

## Proposed Syntax

### Flow Definition

Use `switch` for conditional routing with familiar `case` syntax.

```wv
flow CustomerJourney(entry_segment: string) = {
  -- Entry stage: define initial audience
  stage entry = from users
    where segment_id = entry_segment

  -- Wait stage
  stage after_purchase = from entry
    | wait('7 days')

  -- Decision point: switch with case conditions
  stage check_engagement = from after_purchase
    | switch {
        case _.email_opens > 5  -> high_engagement
        case _.email_opens <= 2 -> low_engagement
        else                    -> moderate_engagement
      }

  -- A/B test: switch with percentages
  stage high_engagement = from check_engagement
    | switch {
        case 50% -> variant_a
        case 50% -> variant_b
      }

  -- Activation stages
  stage variant_a = from high_engagement
    | activate('email', template: 'promo_v1')

  stage variant_b = from high_engagement
    | activate('email', template: 'promo_v2')

  -- Jump to another flow (using -> for consistency)
  stage low_engagement = from check_engagement
    | -> RetentionFlow

  -- Default path ends
  stage moderate_engagement = from check_engagement
    | activate('push_notification')
    | end()
}
```

### Chaining Multiple Flows

```wv
flow RetentionFlow = {
  stage entry = from _  -- receives input from caller

  stage nurture = from entry
    | wait('14 days')
    | activate('email', template: 'win_back')
    | end()
}
```

### Key Syntax Elements

| Element | Syntax | Description |
|---------|--------|-------------|
| Flow | `flow Name = { ... }` | Define a named flow |
| Stage | `stage name = from ... \| ...` | Named step in flow |
| Switch | `switch { case ... -> target }` | Route to different stages |
| Case (condition) | `case expr -> target` | Route based on condition |
| Case (percentage) | `case N% -> target` | Route by percentage (A/B test) |
| Wait | `wait('duration')` | Pause execution |
| Activate | `activate(target, ...)` | Send to external system |
| Jump | `-> Flow` | Jump to another flow |
| End | `end()` | Terminate flow |


## Review Feedback (Codex)

### High Priority Gaps

1. **No multi-parent dependencies / fan-in**: Cannot model DAG merges (dbt model depending on multiple upstreams, Airflow join) beyond single-input chaining.
   - **Suggestion**: Add `merge a, b | join on ...` or `merge a, b | union` syntax

2. **`from` conflates data input with control dependency**: No explicit `depends on/after` for tasks without data input (sensors, checks, side-effect tasks).
   - **Suggestion**: Add `depends on a, b` separate from `from` for control-only dependencies

### Medium Priority Gaps

3. **Parallelism/fan-out is ambiguous**: `switch` is exclusive routing, doesn't model "run these tasks in parallel with same input".
   - **Suggestion**: Add `fork { stage a = ...; stage b = ... }` with concurrency controls

4. **No error handling/retries/timeouts**: Cannot declare reliability behaviors.
   - **Suggestion**: Add `with retry(max=3, backoff='1m') on error { skip | abort }`

### Low Priority

5. **`switch` mixes conditions and percentages**: Blurs row-level vs task-level branching semantics.
   - **Suggestion**: Consider separate `route` (conditions) and `split` (percentages) constructs

### Open Questions

1. **Stage semantics**: Are stages row-level transforms (dataflow) or task-level operators (DAG scheduling)?
   - Current design is data-flow centric (stages transform relations)
   - **Do we need a separate `task` construct for control-flow tasks?**

2. **`from a, b` vs `merge a, b`**:
   - `from a` = single source input
   - `merge a, b` = fan-in from multiple sources
   - Support `merge a, b on (cond)` for join conditions

3. Should flows have explicit triggers/schedules (cron/event)?

4. **`jump to` vs `->`**: Can simplify `jump to Flow` to `-> Flow` for consistency with switch cases?

---

## Revised Syntax Proposal (Addressing Feedback)

### Multi-source Inputs (Fan-in)

```wv
-- Join multiple upstream stages (with condition)
stage merged = merge stage_a, stage_b on _.user_id = _.user_id

-- Union multiple stages (no condition)
stage combined = merge stage_a, stage_b
  | union

-- Difference from `from`:
-- `from a` = single source
-- `merge a, b` = fan-in from multiple sources
```

### Control Dependencies (No Data)

```wv
-- Pure control dependency (no data flow)
stage validate = depends on etl_complete
  | run_validation()

-- Mixed: data from one, depends on another
stage report = from data_stage
  | depends on validation_stage
  | generate_report()
```

### Parallel Fan-out

```wv
-- Explicit parallel execution
stage process = from entry
  | fork {
      stage email = activate('email')
      stage sms = activate('sms')
      stage push = activate('push')
    }
```

### Error Handling

```wv
stage risky_op = from input
  | external_call()
  | with retry(max: 3, backoff: '1m', timeout: '30m')
  | on error {
      case timeout -> skip
      case failure -> abort
    }
```

### Updated Syntax Elements

| Element | Syntax | Description |
|---------|--------|-------------|
| Flow | `flow Name = { ... }` | Define a named flow |
| Stage | `stage name = from ... \| ...` | Named step with data input |
| Merge | `merge a, b` or `merge a, b on cond` | Fan-in from multiple stages |
| Control dep | `depends on a, b` | Control-only dependency |
| Route | `switch { case cond -> target }` | Condition-based routing |
| Split | `split { case N% -> target }` | Percentage-based A/B test |
| Fork | `fork { stage a = ...; ... }` | Parallel execution |
| Wait | `wait('duration')` | Pause execution |
| Activate | `activate(target, ...)` | Send to external system |
| Retry | `with retry(max: N, ...)` | Retry policy |
| Error | `on error { case ... -> action }` | Error handling |
| Jump | `-> Flow` | Jump to another flow |
| End | `end()` | Terminate flow |

---

## Discussion: Data Flow vs Task Flow

Current `stage` is **data-flow centric** - each stage transforms a relation (RelationType in/out).

### Option A: Unified Syntax (Current Design)

Use `stage` for both data-flow and task-flow, differentiate by operator:

```wv
-- Data flow stage (has relationType)
stage transform = from input
  | select user_id, sum(amount)
  | group by user_id

-- Task flow stage (side-effect only, uses depends on)
stage notify = depends on transform
  | send_notification()
```

### Option B: Separate Constructs

Introduce `task` for control-only operations:

```wv
flow ETLPipeline = {
  -- Data flow stages
  stage extract = from source_table
  stage transform = from extract | ...
  stage load = from transform | save to target_table

  -- Control flow tasks
  task validate = depends on load
    | run_validation()

  task notify = depends on validate
    | send_slack_message()
}
```

### Recommendation

Start with **Option A** (unified `stage`) for simplicity. The `depends on` clause already distinguishes control-only tasks. Can introduce `task` later if needed.

---

## Implementation Notes (PR #1527)

### What Was Implemented

**Scope**: Design-only PR - AST nodes, parser, and code generation. No type checking or execution support yet.

#### New Tokens (WvletToken.scala)
Added 10 new keywords as **non-reserved**:
- `flow`, `stage`, `switch`, `split`, `fork`, `merge`, `depends`, `wait`, `activate`, `end`

#### AST Nodes

**In plan.scala:**
- `FlowDef(name: TermName, params: List[DefArg], stages: List[StageDef], span: Span)` - Top-level flow definition

**In relation.scala:**
- `StageDef(name, inputRefs, dependsOn, body, span)` - Named stage within a flow
- `FlowSwitch(child, cases, elseTarget, span)` - Conditional routing
- `FlowCase(condition, target, span)` - Single case in switch
- `FlowSplit(child, cases, span)` - Percentage-based routing
- `FlowSplitCase(percentage, target, span)` - Single case in split
- `FlowFork(child, stages, span)` - Parallel execution
- `FlowMerge(sources, joinCondition, span)` - Fan-in from multiple stages
- `FlowWait(child, duration, span)` - Time-based delay
- `FlowActivate(child, target, params, span)` - External activation
- `FlowJump(child, targetFlow, span)` - Jump to another flow
- `FlowEnd(child, span)` - Terminate flow path

### Key Implementation Decisions

1. **Non-reserved keywords**: All flow keywords are non-reserved to avoid breaking existing code that uses them as identifiers (e.g., `end` as a parameter name in `def substring(start:int, end:int)`).

2. **Stage body uses TableRef**: When parsing `stage x = from y`, the stage reference `y` is parsed as a `TableRef` (not a new AST type), allowing it to flow through existing relation handling.

3. **Split uses integers, not percentages**: Implemented `split { case 50 -> target }` instead of `case 50% -> target` to avoid adding a new `Percentage` expression type. Percentages are specified as integers (0-100).

4. **Activate simplified**: Named parameters like `activate('email', template: 'x')` not yet supported. Use `activate('email_template_x')` for now.

5. **Depends on partially implemented**: Parser supports `depends on` in stage definitions, but control-only stages (without `from`) need more work.

### Lessons Learned

1. **isIdentifier vs isNonReservedKeyword**: Changing `WvletToken.isIdentifier` to include non-reserved keywords broke the `count.wv` test because `COUNT` was being parsed as a grouping key instead of an operator. Solution: Keep `isIdentifier` unchanged, but explicitly check for `isNonReservedKeyword` in the `identifier()` and `identifierSingle()` parser methods.

2. **Relation children requirement**: Classes extending `Relation` must implement `def children: List[LogicalPlan]`. For `StageDef`, this returns `body.toList`. For `FlowMerge`, it returns `Nil` since sources are name references.

3. **RelationType.empty doesn't exist**: Use `EmptyRelationType` from `wvlet.lang.model.DataType.*`.

4. **LongLiteral pattern matching**: `LongLiteral` has 3 parameters `(value, stringValue, span)`, not 2.

5. **Error reporting**: Use `StatusCode.SYNTAX_ERROR.newException(msg, tokenData.sourceLocation)` not `spanFrom(...)`.

6. **Roundtrip parsing for stage bodies**: The WvletGenerator initially output stage bodies with newlines (using VList/`/` operator), but the newline-as-implicit-pipe behavior doesn't work inside flow definitions because the parser expects `stage` or `}` tokens. Solution: Added `flattenWithPipes` helper that converts VList to inline format with explicit `|` pipe operators. Stage bodies are now output as `stage x = from y | select z` instead of multi-line format.

7. **Merge inputRefs should be empty**: For `merge` in `parseStageInput`, the sources are already captured in `FlowMerge.sources`, so `inputRefs` should return `Nil` to avoid redundant information in the AST.

8. **Add range validation for percentages**: Split cases should validate that percentage values are in [0, 100] range at parse time.

### What's NOT Implemented Yet

1. **Type checking**: No `SymbolLabeler` or `TypeResolver` changes for flow/stage symbols
2. **Execution**: No runtime support for flow execution
3. **Named parameters in activate**: `activate('email', template: 'x')` syntax
4. **Depends on only stages**: `stage x = depends on y | ...` without `from`
5. **Error handling**: `with retry(...)` and `on error { ... }`
6. **Stage input from `_`**: `stage entry = from _` for receiving flow input

### Test Files

- `spec/basic/flow-syntax.wv` - Parsing test for all flow constructs
- All 122 parser tests pass
- All 121 runner tests pass

### Follow-up PRs Needed

1. **Type Checking PR**: Add flow/stage symbol types, resolve stage references
2. **Execution PR**: Runtime support for flow execution
3. **Enhanced Syntax PR**: Named params, depends-on-only stages, error handling
