# Task-Oriented Syntax Design for Wvlet Flow

## Overview

Design syntax extensions for wvlet's flow language to support task management features found in modern workflow orchestration tools (Temporal, Airflow, Dagster, Prefect).

## Research Summary

### Key Concepts from Workflow Tools

| Concept | Temporal | Airflow | Dagster | Wvlet Flow (Current) |
|---------|----------|---------|---------|---------------------|
| Orchestration unit | Workflow | DAG | Job | `flow` |
| Work unit | Activity | Task/Operator | Op/Asset | `stage` |
| Composition | Child Workflows | SubDAGs | Nested Jobs | `-> FlowName` (jump) |
| Branching | Signals/Conditions | BranchOperator | Dynamic outputs | `route { case ... }` |
| Parallel | Promises/Futures | Parallel tasks | Parallel ops | `fork { ... }` |
| Fan-in | Promise.all() | TriggerRule | Graph edges | `merge` |

### Temporal's Key Design Patterns

1. **Workflows are deterministic** - orchestration logic only, no side effects
2. **Activities do the real work** - can be non-deterministic, have retries
3. **Four timeout types**: start-to-close, schedule-to-close, heartbeat, schedule-to-start
4. **Retry policies**: initial interval, backoff coefficient, max interval, max attempts
5. **Parent close policies**: terminate, request_cancel, abandon
6. **Signals/Queries**: external interaction with running workflows
7. **Continue-As-New**: reset history for long-running workflows

---

## Execution Model

### Stage States

Each stage has a well-defined execution state:

```
                    ┌─────────────────────────────────┐
                    ↓                                 │
pending ──→ running ──→ success (terminal)           │
   │           │                                     │
   │           ↓                                     │
   │       attempt_failed ──→ retrying ──────────────┘
   │                              ↓
   │                         max_retries_exceeded
   │                              ↓
   │                         failed (terminal)
   │
   ├──→ skipped (terminal)   [trigger rule evaluated upstream as non-success]
   │
   └──→ cancelled (terminal) [user/parent cancellation at any point]
```

**State Definitions:**

| State | Terminal? | Description |
|-------|-----------|-------------|
| `pending` | No | Waiting for upstream dependencies to reach terminal state |
| `running` | No | Currently executing |
| `success` | **Yes** | Completed successfully |
| `attempt_failed` | No | Current attempt failed, will retry if attempts remain |
| `retrying` | No | Waiting for retry delay before next attempt |
| `failed` | **Yes** | All retry attempts exhausted, permanently failed |
| `skipped` | **Yes** | Bypassed due to trigger rule (upstream non-success) |
| `cancelled` | **Yes** | Stopped by user action or parent flow closure |

**Transitions:**
- `pending → skipped`: When trigger rule evaluates and upstream has non-success terminal state
- `pending → cancelled`: External cancellation before execution starts
- `running → cancelled`: External cancellation during execution
- `running → attempt_failed → retrying → running`: Retry loop continues
- `retrying → failed`: When retry count exceeds `retries` config (max retries exceeded)

**Note:** `max_retries_exceeded` in the diagram is an **event** (transition condition), not a state.

### Dependency and Trigger Model

**Design principle:** Separate data source from execution trigger:
- `from` → **where data comes from** (data dependency)
- `triggers on` → **when to execute** (execution condition)
- `with { }` → **how to execute** (retry, timeout, backoff)

### Trigger Clause Syntax

Use `triggers on <condition>` as a **prefix clause** before `=`:

```wv
stage <name> [triggers on <condition>] [with { config }] = <body>
```

**Trigger conditions** use function-like predicates with **single stage argument**:

| Condition | Meaning |
|-----------|---------|
| `success(A)` | Stage A succeeded |
| `failure(A)` | Stage A failed (after all retries) |
| `done(A)` | Stage A reached any terminal state |

**Boolean operators** for combining conditions:
- `and` — both conditions must be true (higher precedence)
- `or` — either condition must be true (lower precedence)
- `()` — parentheses for explicit grouping

**Operator precedence:** `and` binds tighter than `or`:
- `success(A) or success(B) and failure(C)` = `success(A) or (success(B) and failure(C))`

**Shorthand predicates** for common multi-stage patterns:

| Shorthand | Equivalent |
|-----------|------------|
| `all_success(A, B)` | `success(A) and success(B)` |
| `one_success(A, B)` | `success(A) or success(B)` |
| `all_failure(A, B)` | `failure(A) and failure(B)` |
| `one_failure(A, B)` | `failure(A) or failure(B)` |

### Default Behavior

When `triggers on` is omitted, default triggers are inferred from the stage input:

| Stage Input | Default Trigger | Meaning |
|-------------|-----------------|---------|
| `from A` | `success(A)` | Run when A succeeds |
| `from A, B` | `all_success(A, B)` | Run when both A and B succeed |
| `from 'file.csv'` | (none) | Run immediately (literal source) |
| `call ChildFlow()` | (none) | Run immediately |
| `depends on A` | `success(A)` | Run when A succeeds (no data) |
| `merge A, B` | `all_success(A, B)` | Run when both A and B succeed |

**Examples:**
```wv
-- These are equivalent:
stage B = from A | transform()
stage B triggers on success(A) = from A | transform()

-- Literal source has no default trigger (runs immediately)
stage load = from 'data.csv' | parse()

-- Explicit trigger required for non-standard behavior
stage fallback triggers on failure(primary) = from 'backup.csv' | parse()
```

### Examples

```wv
flow ResilientPipeline = {
  -- Simple: run when A succeeds (default behavior)
  stage process = from load | transform()

  -- Explicit trigger: same as above but explicit
  stage process triggers on success(load) = from load | transform()

  -- Fallback: run when primary fails, get data from source (not primary)
  stage fallback triggers on failure(primary) = from source | backup_api()

  -- Fan-in: run when either A or B succeeds
  stage merge triggers on one_success(A, B) = from A, B | combine()

  -- Mixed: get data from A, but only run when B fails
  stage recover triggers on failure(B) = from A | recovery_logic()

  -- Complex: run when A succeeds AND B fails
  stage conditional triggers on success(A) and failure(B) = from A | special_case()
}
```

### Trigger Evaluation

Triggers evaluate based on **terminal states** of referenced stages:

| Terminal State | Matches |
|----------------|---------|
| `success` | `success()`, `one_success()`, `all_success()`, `done()` |
| `failed` | `failure()`, `one_failure()`, `all_failure()`, `done()` |
| `skipped` | `done()` only |
| `cancelled` | `done()` only |

**Note:** `cancelled` and `skipped` do NOT match `success()` or `failure()` predicates.

---

## Type System Extensions

### Duration Literals

Introduce typed duration literals (not strings):

```wv
-- Duration literal syntax: number followed by unit
5m      -- 5 minutes
30s     -- 30 seconds
2h      -- 2 hours
1d      -- 1 day
100ms   -- 100 milliseconds
```

Duration units: `ms`, `s`, `m`, `h`, `d`

### Backoff Strategy Enum

```wv
-- Backoff strategies are keywords, not strings
backoff: constant     -- Fixed delay
backoff: linear       -- Linearly increasing delay
backoff: exponential  -- Exponentially increasing delay (default)
```

---

## Grammar Rules

### Flow Definition

```
flow_def := "flow" IDENT [flow_params] [flow_config] "=" "{" stage_def* "}"

flow_params := "(" param_list ")"

param_list := param_def ("," param_def)*

param_def := IDENT ":" type_expr ["=" expression]  -- name: type [= default]

flow_config := "with" "{" flow_config_items "}"

flow_config_items := (flow_config_item NEWLINE)*

flow_config_item := "schedule" ":" schedule_expr
                  | "timezone" ":" STRING
                  | "concurrency" ":" INTEGER
                  | "timeout" ":" DURATION

schedule_expr := "cron" "(" STRING ")"
               | "interval" "(" DURATION ")"
```

### Stage Definition

```
stage_def := "stage" IDENT [trigger_clause] [stage_config] "=" stage_body

trigger_clause := "triggers" "on" trigger_or_expr

-- Precedence: 'and' binds tighter than 'or'
trigger_or_expr := trigger_and_expr ("or" trigger_and_expr)*

trigger_and_expr := trigger_primary ("and" trigger_primary)*

trigger_primary := trigger_predicate
                 | "(" trigger_or_expr ")"

-- Single-stage predicates
trigger_predicate := "success" "(" IDENT ")"
                   | "failure" "(" IDENT ")"
                   | "done" "(" IDENT ")"
                   -- Multi-stage shorthands
                   | "all_success" "(" stage_list ")"
                   | "one_success" "(" stage_list ")"
                   | "all_failure" "(" stage_list ")"
                   | "one_failure" "(" stage_list ")"

stage_list := IDENT ("," IDENT)+  -- requires 2+ stages

stage_config := "with" "{" stage_config_items "}"

stage_config_items := (stage_config_item NEWLINE)*

stage_config_item := "retries" ":" INTEGER
                   | "timeout" ":" DURATION
                   | "retry_delay" ":" DURATION
                   | "max_retry_delay" ":" DURATION
                   | "heartbeat" ":" DURATION
                   | "backoff" ":" BACKOFF_STRATEGY
                   | "parent_close" ":" PARENT_CLOSE_POLICY
                   | "idempotency_key" ":" IDENT

stage_body := stage_input [pipe_chain]

stage_input := "from" stage_ref_list
             | "call" flow_call
             | "merge" stage_ref_list [join_clause]
             | "depends" "on" IDENT  -- control-only, no data

stage_ref_list := stage_ref ("," stage_ref)*

stage_ref := IDENT                    -- reference to another stage
           | STRING                   -- literal data source (file, table)
           | expression               -- inline data expression

pipe_chain := ("|" pipe_op)*

pipe_op := query_operator             -- existing wvlet operators: select, where, group by, etc.
         | flow_operator              -- route, fork, wait, activate, end

-- Note: query_operator and flow_operator are defined in existing wvlet grammar
-- This document extends the grammar with task configuration, not redefines base operators

flow_call := IDENT "(" [call_args] ")"

call_args := call_arg ("," call_arg)*

call_arg := IDENT ":" expression      -- named argument
          | expression                -- positional argument

join_clause := "on" expression
```

### Tokens and Literals

```
DURATION := INTEGER DURATION_UNIT
DURATION_UNIT := "ms" | "s" | "m" | "h" | "d"

BACKOFF_STRATEGY := "constant" | "linear" | "exponential"

PARENT_CLOSE_POLICY := "terminate" | "request_cancel" | "abandon"

-- Note: Trigger predicates (success, failure, done, all_success, etc.)
-- are defined in trigger_predicate rule above, not as separate tokens
```

### Precedence

1. `with { }` binds to the immediately preceding `flow` or `stage` name
2. `=` separates config from body
3. `|` chains operations within stage body
4. `call` is a stage body expression for child flow invocation

---

## Syntax Design

### Core Syntax: Stage Configuration (v1 Scope)

Extend stages with task configuration using `with { }` block:

```wv
flow DataPipeline = {
  stage extract with {
    retries: 3
    timeout: 5m
    retry_delay: 1s
    backoff: exponential
  } = from api_source | fetch_data()

  stage transform = from extract | normalize()

  stage load with {
    retries: 5
    timeout: 10m
    heartbeat: 30s
  } = from transform | save to warehouse
}
```

**Stage-level properties:**

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `retries` | Int | 0 | Max retry attempts |
| `timeout` | Duration | none | Start-to-close timeout |
| `retry_delay` | Duration | 1s | Initial retry delay |
| `backoff` | Enum | exponential | Backoff strategy |
| `max_retry_delay` | Duration | none | Cap on backoff delay |
| `heartbeat` | Duration | none | Heartbeat interval for long-running stages |

---

### Error Handling with Triggers

Explicit error handling using `triggers on` clause:

```wv
flow ResilientPipeline = {
  -- Primary stage with retries
  stage primary with {
    retries: 3
  } = from source | call_primary_api()

  -- Fallback: runs only if primary fails, gets fresh data from source
  stage fallback triggers on failure(primary) = from source | call_backup_api()

  -- Continue: runs if either primary or fallback succeeds
  stage continue triggers on one_success(primary, fallback) = from primary, fallback | process()
}
```

**Execution semantics:**

1. `source` runs first (no trigger = runs immediately)
2. `primary` runs after `source` succeeds (implicit `triggers on success(source)`)
3. `fallback` triggers on `failure(primary)`:
   - Waits for `primary` to reach terminal `failed` state
   - Gets data from `source` (not from `primary`)
4. `continue` triggers on `one_success(primary, fallback)`:
   - Runs when either `primary` OR `fallback` reaches `success`

**Key benefits of this syntax:**
- Clear separation: `triggers on` controls WHEN, `from` controls WHERE data comes from
- Readable: `triggers on failure(X)` clearly means "run when X fails"
- Composable: boolean operators allow complex conditions

---

### Flow-Level Configuration

Flow-level properties apply to the entire flow:

```wv
flow DailyETL with {
  schedule: cron('0 2 * * *')  -- 2 AM daily
  timezone: 'UTC'
  concurrency: 1               -- max concurrent executions
} = {
  stage extract = from source | fetch_daily_data()
  stage transform = from extract | clean_and_normalize()
  stage load = from transform | save to warehouse
}
```

**Flow-level properties:**

| Property | Type | Description |
|----------|------|-------------|
| `schedule` | Schedule | Cron or interval schedule |
| `timezone` | String | Timezone for schedule |
| `concurrency` | Int | Max concurrent flow executions |
| `timeout` | Duration | Total flow timeout |

---

### Child Flow Invocation

Use `call` keyword for explicit child flow invocation:

```wv
flow ParentFlow = {
  stage setup = from config | initialize()

  -- Invoke child flow with explicit call syntax
  stage child with {
    timeout: 1h
    parent_close: abandon  -- terminate | request_cancel | abandon
  } = call ChildFlow(param: setup.value)

  stage cleanup = from child | finalize()
}

flow ChildFlow(param: string) = {
  stage work = from param | long_running_task()
}
```

**Parent close policies:**
- `terminate` - forcefully stop child when parent closes (default)
- `request_cancel` - send cancellation request to child
- `abandon` - let child continue independently

---

### Idempotency Keys

Ensure idempotent execution with explicit key specification:

```wv
flow PaymentProcessing = {
  stage charge with {
    retries: 3
    idempotency_key: transaction_id  -- field name from input schema
  } = from payment_request | process_payment()
}
```

The `idempotency_key` references a field from the stage's input relation schema.

---

## Future Extensions (Out of Scope for v1)

These features are documented for future consideration but not included in the initial implementation:

### Signals and Queries (Temporal Pattern)

```wv
-- Future: External interaction with running flows
flow LongRunning with {
  signal pause: () -> set_paused(true)
  signal resume: () -> set_paused(false)
  query status: () -> current_status()
} = {
  stage loop = from source | process_batch()
}
```

### Saga Pattern for Compensation

```wv
-- Future: Transaction-like semantics
flow BookingFlow = {
  stage book_flight with {
    compensate: cancel_flight  -- runs on rollback
  } = from request | reserve_flight()

  stage book_hotel with {
    compensate: cancel_hotel
  } = from book_flight | reserve_hotel()
}
```

### Workflow Versioning

```wv
-- Future: Versioned flow definitions
flow OrderProcess with {
  version: 2
} = {
  stage validate = from order | validate_inventory()
  stage process = from validate | process_order()
}

-- Invoke specific version
stage order = call OrderProcess(version: 1)
```

### Dynamic Task Mapping

```wv
-- Future: Fan-out over dynamic inputs
flow BatchProcess = {
  stage list_files = from bucket | list_objects()

  stage process_each with {
    map: list_files.files  -- creates N parallel stages
  } = from _ | process_file()
}
```

---

## Recommended Implementation Phases

### Phase 1: Core Stage Configuration
- `retries`, `timeout`, `retry_delay`, `backoff` properties
- Duration literal parsing
- Backoff strategy enum
- Basic `with { }` syntax

### Phase 2: Trigger Rules and Error Handling
- Stage state model
- `trigger` property with rules
- `on_failure` conditional dependencies

### Phase 3: Flow-Level Configuration
- `schedule` and `timezone`
- `concurrency` limits
- Flow-level `timeout`

### Phase 4: Child Flow Invocation
- `call` keyword for child flows
- `parent_close` policy
- Child flow parameter passing

---

## Design Principles

1. **Backward compatible** - existing flows work unchanged
2. **SQL-inspired** - familiar syntax for data engineers
3. **Explicit over implicit** - clear dependency and execution semantics
4. **Declarative** - configuration over imperative code
5. **Type-safe** - duration literals and enums prevent errors
6. **Unambiguous grammar** - single syntax form, clear precedence

---

## Open Questions

1. Should `heartbeat` trigger automatic stage restart or just monitoring?
2. How should `schedule` interact with manual flow triggers?
3. Should child flows inherit parent's retry configuration by default?
4. How to handle circular dependencies in `on_failure` chains?
