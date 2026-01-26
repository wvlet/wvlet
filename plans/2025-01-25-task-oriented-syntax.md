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

**Design principle:** Clear separation between stages and flows:

| Scope | Data + Success | Error/Cleanup |
|-------|----------------|---------------|
| **Stage** | `from A` | `if A.failed`, `if A.done` |
| **Flow** | `depends on FlowA` | `if FlowA.failed`, `if FlowA.done` |

**Stage dependencies** (within a flow):
- `from A` → data from A, runs when A succeeds (implicit)
- `if A.failed` → run when A fails
- `if A.done` → run when A finishes (any state)

**Flow dependencies** (cross-flow):
- `depends on FlowA` → run when FlowA succeeds
- `if FlowA.failed` → run when FlowA fails
- `if FlowA.done` → run when FlowA finishes (any state)

**Configuration:**
- `with { }` → **how to execute** (retry, timeout, backoff)

### Stage Trigger Syntax

Stages use `from` for data dependencies (success implied), with optional `if` override:

```wv
stage <name> [if <condition>] [with { config }] = <body>
```

**State predicates** (for `if` clause):

| Predicate | Meaning |
|-----------|---------|
| `A.failed` | Stage A failed (after all retries) |
| `A.done` | Stage A reached any terminal state |

**Boolean operators** for combining conditions:
- `and` — both conditions must be true (higher precedence)
- `or` — either condition must be true (lower precedence)
- `()` — parentheses for explicit grouping

**Examples:**
```wv
-- Data dependency (success implied)
stage transform = from extract | clean()
stage merge = from A, B | combine()

-- Error handling
stage fallback if primary.failed = from backup | recover()

-- Cleanup (runs regardless of success/failure)
stage cleanup if process.done = call cleanup_service()

-- Complex error handling
stage alert if A.failed or B.failed = call send_alert()
```

### Flow Dependency Syntax

Flows use `depends on` for success, `if` for error/cleanup:

```wv
flow <name> depends on <FlowName> [with { config }] = { ... }
flow <name> if <FlowName>.<state> [with { config }] = { ... }
```

**Examples:**
```wv
-- Run after another flow succeeds
flow Reporting depends on DailyETL = {
  stage generate = from warehouse | create_report()
}

-- Run after another flow fails (recovery flow)
flow Recovery if DailyETL.failed = {
  stage alert = call send_alert()
  stage retry = call manual_retry()
}

-- Run after another flow finishes (cleanup)
flow Cleanup if DailyETL.done = {
  stage archive = from logs | compress()
}
```

### Default Behavior

**Stages:** The `from` clause implies success dependency on the source:

| Stage Input | Implied Trigger | Meaning |
|-------------|-----------------|---------|
| `from A` | A succeeds | Run when A succeeds |
| `from A, B` | A and B succeed | Run when both succeed |
| `from 'file.csv'` | (none) | Run immediately (literal source) |
| `call ChildFlow()` | (none) | Invoked when parent stage executes |

**Flows:** No implicit dependency. Use `depends on` explicitly.

**Examples:**
```wv
flow Pipeline = {
  -- 'from' implies success dependency
  stage extract = from 'data.csv' | parse()      -- runs immediately
  stage transform = from extract | clean()       -- runs when extract succeeds
  stage load = from transform | save()           -- runs when transform succeeds

  -- 'if' for error/cleanup
  stage fallback if extract.failed = from backup | recover()
  stage notify if load.done = call notify_service()
}

-- Flow dependency requires explicit 'depends on'
flow Reporting depends on Pipeline = {
  stage report = from warehouse | generate()
}
```

### Examples

```wv
flow ResilientPipeline = {
  -- Data dependency via 'from' (success implied)
  stage extract = from source | fetch_data()
  stage transform = from extract | clean()
  stage load = from transform | save to warehouse

  -- Error handling with 'if'
  stage fallback if extract.failed = from backup_source | fetch_data()

  -- Fan-in: data from multiple stages
  stage merge = from A, B | combine()

  -- Cleanup: runs when load finishes (any state)
  stage cleanup if load.done = call cleanup_service()

  -- Alert: runs if any stage fails
  stage alert if extract.failed or transform.failed = call send_alert()
}
```

### Cross-Flow Dependencies

Flows use `depends on` for success, `if` for error/cleanup:

```wv
-- Flow runs after another flow succeeds
flow Reporting depends on DailyETL = {
  stage generate = from warehouse | create_report()
  stage publish = from generate | upload_to_dashboard()
}

-- Flow with schedule AND dependency (both must be satisfied)
-- Runs at 3 AM only if Ingestion has succeeded since last run
flow Analytics depends on Ingestion with {
  schedule: cron('0 3 * * *')
} = {
  stage analyze = from data | run_analytics()
}

-- Note: When both 'depends on' and 'schedule' are specified:
-- - The schedule determines WHEN to check the dependency
-- - The dependency determines IF the flow actually runs
-- - If Ingestion hasn't succeeded, the scheduled run is skipped

-- Recovery flow: runs when Pipeline fails
flow Recovery if Pipeline.failed = {
  stage alert = call send_alert()
  stage log = call log_failure()
}

-- Cleanup flow: runs when Pipeline finishes (any state)
flow Cleanup if Pipeline.done = {
  stage archive = from logs | compress()
}
```

### Trigger Evaluation

Triggers evaluate based on **terminal states**:

| Terminal State | `from A` (stage) | `depends on` (flow) | `if X.failed` | `if X.done` |
|----------------|------------------|---------------------|---------------|-------------|
| `success` | ✓ runs | ✓ runs | — | ✓ runs |
| `failed` | — skipped | — skipped | ✓ runs | ✓ runs |
| `skipped` | — skipped | — skipped | — | ✓ runs |
| `cancelled` | — skipped | — skipped | — | ✓ runs |

**Summary:**
- `from A` (stages): Implicit success trigger from data source
- `depends on FlowA` (flows): Explicit success dependency
- `if X.failed`: Error handling (runs only on failure)
- `if X.done`: Cleanup (runs on any terminal state)

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
flow_def := "flow" IDENT [flow_dependency] [flow_params] [flow_config] "=" "{" stage_def* "}"

-- Flow-level dependencies (cross-flow)
flow_dependency := "depends" "on" IDENT          -- success dependency
                 | "if" IDENT "." STATE_NAME     -- error/cleanup dependency

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
stage_def := "stage" IDENT [trigger_override] [stage_config] "=" stage_body

-- Stage trigger override (for error handling / cleanup)
-- Note: Success dependency is implicit via 'from' clause in stage_body
trigger_override := "if" trigger_expr

-- Precedence: 'and' binds tighter than 'or'
trigger_expr := trigger_and ("or" trigger_and)*

trigger_and := trigger_primary ("and" trigger_primary)*

trigger_primary := state_predicate
                 | "(" trigger_expr ")"

-- Property-access style predicates (flow-style, left-to-right)
state_predicate := IDENT "." STATE_NAME

STATE_NAME := "failed" | "done"

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

-- Note: Stages use 'from' for data dependencies (success implied).
-- 'depends on' is only used at the flow level for cross-flow dependencies.

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

Explicit error handling using `if` clause:

```wv
flow ResilientPipeline = {
  -- Primary stage with retries
  stage primary with {
    retries: 3
  } = from source | call_primary_api()

  -- Fallback: runs only if primary fails
  stage fallback if primary.failed = from source | call_backup_api()

  -- Continue: runs after both primary and fallback complete
  stage continue = from primary, fallback | process()
}
```

**Execution semantics:**

1. `source` runs first (literal source = runs immediately)
2. `primary` runs after `source` succeeds (implicit from `from source`)
3. `fallback` triggers if `primary.failed`:
   - Waits for `primary` to reach terminal `failed` state
   - Gets fresh data from `source` (not from `primary`)
4. `continue` runs when both `primary` and `fallback` reach terminal state:
   - Uses data from the successful upstream path (either `primary` or `fallback`)

**Key benefits:**
- **Simple**: `from` for data + success trigger (stages)
- **Explicit**: `depends on` for flow-level dependencies
- **Readable**: `if primary.failed` reads naturally
- **Clear separation**: `from` = data source, `if` = conditional trigger

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
4. How to handle circular dependencies in `if X.failed` chains?

### Circular Dependency Detection

Circular dependencies in error handling chains (e.g., `A if B.failed`, `B if A.failed`) must be
detected at compile time. The compiler should:

1. **Build a dependency graph** including both success (`from`) and failure (`if X.failed`) edges
2. **Detect cycles** using standard graph algorithms (DFS-based cycle detection)
3. **Report clear errors** identifying the cycle path (e.g., "Circular dependency: A → B → A")

**Prevention strategy:** The type checker will reject flows containing cycles in the combined
dependency graph. This is a compile-time check, not a runtime check, ensuring invalid flows
cannot be deployed.
