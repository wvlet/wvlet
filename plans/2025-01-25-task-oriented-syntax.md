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

### Dependency Rules

Stages form a DAG based on three types of edges:

| Edge Type | Syntax | Trigger Behavior |
|-----------|--------|------------------|
| **Data dependency** | `from stage_name` | Requires upstream `success` (default `all_success` trigger) |
| **Control dependency** | `depends on stage_name` | Requires upstream `success` (default `all_success` trigger) |
| **Failure dependency** | `on_failure: stage_name` | Requires upstream `failed` (implicit `all_failed` trigger) |

**Important:** `on_failure` modifies trigger evaluation:

1. **Without explicit `trigger`:** Each dependency type has its own requirement:
   - Data dependencies (`from`) require `success`
   - Control dependencies (`depends on`) require `success`
   - Failure dependencies (`on_failure`) require `failed`
   - ALL requirements must be met for the stage to run

2. **With explicit `trigger`:** The `trigger` rule applies to success-based dependencies only:
   - `on_failure` dependencies still require `failed` (not affected by `trigger`)
   - `trigger` modifies how `from` and `depends on` are evaluated
   - Example: `trigger: one_success` + `on_failure: X` means "X must fail AND at least one other upstream must succeed"

3. **`cancelled` and `skipped` handling:**
   - `on_failure` triggers ONLY on `failed` state (not `cancelled` or `skipped`)
   - `cancelled` is treated as non-success for success-based triggers
   - `skipped` is treated as non-failure for `none_failed` trigger

### Trigger Rules

Determine when a stage executes based on **terminal states** of upstream stages:

| Rule | Runs when... | Skipped when... |
|------|--------------|-----------------|
| `all_success` | ALL upstreams are `success` | Any upstream is `failed`, `skipped`, or `cancelled` |
| `one_success` | AT LEAST ONE upstream is `success` | ALL upstreams are `failed`, `skipped`, or `cancelled` |
| `none_failed` | NO upstream is `failed` (allows `success`, `skipped`) | Any upstream is `failed` |
| `all_done` | ALL upstreams reached terminal state | Never (always runs) |
| `always` | Immediately when dependencies allow | Never (always runs) |

**Note:** `cancelled` is treated as a non-success terminal state. Stages with `cancelled` upstreams will be skipped under `all_success` but may run under `one_success` if other upstreams succeeded.

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
stage_def := "stage" IDENT [stage_config] "=" stage_body

stage_config := "with" "{" stage_config_items "}"

stage_config_items := (stage_config_item NEWLINE)*

stage_config_item := "retries" ":" INTEGER
                   | "timeout" ":" DURATION
                   | "retry_delay" ":" DURATION
                   | "max_retry_delay" ":" DURATION
                   | "heartbeat" ":" DURATION
                   | "backoff" ":" BACKOFF_STRATEGY
                   | "trigger" ":" TRIGGER_RULE
                   | "on_failure" ":" IDENT
                   | "parent_close" ":" PARENT_CLOSE_POLICY
                   | "idempotency_key" ":" IDENT

stage_body := "from" stage_ref_list [pipe_chain]
            | "call" flow_call
            | "merge" stage_ref_list [join_clause]
            | "depends" "on" IDENT

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

TRIGGER_RULE := "all_success" | "one_success" | "none_failed"
              | "all_done" | "always"

PARENT_CLOSE_POLICY := "terminate" | "request_cancel" | "abandon"
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

### Error Handling with Trigger Rules

Explicit error handling with clear dependency semantics:

```wv
flow ResilientPipeline = {
  stage primary with {
    retries: 3
  } = from source | call_primary_api()

  -- Runs only if primary fails (after all retries exhausted)
  -- on_failure creates a failure dependency (waits for primary to fail)
  -- from source creates a data dependency (requires source success)
  stage fallback with {
    on_failure: primary
  } = from source | call_backup_api()

  -- Runs if either primary or fallback succeeds
  stage continue with {
    trigger: one_success
  } = from primary, fallback | process()
}
```

**Execution semantics:**

1. `source` runs first
2. `primary` runs after `source` succeeds (data dependency)
3. `fallback` has two dependencies:
   - `from source` → requires `source` to be `success`
   - `on_failure: primary` → requires `primary` to be `failed`
   - Both conditions must be met: source succeeded AND primary failed
4. `continue` runs when either `primary` OR `fallback` reaches `success`

**Key rules:**
- `on_failure` does NOT override other dependencies; all must be satisfied
- A stage with only `on_failure` (no `from`) will have no data input
- `trigger: one_success` evaluates ALL listed upstreams (`primary, fallback`)

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
