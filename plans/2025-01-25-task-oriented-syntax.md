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

## Syntax Design Ideas

### Idea 1: Stage Configuration with `with` Clause

Extend stages with task configuration using `with { }` block:

```wv
flow DataPipeline = {
  stage extract with {
    retries: 3
    timeout: '5m'
    retry_delay: '1s'
    backoff: exponential
  } = from api_source | fetch_data()

  stage transform = from extract | normalize()

  stage load with {
    retries: 5
    timeout: '10m'
    heartbeat: '30s'
  } = from transform | save to warehouse
}
```

**Properties (inspired by Temporal):**
- `retries` - max retry attempts (0 = no retry, default)
- `timeout` - start-to-close timeout
- `schedule_timeout` - schedule-to-close timeout (total including retries)
- `retry_delay` - initial retry interval
- `backoff` - `constant`, `linear`, `exponential`
- `max_retry_delay` - cap on backoff delay
- `heartbeat` - heartbeat interval for long-running stages

---

### Idea 2: Activity-Style Stage Separation

Separate orchestration (`flow`) from execution (`activity`):

```wv
-- Define reusable activities with default configs
activity fetch_api with {
  retries: 3
  timeout: '30s'
  backoff: exponential
}

activity send_email with {
  retries: 2
  timeout: '10s'
}

-- Flow uses activities (orchestration only)
flow CustomerOnboarding = {
  stage fetch = from customer_id | fetch_api(endpoint: '/users')
  stage welcome = from fetch | send_email(template: 'welcome')
}
```

**Benefits:**
- Reusable retry/timeout configs
- Clear separation of concerns (Temporal pattern)
- Activities can be shared across flows

---

### Idea 3: Inline vs Block Configuration

Support both compact and verbose syntax:

```wv
flow Pipeline = {
  -- Compact: single property
  stage fast [timeout: '1m'] = from source

  -- Compact: multiple properties
  stage reliable [retries: 3, backoff: exponential] = from fast

  -- Block: complex configuration
  stage critical with {
    retries: 5
    timeout: '10m'
    retry_delay: '5s'
    backoff: exponential
    max_retry_delay: '2m'
    on_failure: skip
  } = from reliable | process()
}
```

---

### Idea 4: Error Handling and Fallback

Explicit error handling inspired by Temporal's non-retryable errors:

```wv
flow ResilientPipeline = {
  stage primary with {
    retries: 3
    on_failure: fallback_stage
  } = from source | call_primary_api()

  stage fallback_stage = from source | call_backup_api()

  stage continue with {
    trigger: one_success  -- run if either primary or fallback succeeds
  } = merge primary, fallback_stage | process()
}
```

**Trigger rules (from Airflow):**
- `all_success` - all upstream stages succeeded (default)
- `one_success` - at least one upstream succeeded
- `none_failed` - no upstream failed (includes skipped)
- `always` - run regardless of upstream state

---

### Idea 5: Child Flow Invocation with Policies

Support child workflow patterns with parent close policies:

```wv
flow ParentFlow = {
  stage setup = from config | initialize()

  -- Invoke child flow with policy
  stage child with {
    parent_close: abandon  -- or: terminate, request_cancel
    timeout: '1h'
  } = from setup | -> ChildFlow(param: _.value)

  stage cleanup = from child | finalize()
}

flow ChildFlow(param: string) = {
  stage work = from param | long_running_task()
}
```

---

### Idea 6: Signal and Query Handlers

Support external interaction with running flows:

```wv
flow LongRunningProcess = {
  -- Define signal handlers
  signal pause = set_paused(true)
  signal resume = set_paused(false)

  -- Define query handlers
  query status = current_status()
  query progress = completed_steps / total_steps

  stage loop with {
    continue_as_new: 1000  -- reset after 1000 events
  } = from source | while not_done() | process_batch()
}
```

---

### Idea 7: Saga Pattern for Compensating Actions

Support transaction-like semantics with compensation:

```wv
flow BookingFlow = {
  stage book_flight with {
    compensate: cancel_flight
  } = from request | reserve_flight()

  stage book_hotel with {
    compensate: cancel_hotel
  } = from book_flight | reserve_hotel()

  stage book_car with {
    compensate: cancel_car
  } = from book_hotel | reserve_car()

  -- If any stage fails, compensations run in reverse order
}
```

---

### Idea 8: Schedule and Cron Support

Define scheduled flow execution:

```wv
flow DailyETL with {
  schedule: '0 2 * * *'  -- cron: 2 AM daily
  timezone: 'UTC'
} = {
  stage extract = from source | fetch_daily_data()
  stage transform = from extract | clean_and_normalize()
  stage load = from transform | save to warehouse
}

-- Or interval-based
flow HeartbeatCheck with {
  interval: '5m'
} = {
  stage check = from services | health_check()
  stage alert = from check | where status = 'unhealthy' | send_alert()
}
```

---

### Idea 9: Idempotency Keys

Ensure idempotent execution:

```wv
flow PaymentProcessing = {
  stage charge with {
    idempotency_key: _.transaction_id  -- use field as idempotency key
    retries: 3
  } = from payment_request | process_payment()
}
```

---

### Idea 10: Workflow Versioning

Support workflow evolution:

```wv
flow OrderProcess @version(2) = {
  -- Version 2 adds new validation step
  stage validate = from order | validate_inventory()
  stage process = from validate | process_order()
}

-- Queries can specify version
from orders | -> OrderProcess @version(1)  -- use old version
```

---

## Recommended Initial Scope

For the first implementation, focus on **Idea 1** (Stage Configuration) with core properties:

```wv
flow ReliablePipeline = {
  stage load with {
    retries: 3
    timeout: '5m'
    retry_delay: '1s'
    backoff: exponential
  } = from source | validate()

  stage process = from load | transform()
}
```

**Core properties for v1:**
| Property | Type | Description |
|----------|------|-------------|
| `retries` | Int | Max retry attempts |
| `timeout` | Duration | Execution timeout |
| `retry_delay` | Duration | Initial retry delay |
| `backoff` | Enum | `constant`, `linear`, `exponential` |

**Future extensions:**
- Trigger rules and error handling (Idea 4)
- Child flow policies (Idea 5)
- Scheduling (Idea 8)
- Saga pattern (Idea 7)

---

## Design Principles

1. **Backward compatible** - existing flows work unchanged
2. **SQL-inspired** - familiar syntax for data engineers
3. **Composable** - stages can reference other stages naturally
4. **Declarative** - configuration over imperative code
5. **Deterministic orchestration** - flows coordinate, stages execute (Temporal pattern)

## Open Questions

1. Should `activity` be a separate top-level definition (Idea 2)?
2. Should we support both `[ ]` and `with { }` syntax (Idea 3)?
3. How should signals/queries work with wvlet's query model (Idea 6)?
4. Should versioning be explicit or implicit (Idea 10)?
