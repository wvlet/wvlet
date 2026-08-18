# @wvlet/flow-runtime

**Experimental** contract package for executing Wvlet agent flows. Wvlet is the
logical plan; external hosts (first target: [Treasure Work]) are the physical
executors. This package defines the contract an executor implements — it is
intentionally **not** a framework:

- `sql/bus-schema.sql` — the per-run SQLite message log ("bus"): append-only
  rows with runtime-stamped senders, correlation threading, and CAS-claimable
  work items. Intentionally isomorphic to Treasure Work's peer_message row.
- `src/busSql.ts` — canonical bus operations: append (runtime-stamped),
  atomic claim (compare-and-swap), monotonic per-consumer cursor advance, and
  a pinned-wait poll query.
- `src/types.ts` — the `FlowPlan` JSON interchange types: the wire format
  between the wvlet compiler and any executor.
- `src/stateMachine.ts` — the stage state machine and trigger-evaluation
  rules as data, mirroring the [Stage Execution Model](https://wvlet.org/wvlet/docs/syntax/flow#stage-execution-model)
  docs exactly.

No runtime dependencies. Tests use only Node built-ins (`node:test`,
`node:sqlite`; Node 22.5+ with `--experimental-sqlite`, unflagged in Node 23+):

```bash
pnpm --filter @wvlet/flow-runtime test
```

This contract is a prototype and may change without notice; it is not yet
published to npm.
