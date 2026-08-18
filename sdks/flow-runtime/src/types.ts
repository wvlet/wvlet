/**
 * FlowPlan: the JSON interchange format between the wvlet compiler and any
 * flow executor.
 *
 * Wvlet is the logical plan: the compiler lowers a `flow` definition into this
 * codec, and an external host (first target: Treasure Work) is the physical
 * executor. Executors should treat unknown extra properties as
 * forward-compatible extensions and ignore them.
 */

/** A compiled flow: the unit an executor schedules and runs. */
export interface FlowPlan {
  /** Flow name, unique within its compilation unit. */
  name: string
  /**
   * Flow parameters bound at run time. Well-known keys:
   *   - `max_steps`: total step budget for the run (number); the executor must
   *     stop scheduling new stage attempts once the budget is exhausted.
   *   - `token_budget`: LLM token budget for the run (number); agent-invoking
   *     executors should stop invoking agents once spent.
   */
  params: Record<string, unknown>
  /** Stages in definition order. Edges are given by each stage's `inputs`. */
  stages: Stage[]
}

/** A single stage of a flow: one node of the execution DAG. */
export interface Stage {
  /** Stage name, unique within the flow. */
  name: string
  /**
   * How the stage executes: `agent` stages are handed to the host's agent
   * runtime; `query` stages run `sql` against the run's SQLite database.
   */
  kind: 'agent' | 'query'
  /**
   * Names of upstream stages this stage reads from (`from` refs). Each input is
   * an implicit success dependency: the stage runs only when all inputs
   * succeeded, unless a trigger overrides that (see the state machine tables).
   */
  inputs: string[]
  /** Agent invocation spec; required when kind is 'agent'. */
  agent?: AgentSpec
  /**
   * Compiled SQLite SQL for query stages, and for SQL-evaluated predicates
   * (e.g. route conditions rewritten against staged tables).
   */
  sql?: string
  /** Conditional routing of this stage's output to downstream stages. */
  route?: RouteSpec
}

/** Specification of an agent invocation, interpreted by the host runtime. */
export interface AgentSpec {
  /** Named skill/command the host should invoke (e.g. a slash command). */
  skill?: string
  /** Freeform role/system-prompt hint for the agent. */
  role?: string
  /** Model identifier or tier hint; hosts map this to their own model names. */
  model?: string
  /** Tool allowlist for the invocation; omitted means host default. */
  tools?: string[]
  /** Whether the agent may edit files/workspace state (host-defined scope). */
  edit?: boolean
  /** Arguments passed to the skill/agent, already bound to flow params. */
  args?: Record<string, unknown>
  /**
   * JSON Schema the agent's structured result must validate against. When set,
   * the executor should enforce it (retry or fail the attempt on mismatch).
   */
  returnsSchema?: Record<string, unknown>
  /**
   * When true, invoke the agent once per input row instead of once per stage
   * (fan-out over the upstream result set).
   */
  perRow?: boolean
  /** Maximum concurrent agent invocations for perRow fan-out. */
  workers?: number
}

/** Conditional routing: the compiled form of wvlet's `route { ... }` operator. */
export interface RouteSpec {
  /** Cases evaluated in order; the first matching case wins. */
  cases: RouteCase[]
  /** Stage to route to when no case matches. */
  elseTarget?: string
}

/** One `case <condition> -> <target>` arm of a route. */
export interface RouteCase {
  /**
   * SQLite boolean expression evaluated against the stage's output (compiled
   * from the wvlet condition, e.g. `_.age > 18`).
   */
  conditionSql: string
  /** Name of the stage that receives rows matching the condition. */
  target: string
}

/** A row appended to the flow-run message bus (see sql/bus-schema.sql). */
export interface BusMessage {
  seq: number
  runId: string
  /** Sender identity, stamped by the runtime — never by the agent. */
  fromMember: string
  /** Topic of the message (e.g. 'stage_state', 'ask', 'reply'). */
  kind: string
  /** JSON-encoded payload. */
  payload: string
  /** Correlates request/response pairs across members. */
  correlationId?: string
  claimedBy?: string
  claimedAt?: string
  postedAt: string
}
