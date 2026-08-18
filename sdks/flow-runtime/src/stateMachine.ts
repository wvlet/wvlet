/**
 * The wvlet stage state machine and trigger-evaluation rules, as data.
 *
 * This mirrors the "Stage Execution Model" section of website/docs/syntax/flow.md
 * exactly; executors should drive their scheduling off these tables rather than
 * re-encoding the rules in host code.
 */

/** All stage states. */
export type StageState =
  | 'pending'
  | 'running'
  | 'success'
  | 'attempt_failed'
  | 'retrying'
  | 'failed'
  | 'skipped'
  | 'cancelled'

/** Terminal states: once entered, a stage's state never changes again. */
export type TerminalStageState = 'success' | 'failed' | 'skipped' | 'cancelled'

export const STAGE_STATES: readonly StageState[] = [
  'pending',
  'running',
  'success',
  'attempt_failed',
  'retrying',
  'failed',
  'skipped',
  'cancelled',
]

export const TERMINAL_STAGE_STATES: readonly TerminalStageState[] = [
  'success',
  'failed',
  'skipped',
  'cancelled',
]

export function isTerminal(state: StageState): state is TerminalStageState {
  return (TERMINAL_STAGE_STATES as readonly StageState[]).includes(state)
}

/** One edge of the stage state machine. */
export interface StageTransition {
  from: StageState
  to: StageState
  /** The event/condition that fires the transition. */
  on: string
}

/**
 * The full transition table of the stage state machine (the mermaid diagram in
 * flow.md, row by row).
 */
export const STAGE_TRANSITIONS: readonly StageTransition[] = [
  { from: 'pending', to: 'running', on: 'dependencies satisfied, attempt starts' },
  { from: 'pending', to: 'skipped', on: 'trigger rule evaluates upstream non-success' },
  { from: 'pending', to: 'cancelled', on: 'user/parent cancellation' },
  { from: 'running', to: 'success', on: 'attempt completed' },
  { from: 'running', to: 'attempt_failed', on: 'attempt raised an error' },
  { from: 'running', to: 'cancelled', on: 'user/parent cancellation' },
  { from: 'attempt_failed', to: 'retrying', on: 'retries remaining' },
  { from: 'retrying', to: 'running', on: 'retry delay elapsed' },
  { from: 'attempt_failed', to: 'failed', on: 'max retries exceeded' },
  { from: 'retrying', to: 'failed', on: 'max retries exceeded' },
]

/** True when `from -> to` is a legal transition of the stage state machine. */
export function canTransition(from: StageState, to: StageState): boolean {
  return STAGE_TRANSITIONS.some((t) => t.from === from && t.to === to)
}

/**
 * The kinds of dependency edges a stage can have on an upstream stage/flow:
 *   - `from`: data dependency (`from upstream` in a stage body)
 *   - `depends_on`: control dependency (`depends on` clause)
 *   - `if_failed`: failure trigger (`if upstream.failed`)
 *   - `if_done`: completion trigger (`if upstream.done`)
 */
export type TriggerKind = 'from' | 'depends_on' | 'if_failed' | 'if_done'

/**
 * Trigger evaluation table: given the upstream's terminal state and the edge
 * kind, does the downstream stage run (true) or get skipped (false)?
 *
 * This is the "Trigger Evaluation" table of flow.md verbatim.
 */
export const TRIGGER_TABLE: Readonly<
  Record<TerminalStageState, Readonly<Record<TriggerKind, boolean>>>
> = {
  success: { from: true, depends_on: true, if_failed: false, if_done: true },
  failed: { from: false, depends_on: false, if_failed: true, if_done: true },
  skipped: { from: false, depends_on: false, if_failed: false, if_done: true },
  cancelled: { from: false, depends_on: false, if_failed: false, if_done: true },
}

/**
 * Evaluate a single dependency edge: should the downstream run, given the
 * upstream's terminal state?
 */
export function triggerSatisfied(upstream: TerminalStageState, kind: TriggerKind): boolean {
  return TRIGGER_TABLE[upstream][kind]
}
