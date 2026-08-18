/**
 * Canonical SQL operations over the flow-run message bus (see sql/bus-schema.sql).
 *
 * These statements are the contract: an executor may inline them, prepare them,
 * or port them to another SQLite binding, but the semantics (runtime stamping,
 * CAS claim, monotonic cursor) must be preserved.
 *
 * All statements use named parameters in SQLite `:name` syntax.
 */

/**
 * Append a message to the bus.
 *
 * `:from_member` MUST be stamped by the runtime that performs the append, never
 * taken from agent-controlled payload content — this is what makes the sender
 * field trustworthy for every consumer.
 */
export const BUS_APPEND = `
INSERT INTO bus (run_id, from_member, kind, payload, correlation_id)
VALUES (:run_id, :from_member, :kind, :payload, :correlation_id)
`.trim()

/**
 * Atomically claim a message via compare-and-swap.
 *
 * Exactly one claimant can win: the UPDATE only matches while `claimed_by` is
 * still NULL. Callers MUST check the statement's change count — 1 means the
 * claim was won, 0 means another consumer already holds it.
 */
export const BUS_CLAIM = `
UPDATE bus
SET claimed_by = :claimed_by, claimed_at = datetime('now')
WHERE seq = :seq AND claimed_by IS NULL
`.trim()

/**
 * Advance a consumer's cursor to `:last_seq`.
 *
 * Monotonic: an attempt to move the cursor backwards keeps the stored maximum,
 * so replayed or out-of-order acknowledgements can never lose progress.
 */
export const BUS_CURSOR_ADVANCE = `
INSERT INTO bus_cursor (run_id, consumer, last_seq)
VALUES (:run_id, :consumer, :last_seq)
ON CONFLICT (run_id, consumer)
DO UPDATE SET last_seq = max(last_seq, excluded.last_seq)
`.trim()

/** Read a consumer's current cursor position (0 when the consumer is new). */
export const BUS_CURSOR_GET = `
SELECT coalesce(
  (SELECT last_seq FROM bus_cursor WHERE run_id = :run_id AND consumer = :consumer),
  0
) AS last_seq
`.trim()

/**
 * Pinned-wait poll: fetch messages after a cursor position, optionally filtered
 * by kind and/or correlation id (pass NULL to leave a filter open).
 *
 * A consumer waiting for the reply to a specific request polls with its
 * `:correlation_id` pinned; a topic subscriber polls with `:kind` pinned.
 */
export const BUS_POLL = `
SELECT seq, run_id, from_member, kind, payload, correlation_id, claimed_by, claimed_at, posted_at
FROM bus
WHERE run_id = :run_id
  AND seq > :after_seq
  AND (:kind IS NULL OR kind = :kind)
  AND (:correlation_id IS NULL OR correlation_id = :correlation_id)
ORDER BY seq
LIMIT :limit
`.trim()
