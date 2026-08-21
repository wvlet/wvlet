-- Wvlet flow runtime: per-run message log ("bus") schema for SQLite.
--
-- The bus is an append-only log shared by every member of a flow run (the
-- scheduler, agent stages, query stages, and the host application). Executors
-- implement the flow contract against this schema; the wvlet compiler never
-- touches it directly.
--
-- Design notes:
--   * `from_member` is stamped by the runtime when a row is appended — never by
--     the agent that produced the payload — so recipients can trust the sender.
--   * `correlation_id` threads request/response pairs (e.g. an ask and its
--     reply) without imposing a topic hierarchy.
--   * `claimed_by`/`claimed_at` support at-most-once handoff of work items via
--     an atomic compare-and-swap (see BUS_CLAIM in the package exports).
--   * This shape is intentionally isomorphic to Treasure Work's peer_message
--     row (from-stamping + correlation threading), the first executor target.

CREATE TABLE IF NOT EXISTS bus (
  seq            INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id         TEXT NOT NULL,
  from_member    TEXT NOT NULL,
  kind           TEXT NOT NULL,
  payload        TEXT NOT NULL,
  correlation_id TEXT,
  claimed_by     TEXT,
  claimed_at     TEXT,
  posted_at      TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS bus_run_seq_idx ON bus (run_id, seq);
CREATE INDEX IF NOT EXISTS bus_run_kind_seq_idx ON bus (run_id, kind, seq);
CREATE INDEX IF NOT EXISTS bus_correlation_seq_idx ON bus (correlation_id, seq);

-- Per-consumer read cursor: each consumer tracks the highest seq it has
-- processed. Advancing is monotonic (see BUS_CURSOR_ADVANCE).
CREATE TABLE IF NOT EXISTS bus_cursor (
  run_id   TEXT NOT NULL,
  consumer TEXT NOT NULL,
  last_seq INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, consumer)
);
