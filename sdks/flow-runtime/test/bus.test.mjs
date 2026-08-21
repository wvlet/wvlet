import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { DatabaseSync } from 'node:sqlite'
import {
  BUS_APPEND,
  BUS_CLAIM,
  BUS_CURSOR_ADVANCE,
  BUS_CURSOR_GET,
  BUS_POLL,
} from '../dist/index.js'

const SCHEMA = readFileSync(new URL('../sql/bus-schema.sql', import.meta.url), 'utf8')

const openBus = () => {
  const db = new DatabaseSync(':memory:')
  db.exec(SCHEMA)
  return db
}

// The runtime stamps from_member; agents never supply it themselves
const append = (db, { runId, fromMember, kind, payload, correlationId = null }) =>
  db.prepare(BUS_APPEND).run({
    run_id: runId,
    from_member: fromMember,
    kind,
    payload,
    correlation_id: correlationId,
  })

test('schema applies cleanly (and is idempotent)', () => {
  const db = openBus()
  db.exec(SCHEMA) // CREATE IF NOT EXISTS: re-applying must not fail
  const tables = db
    .prepare("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
    .all()
    .map((r) => r.name)
  assert.ok(tables.includes('bus'))
  assert.ok(tables.includes('bus_cursor'))
})

test('append stamps the sender and assigns increasing seq', () => {
  const db = openBus()
  append(db, { runId: 'r1', fromMember: 'scheduler', kind: 'stage_state', payload: '{"stage":"a"}' })
  append(db, { runId: 'r1', fromMember: 'agent:extract', kind: 'ask', payload: '{}', correlationId: 'c1' })

  const rows = db.prepare('SELECT * FROM bus ORDER BY seq').all()
  assert.equal(rows.length, 2)
  assert.equal(rows[0].from_member, 'scheduler')
  assert.equal(rows[1].from_member, 'agent:extract')
  assert.equal(rows[1].correlation_id, 'c1')
  assert.ok(rows[1].seq > rows[0].seq)
  assert.ok(rows[0].posted_at, 'posted_at is stamped by default')
})

test('claim is an atomic compare-and-swap: exactly one of two claimants wins', () => {
  const db = openBus()
  append(db, { runId: 'r1', fromMember: 'scheduler', kind: 'task', payload: '{}' })
  const seq = db.prepare('SELECT max(seq) AS seq FROM bus').get().seq

  const first = db.prepare(BUS_CLAIM).run({ claimed_by: 'worker-1', seq })
  const second = db.prepare(BUS_CLAIM).run({ claimed_by: 'worker-2', seq })

  assert.equal(first.changes, 1, 'first claimant wins')
  assert.equal(second.changes, 0, 'second claimant loses the CAS')

  const row = db.prepare('SELECT claimed_by, claimed_at FROM bus WHERE seq = ?').get(seq)
  assert.equal(row.claimed_by, 'worker-1')
  assert.ok(row.claimed_at)
})

test('cursor advance is monotonic', () => {
  const db = openBus()
  const advance = (lastSeq) =>
    db.prepare(BUS_CURSOR_ADVANCE).run({ run_id: 'r1', consumer: 'ui', last_seq: lastSeq })
  const cursor = () => db.prepare(BUS_CURSOR_GET).get({ run_id: 'r1', consumer: 'ui' }).last_seq

  assert.equal(cursor(), 0, 'new consumer starts at 0')
  advance(5)
  assert.equal(cursor(), 5)
  advance(3) // going backwards must not regress the cursor
  assert.equal(cursor(), 5)
  advance(9)
  assert.equal(cursor(), 9)
})

test('pinned-wait poll filters by kind and correlation id after the cursor', () => {
  const db = openBus()
  append(db, { runId: 'r1', fromMember: 'scheduler', kind: 'stage_state', payload: '{}' })
  append(db, { runId: 'r1', fromMember: 'agent:a', kind: 'ask', payload: '{}', correlationId: 'c1' })
  append(db, { runId: 'r1', fromMember: 'host', kind: 'reply', payload: '{}', correlationId: 'c1' })
  append(db, { runId: 'r2', fromMember: 'host', kind: 'reply', payload: '{}', correlationId: 'c1' })

  const poll = (params) =>
    db
      .prepare(BUS_POLL)
      .all({ run_id: 'r1', after_seq: 0, kind: null, correlation_id: null, limit: 100, ...params })

  assert.equal(poll({}).length, 3, 'unfiltered poll sees only its run')
  const replies = poll({ kind: 'reply', correlation_id: 'c1' })
  assert.equal(replies.length, 1)
  assert.equal(replies[0].from_member, 'host')
  assert.equal(poll({ after_seq: replies[0].seq }).length, 0, 'cursor excludes seen rows')
})
