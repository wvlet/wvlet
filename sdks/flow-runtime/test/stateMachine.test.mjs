import { test } from 'node:test'
import assert from 'node:assert/strict'
import {
  STAGE_STATES,
  TERMINAL_STAGE_STATES,
  TRIGGER_TABLE,
  canTransition,
  isTerminal,
  triggerSatisfied,
} from '../dist/index.js'

test('terminal states admit no outgoing transitions', () => {
  for (const from of TERMINAL_STAGE_STATES) {
    for (const to of STAGE_STATES) {
      assert.equal(canTransition(from, to), false, `${from} -> ${to} must be illegal`)
    }
  }
})

test('the retry loop follows the documented path', () => {
  assert.ok(canTransition('pending', 'running'))
  assert.ok(canTransition('running', 'attempt_failed'))
  assert.ok(canTransition('attempt_failed', 'retrying'))
  assert.ok(canTransition('retrying', 'running'))
  assert.ok(canTransition('attempt_failed', 'failed'))
  assert.ok(canTransition('retrying', 'failed'))
  assert.equal(canTransition('pending', 'success'), false, 'success requires a running attempt')
})

test('trigger table matches the docs: from/depends_on need success, if x.done always runs', () => {
  for (const state of TERMINAL_STAGE_STATES) {
    assert.ok(isTerminal(state))
    assert.equal(triggerSatisfied(state, 'from'), state === 'success')
    assert.equal(triggerSatisfied(state, 'depends_on'), state === 'success')
    assert.equal(triggerSatisfied(state, 'if_failed'), state === 'failed')
    assert.equal(triggerSatisfied(state, 'if_done'), true)
  }
  assert.equal(Object.keys(TRIGGER_TABLE).length, TERMINAL_STAGE_STATES.length)
})
