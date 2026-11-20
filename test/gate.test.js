import test from 'node:test';
import assert from 'node:assert/strict';
import { Gate, GateError } from '../src/gate.js';

class MemoryStore {
  state = { version: 1, conversations: {}, idempotency: {} };
  failSave = false;
  async load() { return structuredClone(this.state); }
  async save(state) {
    if (this.failSave) throw new Error('disk unavailable');
    this.state = structuredClone(state);
  }
}
class MemoryJournal {
  events = [];
  async append(event) { this.events.push(structuredClone(event)); }
  async readConversation(conversationId) { return this.events.filter((event) => event.conversationId === conversationId); }
}

const baseContext = (conversationId = 'conversation-a', actorGuid = 'actor-1') => ({
  conversationId,
  subjectLine: 'Customer A',
  actorGuid,
  actorType: 'user',
  meetingContext: { meetingId: 'meeting-1' },
  portContext: { portId: 'port-1' },
  permissionProvenance: { role: 'operator', revision: 3 },
});
const request = (idempotencyKey = 'key-1') => ({
  operation: 'marketing.follow_up',
  idempotencyKey,
  payload: { message: 'Consented test follow-up' },
});
function setup({ authorize = async () => true, dispatch = async () => ({ ok: true }), journal = new MemoryJournal() } = {}) {
  const store = new MemoryStore();
  const calls = [];
  const adapter = { dispatch: async (action) => { calls.push(action); return dispatch(action); } };
  const gate = new Gate({ store, authorize, adapter, journal });
  return gate.initialize().then(() => ({ gate, store, calls, journal }));
}

test('gated actions wait for explicit approval; rejected actions are cancelled', async () => {
  const { gate, calls } = await setup();
  const context = baseContext();
  await gate.setMode(context, 'gated');
  const held = await gate.submit(context, request());
  assert.equal(held.action.status, 'held');
  assert.equal(calls.length, 0);
  const denied = await gate.resolve(context, held.action.operationId, 'reject');
  assert.equal(denied.action.status, 'rejected');
  assert.equal(calls.length, 0);
});

test('approval rechecks authorization immediately before dispatch', async () => {
  let allowed = true;
  const { gate, calls } = await setup({ authorize: async () => allowed });
  const context = baseContext();
  await gate.setMode(context, 'gated');
  const held = await gate.submit(context, request());
  allowed = false;
  await assert.rejects(gate.resolve(context, held.action.operationId, 'approve'), (error) => error.code === 'FORBIDDEN');
  assert.equal(calls.length, 0);
});

test('idempotency prevents duplicate external effects', async () => {
  const { gate, calls } = await setup();
  const context = baseContext();
  await gate.setMode(context, 'ungated');
  const first = await gate.submit(context, request('same-key'));
  const second = await gate.submit(context, request('same-key'));
  assert.equal(first.action.status, 'completed');
  assert.equal(second.duplicate, true);
  await assert.rejects(gate.submit(context, { ...request('same-key'), payload: { different: true } }), (error) => error.code === 'IDEMPOTENCY_CONFLICT');
  assert.equal(calls.length, 1);
});

test('gated and ungated conversations remain independent', async () => {
  const { gate, calls } = await setup();
  const a = baseContext('conversation-a');
  const b = baseContext('conversation-b');
  await gate.setMode(a, 'gated');
  assert.equal((await gate.submit(a, request('a'))).action.status, 'held');
  assert.equal((await gate.submit(b, request('b'))).action.status, 'completed');
  assert.equal(calls.length, 1);
  assert.equal((await gate.review(a)).pending[0].status, 'held');
});

test('restart fails closed when an action outcome is uncertain', async () => {
  const first = await setup();
  const context = baseContext();
  await first.gate.setMode(context, 'ungated');
  first.store.state.conversations[context.conversationId] = {
    conversationId: context.conversationId,
    mode: 'ungated',
    pending: { operation: { operationId: 'operation', status: 'dispatching' } },
    inFlight: 1,
    revision: 1,
  };
  const restarted = new Gate({ store: first.store, authorize: async () => true, adapter: { dispatch: async () => ({ ok: true }) } });
  await assert.rejects(restarted.initialize(), (error) => error.code === 'RECONCILIATION_REQUIRED');
  await assert.rejects(restarted.review(context), (error) => error.code === 'NOT_READY');
});

test('restart restores gated mode and held action', async () => {
  const first = await setup();
  const context = baseContext();
  await first.gate.setMode(context, 'gated');
  const held = await first.gate.submit(context, request());
  const restarted = new Gate({ store: first.store, authorize: async () => true, adapter: { dispatch: async () => ({ ok: true }) } });
  await restarted.initialize();
  const restored = await restarted.review(context);
  assert.equal(restored.mode, 'gated');
  assert.equal(restored.pending[0].operationId, held.action.operationId);
});

test('store failure fails closed', async () => {
  const { gate, store } = await setup();
  store.failSave = true;
  await assert.rejects(gate.setMode(baseContext(), 'gated'), (error) => error instanceof GateError && error.code === 'STATE_UNAVAILABLE');
  await assert.rejects(gate.review(baseContext()), (error) => error.code === 'NOT_READY');
});

test('control and action outcomes are appended to the event journal', async () => {
  const { gate, journal } = await setup();
  const context = baseContext();
  await gate.setMode(context, 'gated');
  const held = await gate.submit(context, request('journal-key'));
  await gate.resolve(context, held.action.operationId, 'approve');
  const history = await gate.history(context);
  assert.deepEqual(history.events.map((event) => event.eventType), [
    'mode.changed', 'action.held', 'action.approved', 'action.dispatching', 'action.completed',
  ]);
  assert.equal(history.events[1].meetingContext.meetingId, 'meeting-1');
  assert.equal(journal.events[1].actorGuid, context.actorGuid);
});

test('unsupported operations are rejected before dispatch', async () => {
  const { gate, calls } = await setup();
  await assert.rejects(gate.submit(baseContext(), { ...request(), operation: 'arbitrary.webhook' }), (error) => error.code === 'OPERATION_NOT_ALLOWED');
  assert.equal(calls.length, 0);
});
