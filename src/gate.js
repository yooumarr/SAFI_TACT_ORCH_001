import { randomUUID } from 'node:crypto';
import { mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import path from 'node:path';

const CONTROLLED_OPERATIONS = new Set([
  'marketing.follow_up',
  'sales.book_appointment',
  'operations.assign_task',
  'customer.update',
  'notification.send',
]);

export class GateError extends Error {
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'GateError';
    this.code = code;
    this.status = status;
  }
}

export class FileGateStore {
  constructor(filePath) {
    this.filePath = path.resolve(filePath);
  }

  async load() {
    try {
      const state = JSON.parse(await readFile(this.filePath, 'utf8'));
      if (state.version !== 1 || !state.conversations || !state.idempotency) {
        throw new Error('Unsupported or corrupt gate state');
      }
      return state;
    } catch (error) {
      if (error.code === 'ENOENT') return { version: 1, conversations: {}, idempotency: {} };
      throw new GateError('STATE_UNAVAILABLE', `Cannot restore gate state: ${error.message}`, 503);
    }
  }

  async save(state) {
    await mkdir(path.dirname(this.filePath), { recursive: true });
    const temporaryPath = `${this.filePath}.${process.pid}.${randomUUID()}.tmp`;
    await writeFile(temporaryPath, JSON.stringify(state, null, 2), { mode: 0o600 });
    await rename(temporaryPath, this.filePath);
  }
}

export class Gate {
  constructor({ store, authorize, adapter, journal }) {
    if (!store || typeof store.load !== 'function' || typeof store.save !== 'function') {
      throw new TypeError('A persistent store is required');
    }
    if (typeof authorize !== 'function' || typeof adapter?.dispatch !== 'function') {
      throw new TypeError('Authorization and dispatch adapters are required');
    }
    this.store = store;
    this.authorize = authorize;
    this.adapter = adapter;
    this.journal = journal;
    this.state = null;
    this.ready = false;
    this.locks = new Map();
    this.persistence = Promise.resolve();
  }

  async initialize() {
    this.state = await this.store.load();
    const unresolved = Object.values(this.state.conversations).some((conversation) =>
      conversation.inFlight > 0 || Object.values(conversation.pending ?? {}).some((action) => action.status === 'dispatching'),
    );
    if (unresolved) {
      throw new GateError('RECONCILIATION_REQUIRED', 'Restored state contains an action with an uncertain dispatch outcome; gate remains closed until reconciled.', 503);
    }
    this.ready = true;
  }

  requireReady() {
    if (!this.ready) throw new GateError('NOT_READY', 'Gate is not reconciled; fail closed.', 503);
  }

  async withConversationLock(conversationId, operation) {
    const previous = this.locks.get(conversationId) ?? Promise.resolve();
    let release;
    const current = new Promise((resolve) => { release = resolve; });
    this.locks.set(conversationId, current);
    await previous;
    try {
      return await operation();
    } finally {
      release();
      if (this.locks.get(conversationId) === current) this.locks.delete(conversationId);
    }
  }

  async record(eventType, action, extra = {}) {
    if (!this.journal) return;
    try {
      await this.journal.append({
        eventId: randomUUID(),
        eventType,
        operationId: action.operationId,
        conversationId: action.conversationId,
        subjectLine: action.subjectLine,
        actorGuid: extra.actorGuid ?? action.actorGuid,
        actorType: extra.actorType ?? action.actorType,
        meetingContext: action.meetingContext,
        portContext: action.portContext,
        permissionProvenance: action.permissionProvenance,
        operation: action.operation,
        idempotencyKey: action.idempotencyKey,
        correlationId: action.correlationId,
        status: action.status,
        outcome: extra.outcome,
        reason: extra.reason,
        occurredAt: new Date().toISOString(),
      });
    } catch (error) {
      this.ready = false;
      throw new GateError('JOURNAL_UNAVAILABLE', `Could not write control event; failing closed: ${error.message}`, 503);
    }
  }

  async persist() {
    const snapshot = structuredClone(this.state);
    const save = this.persistence.then(() => this.store.save(snapshot));
    this.persistence = save.catch(() => {});
    try {
      await save;
    } catch (error) {
      this.ready = false;
      throw new GateError('STATE_UNAVAILABLE', `Could not persist gate state; failing closed: ${error.message}`, 503);
    }
  }

  ensureConversation(conversationId) {
    let conversation = this.state.conversations[conversationId];
    if (!conversation) {
      conversation = { conversationId, mode: 'ungated', pending: {}, inFlight: 0, revision: 0 };
      this.state.conversations[conversationId] = conversation;
    }
    return conversation;
  }

  async permitted(context, operation) {
    const allowed = await this.authorize({
      actorGuid: context.actorGuid,
      actorType: context.actorType,
      conversationId: context.conversationId,
      meetingContext: context.meetingContext,
      portContext: context.portContext,
      operation,
    });
    if (!allowed) throw new GateError('FORBIDDEN', 'Current actor is not authorized for this operation.', 403);
  }

  async setMode(context, mode) {
    this.requireReady();
    if (!['gated', 'ungated'].includes(mode)) throw new GateError('INVALID_MODE', 'Mode must be gated or ungated.');
    await this.permitted(context, `gate.${mode}`);
    return this.withConversationLock(context.conversationId, async () => {
      const conversation = this.ensureConversation(context.conversationId);
      if (mode === 'gated' && conversation.inFlight > 0) {
        throw new GateError('RECONCILIATION_REQUIRED', 'Cannot confirm Gated mode until in-flight actions reconcile.', 409);
      }
      conversation.mode = mode;
      conversation.revision += 1;
      const eventAction = {
        operationId: randomUUID(),
        conversationId: context.conversationId,
        subjectLine: context.subjectLine,
        actorGuid: context.actorGuid,
        actorType: context.actorType,
        meetingContext: context.meetingContext,
        portContext: context.portContext,
        permissionProvenance: context.permissionProvenance,
        operation: `gate.${mode}`,
        idempotencyKey: `mode:${context.conversationId}:${conversation.revision}`,
        correlationId: randomUUID(),
        status: mode,
      };
      await this.record('mode.changed', eventAction);
      await this.persist();
      return this.snapshot(conversation);
    });
  }

  snapshot(conversation) {
    return {
      conversationId: conversation.conversationId,
      mode: conversation.mode,
      revision: conversation.revision,
      pending: Object.values(conversation.pending).map((action) => ({ ...action })),
      inFlight: conversation.inFlight,
    };
  }

  async submit(context, request) {
    this.requireReady();
    if (!CONTROLLED_OPERATIONS.has(request.operation)) throw new GateError('OPERATION_NOT_ALLOWED', 'Operation is not in the pilot allowlist.', 403);
    if (!request.idempotencyKey || typeof request.idempotencyKey !== 'string') throw new GateError('IDEMPOTENCY_REQUIRED', 'An idempotency key is required.');
    await this.permitted(context, request.operation);
    return this.withConversationLock(context.conversationId, async () => {
      const priorId = this.state.idempotency[`${context.conversationId}:${request.idempotencyKey}`];
      if (priorId) {
        const prior = this.ensureConversation(context.conversationId).pending[priorId];
        if (prior && (prior.operation !== request.operation || JSON.stringify(prior.payload) !== JSON.stringify(request.payload))) {
          throw new GateError('IDEMPOTENCY_CONFLICT', 'Idempotency key was already used for a different operation or payload.', 409);
        }
        return { duplicate: true, action: prior ? { ...prior } : { operationId: priorId, status: 'already_dispatched' } };
      }
      const conversation = this.ensureConversation(context.conversationId);
      const action = {
        operationId: randomUUID(),
        conversationId: context.conversationId,
        subjectLine: context.subjectLine,
        actorGuid: context.actorGuid,
        actorType: context.actorType,
        meetingContext: context.meetingContext,
        portContext: context.portContext,
        permissionProvenance: context.permissionProvenance,
        operation: request.operation,
        payload: request.payload,
        idempotencyKey: request.idempotencyKey,
        correlationId: request.correlationId ?? randomUUID(),
        status: conversation.mode === 'gated' ? 'held' : 'dispatching',
        createdAt: new Date().toISOString(),
      };
      conversation.pending[action.operationId] = action;
      this.state.idempotency[`${context.conversationId}:${request.idempotencyKey}`] = action.operationId;
      if (action.status === 'dispatching') conversation.inFlight += 1;
      if (action.status === 'held') {
        await this.record('action.held', action);
        await this.persist();
        return { duplicate: false, action: { ...action } };
      }
      await this.persist();
      await this.record('action.dispatching', action);
      return this.dispatchLocked(conversation, action, context);
    });
  }

  async dispatchLocked(conversation, action, context) {
    try {
      await this.permitted(context, action.operation);
      const result = await this.adapter.dispatch({
        ...action,
        currentAuthorization: true,
      });
      action.status = 'completed';
      action.outcome = result;
      action.completedAt = new Date().toISOString();
      await this.record('action.completed', action, { outcome: result });
      return { duplicate: false, action: { ...action } };
    } catch (error) {
      action.status = error instanceof GateError && error.code === 'FORBIDDEN' ? 'rejected' : 'uncertain';
      action.error = error.message;
      await this.record(action.status === 'rejected' ? 'action.rejected' : 'action.uncertain', action, { reason: error.message });
      throw error;
    } finally {
      conversation.inFlight -= 1;
      conversation.revision += 1;
      await this.persist();
    }
  }

  async review(context) {
    this.requireReady();
    await this.permitted(context, 'gate.review');
    return this.withConversationLock(context.conversationId, async () => this.snapshot(this.ensureConversation(context.conversationId)));
  }

  async history(context) {
    this.requireReady();
    await this.permitted(context, 'gate.history');
    if (!this.journal?.readConversation) throw new GateError('HISTORY_UNAVAILABLE', 'Event history adapter is unavailable.', 503);
    const events = await this.journal.readConversation(context.conversationId);
    return { conversationId: context.conversationId, events };
  }

  async resolve(context, operationId, decision) {
    this.requireReady();
    if (!['approve', 'reject', 'cancel'].includes(decision)) throw new GateError('INVALID_DECISION', 'Decision must be approve, reject, or cancel.');
    await this.permitted(context, `gate.${decision}`);
    return this.withConversationLock(context.conversationId, async () => {
      const conversation = this.ensureConversation(context.conversationId);
      const action = conversation.pending[operationId];
      if (!action || action.status !== 'held') throw new GateError('ACTION_NOT_HELD', 'Action is not awaiting review.', 404);
      if (decision !== 'approve') {
        action.status = decision === 'reject' ? 'rejected' : 'cancelled';
        action.resolvedAt = new Date().toISOString();
        action.resolution = decision;
        action.resolvedBy = context.actorGuid;
        conversation.revision += 1;
        await this.record(decision === 'reject' ? 'action.rejected' : 'action.cancelled', action, { actorGuid: context.actorGuid, actorType: context.actorType });
        await this.persist();
        return { action: { ...action } };
      }
      action.status = 'dispatching';
      action.approvedBy = context.actorGuid;
      action.approvedAt = new Date().toISOString();
      conversation.inFlight += 1;
      conversation.revision += 1;
      await this.persist();
      await this.record('action.approved', action, { actorGuid: context.actorGuid, actorType: context.actorType });
      await this.record('action.dispatching', action, { actorGuid: context.actorGuid, actorType: context.actorType });
      return this.dispatchLocked(conversation, action, context);
    });
  }
}
