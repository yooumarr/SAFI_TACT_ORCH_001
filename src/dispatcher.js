import { createHash, randomUUID } from 'node:crypto';
import { connect, StringCodec } from 'nats';

const sc = StringCodec();
const SUBJECT = 'orch.dispatch.request';
const WINDMILL_SUBJECT = 'orch.windmill.assign';

export class BusinessDispatcher {
  constructor({ natsUrl, timeoutMs = 10000 }) {
    this.natsUrl = natsUrl;
    this.timeoutMs = timeoutMs;
    this.connection = null;
    this.subscription = null;
    this.processing = new Map();
    this.idempotency = new Map();
    this.bookingKeys = new Map();
    this.windmillBookings = new Map();
  }

  async connect() {
    this.connection = await connect({ servers: this.natsUrl, name: 'local-business-dispatcher' });
    this.subscription = this.connection.subscribe(SUBJECT);
    this.windmillSubscription = this.connection.subscribe(WINDMILL_SUBJECT);
    void this.consume();
    void this.consumeWindmill();
  }

  async consume() {
    for await (const message of this.subscription) void this.processDispatchMessage(message);
  }

  async processDispatchMessage(message) {
    try {
      const action = JSON.parse(sc.decode(message.data));
      const fingerprint = createHash('sha256').update(JSON.stringify({ operation: action.operation, payload: action.payload })).digest('hex');
      const prior = this.idempotency.get(action.idempotencyKey);
      if (prior) {
        if (prior.fingerprint !== fingerprint) throw new Error('Idempotency key reused with a different operation or payload.');
        if (prior.status !== 'completed') throw new Error('Previous dispatch outcome is uncertain; reconcile before retrying.');
        message.respond(sc.encode(JSON.stringify(prior.result)));
        return;
      }
      const entry = { fingerprint, status: 'in_progress', result: null };
      this.idempotency.set(action.idempotencyKey, entry);
      try {
        const requestedDelay = action.payload?.testOnly === true ? Number(action.payload.testDelayMs ?? 0) : 0;
        if (Number.isFinite(requestedDelay) && requestedDelay > 0 && requestedDelay <= 10000) {
          await new Promise((resolve) => setTimeout(resolve, requestedDelay));
        }
        const result = await this.process(action);
        entry.status = 'completed';
        entry.result = result;
        message.respond(sc.encode(JSON.stringify(result)));
      } catch (error) {
        entry.status = 'uncertain';
        throw error;
      }
    } catch (error) {
      message.respond(sc.encode(JSON.stringify({ error: error.message })));
    }
  }

  async consumeWindmill() {
    for await (const message of this.windmillSubscription) {
      try {
        const action = JSON.parse(sc.decode(message.data));
        const fingerprint = createHash('sha256').update(JSON.stringify({ operation: action.operation, payload: action.payload })).digest('hex');
        const prior = this.windmillBookings.get(action.idempotencyKey);
        if (prior) {
          if (prior.fingerprint !== fingerprint || prior.status !== 'completed') throw new Error('Previous Windmill dispatch is conflicting or uncertain; reconcile before retrying.');
          message.respond(sc.encode(JSON.stringify(prior.result)));
          continue;
        }
        const entry = { fingerprint, status: 'in_progress', result: null };
        this.windmillBookings.set(action.idempotencyKey, entry);
        try {
          const output = await this.runWindmillTask(action);
          entry.status = 'completed';
          entry.result = output;
          message.respond(sc.encode(JSON.stringify(output)));
        } catch (error) {
          entry.status = 'uncertain';
          throw error;
        }
      } catch (error) {
        message.respond(sc.encode(JSON.stringify({ error: error.message })));
      }
    }
  }

  async runWindmillTask(action) {
    const payload = action.payload ?? {};
    if (payload.allowlisted !== true || payload.testOnly !== true) throw new Error('Only allowlisted test task assignments are enabled.');
    const token = process.env.WINDMILL_TOKEN;
    const workspace = process.env.WINDMILL_WORKSPACE ?? 'admins';
    if (token) {
      const path = process.env.WINDMILL_ASSIGNMENT_FLOW ?? 'f/orchestration/assign_task';
      const route = path.startsWith('f/') ? path : `f/${path}`;
      const result = await this.request(`http://windmill-server:8000/api/w/${encodeURIComponent(workspace)}/jobs/run/${route}`, 'POST', {
        operationId: action.operationId,
        conversationId: action.conversationId,
        task: payload,
      }, `Bearer ${token}`);
      return { service: 'windmill', jobId: result.id ?? result.job_id ?? result, status: 'queued', result };
    }
    const assignee = typeof payload.assignee === 'string' && payload.assignee.trim() ? payload.assignee.trim() : 'test-operator';
    const title = typeof payload.title === 'string' && payload.title.trim() ? payload.title.trim() : 'Follow up on test customer inquiry';
    return {
      service: 'windmill-test-adapter',
      jobId: `test-job-${action.operationId}`,
      taskId: `task-${action.operationId}`,
      title,
      assignee,
      status: 'completed',
      completedAt: new Date().toISOString(),
      externalNotifications: 'disabled',
      conversationId: action.conversationId,
    };
  }

  async process(action) {
    switch (action.operation) {
      case 'marketing.follow_up': return this.processMarketing(action);
      case 'sales.book_appointment': return this.processBooking(action);
      case 'operations.assign_task': return this.processTask(action);
      case 'customer.update': return this.processCustomerUpdate(action);
      case 'notification.send': return this.processNotification(action);
      default: throw new Error('Operation is not supported by the pilot dispatcher.');
    }
  }

  async processMarketing(action) {
    const payload = action.payload ?? {};
    if (payload.consent !== true || payload.allowlisted !== true) {
      throw new Error('Marketing follow-up requires explicit test consent and an allowlisted recipient.');
    }
    const workflowId = process.env.N8N_MARKETING_WORKFLOW_ID;
    if (!workflowId) throw new Error('N8N_MARKETING_WORKFLOW_ID is not configured.');
    const execution = await fetch(`http://n8n:5678/webhook/safi/marketing-intake`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-idempotency-key': action.idempotencyKey },
      body: JSON.stringify({ action, lead: payload.lead }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    const result = await execution.json().catch(() => ({}));
    if (!execution.ok || result.status === 'blocked') throw new Error(`Marketing workflow failed or blocked (${execution.status}): ${JSON.stringify(result).slice(0, 240)}`);
    if (typeof payload.recipient !== 'string' || !payload.recipient.endsWith('.invalid')) {
      throw new Error('Pilot marketing accepts only reserved .invalid recipient domains.');
    }
    return { service: 'n8n', status: 'prepared', executionId: result.executionId ?? null, leadId: result.leadId ?? payload.lead?.id ?? null };
  }

  async processBooking(action) {
    const payload = action.payload ?? {};
    if (!payload.allowlisted || !payload.testRecipient) throw new Error('Booking requires an allowlisted test recipient.');
    const apiKey = process.env.CALNODE_API_KEY;
    if (!apiKey) throw new Error('CALNODE_API_KEY is not configured. Set up a test event type and API key first.');
    const slug = requiredString(payload.eventTypeSlug, 'eventTypeSlug');
    if (typeof payload.from !== 'string' || typeof payload.to !== 'string') throw new Error('Booking availability requires ISO date range fields from/to.');
    const query = new URLSearchParams({ from: payload.from, to: payload.to, tz: payload.timezone ?? 'UTC' });
    const availability = await this.calnodeRequest('GET', `/v1/event-types/${encodeURIComponent(slug)}/slots?${query}`);
    const slots = availability.slots ?? [];
    const chosenStart = payload.startAt ?? slots[0]?.start;
    if (!chosenStart || !slots.some((slot) => slot.start === chosenStart)) throw new Error('No matching live available slot. Configure Calnode availability and provide an available startAt.');
    const booking = await this.calnodeRequest('POST', '/v1/bookings', {
      event_type_slug: slug,
      start_at: chosenStart,
      name: requiredString(payload.name, 'name'),
      email: requiredString(payload.email, 'email'),
      timezone: payload.timezone ?? 'UTC',
    }, { 'Idempotency-Key': action.idempotencyKey });
    const result = { service: 'calnode', bookingId: booking.id ?? booking.booking?.id ?? null, startAt: chosenStart, status: booking.status ?? booking.booking?.status ?? 'created' };
    return result;
  }

  async processTask(action) {
    const result = await this.connection.request('orch.windmill.assign', sc.encode(JSON.stringify(action)), { timeout: this.timeoutMs });
    const output = JSON.parse(sc.decode(result.data));
    if (output.error) throw new Error(output.error);
    return output;
  }

  async processCustomerUpdate(action) {
    return { service: 'local-operations', operation: action.operation, status: 'recorded', update: action.payload };
  }

  async processNotification(action) {
    const payload = action.payload ?? {};
    if (payload.allowlisted !== true || typeof payload.recipient !== 'string') throw new Error('Notification requires an allowlisted recipient.');
    const response = await fetch('http://mailpit:8025/api/v1/send', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ From: { Email: 'orchestration@example.invalid', Name: 'Orchestration Test' }, To: [{ Email: payload.recipient, Name: payload.name ?? '' }], Subject: requiredString(payload.subject, 'subject'), Text: requiredString(payload.text, 'text') }),
    });
    if (!response.ok) throw new Error(`Mailpit returned HTTP ${response.status}`);
    return { service: 'mailpit', status: 'captured' };
  }

  async request(url, method, body, authHeader, extraHeaders = {}) {
    const response = await fetch(url, {
      method,
      headers: { ...(authHeader ? { Authorization: authHeader } : {}), ...extraHeaders, ...(body === undefined ? {} : { 'content-type': 'application/json' }) },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    const text = await response.text();
    let value;
    try { value = JSON.parse(text); } catch { value = text; }
    if (!response.ok) throw new Error(`${method} ${new URL(url).pathname} returned HTTP ${response.status}: ${typeof value === 'string' ? value.slice(0, 160) : JSON.stringify(value).slice(0, 240)}`);
    return value;
  }

  calnodeRequest(method, path, body, headers = {}) {
    return this.request(`http://calnode:3000${path}`, method, body, null, { 'X-API-Key': requiredEnv('CALNODE_API_KEY'), ...headers });
  }


  async dispatch(action) {
    if (!this.connection) throw new Error('Dispatcher has not connected to NATS.');
    const requestId = randomUUID();
    const response = await this.connection.request(SUBJECT, sc.encode(JSON.stringify({ ...action, requestId })), { timeout: this.timeoutMs });
    const result = JSON.parse(sc.decode(response.data));
    if (result.error) throw new Error(result.error);
    return { ...result, requestId };
  }

  async close() {
    this.subscription?.unsubscribe();
    this.windmillSubscription?.unsubscribe();
    await this.connection?.drain();
  }
}

function requiredString(value, name) {
  if (typeof value !== 'string' || !value.trim()) throw new Error(`${name} is required.`);
  return value.trim();
}

function requiredEnv(name) {
  if (!process.env[name]) throw new Error(`${name} is not configured.`);
  return process.env[name];
}
