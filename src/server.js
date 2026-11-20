import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { Gate, GateError, FileGateStore } from './gate.js';
import { JetStreamEventJournal } from './event-journal.js';
import { BusinessDispatcher } from './dispatcher.js';

const port = Number(process.env.PORT ?? 8787);
const statePath = process.env.GATE_STATE_PATH ?? './data/gate-state.json';
const trustedToken = process.env.GATE_API_TOKEN;
const actorMap = JSON.parse(process.env.GATE_ACTORS_JSON ?? '{}');

if (!trustedToken) {
  console.error('GATE_API_TOKEN is required. Refusing to start without authentication.');
  process.exit(1);
}

const dispatcher = new BusinessDispatcher({ natsUrl: process.env.NATS_URL ?? 'nats://127.0.0.1:4222' });

const journal = new JetStreamEventJournal({ servers: process.env.NATS_URL ?? 'nats://127.0.0.1:4222' });
const gate = new Gate({
  store: new FileGateStore(statePath),
  authorize: async ({ actorGuid, conversationId }) => actorMap[actorGuid]?.conversations?.includes(conversationId) === true,
  adapter: { dispatch: (action) => dispatcher.dispatch(action) },
  journal,
});

try {
  await journal.connect();
  await dispatcher.connect();
  await gate.initialize();
} catch (error) {
  console.error(error.message);
  await dispatcher.close().catch(() => {});
  await journal.close().catch(() => {});
  process.exit(1);
}

async function readJson(request) {
  let body = '';
  for await (const chunk of request) {
    body += chunk;
    if (body.length > 1_000_000) throw new GateError('BODY_TOO_LARGE', 'Request body exceeds 1 MB.', 413);
  }
  try { return JSON.parse(body || '{}'); }
  catch { throw new GateError('INVALID_JSON', 'Request body must be valid JSON.'); }
}

function getContext(request, body) {
  const actorGuid = request.headers['x-actor-guid'];
  const actor = actorMap[actorGuid];
  if (!actorGuid || !actor) throw new GateError('UNAUTHENTICATED', 'Unknown actor.', 401);
  return {
    actorGuid,
    actorType: actor.type,
    conversationId: body.conversationId ?? body.body?.action?.conversationId,
    subjectLine: body.subjectLine ?? body.body?.action?.subjectLine,
    meetingContext: body.meetingContext ?? body.body?.action?.meetingContext,
    portContext: body.portContext ?? body.body?.action?.portContext,
    permissionProvenance: actor.permissionProvenance,
  };
}

const server = createServer(async (request, response) => {
  response.setHeader('content-type', 'application/json');
  response.setHeader('cache-control', 'no-store');
  const url = new URL(request.url, 'http://localhost');
  if (request.method === 'GET' && url.pathname === '/healthz') {
    response.writeHead(200).end(JSON.stringify({ status: 'ok' }));
    return;
  }
  if (request.headers.authorization !== `Bearer ${trustedToken}`) {
    response.writeHead(401).end(JSON.stringify({ error: 'UNAUTHENTICATED' }));
    return;
  }
  try {
    const body = request.method === 'GET'
      ? Object.fromEntries(new URL(request.url, 'http://localhost').searchParams.entries())
      : await readJson(request);
    const context = getContext(request, body);
    if (!context.conversationId) throw new GateError('CONVERSATION_REQUIRED', 'conversationId is required.');
    const actionRequest = body.body?.action;
    if (actionRequest && (!Array.isArray(actorMap[context.actorGuid]?.conversations) || !actorMap[context.actorGuid].conversations.includes(context.conversationId))) {
      throw new GateError('FORBIDDEN', 'Actor is not authorized for the workflow conversation.', 403);
    }
    let result;
    if (request.method === 'POST' && url.pathname === '/actions') {
      result = await gate.submit(context, body.body?.action ?? body);
    } else if (request.method === 'POST' && url.pathname === '/mode') {
      result = await gate.setMode(context, body.mode);
    } else if (request.method === 'GET' && url.pathname === '/actions') {
      result = await gate.review(context);
    } else if (request.method === 'GET' && url.pathname === '/history') {
      result = await gate.history(context);
    } else if (request.method === 'POST' && url.pathname === '/actions/resolve') {
      result = await gate.resolve(context, body.operationId, body.decision);
    } else {
      throw new GateError('NOT_FOUND', 'Route not found.', 404);
    }
    response.writeHead(200).end(JSON.stringify(result));
  } catch (error) {
    const status = error instanceof GateError ? error.status : 502;
    response.writeHead(status).end(JSON.stringify({ error: error.code ?? 'DISPATCH_ERROR', message: error.message }));
  }
});

server.listen(port, '0.0.0.0', () => console.log(`Gate listening on port ${port}; Compose publishes it on host loopback only.`));
