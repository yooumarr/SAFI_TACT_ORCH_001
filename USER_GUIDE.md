# Local Orchestration Prototype — User Guide

This guide covers the local prototype only. Use synthetic conversations and reserved `.invalid` recipients; no message leaves the local Mailpit inbox. The prototype is not a SafiChat client.

## Start and open services

Start Docker Desktop, then from the project folder run `docker compose up -d`. Open:

- Gate: `http://localhost:8787` (API)
- n8n: `http://localhost:5678`
- Calnode: `http://localhost:3000/admin`
- Windmill: `http://localhost:8000`
- Mailpit: `http://localhost:8025`

## Conversation gate

The local API uses a configured synthetic actor ID in `.env` and `GATE_API_TOKEN`. Keep `.env private; it is gitignored. Requests require `Authorization: Bearer <GATE_API_TOKEN>` and `X-Actor-Guid: <allowed test actor>`.

Example request context:

```json
{
  "conversationId": "test-conversation-a",
  "subjectLine": "Synthetic lead A",
  "meetingContext": { "source": "local-test" },
  "portContext": { "fixture": "local-test" }
}
```

- Set `POST /mode` with `mode` equal to `gated` or `ungated`.
- Submit an allowlisted operation to `POST /actions` with an `idempotencyKey` and test payload.
- Inspect pending work using `GET /actions?conversationId=<id>`.
- Resolve held work using `POST /actions/resolve` with `operationId` and `decision` (`approve`, `reject`, or `cancel`).
- Inspect the local prototype journal using `GET /history?conversationId=<id>`.

All actions must use reserved synthetic data. Marketing requires `consent: true`, `allowlisted: true`, and an `@example.invalid` recipient. Calendar bookings require the configured test Calnode event and an available slot. Windmill task runs require `allowlisted: true` and `testOnly: true`. Notifications are sent only to local Mailpit.

## Stop and recover

Run `docker compose down` to stop services while retaining persistent volumes. Run `docker compose up -d` to restart. Do not use `docker compose down -v` unless intentionally deleting all local test data. After restart, verify Gated state and held actions via the API; an action with uncertain dispatch outcome must be reconciled before retry.

## Safety and limitations

Services bind to host loopback. Do not expose them publicly or use production credentials/data. Local NATS JetStream is only a prototype control journal, not SAFI History. Test actor-map authorization is not SAFI identity/permission enforcement. A local test pass does not prove full assessment acceptance; record only observed outcomes in the evidence report.
