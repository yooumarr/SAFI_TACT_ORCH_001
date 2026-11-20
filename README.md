# SAFI Local Business Orchestration Gate — prototype

A local prototype for the per-conversation control gate described in TACT-ORCH-001. Docker Compose starts the gate, NATS/JetStream, n8n, Calnode, and Windmill with persistent volumes. This is not yet SAFI-integrated: no assigned SAFI repository, SafiChat, Meeting Manager adapter, approved service credentials, or consented test contacts were provided. Do not present any SOW integration test as passed based on this prototype alone.

## Current prototype

- Local HTTP API for action submission, mode changes, pending-action review, approve/reject/cancel, and per-conversation event history.
- Per-conversation serialization and persisted gated/ungated state with held-action references.
- Fail-closed startup on corrupt state or unresolved dispatches; persistent write failure disables further requests.
- Actor/conversation authorization hook and permission recheck before dispatch, including approval.
- Pilot operation allowlist and idempotency key enforcement (the downstream dispatcher must also enforce idempotency).
- The local gate connects to a persistent JetStream `ORCH_CONTROL` stream for mode/action events and uses a single NATS request/reply dispatcher.
- The local Compose stack includes Mailpit for optional email capture; n8n prepares a consented follow-up, and the dispatcher restricts pilot recipients to reserved `.invalid` domains.
- Windmill assignment script/flow definitions and import helper are in `src/`; live execution still needs verification.
- JSON Schema and a synthetic event example are in `schemas/` and `examples/`.

The provided HTTP server is a local development adapter only. Its `GATE_ACTORS_JSON` actor map and bearer token are not an implementation of SAFI identity, permissions, history, or attribution. The container listens internally on all interfaces so Compose can reach it; the host port is bound to loopback only. Do not expose it to a network or production environment.

## Requirements

- Node.js 20 or newer.
- No npm dependencies.
- Docker Compose configuration pins NATS 2.15.3, n8n 2.48.4, Calnode 0.14.0, Windmill 1.876.1, and PostgreSQL 18. These are local prototype pins, not SAFI-approved production versions.
- Services bind to loopback only. Windmill is configured as a single-server, single-worker pilot; no external services or inbound public webhooks are configured.

## Run tests

```powershell
npm test
```

## Local-only start

Copy `.env.example` to `.env`, set strong random passwords/tokens, and configure test-only `GATE_ACTORS_JSON`. Never commit `.env`. From this directory, run `docker compose config --quiet` and `docker compose up -d --build`; check status with `docker compose ps`. The services bind to `127.0.0.1`. Keep `DISPATCH_URL` blank until a trusted, authenticated test dispatcher that enforces idempotency is available; without it no external actions can complete. Do not use these local credentials or fixtures for SAFI production.

Example actor map shape (replace with synthetic test IDs; never use production credentials here):

```json
{"test-actor-guid":{"type":"user","permissionProvenance":{"source":"local-test-fixture"},"conversations":["test-conversation-a"]}}
```

API operations use JSON bodies with `conversationId`, `subjectLine`, `meetingContext`, and `portContext`. Use `POST /mode` with `{ "mode": "gated" }` or `{ "mode": "ungated" }`; `POST /actions` for an allowlisted action plus `idempotencyKey`; `GET /actions` to inspect; and `POST /actions/resolve` with `operationId` and `decision` to approve/reject/cancel. Each request requires `Authorization: Bearer ...` and `X-Actor-Guid: ...`.

## Stop, restore, remove

Stop services with `docker compose down` (keeps volumes); remove stack data with `docker compose down -v` only when you intend to delete all local service data. The gate volume stores `gate-state.json`; held actions restore. If an action was dispatching at crash time, startup refuses to serve until the outcome is reconciled; do not blindly retry an uncertain external operation. Keep backups local and private.

## Service URLs and default setup

- Gate health: `http://localhost:8787/healthz` (Bearer token required; token is in local `.env`).
- NATS monitoring: `http://localhost:8222/healthz`; JetStream enabled. NATS client: `nats://localhost:4222`.
- n8n: `http://localhost:5678`; create the local owner user on first visit.
- Calnode: `http://localhost:3000`; finish its first-run owner/event-type setup before booking tests.
- Windmill: `http://localhost:8000`; use the official first-login/setup flow. Keep all admin access local-only.

These are local prototype services. JetStream holds prototype gate control/action events in the `ORCH_CONTROL` stream; it is not SAFI History and does not use SAFI Subject names. No n8n workflow has been imported into this fresh n8n instance yet. Windmill flow definitions were imported to the local Admins workspace, but the assignment run endpoint still needs verification. No Calnode test event type or booking has been configured.