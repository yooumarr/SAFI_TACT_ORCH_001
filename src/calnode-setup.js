const baseUrl = process.env.CALNODE_URL ?? 'http://127.0.0.1:3000';

async function request(path, { method = 'GET', body, apiKey } = {}) {
  const response = await fetch(new URL(path, baseUrl), {
    method,
    headers: { ...(apiKey ? { Authorization: `Bearer ${apiKey}` } : {}), ...(body ? { 'content-type': 'application/json' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
    signal: AbortSignal.timeout(15000),
  });
  const text = await response.text();
  let value;
  try { value = JSON.parse(text); } catch { value = text; }
  if (!response.ok) throw new Error(`${method} ${path} failed (${response.status}): ${typeof value === 'string' ? value.slice(0, 500) : JSON.stringify(value).slice(0, 500)}`);
  return value;
}

const apiKey = process.env.CALNODE_API_KEY;
if (!apiKey) throw new Error('Create a test-only Calnode API key in the admin UI and set CALNODE_API_KEY locally.');

const eventTypes = await request('/v1/event-types', { apiKey });
const items = Array.isArray(eventTypes) ? eventTypes : eventTypes.data ?? eventTypes.event_types ?? [];
const eventType = items.find((item) => item.slug === (process.env.CALNODE_EVENT_TYPE_SLUG ?? 'test-intro-call'));
if (!eventType) {
  console.log('No test event type found. Create a 30-minute event type with a few weekday slots in the Calnode admin UI, then re-run with CALNODE_API_KEY and CALNODE_EVENT_TYPE_SLUG.');
  process.exit(0);
}
const now = new Date();
const from = now.toISOString().slice(0, 10);
const toDate = new Date(now.getTime() + 14 * 86400000);
const to = toDate.toISOString().slice(0, 10);
const slots = await request(`/v1/event-types/${encodeURIComponent(eventType.slug)}/slots?from=${from}&to=${to}&tz=Asia%2FKolkata`, { apiKey });
console.log(JSON.stringify({ eventType: eventType.slug, availableSlots: slots.slots?.length ?? 0, sample: slots.slots?.[0] ?? null }));
