import { marketingWorkflow } from './n8n-workflow.js';

export async function importN8nMarketingWorkflow({ baseUrl, apiKey, webhookPath = 'safi/marketing-intake' }) {
  const workflow = structuredClone(marketingWorkflow);
  delete workflow.active;
  delete workflow.meta;
  delete workflow.tags;
  workflow.nodes[0].parameters.path = webhookPath;
  const existing = await n8nRequest(baseUrl, apiKey, '/api/v1/workflows?limit=100');
  const current = existing.data?.find((item) => item.name === workflow.name);
  if (current) throw new Error(`Workflow ${workflow.name} already exists. Review it in n8n; importer will not overwrite or activate workflows.`);
  const created = await n8nRequest(baseUrl, apiKey, '/api/v1/workflows', 'POST', workflow);
  return { id: created.id, name: workflow.name, active: false, webhookPath, workflow };
}

export async function activateN8nMarketingWorkflow({ baseUrl, apiKey, id }) {
  const current = await n8nRequest(baseUrl, apiKey, `/api/v1/workflows/${encodeURIComponent(id)}`);
  if (current.active) return current;
  return n8nRequest(baseUrl, apiKey, `/api/v1/workflows/${encodeURIComponent(id)}/activate`, 'POST');
}

async function n8nRequest(baseUrl, apiKey, path, method = 'GET', body) {
  const response = await fetch(new URL(path, baseUrl), {
    method,
    headers: { 'X-N8N-API-KEY': apiKey, ...(body ? { 'content-type': 'application/json' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
    signal: AbortSignal.timeout(15000),
  });
  const text = await response.text();
  let result;
  try { result = JSON.parse(text); } catch { result = text; }
  if (!response.ok) throw new Error(`n8n ${method} ${path} failed (${response.status}): ${typeof result === 'string' ? result.slice(0, 300) : JSON.stringify(result).slice(0, 300)}`);
  return result;
}

if (import.meta.url === `file://${process.argv[1]?.replaceAll('\\', '/')}`) {
  const baseUrl = process.env.N8N_URL ?? 'http://127.0.0.1:5678';
  const apiKey = process.env.N8N_API_KEY;
  if (!apiKey) {
    console.error('Set N8N_API_KEY locally after enabling API access and creating a test API key.');
    process.exit(2);
  }
  try {
    const result = process.argv[2] === 'activate'
      ? await activateN8nMarketingWorkflow({ baseUrl, apiKey, id: process.env.N8N_MARKETING_WORKFLOW_ID })
      : await importN8nMarketingWorkflow({ baseUrl, apiKey });
    if (process.argv[2] !== 'activate') {
      const exportPath = new URL('../workflows/n8n-marketing-follow-up.json', import.meta.url);
      const { writeFile, mkdir } = await import('node:fs/promises');
      await mkdir(new URL('../workflows/', import.meta.url), { recursive: true });
      await writeFile(exportPath, `${JSON.stringify(marketingWorkflow, null, 2)}\n`, { mode: 0o600 });
    }
    console.log(JSON.stringify(result));
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}
