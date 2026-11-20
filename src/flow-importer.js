import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { assignmentFlow, ASSIGNMENT_FLOW_PATH } from './windmill-flow.js';
import { assignmentScript, ASSIGNMENT_SCRIPT_PATH } from './windmill-scripts.js';

async function windmillRequest(baseUrl, workspace, token, suffix, body, method = 'POST') {
  const response = await fetch(new URL(`/api/w/${encodeURIComponent(workspace)}/${suffix}`, baseUrl), {
    method,
    headers: { Authorization: `Bearer ${token}`, ...(body ? { 'content-type': 'application/json' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : {}),
    signal: AbortSignal.timeout(15000),
  });
  const text = await response.text();
  let result;
  try { result = JSON.parse(text); } catch { result = text; }
  if (!response.ok) throw new Error(`Windmill ${suffix} failed (${response.status}): ${typeof result === 'string' ? result.slice(0, 300) : JSON.stringify(result).slice(0, 300)}`);
  return result;
}

export async function importWindmillAssignmentFlow({ baseUrl, workspace, token }) {
  let script;
  try {
    script = await windmillRequest(baseUrl, workspace, token, 'scripts/create', assignmentScript);
  } catch (error) {
    if (!error.message.includes('same hash')) throw error;
    script = { status: 'already_exists' };
  }
  const flowPath = `/api/w/${encodeURIComponent(workspace)}/flows/get/${ASSIGNMENT_FLOW_PATH}`;
  const currentResponse = await fetch(new URL(flowPath, baseUrl), { headers: { Authorization: `Bearer ${token}` } });
  let flow;
  if (currentResponse.ok) {
    const current = await currentResponse.json();
    flow = await windmillRequest(baseUrl, workspace, token, `flows/update/${ASSIGNMENT_FLOW_PATH}`, { ...current, ...assignmentFlow, path: ASSIGNMENT_FLOW_PATH }, 'POST');
  } else if (currentResponse.status === 404) {
    flow = await windmillRequest(baseUrl, workspace, token, 'flows/create', { ...assignmentFlow, path: ASSIGNMENT_FLOW_PATH });
  } else {
    throw new Error(`Windmill flow lookup failed (${currentResponse.status})`);
  }
  return { scriptPath: ASSIGNMENT_SCRIPT_PATH, flowPath: ASSIGNMENT_FLOW_PATH, script, flow };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const baseUrl = process.env.WINDMILL_URL ?? 'http://127.0.0.1:8000';
  const workspace = process.env.WINDMILL_WORKSPACE;
  const token = process.env.WINDMILL_TOKEN;
  if (!workspace || !token) {
    console.error('Set WINDMILL_WORKSPACE and WINDMILL_TOKEN after creating a Windmill API token.');
    process.exit(2);
  }
  try {
    const result = await importWindmillAssignmentFlow({ baseUrl, workspace, token });
    console.log(JSON.stringify(result));
  } catch (error) {
    console.error(error.message);
    process.exit(1);
  }
}
