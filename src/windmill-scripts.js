export const ASSIGNMENT_SCRIPT_PATH = 'orchestration/assign_task_script';

export const assignmentScript = {
  summary: 'Validate and complete an allowlisted test task assignment',
  description: 'Returns an assigned test task result without sending an external notification.',
  content: `export async function main(operationId: string, conversationId: string, task: Record<string, unknown>) {
  if (!operationId || !conversationId) throw new Error('operationId and conversationId are required');
  if (task.allowlisted !== true || task.testOnly !== true) throw new Error('Only allowlisted test task assignments are enabled');
  const assignee = typeof task.assignee === 'string' && task.assignee.trim() ? task.assignee.trim() : 'test-operator';
  const title = typeof task.title === 'string' && task.title.trim() ? task.title.trim() : 'Follow up on test customer inquiry';
  return {
    operationId,
    conversationId,
    taskId: 'task-' + operationId,
    title,
    assignee,
    status: 'completed',
    completedAt: new Date().toISOString(),
    externalNotifications: 'disabled',
  };
}`,
  language: 'bun',
  kind: 'script',
  path: ASSIGNMENT_SCRIPT_PATH,
};
