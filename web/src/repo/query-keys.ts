export interface DatabaseSchemaLookupParams {
  scope?: 'tables'
  table?: string
}

export const flowTaskKeys = {
  all: ['flow-tasks'] as const,
  detail: (flowTaskId: number) => ['flow-tasks', flowTaskId] as const,
  graph: (flowTaskId: number) => ['flow-tasks', flowTaskId, 'graph'] as const,
  list: (page = 1, pageSize = 20) =>
    ['flow-tasks', 'list', { page, pageSize }] as const,
  nodeSchema: (flowTaskId: number, nodeId: string, fingerprintKey: string) =>
    ['node-schema', flowTaskId, nodeId, fingerprintKey] as const,
  previewStatus: (previewTaskId: string | null) =>
    ['flow-task-preview-status', previewTaskId] as const,
  runStatus: (runTaskId: string | null) =>
    ['flow-task-run-status', runTaskId] as const,
  runs: (flowTaskId: number) => ['flow-tasks', flowTaskId, 'runs'] as const,
  status: (taskId: string | null) => ['flow-task-status', taskId] as const,
  versions: (flowTaskId: number) => ['flow-task-versions', flowTaskId] as const,
}

export const linkedTaskKeys = {
  all: ['linked-tasks'] as const,
  detail: (linkedTaskId: number) => ['linked-task', linkedTaskId] as const,
  list: (page = 1, pageSize = 20) =>
    ['linked-tasks', 'list', { page, pageSize }] as const,
  runs: (linkedTaskId: number) => ['linked-task-runs', linkedTaskId] as const,
}

export const pipelineKeys = {
  all: ['pipelines'] as const,
  detail: (pipelineId: number) => ['pipeline', pipelineId] as const,
  destinationSchema: (
    destinationId: number,
    params: DatabaseSchemaLookupParams
  ) => ['destination-schema', destinationId, params] as const,
  previewStatus: (pipelineId: number, previewTaskId: string | null) =>
    ['pipeline-preview-status', pipelineId, previewTaskId] as const,
  sourceSchema: (sourceId: number, params: DatabaseSchemaLookupParams) =>
    ['source-schema', sourceId, params] as const,
  tableSyncDetails: (
    pipelineId: number,
    destinationId: number,
    syncId: number
  ) => ['table-sync-details', pipelineId, destinationId, syncId] as const,
}
