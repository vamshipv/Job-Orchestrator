export interface PipelineDefinition {
  id: string;
  name: string;
  description: string;
  steps: PipelineStepDefinition[];
  createdAt: string;
}

export interface PipelineStepDefinition {
  stepId: string;
  name: string;
  estimatedSeconds: number;
  dependsOn: string[];
}

export type PipelineRunStatus = 'Queued' | 'Running' | 'Succeeded' | 'Failed';
export type ExecutionState = 'Pending' | 'Running' | 'Succeeded' | 'Failed';

export interface PipelineRun {
  runId: string;
  pipelineId: string;
  status: PipelineRunStatus;
  startedAt: string;
  completedAt?: string;
  stepStatuses: StepExecutionStatus[];
}

export interface StepExecutionStatus {
  stepId: string;
  state: ExecutionState;
  startedAt?: string;
  completedAt?: string;
  errorMessage?: string;
}

export interface LogEntry {
  stepId: string;
  message: string;
  level: 'info' | 'success' | 'warn' | 'error';
  timestamp: string;
}