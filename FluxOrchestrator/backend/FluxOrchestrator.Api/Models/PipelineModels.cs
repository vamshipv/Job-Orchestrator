namespace FluxOrchestrator.Api.Models;

/// <summary>
/// Represents the static blueprint/definition of a workflow pipeline.
/// </summary>
public class PipelineDefinition
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public string Name { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public List<PipelineStepDefinition> Steps { get; set; } = [];
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

/// <summary>
/// Represents an individual task/node within the pipeline DAG.
/// </summary>
public class PipelineStepDefinition
{
    // A machine-readable key, e.g., "extract-orders"
    public string StepId { get; set; } = string.Empty;

    // A human-friendly label for the UI, e.g., "Extract Orders from Database"
    public string Name { get; set; } = string.Empty;

    // How many seconds this step will take when simulated
    public int EstimatedSeconds { get; set; } = 2;

    // The list of StepIds that must finish with 'Succeeded' before this step can start
    public List<string> DependsOn { get; set; } = [];
}

/// <summary>
/// Represents an actual execution instance of a pipeline.
/// </summary>
public class PipelineRun
{
    public Guid RunId { get; set; } = Guid.NewGuid();
    public Guid PipelineId { get; set; }
    public PipelineRunStatus Status { get; set; } = PipelineRunStatus.Queued;
    public DateTime StartedAt { get; set; } = DateTime.UtcNow;
    public DateTime? CompletedAt { get; set; }
    public List<StepExecutionStatus> StepStatuses { get; set; } = [];
}

/// <summary>
/// Tracks the live state and execution timings of an individual step during a run.
/// </summary>
public class StepExecutionStatus
{
    public string StepId { get; set; } = string.Empty;
    public ExecutionState State { get; set; } = ExecutionState.Pending;
    public DateTime? StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public string? ErrorMessage { get; set; }
}

public enum PipelineRunStatus
{
    Queued,
    Running,
    Succeeded,
    Failed
}

public enum ExecutionState
{
    Pending,
    Running,
    Succeeded,
    Failed
}