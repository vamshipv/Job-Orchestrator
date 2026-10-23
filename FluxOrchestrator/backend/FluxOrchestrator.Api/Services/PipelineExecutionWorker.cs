using FluxOrchestrator.Api.Data;
using FluxOrchestrator.Api.Models;
using Microsoft.EntityFrameworkCore;

namespace FluxOrchestrator.Api.Services;

/// <summary>
/// A long-running background worker that processes pipeline runs in DAG dependency order.
/// </summary>
public class PipelineExecutionWorker : BackgroundService
{
    private readonly JobExecutionQueue _queue;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<PipelineExecutionWorker> _logger;

    public PipelineExecutionWorker(JobExecutionQueue queue, IServiceScopeFactory scopeFactory, ILogger<PipelineExecutionWorker> logger)
    {
        _queue = queue;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Pipeline Execution Worker is running and waiting for jobs...");

        // Continuously read queued run IDs as they arrive
        await foreach (var runId in _queue.ReadAllAsync(stoppingToken))
        {
            try
            {
                await ProcessPipelineRunAsync(runId, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred while executing RunId: {RunId}", runId);
            }
        }
    }

    private async Task ProcessPipelineRunAsync(Guid runId, CancellationToken cancellationToken)
    {
        // 1. Create a scoped DbContext for this execution unit of work
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        var run = await db.Runs.FindAsync([runId], cancellationToken);
        if (run == null)
        {
            _logger.LogWarning("RunId {RunId} not found in database. Skipping.", runId);
            return;
        }

        var pipeline = await db.Pipelines.FindAsync([run.PipelineId], cancellationToken);
        if (pipeline == null)
        {
            _logger.LogWarning("PipelineId {PipelineId} not found. Failing run.", run.PipelineId);
            run.Status = PipelineRunStatus.Failed;
            await db.SaveChangesAsync(cancellationToken);
            return;
        }

        // 2. Mark the run as Running
        run.Status = PipelineRunStatus.Running;
        run.StartedAt = DateTime.UtcNow;
        await db.SaveChangesAsync(cancellationToken);
        _logger.LogInformation("Starting execution of Pipeline '{Name}' (RunId: {RunId})", pipeline.Name, runId);

        // Map for fast step definition lookup
        var stepDefs = pipeline.Steps.ToDictionary(s => s.StepId);

        // 3. Execution Loop: Continue until all steps are finished
        while (!cancellationToken.IsCancellationRequested)
        {
            // Find all pending steps whose dependencies have ALL succeeded
            var readySteps = run.StepStatuses
                .Where(s => s.State == ExecutionState.Pending)
                .Where(s =>
                {
                    var def = stepDefs[s.StepId];
                    // If no dependencies, it's ready. If it has dependencies, all must be Succeeded.
                    return def.DependsOn.Count == 0 ||
                           def.DependsOn.All(depId =>
                               run.StepStatuses.Any(other => other.StepId == depId && other.State == ExecutionState.Succeeded));
                })
                .ToList();

            // If no steps are ready, check if all steps are completed
            if (readySteps.Count == 0)
            {
                var anyRunning = run.StepStatuses.Any(s => s.State == ExecutionState.Running);
                var anyPending = run.StepStatuses.Any(s => s.State == ExecutionState.Pending);

                if (!anyRunning && !anyPending)
                {
                    // Everything is finished!
                    break;
                }

                // If some steps are running, wait a short moment and check again
                await Task.Delay(500, cancellationToken);
                continue;
            }

            // 4. Execute the ready steps concurrently
            var stepTasks = readySteps.Select(step => ExecuteStepAsync(step, stepDefs[step.StepId], db, cancellationToken));
            await Task.WhenAll(stepTasks);

            // Save updated states to DB after this batch finishes
            await db.SaveChangesAsync(cancellationToken);
        }

        // 5. Finalize Run Status
        var hasFailed = run.StepStatuses.Any(s => s.State == ExecutionState.Failed);
        run.Status = hasFailed ? PipelineRunStatus.Failed : PipelineRunStatus.Succeeded;
        run.CompletedAt = DateTime.UtcNow;
        await db.SaveChangesAsync(cancellationToken);

        _logger.LogInformation("Finished Pipeline '{Name}' (RunId: {RunId}) with status: {Status}", 
            pipeline.Name, runId, run.Status);
    }

    private async Task ExecuteStepAsync(StepExecutionStatus stepStatus, PipelineStepDefinition stepDef, AppDbContext db,
        CancellationToken cancellationToken)
    {
        // Transition: Pending -> Running
        stepStatus.State = ExecutionState.Running;
        stepStatus.StartedAt = DateTime.UtcNow;
        await db.SaveChangesAsync(cancellationToken);
        _logger.LogInformation("--> Step '{StepName}' started (Duration: {Seconds}s)", stepDef.Name, stepDef.EstimatedSeconds);

        // Simulate actual work based on the step's estimated seconds
        await Task.Delay(stepDef.EstimatedSeconds * 1000, cancellationToken);

        // Transition: Running -> Succeeded
        stepStatus.State = ExecutionState.Succeeded;
        stepStatus.CompletedAt = DateTime.UtcNow;
        await db.SaveChangesAsync(cancellationToken);
        _logger.LogInformation("<-- Step '{StepName}' completed successfully", stepDef.Name);
    }
}