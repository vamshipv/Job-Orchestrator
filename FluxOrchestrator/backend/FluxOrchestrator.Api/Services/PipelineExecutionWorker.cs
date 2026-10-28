using FluxOrchestrator.Api.Data;
using FluxOrchestrator.Api.Hubs;
using FluxOrchestrator.Api.Models;
using Microsoft.AspNetCore.SignalR;
using Microsoft.EntityFrameworkCore;

namespace FluxOrchestrator.Api.Services;

/// <summary>
/// A long-running background worker that processes pipeline runs in DAG dependency order.
/// Uses SemaphoreSlim to synchronize database writes across concurrent parallel tasks.
/// </summary>
public class PipelineExecutionWorker : BackgroundService
{
    private readonly JobExecutionQueue _queue;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly IHubContext<PipelineHub> _hubContext;
    private readonly ILogger<PipelineExecutionWorker> _logger;

    public PipelineExecutionWorker(
        JobExecutionQueue queue,
        IServiceScopeFactory scopeFactory,
        IHubContext<PipelineHub> hubContext,
        ILogger<PipelineExecutionWorker> logger)
    {
        _queue = queue;
        _scopeFactory = scopeFactory;
        _hubContext = hubContext;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Pipeline Execution Worker is running and waiting for jobs...");

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
        using var scope = _scopeFactory.CreateScope();
        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();

        // An async lock to prevent parallel tasks from calling DbContext at the same exact millisecond
        using var dbLock = new SemaphoreSlim(1, 1);

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

        // 1. Mark run as Running
        run.Status = PipelineRunStatus.Running;
        run.StartedAt = DateTime.UtcNow;
        await db.SaveChangesAsync(cancellationToken);

        var runRoom = runId.ToString();
        await BroadcastLogAsync(runRoom, "system", $"[START] Pipeline '{pipeline.Name}' initiated.", "info");

        var stepDefs = pipeline.Steps.ToDictionary(s => s.StepId);

        // 2. DAG Execution Loop
        while (!cancellationToken.IsCancellationRequested)
        {
            var readySteps = run.StepStatuses
                .Where(s => s.State == ExecutionState.Pending)
                .Where(s =>
                {
                    var def = stepDefs[s.StepId];
                    return def.DependsOn.Count == 0 ||
                           def.DependsOn.All(depId =>
                               run.StepStatuses.Any(other => other.StepId == depId && other.State == ExecutionState.Succeeded));
                })
                .ToList();

            if (readySteps.Count == 0)
            {
                var anyRunning = run.StepStatuses.Any(s => s.State == ExecutionState.Running);
                var anyPending = run.StepStatuses.Any(s => s.State == ExecutionState.Pending);

                if (!anyRunning && !anyPending)
                {
                    break; // All steps finished!
                }

                await Task.Delay(300, cancellationToken);
                continue;
            }

            // Execute ready steps concurrently with safe database synchronization
            var stepTasks = readySteps.Select(step => 
                ExecuteStepAsync(runId, run, step, stepDefs[step.StepId], db, dbLock, cancellationToken));
            await Task.WhenAll(stepTasks);
        }

        // 3. Finalize Run Status
        var hasFailed = run.StepStatuses.Any(s => s.State == ExecutionState.Failed);
        run.Status = hasFailed ? PipelineRunStatus.Failed : PipelineRunStatus.Succeeded;
        run.CompletedAt = DateTime.UtcNow;

        await dbLock.WaitAsync(cancellationToken);
        try
        {
            db.Entry(run).Property(r => r.StepStatuses).IsModified = true;
            await db.SaveChangesAsync(cancellationToken);
        }
        finally
        {
            dbLock.Release();
        }

        // Broadcast completion to SignalR clients
        await _hubContext.Clients.Group(runRoom).SendAsync("PipelineCompleted", new
        {
            runId = run.RunId,
            status = run.Status.ToString(),
            completedAt = run.CompletedAt
        }, cancellationToken);

        await BroadcastLogAsync(runRoom, "system", $"[COMPLETE] Pipeline finished with status: {run.Status}", "success");
    }

    private async Task ExecuteStepAsync(
        Guid runId,
        PipelineRun run,
        StepExecutionStatus stepStatus,
        PipelineStepDefinition stepDef,
        AppDbContext db,
        SemaphoreSlim dbLock,
        CancellationToken cancellationToken)
    {
        var runRoom = runId.ToString();

        // A. Transition to Running (Lock DB for 1ms to save state)
        await dbLock.WaitAsync(cancellationToken);
        try
        {
            stepStatus.State = ExecutionState.Running;
            stepStatus.StartedAt = DateTime.UtcNow;
            db.Entry(run).Property(r => r.StepStatuses).IsModified = true;
            await db.SaveChangesAsync(cancellationToken);
        }
        finally
        {
            dbLock.Release();
        }

        // Broadcast to SignalR
        await _hubContext.Clients.Group(runRoom).SendAsync("StepStatusUpdated", new
        {
            runId,
            stepId = stepDef.StepId,
            state = "Running",
            timestamp = DateTime.UtcNow
        }, cancellationToken);

        await BroadcastLogAsync(runRoom, stepDef.StepId, $"--> Started step '{stepDef.Name}' ({stepDef.EstimatedSeconds}s)", "info");

        // B. Simulate real work in parallel (NO lock here, runs freely on separate threads!)
        await Task.Delay((stepDef.EstimatedSeconds * 1000) / 2, cancellationToken);
        await BroadcastLogAsync(runRoom, stepDef.StepId, $"[PROCESSING] Executing payload for '{stepDef.Name}'...", "info");
        await Task.Delay((stepDef.EstimatedSeconds * 1000) / 2, cancellationToken);

        // C. Transition to Succeeded (Lock DB for 1ms to save state)
        await dbLock.WaitAsync(cancellationToken);
        try
        {
            stepStatus.State = ExecutionState.Succeeded;
            stepStatus.CompletedAt = DateTime.UtcNow;
            db.Entry(run).Property(r => r.StepStatuses).IsModified = true;
            await db.SaveChangesAsync(cancellationToken);
        }
        finally
        {
            dbLock.Release();
        }

        // Broadcast to SignalR
        await _hubContext.Clients.Group(runRoom).SendAsync("StepStatusUpdated", new
        {
            runId,
            stepId = stepDef.StepId,
            state = "Succeeded",
            timestamp = DateTime.UtcNow
        }, cancellationToken);

        await BroadcastLogAsync(runRoom, stepDef.StepId, $"<-- Completed step '{stepDef.Name}' successfully.", "success");
    }

    private async Task BroadcastLogAsync(string room, string stepId, string message, string level)
    {
        await _hubContext.Clients.Group(room).SendAsync("LogStreamed", new
        {
            stepId,
            message,
            level,
            timestamp = DateTime.UtcNow.ToString("HH:mm:ss")
        });
    }
}