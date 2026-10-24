using FluxOrchestrator.Api.Data;
using FluxOrchestrator.Api.Hubs;
using FluxOrchestrator.Api.Models;
using FluxOrchestrator.Api.Services;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// 1. Register OpenAPI / Swagger
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new Microsoft.OpenApi.OpenApiInfo
    {
        Title = "FluxOrchestrator API",
        Version = "v1",
        Description = "Workflow DAG & Background Job Orchestrator API"
    });
});

// 2. Register EF Core In-Memory Database
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseInMemoryDatabase("FluxOrchestratorDb"));

// 3. Register our Queue and Background Worker
builder.Services.AddSingleton<JobExecutionQueue>();
builder.Services.AddHostedService<PipelineExecutionWorker>();

// 4. Register SignalR
builder.Services.AddSignalR();

// 5. Configure CORS (Configured specifically to support SignalR WebSockets & Credentials)
builder.Services.AddCors(options =>
{
    options.AddPolicy("AllowFrontend", policy =>
    {
        policy.SetIsOriginAllowed(_ => true) // Allows any local dev port (4200, 3000, 5173)
              .AllowAnyHeader()
              .AllowAnyMethod()
              .AllowCredentials();
    });
});

var app = builder.Build();

// Configure HTTP Request Pipeline
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c =>
    {
        c.SwaggerEndpoint("/swagger/v1/swagger.json", "FluxOrchestrator API v1");
    });
}

app.UseCors("AllowFrontend");

// ==========================================
// SignalR Hub Endpoint
// ==========================================
app.MapHub<PipelineHub>("/hubs/pipeline");

// ==========================================
// Minimal API Endpoints
// ==========================================

// GET /api/pipelines - Returns all pipeline definitions
app.MapGet("/api/pipelines", async (AppDbContext db) =>
{
    var pipelines = await db.Pipelines.ToListAsync();
    return Results.Ok(pipelines);
})
.WithName("GetPipelines");

// GET /api/pipelines/{id} - Returns a single pipeline definition by ID
app.MapGet("/api/pipelines/{id:guid}", async (Guid id, AppDbContext db) =>
{
    var pipeline = await db.Pipelines.FindAsync(id);
    return pipeline is not null ? Results.Ok(pipeline) : Results.NotFound();
})
.WithName("GetPipelineById");

// POST /api/pipelines - Creates a new pipeline definition
app.MapPost("/api/pipelines", async (PipelineDefinition newPipeline, AppDbContext db) =>
{
    newPipeline.Id = Guid.NewGuid();
    newPipeline.CreatedAt = DateTime.UtcNow;
    db.Pipelines.Add(newPipeline);
    await db.SaveChangesAsync();
    return Results.Created($"/api/pipelines/{newPipeline.Id}", newPipeline);
})
.WithName("CreatePipeline");

// POST /api/pipelines/{id}/trigger - Enqueues a new pipeline run
app.MapPost("/api/pipelines/{id:guid}/trigger", async (Guid id, AppDbContext db, JobExecutionQueue queue) =>
{
    var pipeline = await db.Pipelines.FindAsync(id);
    if (pipeline is null)
    {
        return Results.NotFound(new { error = $"Pipeline with ID '{id}' was not found." });
    }

    var run = new PipelineRun
    {
        RunId = Guid.NewGuid(),
        PipelineId = pipeline.Id,
        Status = PipelineRunStatus.Queued,
        StartedAt = DateTime.UtcNow,
        StepStatuses = pipeline.Steps.Select(step => new StepExecutionStatus
        {
            StepId = step.StepId,
            State = ExecutionState.Pending
        }).ToList()
    };

    db.Runs.Add(run);
    await db.SaveChangesAsync();

    // Hand off to the background channel
    await queue.EnqueueRunAsync(run.RunId);

    return Results.Accepted($"/api/runs/{run.RunId}", new
    {
        runId = run.RunId,
        status = run.Status.ToString(),
        message = "Pipeline run queued successfully."
    });
})
.WithName("TriggerPipeline");

// GET /api/runs/{runId} - Check status of a run
app.MapGet("/api/runs/{runId:guid}", async (Guid runId, AppDbContext db) =>
{
    var run = await db.Runs.FindAsync(runId);
    return run is not null ? Results.Ok(run) : Results.NotFound();
})
.WithName("GetRunStatus");

// --- Seed Initial Pipelines on Startup ---
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    if (!db.Pipelines.Any())
    {
        db.Pipelines.AddRange(
            new PipelineDefinition
            {
                Id = Guid.Parse("11111111-1111-1111-1111-111111111111"),
                Name = "Daily E-Commerce ETL",
                Description = "Extracts orders and inventory, cleanses anomalies, loads warehouse, and posts Slack summary.",
                Steps = [
                    new() { StepId = "extract-sales", Name = "Extract Sales Data", EstimatedSeconds = 2, DependsOn = [] },
                    new() { StepId = "extract-inventory", Name = "Extract Inventory", EstimatedSeconds = 2, DependsOn = [] },
                    new() { StepId = "cleanse-records", Name = "Cleanse & Deduplicate", EstimatedSeconds = 3, DependsOn = ["extract-sales", "extract-inventory"] },
                    new() { StepId = "load-warehouse", Name = "Load to Data Warehouse", EstimatedSeconds = 2, DependsOn = ["cleanse-records"] },
                    new() { StepId = "send-report", Name = "Send Slack Summary", EstimatedSeconds = 1, DependsOn = ["load-warehouse"] }
                ]
            },
            new PipelineDefinition
            {
                Id = Guid.Parse("22222222-2222-2222-2222-222222222222"),
                Name = "Production Deploy Pipeline",
                Description = "Lints code, runs unit test suite, builds Docker image, and deploys to Kubernetes cluster.",
                Steps = [
                    new() { StepId = "lint", Name = "Static Code Analysis", EstimatedSeconds = 1, DependsOn = [] },
                    new() { StepId = "unit-tests", Name = "Run Unit Tests", EstimatedSeconds = 3, DependsOn = ["lint"] },
                    new() { StepId = "build-image", Name = "Build Docker Image", EstimatedSeconds = 4, DependsOn = ["unit-tests"] },
                    new() { StepId = "deploy-k8s", Name = "Deploy to Cluster", EstimatedSeconds = 2, DependsOn = ["build-image"] }
                ]
            }
        );
        db.SaveChanges();
    }
}

app.Run();