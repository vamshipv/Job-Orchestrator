using FluxOrchestrator.Api.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace FluxOrchestrator.Api.Data;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    public DbSet<PipelineDefinition> Pipelines => Set<PipelineDefinition>();
    public DbSet<PipelineRun> Runs => Set<PipelineRun>();

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        Converters = { new JsonStringEnumConverter() }
    };

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<PipelineRun>().HasKey(r => r.RunId);

        // Steps Conversion
        modelBuilder.Entity<PipelineDefinition>()
            .Property(p => p.Steps)
            .HasConversion(
                v => JsonSerializer.Serialize(v, JsonOptions),
                v => JsonSerializer.Deserialize<List<PipelineStepDefinition>>(v, JsonOptions) ?? new List<PipelineStepDefinition>()
            );

        // StepStatuses Conversion with explicit ChangeTracker ValueComparer
        var stepStatusComparer = new ValueComparer<List<StepExecutionStatus>>(
            (c1, c2) => JsonSerializer.Serialize(c1, JsonOptions) == JsonSerializer.Serialize(c2, JsonOptions),
            c => c.Aggregate(0, (a, v) => HashCode.Combine(a, v.GetHashCode())),
            c => JsonSerializer.Deserialize<List<StepExecutionStatus>>(JsonSerializer.Serialize(c, JsonOptions), JsonOptions)!
        );

        modelBuilder.Entity<PipelineRun>()
            .Property(r => r.StepStatuses)
            .HasConversion(
                v => JsonSerializer.Serialize(v, JsonOptions),
                v => JsonSerializer.Deserialize<List<StepExecutionStatus>>(v, JsonOptions) ?? new List<StepExecutionStatus>()
            )
            .Metadata.SetValueComparer(stepStatusComparer);
    }
}