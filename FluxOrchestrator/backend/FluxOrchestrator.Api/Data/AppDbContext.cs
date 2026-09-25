using FluxOrchestrator.Api.Models;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;

namespace FluxOrchestrator.Api.Data;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    // Tables
    public DbSet<PipelineDefinition> Pipelines => Set<PipelineDefinition>();
    public DbSet<PipelineRun> Runs => Set<PipelineRun>();

    
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.Entity<PipelineRun>().HasKey(r => r.RunId);
        // Store the Steps list as a JSON string inside the database
        modelBuilder.Entity<PipelineDefinition>()
            .Property(p => p.Steps)
            .HasConversion(
                v => JsonSerializer.Serialize(v, (JsonSerializerOptions)null!),
                v => JsonSerializer.Deserialize<List<PipelineStepDefinition>>(v, (JsonSerializerOptions)null!) 
                     ?? new List<PipelineStepDefinition>()
            );

        // Store the StepStatuses list as a JSON string inside the database
        modelBuilder.Entity<PipelineRun>()
            .Property(r => r.StepStatuses)
            .HasConversion(
                v => JsonSerializer.Serialize(v, (JsonSerializerOptions)null!),
                v => JsonSerializer.Deserialize<List<StepExecutionStatus>>(v, (JsonSerializerOptions)null!) 
                     ?? new List<StepExecutionStatus>()
            );
    }
}