using Microsoft.AspNetCore.SignalR;

namespace FluxOrchestrator.Api.Hubs;

/// <summary>
/// SignalR Hub that manages WebSocket connections for real-time pipeline monitoring.
/// Allows clients to subscribe only to the specific pipeline run they are viewing.
/// </summary>
public class PipelineHub : Hub
{
    private readonly ILogger<PipelineHub> _logger;

    public PipelineHub(ILogger<PipelineHub> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Called by the Angular/React frontend to join a specific run's notification room.
    /// </summary>
    public async Task JoinRun(string runId)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, runId);
        _logger.LogInformation("Client {ConnectionId} joined room for RunId: {RunId}", Context.ConnectionId, runId);
    }

    /// <summary>
    /// Called when the user navigates away from the run monitor page.
    /// </summary>
    public async Task LeaveRun(string runId)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, runId);
        _logger.LogInformation("Client {ConnectionId} left room for RunId: {RunId}", Context.ConnectionId, runId);
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        _logger.LogInformation("Client {ConnectionId} disconnected.", Context.ConnectionId);
        return base.OnDisconnectedAsync(exception);
    }
}