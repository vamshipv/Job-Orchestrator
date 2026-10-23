using System.Threading.Channels;

namespace FluxOrchestrator.Api.Services;

/// <summary>
/// A thread-safe, in-memory queue for scheduling pipeline runs.
/// Decouples the incoming HTTP trigger request from the background execution.
/// </summary>
public class JobExecutionQueue
{
    private readonly Channel<Guid> _queue;

    public JobExecutionQueue()
    {
        // Unbounded channel: accepts runs without blocking the producer
        _queue = Channel.CreateUnbounded<Guid>(new UnboundedChannelOptions
        {
            SingleReader = true // Only our background worker reads from this queue
        });
    }

    /// <summary>
    /// Called by the API endpoint to enqueue a run.
    /// </summary>
    public async ValueTask EnqueueRunAsync(Guid runId, CancellationToken cancellationToken = default)
    {
        await _queue.Writer.WriteAsync(runId, cancellationToken);
    }

    /// <summary>
    /// Called by the background worker to continuously read queued runs.
    /// </summary>
    public IAsyncEnumerable<Guid> ReadAllAsync(CancellationToken cancellationToken = default)
    {
        return _queue.Reader.ReadAllAsync(cancellationToken);
    }
}