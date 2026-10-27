namespace BuildingBlocks.Messaging.Consuming;

/// <summary>
/// Handles a batch of deliveries from one queue (a batch of one when batching is off). Resolved from a fresh DI scope
/// per batch; batches may run concurrently, so implementations must not share mutable state between calls.
/// Outcome per item:
/// <list type="bullet">
/// <item>return normally → every item not marked with <see cref="BatchItem{TMessage}.Fail"/> is acked</item>
/// <item><see cref="BatchItem{TMessage}.Fail"/> → that item alone is settled by its exception:
/// <see cref="PermanentMessageException"/> dead-letters, <see cref="DeferMessageException"/> re-delivers later,
/// anything else retries</item>
/// <item>throw → every item not already failed is settled by that exception (e.g. the database is down)</item>
/// </list>
/// </summary>
public interface IBatchMessageHandler<TMessage>
{
    Task HandleAsync(IReadOnlyList<BatchItem<TMessage>> batch, CancellationToken cancellationToken);

    /// <summary>Called once per message, after its last retry tier failed and before it goes to the DLQ.</summary>
    Task OnRetriesExhaustedAsync(TMessage message, MessageContext context, Exception lastError, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}

/// <summary>One delivery of a batch. The consumer settles it once the batch's handler returns.</summary>
public sealed class BatchItem<TMessage>
{
    private readonly TaskCompletionSource<Exception?> _settled = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Exception? _error;

    internal BatchItem(TMessage message, MessageContext context)
    {
        Message = message;
        Context = context;
    }

    public TMessage Message { get; }

    public MessageContext Context { get; }

    /// <summary>Settles this item by <paramref name="error"/> instead of acking it; the rest of the batch is unaffected.</summary>
    public void Fail(Exception error) => _error = error;

    /// <summary>Completes with null on success, or with the exception the item is to be settled by.</summary>
    internal Task<Exception?> Outcome => _settled.Task;

    internal void Complete(Exception? batchError) => _settled.TrySetResult(_error ?? batchError);
}

/// <param name="RetryCount">Broker-level retries so far (0 on first delivery).</param>
/// <param name="Redelivered">Set by the broker when an earlier delivery of this message was not acked (e.g. a crash).</param>
public sealed record MessageContext(string MessageId, string Queue, int RetryCount, bool Redelivered);

/// <summary>The message can never succeed (invalid content, business rule, SMTP 5xx): no retry, straight to the DLQ.</summary>
public sealed class PermanentMessageException(string message, Exception? innerException = null)
    : Exception(message, innerException);

/// <summary>
/// Not a failure: the work is in progress elsewhere (e.g. another worker holds the lease).
/// The message is re-delivered after about <see cref="Delay"/> without consuming the retry budget.
/// </summary>
public sealed class DeferMessageException(TimeSpan delay, string reason) : Exception(reason)
{
    public TimeSpan Delay { get; } = delay;
}
