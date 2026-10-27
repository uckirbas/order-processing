namespace BuildingBlocks.Messaging.Consuming;

/// <summary>
/// Handles one message. Resolved from a fresh DI scope per delivery; may run concurrently with other deliveries,
/// so implementations must not share mutable state between calls.
/// Outcome is signalled by exceptions: return normally to ack, throw <see cref="PermanentMessageException"/> to
/// dead-letter, <see cref="DeferMessageException"/> to re-deliver later, anything else to retry.
/// </summary>
public interface IMessageHandler<in TMessage>
{
    Task HandleAsync(TMessage message, MessageContext context, CancellationToken cancellationToken);

    /// <summary>Called once, after the last retry tier failed and before the message goes to the DLQ.</summary>
    Task OnRetriesExhaustedAsync(TMessage message, MessageContext context, Exception lastError, CancellationToken cancellationToken) =>
        Task.CompletedTask;
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
