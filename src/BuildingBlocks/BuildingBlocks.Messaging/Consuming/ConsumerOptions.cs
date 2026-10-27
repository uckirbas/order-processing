using System.Text.Json.Serialization.Metadata;
using Contracts;

namespace BuildingBlocks.Messaging.Consuming;

public sealed class ConsumerOptions
{
    /// <summary>Batches handled in parallel (async handlers: no thread is held while waiting on I/O).</summary>
    public ushort Concurrency { get; set; } = 16;

    /// <summary>
    /// Unacked deliveries the broker may push ahead. Must be at least <see cref="MaxBatchSize"/>; about
    /// 2 × <see cref="Concurrency"/> × <see cref="MaxBatchSize"/> keeps every slot fed without hoarding.
    /// </summary>
    public ushort Prefetch { get; set; } = 32;

    /// <summary>
    /// Upper bound of deliveries handed to the handler at once. 1 turns batching off (one message per handler call),
    /// which is kept selectable so both modes can be measured against each other.
    /// </summary>
    public int MaxBatchSize { get; set; } = 1;

    /// <summary>
    /// How long a batch that is not full yet may wait for more deliveries. Zero takes only what is already buffered:
    /// batches then grow by themselves under load, while a lone message is never delayed.
    /// </summary>
    public TimeSpan MaxBatchDelay { get; set; } = TimeSpan.Zero;

    /// <summary>How long consumption pauses when a dependency's circuit is open and the breaker gives no hint.</summary>
    public TimeSpan DefaultPause { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>On shutdown, in-flight deliveries get this long to finish before their work is cancelled.</summary>
    public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(10);
}

internal sealed record ConsumerRegistration<TMessage>(
    ConsumerQueue Queue, string EventType, JsonTypeInfo<TMessage> TypeInfo, ConsumerOptions Options);
