using System.Text.Json.Serialization.Metadata;
using Contracts;

namespace BuildingBlocks.Messaging.Consuming;

public sealed class ConsumerOptions
{
    /// <summary>Deliveries processed in parallel on the consumer's channel (async handlers: no thread is held while waiting on I/O).</summary>
    public ushort Concurrency { get; set; } = 16;

    /// <summary>Unacked deliveries the broker may push ahead; about 2× concurrency keeps handlers busy without hoarding.</summary>
    public ushort Prefetch { get; set; } = 32;

    /// <summary>How long consumption pauses when a dependency's circuit is open and the breaker gives no hint.</summary>
    public TimeSpan DefaultPause { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>On shutdown, in-flight deliveries get this long to finish before their work is cancelled.</summary>
    public TimeSpan DrainTimeout { get; set; } = TimeSpan.FromSeconds(10);
}

internal sealed record ConsumerRegistration<TMessage>(
    ConsumerQueue Queue, string EventType, JsonTypeInfo<TMessage> TypeInfo, ConsumerOptions Options);
