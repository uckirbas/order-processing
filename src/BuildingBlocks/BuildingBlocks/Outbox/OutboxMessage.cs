using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace BuildingBlocks.Outbox;

/// <summary>
/// A message written in the same transaction as the business change and published later by the relay.
/// Rows are deleted once the broker confirms them, so the table only ever holds pending messages.
/// </summary>
public sealed class OutboxMessage
{
    private OutboxMessage()
    {
    }

    /// <summary>UUIDv7; also used as the AMQP message id so consumers can correlate duplicates.</summary>
    public Guid Id { get; private set; }

    public string Type { get; private set; } = null!;

    public string RoutingKey { get; private set; } = null!;

    public string Payload { get; private set; } = null!;

    public DateTimeOffset CreatedAt { get; private set; }

    /// <summary>Lease set by the relay when it claims the row; null or expired means claimable.</summary>
    public DateTimeOffset? LockedUntil { get; private set; }

    public int Attempts { get; private set; }

    public static OutboxMessage Create<T>(
        T message, string type, string routingKey, JsonTypeInfo<T> typeInfo, DateTimeOffset createdAt) => new()
    {
        Id = Guid.CreateVersion7(createdAt),
        Type = type,
        RoutingKey = routingKey,
        Payload = JsonSerializer.Serialize(message, typeInfo),
        CreatedAt = createdAt,
    };
}
