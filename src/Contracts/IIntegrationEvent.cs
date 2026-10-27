namespace Contracts;

/// <summary>
/// An event published to other services.
/// <see cref="EventType"/> is versioned and travels as the AMQP <c>type</c> property;
/// <see cref="RoutingKey"/> is the topic routing key on the publishing service's exchange.
/// </summary>
public interface IIntegrationEvent
{
    static abstract string EventType { get; }

    static abstract string RoutingKey { get; }
}
