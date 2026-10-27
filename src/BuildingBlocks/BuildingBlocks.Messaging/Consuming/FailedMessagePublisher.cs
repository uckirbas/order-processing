using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace BuildingBlocks.Messaging.Consuming;

/// <summary>
/// Re-publishes a delivery to one of its retry tiers or to its DLQ (default exchange, routing key = queue name),
/// waiting for the broker's confirm before the original is acked.
/// Shared by every concurrent handler in the process. The client does not document concurrent use of one channel,
/// so publishes are serialised through a semaphore; this is the failure path only, so the cost is negligible.
/// </summary>
internal sealed class FailedMessagePublisher(RabbitMqConnections connections) : IAsyncDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private IChannel? _channel;

    /// <remarks>Must be called while the delivery handler is running: the delivery body is only valid until it returns.</remarks>
    public async Task PublishAsync(
        string queue, BasicDeliverEventArgs delivery, Dictionary<string, object?> headers, CancellationToken cancellationToken)
    {
        var source = delivery.BasicProperties;
        var properties = new BasicProperties
        {
            MessageId = source.MessageId,
            Type = source.Type,
            ContentType = source.ContentType,
            Timestamp = source.Timestamp,
            DeliveryMode = DeliveryModes.Persistent,
            Headers = headers,
        };

        await _gate.WaitAsync(cancellationToken);
        try
        {
            var channel = await GetChannelAsync(cancellationToken);
            await channel.BasicPublishAsync(string.Empty, queue, mandatory: true, properties, delivery.Body, cancellationToken);
        }
        finally
        {
            _gate.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.DisposeAsync();
        }

        _gate.Dispose();
    }

    private async ValueTask<IChannel> GetChannelAsync(CancellationToken cancellationToken)
    {
        if (_channel is { IsOpen: true })
        {
            return _channel;
        }

        if (_channel is not null)
        {
            await _channel.DisposeAsync();
        }

        var connection = await connections.GetPublishConnectionAsync(cancellationToken);
        _channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            cancellationToken);
        return _channel;
    }
}
