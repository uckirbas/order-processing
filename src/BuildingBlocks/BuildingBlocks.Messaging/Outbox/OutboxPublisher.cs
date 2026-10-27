using System.Text;
using BuildingBlocks.Outbox;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace BuildingBlocks.Messaging.Outbox;

/// <summary>
/// Publishes a batch on one confirm-enabled channel and reports which messages the broker confirmed.
/// Not thread-safe by design: the relay loop is its only caller and runs one batch at a time.
/// </summary>
internal sealed class OutboxPublisher(RabbitMqConnections connections) : IAsyncDisposable
{
    private IChannel? _channel;

    public async Task<PublishOutcome> PublishAsync(
        string exchange, IReadOnlyList<PendingOutboxMessage> messages, CancellationToken cancellationToken)
    {
        var channel = await GetChannelAsync(cancellationToken);

        // Start every publish first, then await the confirms: one network round-trip for the whole batch instead of one
        // per message. The client caps outstanding confirms (128 by default) and throttles beyond that.
        // AsTask(): a ValueTask must be consumed exactly once and must not be stored (CA2012); a Task can be.
        var publishes = new List<Task>(messages.Count);
        foreach (var message in messages)
        {
            var properties = new BasicProperties
            {
                MessageId = message.Id.ToString(),
                Type = message.Type,
                ContentType = "application/json",
                DeliveryMode = DeliveryModes.Persistent,
                Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
            };

            // mandatory: a message that reaches no queue comes back as basic.return instead of vanishing.
            publishes.Add(channel.BasicPublishAsync(
                exchange, message.RoutingKey, mandatory: true, properties, Encoding.UTF8.GetBytes(message.Payload), cancellationToken)
                .AsTask());
        }

        var confirmed = new List<Guid>(messages.Count);
        var rejected = new List<(Guid Id, string Reason)>();
        Exception? failure = null;

        // Every publish is awaited, even after a failure, so no exception is left unobserved.
        for (var i = 0; i < publishes.Count; i++)
        {
            try
            {
                await publishes[i];
                confirmed.Add(messages[i].Id);
            }
            catch (PublishException ex)
            {
                rejected.Add((messages[i].Id, ex.IsReturn ? "unroutable (basic.return)" : "nacked by broker"));
            }
            catch (Exception ex)
            {
                failure ??= ex;
            }
        }

        return new PublishOutcome(confirmed, rejected, failure);
    }

    public async ValueTask DisposeAsync()
    {
        if (_channel is not null)
        {
            await _channel.DisposeAsync();
        }
    }

    private async ValueTask<IChannel> GetChannelAsync(CancellationToken cancellationToken)
    {
        if (_channel is { IsOpen: true })
        {
            return _channel;
        }

        // A channel closed by the broker (e.g. after a protocol error) is not recovered automatically: replace it.
        if (_channel is not null)
        {
            await _channel.DisposeAsync();
            _channel = null;
        }

        var connection = await connections.GetPublishConnectionAsync(cancellationToken);
        _channel = await connection.CreateChannelAsync(
            new CreateChannelOptions(publisherConfirmationsEnabled: true, publisherConfirmationTrackingEnabled: true),
            cancellationToken);
        return _channel;
    }
}

/// <param name="Confirmed">Safe to delete from the outbox.</param>
/// <param name="Rejected">Nacked or unroutable; the rows stay and are retried once their lease expires.</param>
/// <param name="Failure">Channel- or connection-level failure; unconfirmed rows stay as well.</param>
internal sealed record PublishOutcome(
    IReadOnlyList<Guid> Confirmed, IReadOnlyList<(Guid Id, string Reason)> Rejected, Exception? Failure);
