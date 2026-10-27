using Contracts;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace BuildingBlocks.Messaging;

/// <summary>
/// Declares <see cref="MessagingTopology"/> once per process; every component awaits it before touching the broker.
/// Declarations are idempotent, so all replicas of all services can run it concurrently.
/// For every consumer queue it creates the full set: main queue, delayed-retry tiers and a dead-letter queue.
/// </summary>
public sealed partial class RabbitMqTopology(RabbitMqConnections connections, ILogger<RabbitMqTopology> logger)
{
    /// <summary>Broker-level retry tiers. Each tier is its own queue with a fixed TTL (see <see cref="RetryQueueArguments"/>).</summary>
    public static readonly IReadOnlyList<TimeSpan> RetryDelays =
        [TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30), TimeSpan.FromMinutes(2), TimeSpan.FromMinutes(10)];

    /// <summary>Redeliveries of the same message (crash before ack) before the broker dead-letters it: a crash-loop guard.</summary>
    private const int DeliveryLimit = 10;

    private readonly SemaphoreSlim _gate = new(1, 1);
    private volatile bool _declared;

    public static string RetryQueueName(string queue, TimeSpan delay) =>
        $"{queue}.retry.{(delay.TotalMinutes >= 1 ? $"{delay.TotalMinutes:0}m" : $"{delay.TotalSeconds:0}s")}";

    public static string DeadLetterQueueName(string queue) => $"{queue}.dlq";

    public async Task EnsureDeclaredAsync(CancellationToken cancellationToken)
    {
        if (_declared)
        {
            return;
        }

        await _gate.WaitAsync(cancellationToken);
        try
        {
            if (_declared)
            {
                return;
            }

            var connection = await connections.GetPublishConnectionAsync(cancellationToken);
            await using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

            foreach (var exchange in MessagingTopology.Exchanges)
            {
                await channel.ExchangeDeclareAsync(
                    exchange, ExchangeType.Topic, durable: true, autoDelete: false, cancellationToken: cancellationToken);
            }

            foreach (var queue in MessagingTopology.ConsumerQueues)
            {
                await DeclareConsumerQueueAsync(channel, queue, cancellationToken);
            }

            _declared = true;
            LogDeclared(MessagingTopology.Exchanges.Count, MessagingTopology.ConsumerQueues.Count);
        }
        finally
        {
            _gate.Release();
        }
    }

    private static async Task DeclareConsumerQueueAsync(IChannel channel, ConsumerQueue queue, CancellationToken cancellationToken)
    {
        await channel.QueueDeclareAsync(
            DeadLetterQueueName(queue.Name), durable: true, exclusive: false, autoDelete: false,
            arguments: new Dictionary<string, object?> { ["x-queue-type"] = "quorum" },
            cancellationToken: cancellationToken);

        foreach (var delay in RetryDelays)
        {
            await channel.QueueDeclareAsync(
                RetryQueueName(queue.Name, delay), durable: true, exclusive: false, autoDelete: false,
                arguments: RetryQueueArguments(queue.Name, delay), cancellationToken: cancellationToken);
        }

        await channel.QueueDeclareAsync(
            queue.Name, durable: true, exclusive: false, autoDelete: false,
            arguments: MainQueueArguments(queue.Name), cancellationToken: cancellationToken);

        await channel.QueueBindAsync(queue.Name, queue.Exchange, queue.BindingKey, cancellationToken: cancellationToken);
    }

    /// <summary>Messages redelivered more than <see cref="DeliveryLimit"/> times go to the DLQ instead of looping forever.</summary>
    private static Dictionary<string, object?> MainQueueArguments(string queue) => new()
    {
        ["x-queue-type"] = "quorum",
        ["x-delivery-limit"] = DeliveryLimit,
        ["x-dead-letter-exchange"] = string.Empty,
        ["x-dead-letter-routing-key"] = DeadLetterQueueName(queue),
        ["x-dead-letter-strategy"] = "at-least-once",
        ["x-overflow"] = "reject-publish",
    };

    /// <summary>
    /// No consumer: a message waits for the queue-wide TTL, then the broker dead-letters it back to the main queue.
    /// A per-queue TTL instead of a per-message one, because per-message TTLs expire only at the head of the queue.
    /// Quorum queues dead-letter at-most-once by default; at-least-once (which requires reject-publish overflow)
    /// makes sure no message is lost on its way back.
    /// </summary>
    private static Dictionary<string, object?> RetryQueueArguments(string queue, TimeSpan delay) => new()
    {
        ["x-queue-type"] = "quorum",
        ["x-message-ttl"] = (long)delay.TotalMilliseconds,
        ["x-dead-letter-exchange"] = string.Empty,
        ["x-dead-letter-routing-key"] = queue,
        ["x-dead-letter-strategy"] = "at-least-once",
        ["x-overflow"] = "reject-publish",
    };

    [LoggerMessage(Level = LogLevel.Information, Message = "RabbitMQ topology declared: {ExchangeCount} exchanges, {QueueCount} consumer queues with retry tiers and DLQs")]
    private partial void LogDeclared(int exchangeCount, int queueCount);
}
