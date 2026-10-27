using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Polly.CircuitBreaker;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace BuildingBlocks.Messaging.Consuming;

/// <summary>
/// Consumes one queue with bounded concurrency and turns every handler outcome into exactly one settlement:
/// <list type="bullet">
/// <item>success → ack</item>
/// <item>poison / unknown type / <see cref="PermanentMessageException"/> → DLQ</item>
/// <item><see cref="DeferMessageException"/> → retry tier, retry budget untouched</item>
/// <item>dependency circuit open → retry tier, budget untouched, and consumption pauses</item>
/// <item>anything else → next retry tier (5 s → 30 s → 2 m → 10 m), then DLQ</item>
/// </list>
/// A retry or DLQ copy is confirmed by the broker before the original is acked, so a crash in between can only
/// duplicate a message, never lose it.
/// </summary>
internal sealed partial class RabbitMqConsumerService<TMessage, THandler>(
    ConsumerRegistration<TMessage> registration,
    RabbitMqConnections connections,
    RabbitMqTopology topology,
    FailedMessagePublisher failedMessages,
    IServiceScopeFactory scopes,
    TimeProvider clock,
    ILogger<RabbitMqConsumerService<TMessage, THandler>> logger) : BackgroundService
    where THandler : class, IMessageHandler<TMessage>
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);

    private readonly string _queue = registration.Queue.Name;
    private readonly ConsumerOptions _options = registration.Options;

    // Guards consumer registration (start / cancel), which the pause task and shutdown can both attempt.
    private readonly SemaphoreSlim _consumeGate = new(1, 1);

    // Cancelled only after the drain timeout on shutdown, so in-flight work normally completes.
    private readonly CancellationTokenSource _processing = new();

    private IChannel? _channel;
    private string? _consumerTag;
    private int _inFlight;
    private int _pausing;
    private CancellationToken _stopping;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _stopping = stoppingToken;
        try
        {
            await ConnectAsync(stoppingToken);
            await Task.Delay(Timeout.InfiniteTimeSpan, clock, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            await DrainAsync();
        }
    }

    public override void Dispose()
    {
        _processing.Dispose();
        _consumeGate.Dispose();
        base.Dispose();
    }

    /// <summary>The broker may not be up yet when the process starts: keep trying until it is.</summary>
    private async Task ConnectAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            try
            {
                await topology.EnsureDeclaredAsync(cancellationToken);
                var connection = await connections.GetConsumeConnectionAsync(cancellationToken);
                _channel = await connection.CreateChannelAsync(
                    new CreateChannelOptions(
                        publisherConfirmationsEnabled: false,
                        publisherConfirmationTrackingEnabled: false,
                        consumerDispatchConcurrency: _options.Concurrency),
                    cancellationToken);
                await _channel.BasicQosAsync(prefetchSize: 0, _options.Prefetch, global: false, cancellationToken);
                await StartConsumingAsync(cancellationToken);
                LogStarted(_queue, _options.Concurrency, _options.Prefetch);
                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                LogConnectFailed(_queue, ex);
                await Task.Delay(ReconnectDelay, clock, cancellationToken);
            }
        }
    }

    private async Task OnReceivedAsync(object sender, BasicDeliverEventArgs delivery)
    {
        Interlocked.Increment(ref _inFlight);
        var channel = ((AsyncEventingBasicConsumer)sender).Channel;
        try
        {
            bool settled;
            try
            {
                settled = await ProcessAsync(delivery, _processing.Token);
            }
            catch (Exception ex)
            {
                // The retry/DLQ copy could not be published (broker trouble): give the original back to the queue.
                LogSettlementFailed(_queue, delivery.BasicProperties.MessageId, ex);
                await channel.BasicNackAsync(delivery.DeliveryTag, multiple: false, requeue: true);
                return;
            }

            // Not settled = shutdown interrupted the work: leave it unacked, the broker re-delivers it.
            if (settled)
            {
                await channel.BasicAckAsync(delivery.DeliveryTag, multiple: false);
            }
        }
        catch (Exception ex)
        {
            // Ack/nack failed because the channel is gone: the broker re-delivers the message.
            LogAckFailed(_queue, ex);
        }
        finally
        {
            Interlocked.Decrement(ref _inFlight);
        }
    }

    /// <returns>True when the delivery is settled and can be acked.</returns>
    private async Task<bool> ProcessAsync(BasicDeliverEventArgs delivery, CancellationToken cancellationToken)
    {
        var properties = delivery.BasicProperties;
        var context = new MessageContext(
            properties.MessageId ?? string.Empty,
            _queue,
            MessageHeaders.GetInt(properties.Headers, MessageHeaders.RetryCount),
            delivery.Redelivered);

        TMessage message;
        try
        {
            if (properties.Type != registration.EventType)
            {
                throw new PermanentMessageException($"Unexpected message type '{properties.Type}', expected '{registration.EventType}'.");
            }

            message = JsonSerializer.Deserialize(delivery.Body.Span, registration.TypeInfo)
                      ?? throw new PermanentMessageException("Message body is null.");
        }
        catch (Exception ex) when (ex is JsonException or NotSupportedException or PermanentMessageException)
        {
            await DeadLetterAsync(delivery, context, "Permanent", ex, cancellationToken);
            return true;
        }

        try
        {
            await using var scope = scopes.CreateAsyncScope();
            await scope.ServiceProvider.GetRequiredService<THandler>().HandleAsync(message, context, cancellationToken);
            return true;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return false;
        }
        catch (DeferMessageException defer)
        {
            await RetryAsync(delivery, context, TierFor(defer.Delay), context.RetryCount, defer, cancellationToken);
            LogDeferred(_queue, context.MessageId, defer.Message);
            return true;
        }
        catch (Exception ex) when (FindOpenCircuit(ex) is { } openCircuit)
        {
            await RetryAsync(delivery, context, tier: 0, context.RetryCount, ex, cancellationToken);
            TriggerPause(openCircuit.RetryAfter);
            return true;
        }
        catch (PermanentMessageException ex)
        {
            await DeadLetterAsync(delivery, context, "Permanent", ex, cancellationToken);
            return true;
        }
        catch (Exception ex)
        {
            if (context.RetryCount < RabbitMqTopology.RetryDelays.Count)
            {
                await RetryAsync(delivery, context, tier: context.RetryCount, context.RetryCount + 1, ex, cancellationToken);
                return true;
            }

            await using (var scope = scopes.CreateAsyncScope())
            {
                await scope.ServiceProvider.GetRequiredService<THandler>()
                    .OnRetriesExhaustedAsync(message, context, ex, cancellationToken);
            }

            await DeadLetterAsync(delivery, context, "RetryExhausted", ex, cancellationToken);
            return true;
        }
    }

    private async Task RetryAsync(
        BasicDeliverEventArgs delivery, MessageContext context, int tier, int retryCount, Exception error,
        CancellationToken cancellationToken)
    {
        var headers = MessageHeaders.CopyForRepublish(delivery.BasicProperties.Headers);
        headers[MessageHeaders.RetryCount] = retryCount;
        headers[MessageHeaders.LastError] = MessageHeaders.Truncate(error.Message, 500);
        headers.TryAdd(MessageHeaders.FirstFailureAt, clock.GetUtcNow().ToString("O"));

        var delay = RabbitMqTopology.RetryDelays[tier];
        await failedMessages.PublishAsync(RabbitMqTopology.RetryQueueName(_queue, delay), delivery, headers, cancellationToken);
        LogRetryScheduled(_queue, context.MessageId, delay, retryCount, error.Message);
    }

    private async Task DeadLetterAsync(
        BasicDeliverEventArgs delivery, MessageContext context, string failureType, Exception error,
        CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow().ToString("O");
        var headers = MessageHeaders.CopyForRepublish(delivery.BasicProperties.Headers);
        headers.Remove(MessageHeaders.RetryCount);
        headers[MessageHeaders.FailureType] = failureType;
        headers[MessageHeaders.OriginalQueue] = _queue;
        headers[MessageHeaders.ExceptionType] = error.GetType().FullName;
        headers[MessageHeaders.ExceptionMessage] = MessageHeaders.Truncate(error.Message, 1000);
        headers[MessageHeaders.FinalRetryCount] = context.RetryCount;
        headers[MessageHeaders.DeadLetteredAt] = now;
        headers.TryAdd(MessageHeaders.FirstFailureAt, now);

        await failedMessages.PublishAsync(RabbitMqTopology.DeadLetterQueueName(_queue), delivery, headers, cancellationToken);
        LogDeadLettered(_queue, context.MessageId, failureType, error);
    }

    private static int TierFor(TimeSpan delay)
    {
        for (var i = 0; i < RabbitMqTopology.RetryDelays.Count; i++)
        {
            if (RabbitMqTopology.RetryDelays[i] >= delay)
            {
                return i;
            }
        }

        return RabbitMqTopology.RetryDelays.Count - 1;
    }

    private static BrokenCircuitException? FindOpenCircuit(Exception exception)
    {
        for (var e = exception; e is not null; e = e.InnerException)
        {
            if (e is BrokenCircuitException open)
            {
                return open;
            }
        }

        return null;
    }

    /// <summary>
    /// A dependency is known to be down: stop taking deliveries instead of cycling them through the retry tiers,
    /// which would burn their retry budget and CPU. Many handlers may hit the open circuit at once; one pause runs.
    /// </summary>
    private void TriggerPause(TimeSpan? retryAfter)
    {
        if (Interlocked.CompareExchange(ref _pausing, 1, 0) != 0)
        {
            return;
        }

        var duration = retryAfter is { } hint && hint > TimeSpan.Zero ? hint : _options.DefaultPause;
        _ = PauseAsync(duration);
    }

    private async Task PauseAsync(TimeSpan duration)
    {
        try
        {
            await StopConsumingAsync();
            LogPaused(_queue, duration);
            await Task.Delay(duration, clock, _stopping);

            // After the break the breaker lets a trial call through; resuming provides that call.
            while (true)
            {
                try
                {
                    await StartConsumingAsync(_stopping);
                    LogResumed(_queue);
                    break;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    LogConnectFailed(_queue, ex);
                    await Task.Delay(ReconnectDelay, clock, _stopping);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutting down.
        }
        catch (Exception ex)
        {
            LogPauseFailed(_queue, ex);
        }
        finally
        {
            Volatile.Write(ref _pausing, 0);
        }
    }

    private async Task StartConsumingAsync(CancellationToken cancellationToken)
    {
        await _consumeGate.WaitAsync(cancellationToken);
        try
        {
            if (_consumerTag is not null || _channel is null)
            {
                return;
            }

            var consumer = new AsyncEventingBasicConsumer(_channel);
            consumer.ReceivedAsync += OnReceivedAsync;
            _consumerTag = await _channel.BasicConsumeAsync(_queue, autoAck: false, consumer, cancellationToken);
        }
        finally
        {
            _consumeGate.Release();
        }
    }

    private async Task StopConsumingAsync()
    {
        await _consumeGate.WaitAsync(CancellationToken.None);
        try
        {
            if (_consumerTag is { } tag && _channel is { IsOpen: true })
            {
                _consumerTag = null;
                await _channel.BasicCancelAsync(tag);
            }
        }
        finally
        {
            _consumeGate.Release();
        }
    }

    /// <summary>Stop taking deliveries, let in-flight ones finish, then cancel stragglers and close the channel.</summary>
    private async Task DrainAsync()
    {
        try
        {
            await StopConsumingAsync();

            var deadline = clock.GetUtcNow() + _options.DrainTimeout;
            while (Volatile.Read(ref _inFlight) > 0 && clock.GetUtcNow() < deadline)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50), clock, CancellationToken.None);
            }

            // Unacked deliveries of cancelled handlers return to the queue when the channel closes.
            await _processing.CancelAsync();

            if (_channel is not null)
            {
                await _channel.CloseAsync();
                await _channel.DisposeAsync();
            }

            LogStopped(_queue);
        }
        catch (Exception ex)
        {
            LogPauseFailed(_queue, ex);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Consuming {Queue} (concurrency {Concurrency}, prefetch {Prefetch})")]
    private partial void LogStarted(string queue, int concurrency, int prefetch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Stopped consuming {Queue}")]
    private partial void LogStopped(string queue);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not start consuming {Queue}; retrying")]
    private partial void LogConnectFailed(string queue, Exception exception);

    [LoggerMessage(Level = LogLevel.Information, Message = "{Queue}: message {MessageId} deferred: {Reason}")]
    private partial void LogDeferred(string queue, string messageId, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Queue}: message {MessageId} scheduled for retry in {Delay} (retry #{RetryCount}): {Error}")]
    private partial void LogRetryScheduled(string queue, string messageId, TimeSpan delay, int retryCount, string error);

    [LoggerMessage(Level = LogLevel.Error, Message = "{Queue}: message {MessageId} dead-lettered ({FailureType})")]
    private partial void LogDeadLettered(string queue, string messageId, string failureType, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Queue}: dependency circuit is open; consumption paused for {Duration}")]
    private partial void LogPaused(string queue, TimeSpan duration);

    [LoggerMessage(Level = LogLevel.Information, Message = "{Queue}: consumption resumed")]
    private partial void LogResumed(string queue);

    [LoggerMessage(Level = LogLevel.Error, Message = "{Queue}: pausing/resuming consumption failed")]
    private partial void LogPauseFailed(string queue, Exception exception);

    [LoggerMessage(Level = LogLevel.Error, Message = "{Queue}: could not publish retry/DLQ copy of {MessageId}; message requeued")]
    private partial void LogSettlementFailed(string queue, string? messageId, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Queue}: ack/nack failed; the broker will re-deliver")]
    private partial void LogAckFailed(string queue, Exception exception);
}
