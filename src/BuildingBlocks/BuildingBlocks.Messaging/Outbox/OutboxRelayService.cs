using BuildingBlocks.Outbox;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.CircuitBreaker;
using Polly.Registry;

namespace BuildingBlocks.Messaging.Outbox;

/// <summary>
/// Moves outbox rows to the broker: claim (short auto-commit) → publish and await confirms (no transaction open) →
/// delete what was confirmed. Several replicas run side by side; SKIP LOCKED gives each a disjoint batch.
/// Delivery is at-least-once: a crash between confirm and delete republishes those rows after their lease expires,
/// which consumers absorb through idempotency.
/// </summary>
internal sealed partial class OutboxRelayService(
    OutboxStore store,
    OutboxPublisher publisher,
    RabbitMqTopology topology,
    ResiliencePipelineProvider<string> pipelines,
    IOptions<OutboxRelayOptions> options,
    TimeProvider clock,
    ILogger<OutboxRelayService> logger) : BackgroundService
{
    public const string PipelineName = "outbox-relay";

    private static readonly TimeSpan FailureBackoff = TimeSpan.FromSeconds(1);

    private readonly OutboxRelayOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var pipeline = pipelines.GetPipeline(PipelineName);
        var idleDelay = _options.MinPollInterval;
        LogStarted(_options.Exchange, _options.BatchSize);

        while (!stoppingToken.IsCancellationRequested)
        {
            TimeSpan delay;
            try
            {
                var claimed = await pipeline.ExecuteAsync(RelayBatchAsync, stoppingToken);
                if (claimed == _options.BatchSize)
                {
                    // Backlog: keep draining without pausing.
                    idleDelay = _options.MinPollInterval;
                    continue;
                }

                delay = claimed > 0 ? _options.MinPollInterval : idleDelay;
                idleDelay = claimed > 0
                    ? _options.MinPollInterval
                    : TimeSpan.FromTicks(Math.Min(idleDelay.Ticks * 2, _options.MaxPollInterval.Ticks));
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (BrokenCircuitException ex)
            {
                // Broker or database is failing: stop hitting it. The API keeps accepting orders meanwhile.
                delay = ex.RetryAfter ?? FailureBackoff;
                LogCircuitOpen(delay);
            }
            catch (Exception ex)
            {
                delay = FailureBackoff;
                LogBatchFailed(ex);
            }

            try
            {
                await Task.Delay(delay, clock, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        LogStopped();
    }

    private async ValueTask<int> RelayBatchAsync(CancellationToken cancellationToken)
    {
        await topology.EnsureDeclaredAsync(cancellationToken);

        var batch = await store.ClaimAsync(_options.BatchSize, _options.LeaseDuration, cancellationToken);
        if (batch.Count == 0)
        {
            return 0;
        }

        PublishOutcome outcome;
        try
        {
            outcome = await publisher.PublishAsync(_options.Exchange, batch, cancellationToken);
        }
        catch
        {
            // Failed before anything could be confirmed (e.g. no channel while the broker is down): hand the batch back.
            await TryReleaseAsync(batch.Select(m => m.Id).ToArray());
            throw;
        }

        if (outcome.Confirmed.Count > 0)
        {
            // Not cancellable: these messages are delivered; skipping the delete on shutdown would only produce duplicates.
            await store.DeleteAsync(outcome.Confirmed, CancellationToken.None);
        }

        foreach (var (id, reason) in outcome.Rejected)
        {
            LogRejected(id, reason);
        }

        LogBatch(batch.Count, outcome.Confirmed.Count, outcome.Rejected.Count);

        if (outcome.Failure is not null)
        {
            // Channel or connection failure (broker down, timeout): release the unconfirmed rows right away. Otherwise
            // the next polls would find nothing claimable, report "success", and the circuit breaker would never see
            // the outage. Rejected rows keep their lease: an unroutable message would just fail again immediately.
            var settled = outcome.Confirmed.Concat(outcome.Rejected.Select(r => r.Id)).ToHashSet();
            var unconfirmed = batch.Select(m => m.Id).Where(id => !settled.Contains(id)).ToArray();
            await TryReleaseAsync(unconfirmed);
            throw new OutboxPublishException(unconfirmed.Length, outcome.Failure);
        }

        return batch.Count;
    }

    private async Task TryReleaseAsync(Guid[] ids)
    {
        if (ids.Length == 0)
        {
            return;
        }

        try
        {
            await store.ReleaseAsync(ids, CancellationToken.None);
        }
        catch (Exception ex)
        {
            LogReleaseFailed(ids.Length, ex);
        }
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not release {Count} outbox leases early; they expire on their own")]
    private partial void LogReleaseFailed(int count, Exception exception);

    [LoggerMessage(Level = LogLevel.Information, Message = "Outbox relay started: exchange {Exchange}, batch size {BatchSize}")]
    private partial void LogStarted(string exchange, int batchSize);

    [LoggerMessage(Level = LogLevel.Information, Message = "Outbox relay stopped")]
    private partial void LogStopped();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Outbox batch: {Claimed} claimed, {Confirmed} confirmed, {Rejected} rejected")]
    private partial void LogBatch(int claimed, int confirmed, int rejected);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Outbox message {MessageId} not accepted by the broker: {Reason}; it stays in the outbox")]
    private partial void LogRejected(Guid messageId, string reason);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Outbox relay circuit is open; pausing {Delay}")]
    private partial void LogCircuitOpen(TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Error, Message = "Outbox batch failed; unconfirmed messages will be retried")]
    private partial void LogBatchFailed(Exception exception);
}

internal sealed class OutboxPublishException(int unconfirmed, Exception inner)
    : Exception($"{unconfirmed} outbox message(s) were not confirmed by the broker.", inner);
