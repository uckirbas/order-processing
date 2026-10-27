using BuildingBlocks.Messaging.Consuming;
using Contracts;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using Notification.Worker.Email;
using Notification.Worker.Persistence;
using Polly;
using Polly.Retry;

namespace Notification.Worker.Notifications;

public sealed class NotificationOptions
{
    public const string SectionName = "Notification";

    /// <summary>Claim lease; must exceed the SMTP pipeline's total timeout.</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>How long to wait before looking again when another worker holds the claim.</summary>
    public TimeSpan DeferDelay { get; set; } = TimeSpan.FromSeconds(30);
}

/// <summary>
/// For a whole batch: claim (one short auto-commit) → send over SMTP concurrently (no transaction, no connection
/// held; the SMTP pool bounds the concurrency) → record the sent ones (one short auto-commit, fenced by the lease id).
/// Failures are recorded and settled per e-mail. E-mail is at-least-once: a crash between an SMTP "250 OK" and the
/// Sent update re-sends that batch's sent e-mails once; the deterministic Message-ID lets mail clients collapse the
/// duplicates.
/// </summary>
internal abstract class NotificationHandler<TMessage>(
    IDbContextFactory<NotificationDbContext> dbContexts,
    IEmailSender emailSender,
    IOptions<NotificationOptions> options,
    TimeProvider clock,
    ILogger logger) : IBatchMessageHandler<TMessage>
{
    // Shared and thread-safe. Retries only the database write; the SMTP call is never repeated because of it.
    private static readonly ResiliencePipeline RecordSentRetry = new ResiliencePipelineBuilder()
        .AddRetry(new RetryStrategyOptions
        {
            MaxRetryAttempts = 3,
            Delay = TimeSpan.FromMilliseconds(100),
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true,
        })
        .Build();

    /// <summary>The single e-mail type this handler sends; a batch therefore never mixes types.</summary>
    protected abstract NotificationType Type { get; }

    protected abstract (Guid OrderId, EmailMessage Email) Describe(TMessage message);

    public async Task HandleAsync(IReadOnlyList<BatchItem<TMessage>> batch, CancellationToken cancellationToken)
    {
        var settings = options.Value;
        var leaseId = Guid.NewGuid();

        // A second copy of an order in the same batch is a duplicate delivery: acked, the first copy carries the work
        // (and is retried or dead-lettered on its own if it fails). Deferring the copies instead would bring them
        // back together, into one batch again, one round per copy.
        var seen = new HashSet<Guid>();
        var outgoing = new List<Outgoing>(batch.Count);
        foreach (var item in batch)
        {
            var (orderId, email) = Describe(item.Message);
            if (seen.Add(orderId))
            {
                outgoing.Add(new Outgoing(item, orderId, email));
            }
            else
            {
                NotificationLog.DuplicateInBatch(logger, Type, orderId, item.Context.MessageId);
            }
        }

        // One context per batch, used sequentially. EF takes a pooled connection per statement and returns it right
        // after, so no connection (and no transaction) is held while the SMTP calls below are in flight.
        await using var db = await dbContexts.CreateDbContextAsync(cancellationToken);
        var store = new NotificationStore(db, clock);

        var claimed = await store.TryClaimAsync(
            Type, outgoing.Select(o => new ClaimRequest(o.OrderId, o.Email.To)).ToList(), leaseId, settings.LeaseDuration,
            cancellationToken);
        var notClaimed = outgoing.Where(o => !claimed.Contains(o.OrderId)).ToList();
        if (notClaimed.Count > 0)
        {
            await SettleNotClaimedAsync(store, notClaimed, settings.DeferDelay, cancellationToken);
        }

        var toSend = outgoing.Where(o => claimed.Contains(o.OrderId)).ToList();
        var errors = await Task.WhenAll(toSend.Select(o => TrySendAsync(o.Email, cancellationToken)));

        // Accepted by the SMTP server. Recorded first, before any failure: the duplicate window ends with this statement.
        var sent = toSend.Where((_, i) => errors[i] is null).Select(o => o.OrderId).ToArray();
        if (sent.Length > 0)
        {
            await RecordSentAsync(store, sent, leaseId);
        }

        for (var i = 0; i < toSend.Count; i++)
        {
            if (errors[i] is { } error)
            {
                await SettleSendFailureAsync(store, toSend[i], leaseId, error);
            }
        }
    }

    /// <summary>Someone else claimed these, or they are already finished.</summary>
    private async Task SettleNotClaimedAsync(
        NotificationStore store, List<Outgoing> notClaimed, TimeSpan deferDelay, CancellationToken cancellationToken)
    {
        var statuses = await store.GetStatusesAsync(Type, notClaimed.Select(o => o.OrderId).ToArray(), cancellationToken);
        foreach (var o in notClaimed)
        {
            var status = statuses.TryGetValue(o.OrderId, out var s) ? s : (NotificationStatus?)null;
            if (status is NotificationStatus.Sent or NotificationStatus.Dead)
            {
                NotificationLog.Duplicate(logger, Type, o.OrderId, status.Value, o.Item.Context.MessageId);
                continue;
            }

            // Another worker holds a live lease and may still be sending. Acking now could lose the e-mail
            // if that worker dies before sending; look again later instead.
            o.Item.Fail(new DeferMessageException(
                deferDelay, $"{Type} for order {o.OrderId} is claimed by another worker ({status})."));
        }
    }

    private async Task<Exception?> TrySendAsync(EmailMessage email, CancellationToken cancellationToken)
    {
        try
        {
            await emailSender.SendAsync(email, cancellationToken);
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private async Task SettleSendFailureAsync(NotificationStore store, Outgoing o, Guid leaseId, Exception error)
    {
        switch (error)
        {
            case PermanentEmailException:
                await TryRecordAsync(store.MarkDeadAsync(Type, o.OrderId, leaseId, error.Message, CancellationToken.None), o.OrderId);
                o.Item.Fail(new PermanentMessageException(error.Message, error));
                break;
            case OperationCanceledException:
                // Shutdown: the claim is left to expire, the unacked delivery comes back.
                o.Item.Fail(error);
                break;
            default:
                // Includes an open circuit: release the claim so the re-delivery, or another worker, can take it over.
                await TryRecordAsync(store.MarkFailedAsync(Type, o.OrderId, leaseId, error.Message, CancellationToken.None), o.OrderId);
                o.Item.Fail(error);
                break;
        }
    }

    private async Task RecordSentAsync(NotificationStore store, Guid[] sent, Guid leaseId)
    {
        try
        {
            var updated = await RecordSentRetry.ExecuteAsync(
                static async (state, token) => await state.store.MarkSentAsync(state.Type, state.sent, state.leaseId, token),
                (store, Type, sent, leaseId),
                CancellationToken.None);

            if (updated < sent.Length)
            {
                NotificationLog.LeasesLost(logger, Type, sent.Length - updated);
            }

            NotificationLog.Sent(logger, Type, sent.Length);
        }
        catch (Exception ex)
        {
            // The e-mails are out. Acking without the Sent mark is better than not acking:
            // a re-delivery would certainly send them a second time.
            NotificationLog.SentNotRecorded(logger, Type, sent.Length, ex);
        }
    }

    /// <summary>Best effort: if the database is down too, the lease simply expires and the row becomes claimable again.</summary>
    private async Task TryRecordAsync(Task<int> update, Guid orderId)
    {
        try
        {
            await update;
        }
        catch (Exception ex)
        {
            NotificationLog.OutcomeNotRecorded(logger, Type, orderId, ex);
        }
    }

    private sealed record Outgoing(BatchItem<TMessage> Item, Guid OrderId, EmailMessage Email);
}

internal sealed class OrderPlacedNotificationHandler(
    IDbContextFactory<NotificationDbContext> dbContexts,
    IEmailSender emailSender,
    IOptions<NotificationOptions> options,
    TimeProvider clock,
    ILogger<OrderPlacedNotificationHandler> logger)
    : NotificationHandler<OrderPlaced>(dbContexts, emailSender, options, clock, logger)
{
    protected override NotificationType Type => NotificationType.OrderReceived;

    protected override (Guid OrderId, EmailMessage Email) Describe(OrderPlaced message) =>
        (message.OrderId, EmailTemplates.OrderReceived(message));
}

internal sealed class InvoiceGeneratedNotificationHandler(
    IDbContextFactory<NotificationDbContext> dbContexts,
    IEmailSender emailSender,
    IOptions<NotificationOptions> options,
    TimeProvider clock,
    ILogger<InvoiceGeneratedNotificationHandler> logger)
    : NotificationHandler<InvoiceGenerated>(dbContexts, emailSender, options, clock, logger)
{
    protected override NotificationType Type => NotificationType.InvoiceReady;

    protected override (Guid OrderId, EmailMessage Email) Describe(InvoiceGenerated message) =>
        (message.OrderId, EmailTemplates.InvoiceReady(message));
}

internal static partial class NotificationLog
{
    [LoggerMessage(Level = LogLevel.Debug, Message = "{Count} {Type} e-mails sent")]
    public static partial void Sent(ILogger logger, NotificationType type, int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "{Type} e-mail for order {OrderId} already {Status}; duplicate message {MessageId} ignored")]
    public static partial void Duplicate(ILogger logger, NotificationType type, Guid orderId, NotificationStatus status, string messageId);

    [LoggerMessage(Level = LogLevel.Information, Message = "{Type} e-mail for order {OrderId} is already in this batch; duplicate message {MessageId} ignored")]
    public static partial void DuplicateInBatch(ILogger logger, NotificationType type, Guid orderId, string messageId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Count} {Type} e-mails sent, but their leases had been taken over; outcome left to the new owner")]
    public static partial void LeasesLost(ILogger logger, NotificationType type, int count);

    [LoggerMessage(Level = LogLevel.Error, Message = "{Count} {Type} e-mails sent, but recording them failed; acking anyway to avoid a second send")]
    public static partial void SentNotRecorded(ILogger logger, NotificationType type, int count, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not record the outcome of {Type} e-mail for order {OrderId}; its lease will expire")]
    public static partial void OutcomeNotRecorded(ILogger logger, NotificationType type, Guid orderId, Exception exception);
}
