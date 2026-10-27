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

internal sealed record OutgoingNotification(Guid OrderId, NotificationType Type, EmailMessage Email);

/// <summary>
/// claim (short auto-commit) → send over SMTP (no transaction, no connection held) → record the outcome
/// (short auto-commit, fenced by the lease id). E-mail is at-least-once: a crash between the SMTP "250 OK" and the
/// Sent update re-sends once; the deterministic Message-ID lets mail clients collapse that duplicate.
/// </summary>
internal abstract class NotificationHandler<TMessage>(
    IDbContextFactory<NotificationDbContext> dbContexts,
    IEmailSender emailSender,
    IOptions<NotificationOptions> options,
    TimeProvider clock,
    ILogger logger) : IMessageHandler<TMessage>
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

    protected abstract OutgoingNotification Describe(TMessage message);

    public async Task HandleAsync(TMessage message, MessageContext context, CancellationToken cancellationToken)
    {
        var outgoing = Describe(message);
        var settings = options.Value;
        var leaseId = Guid.NewGuid();

        // One context per message. EF takes a pooled connection per statement and returns it right after,
        // so no connection (and no transaction) is held while the SMTP call below is in flight.
        await using var db = await dbContexts.CreateDbContextAsync(cancellationToken);
        var store = new NotificationStore(db, clock);

        var claimed = await store.TryClaimAsync(
            outgoing.OrderId, outgoing.Type, outgoing.Email.To, leaseId, settings.LeaseDuration, cancellationToken);
        if (!claimed)
        {
            var status = await store.GetStatusAsync(outgoing.OrderId, outgoing.Type, cancellationToken);
            if (status is NotificationStatus.Sent or NotificationStatus.Dead)
            {
                NotificationLog.Duplicate(logger, outgoing.Type, outgoing.OrderId, status.Value, context.MessageId);
                return;
            }

            // Another worker holds a live lease and may still be sending. Acking now could lose the e-mail
            // if that worker dies before sending; look again later instead.
            throw new DeferMessageException(
                settings.DeferDelay, $"{outgoing.Type} for order {outgoing.OrderId} is claimed by another worker ({status}).");
        }

        try
        {
            await emailSender.SendAsync(outgoing.Email, cancellationToken);
        }
        catch (PermanentEmailException ex)
        {
            await TryRecordAsync(store.MarkDeadAsync(outgoing.OrderId, outgoing.Type, leaseId, ex.Message, CancellationToken.None), outgoing);
            throw new PermanentMessageException(ex.Message, ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Includes an open circuit: release the claim so the re-delivery, or another worker, can take it over.
            await TryRecordAsync(store.MarkFailedAsync(outgoing.OrderId, outgoing.Type, leaseId, ex.Message, CancellationToken.None), outgoing);
            throw;
        }

        // Accepted by the SMTP server. Record it immediately: the duplicate window is exactly this statement.
        await RecordSentAsync(store, outgoing, leaseId);
        NotificationLog.Sent(logger, outgoing.Type, outgoing.OrderId, context.RetryCount);
    }

    private async Task RecordSentAsync(NotificationStore store, OutgoingNotification outgoing, Guid leaseId)
    {
        try
        {
            var updated = await RecordSentRetry.ExecuteAsync(
                static async (state, token) => await state.store.MarkSentAsync(state.OrderId, state.Type, state.leaseId, token),
                (store, outgoing.OrderId, outgoing.Type, leaseId),
                CancellationToken.None);

            if (updated == 0)
            {
                NotificationLog.LeaseLost(logger, outgoing.Type, outgoing.OrderId);
            }
        }
        catch (Exception ex)
        {
            // The e-mail is out. Acking without the Sent mark is better than not acking:
            // a re-delivery would certainly send it a second time.
            NotificationLog.SentNotRecorded(logger, outgoing.Type, outgoing.OrderId, ex);
        }
    }

    /// <summary>Best effort: if the database is down too, the lease simply expires and the row becomes claimable again.</summary>
    private async Task TryRecordAsync(Task<int> update, OutgoingNotification outgoing)
    {
        try
        {
            await update;
        }
        catch (Exception ex)
        {
            NotificationLog.OutcomeNotRecorded(logger, outgoing.Type, outgoing.OrderId, ex);
        }
    }
}

internal sealed class OrderPlacedNotificationHandler(
    IDbContextFactory<NotificationDbContext> dbContexts,
    IEmailSender emailSender,
    IOptions<NotificationOptions> options,
    TimeProvider clock,
    ILogger<OrderPlacedNotificationHandler> logger)
    : NotificationHandler<OrderPlaced>(dbContexts, emailSender, options, clock, logger)
{
    protected override OutgoingNotification Describe(OrderPlaced message) =>
        new(message.OrderId, NotificationType.OrderReceived, EmailTemplates.OrderReceived(message));
}

internal sealed class InvoiceGeneratedNotificationHandler(
    IDbContextFactory<NotificationDbContext> dbContexts,
    IEmailSender emailSender,
    IOptions<NotificationOptions> options,
    TimeProvider clock,
    ILogger<InvoiceGeneratedNotificationHandler> logger)
    : NotificationHandler<InvoiceGenerated>(dbContexts, emailSender, options, clock, logger)
{
    protected override OutgoingNotification Describe(InvoiceGenerated message) =>
        new(message.OrderId, NotificationType.InvoiceReady, EmailTemplates.InvoiceReady(message));
}

internal static partial class NotificationLog
{
    [LoggerMessage(Level = LogLevel.Debug, Message = "{Type} e-mail for order {OrderId} sent (retry #{RetryCount})")]
    public static partial void Sent(ILogger logger, NotificationType type, Guid orderId, int retryCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "{Type} e-mail for order {OrderId} already {Status}; duplicate message {MessageId} ignored")]
    public static partial void Duplicate(ILogger logger, NotificationType type, Guid orderId, NotificationStatus status, string messageId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "{Type} e-mail for order {OrderId} sent, but the lease had been taken over; outcome left to the new owner")]
    public static partial void LeaseLost(ILogger logger, NotificationType type, Guid orderId);

    [LoggerMessage(Level = LogLevel.Error, Message = "{Type} e-mail for order {OrderId} sent, but recording it failed; acking anyway to avoid a second send")]
    public static partial void SentNotRecorded(ILogger logger, NotificationType type, Guid orderId, Exception exception);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Could not record the outcome of {Type} e-mail for order {OrderId}; its lease will expire")]
    public static partial void OutcomeNotRecorded(ILogger logger, NotificationType type, Guid orderId, Exception exception);
}
