using Microsoft.EntityFrameworkCore;

namespace Notification.Worker.Persistence;

internal sealed record ClaimRequest(Guid OrderId, string Recipient);

internal sealed record ClaimedNotification(Guid OrderId);

/// <summary>
/// The claim/lease state machine of <see cref="Notification"/>. Every transition is one short, auto-committed
/// conditional statement (compare-and-set) covering a whole batch; nothing holds a row lock while e-mails are being
/// sent. All timestamps that leases are compared against come from the database clock, so pod clock skew does not
/// matter.
/// </summary>
internal sealed class NotificationStore(NotificationDbContext db, TimeProvider clock)
{
    /// <summary>
    /// Takes ownership of (order, <paramref name="type"/>) for each request: inserts the row, or takes over a Failed
    /// row or a Sending row whose lease expired. Concurrent claims for the same key are serialised by the unique
    /// index; exactly one wins. The requests must not repeat an order: one statement cannot upsert a row twice.
    /// </summary>
    /// <returns>The orders this caller now owns and must send.</returns>
    public async Task<HashSet<Guid>> TryClaimAsync(
        NotificationType type, IReadOnlyList<ClaimRequest> requests, Guid leaseId, TimeSpan lease,
        CancellationToken cancellationToken)
    {
        var typeName = type.ToString();
        var ids = requests.Select(_ => Guid.CreateVersion7()).ToArray();
        var orderIds = requests.Select(r => r.OrderId).ToArray();
        var recipients = requests.Select(r => r.Recipient).ToArray();

        // Parameterised: every interpolation becomes a DbParameter (EF1002 would reject the *Raw* variant).
        // Rows are written in order_id order, so concurrent batches take the unique-index locks in the same order.
        var claimed = await db.Database.SqlQuery<ClaimedNotification>(
            $"""
            INSERT INTO notifications (id, order_id, type, recipient, status, lease_id, locked_until, attempts, created_at, updated_at)
            SELECT r.id, r.order_id, {typeName}, r.recipient, 'Sending', {leaseId}, now() + {lease}, 1, now(), now()
            FROM   unnest({ids}, {orderIds}, {recipients}) AS r(id, order_id, recipient)
            ORDER  BY r.order_id
            ON CONFLICT (order_id, type) DO UPDATE
            SET    status = 'Sending', lease_id = EXCLUDED.lease_id, locked_until = EXCLUDED.locked_until,
                   recipient = EXCLUDED.recipient, attempts = notifications.attempts + 1, updated_at = now()
            WHERE  notifications.status = 'Failed'
               OR (notifications.status = 'Sending' AND notifications.locked_until < now())
            RETURNING order_id
            """).ToListAsync(cancellationToken);

        return [.. claimed.Select(c => c.OrderId)];
    }

    public Task<Dictionary<Guid, NotificationStatus>> GetStatusesAsync(
        NotificationType type, Guid[] orderIds, CancellationToken cancellationToken) =>
        db.Notifications.AsNoTracking()
            .Where(n => n.Type == type && orderIds.Contains(n.OrderId))
            .ToDictionaryAsync(n => n.OrderId, n => n.Status, cancellationToken);

    /// <returns>
    /// How many were recorded. Fewer than <paramref name="orderIds"/> means some leases were lost meanwhile (another
    /// worker took over): those outcomes are not ours to record.
    /// </returns>
    public Task<int> MarkSentAsync(NotificationType type, Guid[] orderIds, Guid leaseId, CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow();
        return Owned(type, orderIds, leaseId).ExecuteUpdateAsync(
            set => set
                .SetProperty(n => n.Status, NotificationStatus.Sent)
                .SetProperty(n => n.SentAt, now)
                .SetProperty(n => n.LockedUntil, (DateTimeOffset?)null)
                .SetProperty(n => n.LastError, (string?)null)
                .SetProperty(n => n.UpdatedAt, now),
            cancellationToken);
    }

    /// <summary>Transient failure: releases the claim so the retry (or another worker) can take it.</summary>
    public Task<int> MarkFailedAsync(NotificationType type, Guid orderId, Guid leaseId, string error, CancellationToken cancellationToken) =>
        Finish(type, orderId, leaseId, NotificationStatus.Failed, error, cancellationToken);

    /// <summary>Permanent failure (e.g. SMTP 550): never retried.</summary>
    public Task<int> MarkDeadAsync(NotificationType type, Guid orderId, Guid leaseId, string error, CancellationToken cancellationToken) =>
        Finish(type, orderId, leaseId, NotificationStatus.Dead, error, cancellationToken);

    private Task<int> Finish(
        NotificationType type, Guid orderId, Guid leaseId, NotificationStatus status, string error, CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow();
        var lastError = error.Length <= 1000 ? error : error[..1000];
        return Owned(type, [orderId], leaseId).ExecuteUpdateAsync(
            set => set
                .SetProperty(n => n.Status, status)
                .SetProperty(n => n.LastError, lastError)
                .SetProperty(n => n.LockedUntil, (DateTimeOffset?)null)
                .SetProperty(n => n.UpdatedAt, now),
            cancellationToken);
    }

    /// <summary>The fencing condition: only the current lease holder, while the row is still Sending.</summary>
    private IQueryable<Notification> Owned(NotificationType type, Guid[] orderIds, Guid leaseId) =>
        db.Notifications.Where(n =>
            n.Type == type && orderIds.Contains(n.OrderId) && n.LeaseId == leaseId && n.Status == NotificationStatus.Sending);
}
