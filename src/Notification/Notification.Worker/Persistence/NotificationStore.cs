using Microsoft.EntityFrameworkCore;

namespace Notification.Worker.Persistence;

/// <summary>
/// The claim/lease state machine of <see cref="Notification"/>. Every transition is one short, auto-committed
/// conditional statement (compare-and-set); nothing holds a row lock while an e-mail is being sent.
/// All timestamps that leases are compared against come from the database clock, so pod clock skew does not matter.
/// </summary>
internal sealed class NotificationStore(NotificationDbContext db, TimeProvider clock)
{
    /// <summary>
    /// Takes ownership of (order, type): inserts the row, or takes over a Failed row or a Sending row whose lease
    /// expired. Concurrent claims for the same key are serialised by the unique index; exactly one wins.
    /// </summary>
    /// <returns>True when this caller now owns the notification and must send it.</returns>
    public async Task<bool> TryClaimAsync(
        Guid orderId, NotificationType type, string recipient, Guid leaseId, TimeSpan lease, CancellationToken cancellationToken)
    {
        var typeName = type.ToString();

        // Parameterised: every interpolation becomes a DbParameter (EF1002 would reject the *Raw* variant).
        var affected = await db.Database.ExecuteSqlAsync(
            $"""
            INSERT INTO notifications (id, order_id, type, recipient, status, lease_id, locked_until, attempts, created_at, updated_at)
            VALUES ({Guid.CreateVersion7()}, {orderId}, {typeName}, {recipient}, 'Sending', {leaseId}, now() + {lease}, 1, now(), now())
            ON CONFLICT (order_id, type) DO UPDATE
            SET    status = 'Sending', lease_id = EXCLUDED.lease_id, locked_until = EXCLUDED.locked_until,
                   recipient = EXCLUDED.recipient, attempts = notifications.attempts + 1, updated_at = now()
            WHERE  notifications.status = 'Failed'
               OR (notifications.status = 'Sending' AND notifications.locked_until < now())
            """,
            cancellationToken);

        return affected == 1;
    }

    public Task<NotificationStatus?> GetStatusAsync(Guid orderId, NotificationType type, CancellationToken cancellationToken) =>
        db.Notifications.AsNoTracking()
            .Where(n => n.OrderId == orderId && n.Type == type)
            .Select(n => (NotificationStatus?)n.Status)
            .SingleOrDefaultAsync(cancellationToken);

    /// <returns>0 when the lease was lost meanwhile (another worker took over): the outcome is not ours to record.</returns>
    public Task<int> MarkSentAsync(Guid orderId, NotificationType type, Guid leaseId, CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow();
        return Owned(orderId, type, leaseId).ExecuteUpdateAsync(
            set => set
                .SetProperty(n => n.Status, NotificationStatus.Sent)
                .SetProperty(n => n.SentAt, now)
                .SetProperty(n => n.LockedUntil, (DateTimeOffset?)null)
                .SetProperty(n => n.LastError, (string?)null)
                .SetProperty(n => n.UpdatedAt, now),
            cancellationToken);
    }

    /// <summary>Transient failure: releases the claim so the retry (or another worker) can take it.</summary>
    public Task<int> MarkFailedAsync(Guid orderId, NotificationType type, Guid leaseId, string error, CancellationToken cancellationToken) =>
        Finish(orderId, type, leaseId, NotificationStatus.Failed, error, cancellationToken);

    /// <summary>Permanent failure (e.g. SMTP 550): never retried.</summary>
    public Task<int> MarkDeadAsync(Guid orderId, NotificationType type, Guid leaseId, string error, CancellationToken cancellationToken) =>
        Finish(orderId, type, leaseId, NotificationStatus.Dead, error, cancellationToken);

    private Task<int> Finish(
        Guid orderId, NotificationType type, Guid leaseId, NotificationStatus status, string error, CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow();
        var lastError = error.Length <= 1000 ? error : error[..1000];
        return Owned(orderId, type, leaseId).ExecuteUpdateAsync(
            set => set
                .SetProperty(n => n.Status, status)
                .SetProperty(n => n.LastError, lastError)
                .SetProperty(n => n.LockedUntil, (DateTimeOffset?)null)
                .SetProperty(n => n.UpdatedAt, now),
            cancellationToken);
    }

    /// <summary>The fencing condition: only the current lease holder, while the row is still Sending.</summary>
    private IQueryable<Notification> Owned(Guid orderId, NotificationType type, Guid leaseId) =>
        db.Notifications.Where(n =>
            n.OrderId == orderId && n.Type == type && n.LeaseId == leaseId && n.Status == NotificationStatus.Sending);
}
