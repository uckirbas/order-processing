namespace Notification.Worker.Persistence;

public enum NotificationType
{
    OrderReceived,
    InvoiceReady,
}

/// <summary>
/// Sending → Sent | Failed | Dead; Failed → Sending (claim); Sending with an expired lease → Sending (take-over).
/// Dead is for permanent errors only: a message whose retries ran out stays Failed so a DLQ replay can claim it again.
/// </summary>
public enum NotificationStatus
{
    Sending,
    Sent,
    Failed,
    Dead,
}

/// <summary>
/// One row per (order, e-mail type): the unique key is what makes sending idempotent across duplicate deliveries.
/// Rows are created by the claim statement (<see cref="NotificationStore.TryClaimAsync"/>); EF maps them for
/// the typed updates and for migrations.
/// </summary>
public sealed class Notification
{
    private Notification()
    {
    }

    public Guid Id { get; private set; }

    public Guid OrderId { get; private set; }

    public NotificationType Type { get; private set; }

    public string Recipient { get; private set; } = null!;

    public NotificationStatus Status { get; private set; }

    /// <summary>Fencing token of the current claim: only its holder may record the outcome.</summary>
    public Guid? LeaseId { get; private set; }

    public DateTimeOffset? LockedUntil { get; private set; }

    public int Attempts { get; private set; }

    public string? LastError { get; private set; }

    public DateTimeOffset? SentAt { get; private set; }

    public DateTimeOffset CreatedAt { get; private set; }

    public DateTimeOffset UpdatedAt { get; private set; }
}
