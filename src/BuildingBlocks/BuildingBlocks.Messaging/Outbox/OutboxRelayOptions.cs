namespace BuildingBlocks.Messaging.Outbox;

public sealed class OutboxRelayOptions
{
    public const string SectionName = "Outbox";

    /// <summary>The publishing service's own exchange; set in code from <c>MessagingTopology</c>, not from configuration.</summary>
    public string Exchange { get; set; } = string.Empty;

    public int BatchSize { get; set; } = 500;

    /// <summary>How long a claimed row stays invisible to other relays. Must exceed <see cref="PublishTimeout"/>.</summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Upper bound for publishing one batch and receiving its confirms.</summary>
    public TimeSpan PublishTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>Delay after a partially filled batch; a full batch is followed immediately by the next one.</summary>
    public TimeSpan MinPollInterval { get; set; } = TimeSpan.FromMilliseconds(50);

    /// <summary>Ceiling for the exponential back-off while the outbox stays empty.</summary>
    public TimeSpan MaxPollInterval { get; set; } = TimeSpan.FromSeconds(1);
}
