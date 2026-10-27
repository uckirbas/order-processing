using System.Text;

namespace BuildingBlocks.Messaging.Consuming;

/// <summary>Headers this pipeline reads and writes when a delivery is retried or dead-lettered.</summary>
public static class MessageHeaders
{
    public const string RetryCount = "x-retry-count";
    public const string FirstFailureAt = "x-first-failure-at";
    public const string LastError = "x-last-error";

    // DLQ diagnostics. x-retry-count is removed on the way to the DLQ (and kept as x-final-retry-count),
    // so a message moved back to its queue for replay starts with a fresh retry budget.
    public const string FailureType = "x-failure-type";
    public const string OriginalQueue = "x-original-queue";
    public const string ExceptionType = "x-exception-type";
    public const string ExceptionMessage = "x-exception-message";
    public const string FinalRetryCount = "x-final-retry-count";
    public const string DeadLetteredAt = "x-dead-lettered-at";

    /// <summary>Set by the broker itself; a re-published copy must not carry them over.</summary>
    private static readonly HashSet<string> BrokerManaged =
    [
        "x-death", "x-delivery-count",
        "x-first-death-exchange", "x-first-death-queue", "x-first-death-reason",
        "x-last-death-exchange", "x-last-death-queue", "x-last-death-reason",
    ];

    internal static int GetInt(IDictionary<string, object?>? headers, string key) =>
        headers is not null && headers.TryGetValue(key, out var value)
            ? value switch
            {
                int i => i,
                long l => (int)l,
                byte[] bytes when int.TryParse(Encoding.UTF8.GetString(bytes), out var parsed) => parsed,
                _ => 0,
            }
            : 0;

    internal static Dictionary<string, object?> CopyForRepublish(IDictionary<string, object?>? headers) =>
        headers?.Where(h => !BrokerManaged.Contains(h.Key)).ToDictionary(h => h.Key, h => h.Value) ?? [];

    internal static string Truncate(string value, int maxLength) =>
        value.Length <= maxLength ? value : value[..maxLength];
}
