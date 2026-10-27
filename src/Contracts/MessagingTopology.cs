namespace Contracts;

/// <summary>
/// The complete broker topology, declared (idempotently) by every service at startup.
/// Declaring all of it everywhere, rather than each consumer declaring only its own queue, closes a startup race:
/// a mandatory publish only has to reach one queue, so a consumer queue created later would silently miss
/// every message published before it existed.
/// </summary>
public static class MessagingTopology
{
    public const string OrderingExchange = "ordering.events";
    public const string InvoicingExchange = "invoicing.events";

    public static readonly ConsumerQueue InvoicingOrderPlaced =
        new("invoicing.order-placed", OrderingExchange, OrderPlaced.RoutingKey);

    public static readonly ConsumerQueue NotificationOrderPlaced =
        new("notification.order-placed", OrderingExchange, OrderPlaced.RoutingKey);

    public static readonly ConsumerQueue NotificationInvoiceGenerated =
        new("notification.invoice-generated", InvoicingExchange, InvoiceGenerated.RoutingKey);

    // One queue per message type (not a single "invoice.*" queue): a consumer accepts exactly one type, and a DLQ
    // then only ever holds one kind of message, which keeps replay simple.
    public static readonly ConsumerQueue OrderingInvoiceGenerated =
        new("ordering.invoice-generated", InvoicingExchange, InvoiceGenerated.RoutingKey);

    public static readonly ConsumerQueue OrderingInvoiceFailed =
        new("ordering.invoice-failed", InvoicingExchange, InvoiceFailed.RoutingKey);

    public static IReadOnlyList<string> Exchanges { get; } = [OrderingExchange, InvoicingExchange];

    public static IReadOnlyList<ConsumerQueue> ConsumerQueues { get; } =
    [
        InvoicingOrderPlaced, NotificationOrderPlaced, NotificationInvoiceGenerated, OrderingInvoiceGenerated, OrderingInvoiceFailed,
    ];
}

/// <summary>A queue owned by one consumer, bound to a topic exchange.</summary>
public sealed record ConsumerQueue(string Name, string Exchange, string BindingKey);
