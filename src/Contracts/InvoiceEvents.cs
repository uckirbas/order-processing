namespace Contracts;

/// <summary>Published by Invoicing after the invoice is stored. Carries what the invoice e-mail needs.</summary>
public sealed record InvoiceGenerated(
    Guid InvoiceId,
    string InvoiceNumber,
    Guid OrderId,
    string OrderReference,
    string CustomerEmail,
    string Currency,
    decimal TotalAmount,
    DateTimeOffset IssuedAt) : IIntegrationEvent
{
    public static string EventType => "InvoiceGenerated.v1";

    public static string RoutingKey => "invoice.generated";
}

/// <summary>Published by Invoicing when an invoice cannot be produced even after all retries.</summary>
public sealed record InvoiceFailed(
    Guid OrderId,
    string OrderReference,
    string Reason,
    DateTimeOffset FailedAt) : IIntegrationEvent
{
    public static string EventType => "InvoiceFailed.v1";

    public static string RoutingKey => "invoice.failed";
}
