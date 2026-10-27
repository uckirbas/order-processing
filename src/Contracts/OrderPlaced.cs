namespace Contracts;

/// <summary>
/// Published by Ordering once an order is durably stored. A fat event: it carries everything
/// Invoicing and Notification need, so neither ever reads the Ordering database.
/// </summary>
public sealed record OrderPlaced(
    Guid OrderId,
    string OrderReference,
    Guid CustomerId,
    string CustomerEmail,
    string Currency,
    decimal TotalAmount,
    IReadOnlyList<OrderPlacedItem> Items,
    DateTimeOffset CreatedAt) : IIntegrationEvent
{
    public static string EventType => "OrderPlaced.v1";

    public static string RoutingKey => "order.placed";
}

public sealed record OrderPlacedItem(string ProductId, string ProductName, int Quantity, decimal UnitPrice);
