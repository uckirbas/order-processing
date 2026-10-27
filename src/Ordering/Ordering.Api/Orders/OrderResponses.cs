using Ordering.Infrastructure.Orders;

namespace Ordering.Api.Orders;

public sealed record OrderAccepted(Guid OrderId, string OrderReference, OrderStatus Status);

public sealed record OrderDetails(
    Guid OrderId,
    string OrderReference,
    OrderStatus Status,
    DateTimeOffset StatusUpdatedAt,
    Guid CustomerId,
    string CustomerEmail,
    string Currency,
    decimal TotalAmount,
    DateTimeOffset CreatedAt,
    IReadOnlyList<OrderLineDetails> Items);

public sealed record OrderLineDetails(string ProductId, string ProductName, int Quantity, decimal UnitPrice);
