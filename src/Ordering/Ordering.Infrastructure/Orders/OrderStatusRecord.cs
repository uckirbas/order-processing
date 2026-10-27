namespace Ordering.Infrastructure.Orders;

public enum OrderStatus
{
    Received,
    Invoiced,
    InvoiceFailed,
}

/// <summary>
/// Current lifecycle state of an order, kept in its own narrow table (<c>order_status</c>).
/// Inserted together with the order; later transitions are conditional updates made by ordering-worker.
/// </summary>
public sealed class OrderStatusRecord
{
    private OrderStatusRecord()
    {
    }

    public Guid OrderId { get; private set; }

    public OrderStatus Status { get; private set; }

    public DateTimeOffset UpdatedAt { get; private set; }

    public static OrderStatusRecord Received(Guid orderId, DateTimeOffset at) =>
        new() { OrderId = orderId, Status = OrderStatus.Received, UpdatedAt = at };
}
