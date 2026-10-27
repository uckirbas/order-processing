namespace Ordering.Infrastructure.Orders;

/// <summary>A snapshot of the product as ordered (name and price at order time). Immutable.</summary>
public sealed class OrderItem
{
    private OrderItem()
    {
    }

    internal OrderItem(Guid orderId, int lineNo, string productId, string productName, int quantity, decimal unitPrice)
    {
        OrderId = orderId;
        LineNo = lineNo;
        ProductId = productId;
        ProductName = productName;
        Quantity = quantity;
        UnitPrice = unitPrice;
    }

    public Guid OrderId { get; private set; }

    public int LineNo { get; private set; }

    public string ProductId { get; private set; } = null!;

    public string ProductName { get; private set; } = null!;

    public int Quantity { get; private set; }

    public decimal UnitPrice { get; private set; }
}
