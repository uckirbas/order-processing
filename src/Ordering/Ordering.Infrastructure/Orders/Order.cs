using BuildingBlocks.Identifiers;

namespace Ordering.Infrastructure.Orders;

/// <summary>
/// An accepted order. Immutable once written (insert-only): its progress lives in <see cref="OrderStatusRecord"/>,
/// so frequent status updates never rewrite this wide row.
/// </summary>
public sealed class Order
{
    private readonly List<OrderItem> _items = [];

    private Order()
    {
    }

    public Guid Id { get; private set; }

    /// <summary>Customer-facing reference: <c>ORD-</c> + Crockford Base32 of the UUIDv7 id. Unique by construction.</summary>
    public string Reference { get; private set; } = null!;

    public Guid CustomerId { get; private set; }

    public string CustomerEmail { get; private set; } = null!;

    public string Currency { get; private set; } = null!;

    public decimal TotalAmount { get; private set; }

    public string IdempotencyKey { get; private set; } = null!;

    /// <summary>SHA-256 of the request body, to tell a genuine retry from a different request reusing the key.</summary>
    public byte[] RequestHash { get; private set; } = null!;

    public DateTimeOffset CreatedAt { get; private set; }

    public IReadOnlyList<OrderItem> Items => _items;

    public static string ReferenceFor(Guid orderId) => "ORD-" + CrockfordBase32.Encode(orderId);

    public static Order Place(
        Guid id,
        Guid customerId,
        string customerEmail,
        string currency,
        IEnumerable<NewOrderLine> lines,
        string idempotencyKey,
        byte[] requestHash,
        DateTimeOffset createdAt)
    {
        var order = new Order
        {
            Id = id,
            Reference = ReferenceFor(id),
            CustomerId = customerId,
            CustomerEmail = customerEmail,
            Currency = currency,
            IdempotencyKey = idempotencyKey,
            RequestHash = requestHash,
            CreatedAt = createdAt,
        };

        var lineNo = 0;
        foreach (var line in lines)
        {
            order._items.Add(new OrderItem(id, ++lineNo, line.ProductId, line.ProductName, line.Quantity, line.UnitPrice));
            order.TotalAmount += line.Quantity * line.UnitPrice;
        }

        return order;
    }
}

public readonly record struct NewOrderLine(string ProductId, string ProductName, int Quantity, decimal UnitPrice);
