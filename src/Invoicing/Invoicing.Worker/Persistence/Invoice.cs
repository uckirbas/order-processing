using System.Text.Json;
using System.Text.Json.Serialization;

namespace Invoicing.Worker.Persistence;

/// <summary>Generated is final; Failed can still become Generated when the order is replayed from the DLQ.</summary>
public enum InvoiceStatus
{
    Generated,
    Failed,
}

public sealed record InvoiceLine(string ProductId, string ProductName, int Quantity, decimal UnitPrice, decimal LineTotal);

/// <summary>
/// At most one invoice per order (unique <c>order_id</c>): that key, not a message id, is what makes invoicing idempotent,
/// so it also catches a replayed or re-published event. Rows are written by <see cref="InvoiceRepository"/>.
/// </summary>
public sealed class Invoice
{
    private Invoice()
    {
    }

    public Guid Id { get; private set; }

    public Guid OrderId { get; private set; }

    public string OrderReference { get; private set; } = null!;

    /// <summary>Taken from a sequence only when the invoice is actually generated; null for a Failed invoice.</summary>
    public string? InvoiceNumber { get; private set; }

    public InvoiceStatus Status { get; private set; }

    public string CustomerEmail { get; private set; } = null!;

    public string Currency { get; private set; } = null!;

    public decimal TotalAmount { get; private set; }

    /// <summary>Snapshot of the invoiced lines (jsonb).</summary>
    public string Lines { get; private set; } = null!;

    public string? FailureReason { get; private set; }

    public DateTimeOffset? IssuedAt { get; private set; }

    public DateTimeOffset CreatedAt { get; private set; }

    public DateTimeOffset UpdatedAt { get; private set; }
}

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(InvoiceLine[]))]
internal sealed partial class InvoicingJsonContext : JsonSerializerContext;
