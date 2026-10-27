using Contracts;
using Microsoft.EntityFrameworkCore;

namespace Invoicing.Worker.Persistence;

internal sealed record IssuedInvoice(Guid Id, string InvoiceNumber, DateTimeOffset IssuedAt);

/// <summary>
/// The two writes EF cannot express (conditional upserts). Both rely on the unique <c>order_id</c>: concurrent
/// deliveries for the same order are serialised by the index, and the WHERE clause is re-evaluated against the row
/// as committed by the winner, so no application-level lock is needed. Every value is a DbParameter.
/// </summary>
internal sealed class InvoiceRepository(InvoicingDbContext db)
{
    /// <summary>Cheap pre-check so a duplicate delivery skips the generation work; the guarantee is the unique key.</summary>
    public Task<bool> HasGeneratedAsync(Guid orderId, CancellationToken cancellationToken) =>
        db.Invoices.AnyAsync(i => i.OrderId == orderId && i.Status == InvoiceStatus.Generated, cancellationToken);

    /// <summary>
    /// Inserts a Generated invoice, or repairs a Failed one (DLQ replay). A Generated invoice is never touched again.
    /// </summary>
    /// <returns>The issued invoice, or null when it already existed (a duplicate delivery): then nothing is to be published.</returns>
    public async Task<IssuedInvoice?> UpsertGeneratedAsync(
        OrderPlaced order, decimal total, string linesJson, CancellationToken cancellationToken)
    {
        // Must not be composed (no FirstOrDefault): EF would wrap the INSERT … RETURNING in a sub-select.
        var issued = await db.Database.SqlQuery<IssuedInvoice>(
            $"""
            INSERT INTO invoices (id, order_id, order_reference, invoice_number, status, customer_email, currency,
                                  total_amount, lines, failure_reason, issued_at, created_at, updated_at)
            VALUES ({Guid.CreateVersion7()}, {order.OrderId}, {order.OrderReference},
                    'INV-' || lpad(nextval('invoice_number_seq')::text, 10, '0'), 'Generated', {order.CustomerEmail},
                    {order.Currency}, {total}, {linesJson}::jsonb, NULL, now(), now(), now())
            ON CONFLICT (order_id) DO UPDATE
            SET    status = 'Generated', invoice_number = EXCLUDED.invoice_number, total_amount = EXCLUDED.total_amount,
                   lines = EXCLUDED.lines, failure_reason = NULL, issued_at = EXCLUDED.issued_at, updated_at = now()
            WHERE  invoices.status = 'Failed'
            RETURNING id, invoice_number, issued_at
            """).ToListAsync(cancellationToken);

        return issued.Count == 0 ? null : issued[0];
    }

    /// <summary>Records a permanent failure once. Never overwrites an existing invoice, Generated or Failed.</summary>
    /// <returns>True when this call recorded it (and InvoiceFailed must be published).</returns>
    public async Task<bool> TryRecordFailedAsync(
        OrderPlaced order, string linesJson, string reason, CancellationToken cancellationToken)
    {
        var failureReason = reason.Length <= 1000 ? reason : reason[..1000];
        var inserted = await db.Database.ExecuteSqlAsync(
            $"""
            INSERT INTO invoices (id, order_id, order_reference, status, customer_email, currency,
                                  total_amount, lines, failure_reason, created_at, updated_at)
            VALUES ({Guid.CreateVersion7()}, {order.OrderId}, {order.OrderReference}, 'Failed', {order.CustomerEmail},
                    {order.Currency}, {order.TotalAmount}, {linesJson}::jsonb, {failureReason}, now(), now())
            ON CONFLICT (order_id) DO NOTHING
            """,
            cancellationToken);

        return inserted == 1;
    }
}
