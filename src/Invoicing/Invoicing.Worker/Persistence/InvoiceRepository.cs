using Contracts;
using Microsoft.EntityFrameworkCore;

namespace Invoicing.Worker.Persistence;

internal sealed record InvoiceDraft(OrderPlaced Order, decimal Total, string LinesJson);

internal sealed record IssuedInvoice(Guid Id, Guid OrderId, string InvoiceNumber, DateTimeOffset IssuedAt);

/// <summary>
/// The two writes EF cannot express (conditional upserts). Both rely on the unique <c>order_id</c>: concurrent
/// deliveries for the same order are serialised by the index, and the WHERE clause is re-evaluated against the row
/// as committed by the winner, so no application-level lock is needed. Every value is a DbParameter.
/// </summary>
internal sealed class InvoiceRepository(InvoicingDbContext db)
{
    /// <summary>Cheap pre-check so duplicate deliveries skip the generation work; the guarantee is the unique key.</summary>
    public async Task<HashSet<Guid>> GetGeneratedAsync(Guid[] orderIds, CancellationToken cancellationToken)
    {
        var generated = await db.Invoices
            .Where(i => orderIds.Contains(i.OrderId) && i.Status == InvoiceStatus.Generated)
            .Select(i => i.OrderId)
            .ToListAsync(cancellationToken);
        return [.. generated];
    }

    /// <summary>
    /// Inserts Generated invoices, or repairs Failed ones (DLQ replay), in one statement. A Generated invoice is never
    /// touched again. The drafts must not repeat an order: one statement cannot upsert the same row twice.
    /// </summary>
    /// <returns>
    /// The invoices issued, by order id. An order missing from it already had one (a duplicate delivery): nothing is
    /// to be published for it.
    /// </returns>
    public async Task<Dictionary<Guid, IssuedInvoice>> UpsertGeneratedAsync(
        IReadOnlyList<InvoiceDraft> drafts, CancellationToken cancellationToken)
    {
        var ids = drafts.Select(_ => Guid.CreateVersion7()).ToArray();
        var orderIds = drafts.Select(d => d.Order.OrderId).ToArray();
        var references = drafts.Select(d => d.Order.OrderReference).ToArray();
        var emails = drafts.Select(d => d.Order.CustomerEmail).ToArray();
        var currencies = drafts.Select(d => d.Order.Currency).ToArray();
        var totals = drafts.Select(d => d.Total).ToArray();
        var lines = drafts.Select(d => d.LinesJson).ToArray();

        // Rows are written in order_id order, so concurrent batches take the unique-index locks in the same order.
        // Must not be composed (no FirstOrDefault): EF would wrap the INSERT … RETURNING in a sub-select.
        var issued = await db.Database.SqlQuery<IssuedInvoice>(
            $"""
            INSERT INTO invoices (id, order_id, order_reference, invoice_number, status, customer_email, currency,
                                  total_amount, lines, failure_reason, issued_at, created_at, updated_at)
            SELECT d.id, d.order_id, d.order_reference, 'INV-' || lpad(nextval('invoice_number_seq')::text, 10, '0'),
                   'Generated', d.customer_email, d.currency, d.total_amount, d.lines, NULL, now(), now(), now()
            FROM   unnest({ids}, {orderIds}, {references}, {emails}, {currencies}, {totals}, {lines}::jsonb[])
                   AS d(id, order_id, order_reference, customer_email, currency, total_amount, lines)
            ORDER  BY d.order_id
            ON CONFLICT (order_id) DO UPDATE
            SET    status = 'Generated', invoice_number = EXCLUDED.invoice_number, total_amount = EXCLUDED.total_amount,
                   lines = EXCLUDED.lines, failure_reason = NULL, issued_at = EXCLUDED.issued_at, updated_at = now()
            WHERE  invoices.status = 'Failed'
            RETURNING id, order_id, invoice_number, issued_at
            """).ToListAsync(cancellationToken);

        return issued.ToDictionary(i => i.OrderId);
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
