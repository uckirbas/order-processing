using System.Text.Json;
using BuildingBlocks.Messaging.Consuming;
using BuildingBlocks.Outbox;
using Contracts;
using Invoicing.Worker.Persistence;
using Microsoft.EntityFrameworkCore;
using Polly;
using Polly.Registry;

namespace Invoicing.Worker.Invoices;

/// <summary>
/// generate the documents (concurrently; no transaction, no connection held: the provider may be slow or fail) →
/// one short transaction for the whole batch: a single conditional upsert + InvoiceGenerated rows in the outbox.
/// Invoices and their events therefore commit together; a duplicate delivery finds the invoice Generated and
/// publishes nothing. An order the provider rejects or keeps failing is settled on its own, not with the batch.
/// </summary>
internal sealed partial class OrderPlacedInvoiceHandler(
    IDbContextFactory<InvoicingDbContext> dbContexts,
    IInvoiceGenerator generator,
    ResiliencePipelineProvider<string> pipelines,
    TimeProvider clock,
    ILogger<OrderPlacedInvoiceHandler> logger) : IBatchMessageHandler<OrderPlaced>
{
    private readonly ResiliencePipeline _generation = pipelines.GetPipeline(InvoiceGeneration.PipelineName);

    public async Task HandleAsync(IReadOnlyList<BatchItem<OrderPlaced>> batch, CancellationToken cancellationToken)
    {
        await using var db = await dbContexts.CreateDbContextAsync(cancellationToken);
        var invoices = new InvoiceRepository(db);

        var orderIds = batch.Select(i => i.Message.OrderId).Distinct().ToArray();
        var alreadyGenerated = await invoices.GetGeneratedAsync(orderIds, cancellationToken);

        // A second copy of an order in the same batch is a duplicate delivery: the first copy does the work.
        var seen = new HashSet<Guid>();
        var toGenerate = new List<BatchItem<OrderPlaced>>(batch.Count);
        foreach (var item in batch)
        {
            var orderId = item.Message.OrderId;
            if (alreadyGenerated.Contains(orderId) || !seen.Add(orderId))
            {
                LogDuplicate(orderId, item.Context.MessageId);
                continue;
            }

            toGenerate.Add(item);
        }

        // The pipeline's concurrency limiter caps how many of these reach the provider at once.
        var results = await Task.WhenAll(toGenerate.Select(item => GenerateAsync(item, cancellationToken)));

        var drafts = results
            .Where(r => r.Document is not null)
            .Select(r => new InvoiceDraft(
                r.Item.Message, r.Document!.Total,
                JsonSerializer.Serialize(r.Document.Lines, InvoicingJsonContext.Default.InvoiceLineArray)))
            .ToList();
        if (drafts.Count > 0)
        {
            await IssueAsync(db, invoices, drafts, cancellationToken);
        }

        // Rare: one short transaction each, after the batch's invoices are safely committed.
        foreach (var (item, _, rejection) in results)
        {
            if (rejection is not null)
            {
                await RecordFailureAsync(db, invoices, item.Message, rejection.Message, cancellationToken);
                item.Fail(new PermanentMessageException(rejection.Message, rejection));
            }
        }
    }

    /// <summary>The provider stayed unavailable through every retry tier: record the outcome before the DLQ.</summary>
    public async Task OnRetriesExhaustedAsync(
        OrderPlaced order, MessageContext context, Exception lastError, CancellationToken cancellationToken)
    {
        await using var db = await dbContexts.CreateDbContextAsync(cancellationToken);
        await RecordFailureAsync(db, new InvoiceRepository(db), order, $"Gave up after retries: {lastError.Message}", cancellationToken);
    }

    private async Task<Generation> GenerateAsync(BatchItem<OrderPlaced> item, CancellationToken cancellationToken)
    {
        try
        {
            var document = await _generation.ExecuteAsync(
                static async (state, token) => await state.generator.GenerateAsync(state.Message, token),
                (generator, item.Message),
                cancellationToken);
            return new Generation(item, document, null);
        }
        catch (InvoiceRejectedException ex)
        {
            return new Generation(item, null, ex);
        }
        catch (Exception ex)
        {
            // Transient, an open circuit, or shutdown: the consumer settles this item accordingly.
            item.Fail(ex);
            return new Generation(item, null, null);
        }
    }

    private async Task IssueAsync(
        InvoicingDbContext db, InvoiceRepository invoices, List<InvoiceDraft> drafts, CancellationToken cancellationToken)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        var issued = await invoices.UpsertGeneratedAsync(drafts, cancellationToken);

        var now = clock.GetUtcNow();
        foreach (var draft in drafts)
        {
            var order = draft.Order;
            if (!issued.TryGetValue(order.OrderId, out var invoice))
            {
                // A concurrent delivery of the same order committed first.
                LogDuplicate(order.OrderId, messageId: null);
                continue;
            }

            var invoiceGenerated = new InvoiceGenerated(
                invoice.Id, invoice.InvoiceNumber, order.OrderId, order.OrderReference, order.CustomerEmail,
                order.Currency, draft.Total, invoice.IssuedAt);
            db.OutboxMessages.Add(OutboxMessage.Create(
                invoiceGenerated, InvoiceGenerated.EventType, InvoiceGenerated.RoutingKey,
                ContractsJsonContext.Default.InvoiceGenerated, now));
        }

        await db.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        LogIssued(issued.Count, drafts.Count);
    }

    /// <summary>Failed invoice + InvoiceFailed event in one transaction, recorded once per order.</summary>
    private async Task RecordFailureAsync(
        InvoicingDbContext db, InvoiceRepository invoices, OrderPlaced order, string reason, CancellationToken cancellationToken)
    {
        var lines = order.Items
            .Select(i => new InvoiceLine(i.ProductId, i.ProductName, i.Quantity, i.UnitPrice, i.Quantity * i.UnitPrice))
            .ToArray();
        var linesJson = JsonSerializer.Serialize(lines, InvoicingJsonContext.Default.InvoiceLineArray);

        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        if (await invoices.TryRecordFailedAsync(order, linesJson, reason, cancellationToken))
        {
            db.OutboxMessages.Add(OutboxMessage.Create(
                new InvoiceFailed(order.OrderId, order.OrderReference, reason, clock.GetUtcNow()),
                InvoiceFailed.EventType, InvoiceFailed.RoutingKey, ContractsJsonContext.Default.InvoiceFailed, clock.GetUtcNow()));
            await db.SaveChangesAsync(cancellationToken);
            LogFailed(order.OrderId, reason);
        }

        await transaction.CommitAsync(cancellationToken);
    }

    /// <summary>Exactly one of the outcomes: a document, a business rejection, or neither (the item was failed).</summary>
    private readonly record struct Generation(BatchItem<OrderPlaced> Item, InvoiceDocument? Document, InvoiceRejectedException? Rejection);

    [LoggerMessage(Level = LogLevel.Debug, Message = "{Issued} invoices issued for a batch of {Count} orders")]
    private partial void LogIssued(int issued, int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Order {OrderId} is already invoiced; duplicate message {MessageId} ignored")]
    private partial void LogDuplicate(Guid orderId, string? messageId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Invoice for order {OrderId} failed permanently: {Reason}")]
    private partial void LogFailed(Guid orderId, string reason);
}
