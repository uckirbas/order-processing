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
/// generate the document (no transaction, no connection held: it may be slow or fail) → one short transaction:
/// conditional upsert + InvoiceGenerated into the outbox. The invoice and its event therefore commit together;
/// a duplicate delivery finds the invoice Generated and publishes nothing.
/// </summary>
internal sealed partial class OrderPlacedInvoiceHandler(
    IDbContextFactory<InvoicingDbContext> dbContexts,
    IInvoiceGenerator generator,
    ResiliencePipelineProvider<string> pipelines,
    TimeProvider clock,
    ILogger<OrderPlacedInvoiceHandler> logger) : IMessageHandler<OrderPlaced>
{
    private readonly ResiliencePipeline _generation = pipelines.GetPipeline(InvoiceGeneration.PipelineName);

    public async Task HandleAsync(OrderPlaced order, MessageContext context, CancellationToken cancellationToken)
    {
        await using var db = await dbContexts.CreateDbContextAsync(cancellationToken);
        var invoices = new InvoiceRepository(db);

        if (await invoices.HasGeneratedAsync(order.OrderId, cancellationToken))
        {
            LogDuplicate(order.OrderId, context.MessageId);
            return;
        }

        InvoiceDocument document;
        try
        {
            document = await _generation.ExecuteAsync(
                static async (state, token) => await state.generator.GenerateAsync(state.order, token),
                (generator, order),
                cancellationToken);
        }
        catch (InvoiceRejectedException ex)
        {
            await RecordFailureAsync(db, invoices, order, ex.Message, cancellationToken);
            throw new PermanentMessageException(ex.Message, ex);
        }

        var linesJson = JsonSerializer.Serialize(document.Lines, InvoicingJsonContext.Default.InvoiceLineArray);

        await using var transaction = await db.Database.BeginTransactionAsync(cancellationToken);
        var issued = await invoices.UpsertGeneratedAsync(order, document.Total, linesJson, cancellationToken);
        if (issued is null)
        {
            // A concurrent delivery of the same order committed first.
            await transaction.RollbackAsync(cancellationToken);
            LogDuplicate(order.OrderId, context.MessageId);
            return;
        }

        var invoiceGenerated = new InvoiceGenerated(
            issued.Id, issued.InvoiceNumber, order.OrderId, order.OrderReference, order.CustomerEmail,
            order.Currency, document.Total, issued.IssuedAt);
        db.OutboxMessages.Add(OutboxMessage.Create(
            invoiceGenerated, InvoiceGenerated.EventType, InvoiceGenerated.RoutingKey,
            ContractsJsonContext.Default.InvoiceGenerated, clock.GetUtcNow()));
        await db.SaveChangesAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);

        LogIssued(issued.InvoiceNumber, order.OrderId);
    }

    /// <summary>The provider stayed unavailable through every retry tier: record the outcome before the DLQ.</summary>
    public async Task OnRetriesExhaustedAsync(
        OrderPlaced order, MessageContext context, Exception lastError, CancellationToken cancellationToken)
    {
        await using var db = await dbContexts.CreateDbContextAsync(cancellationToken);
        await RecordFailureAsync(db, new InvoiceRepository(db), order, $"Gave up after retries: {lastError.Message}", cancellationToken);
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

    [LoggerMessage(Level = LogLevel.Debug, Message = "Invoice {InvoiceNumber} issued for order {OrderId}")]
    private partial void LogIssued(string invoiceNumber, Guid orderId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Order {OrderId} is already invoiced; duplicate message {MessageId} ignored")]
    private partial void LogDuplicate(Guid orderId, string messageId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Invoice for order {OrderId} failed permanently: {Reason}")]
    private partial void LogFailed(Guid orderId, string reason);
}
