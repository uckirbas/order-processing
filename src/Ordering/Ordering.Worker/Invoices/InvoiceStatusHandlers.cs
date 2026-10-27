using BuildingBlocks.Messaging.Consuming;
using Contracts;
using Microsoft.EntityFrameworkCore;
using Ordering.Infrastructure.Orders;
using Ordering.Infrastructure.Persistence;

namespace Ordering.Worker.Invoices;

/// <summary>
/// Keeps the order's status in step with Invoicing, which owns invoices and only reports outcomes through events.
/// A whole batch is one conditional UPDATE, so duplicates and out-of-order events are harmless:
/// <list type="bullet">
/// <item>InvoiceGenerated: Received or InvoiceFailed → Invoiced (InvoiceFailed covers a successful DLQ replay)</item>
/// <item>InvoiceFailed: Received → InvoiceFailed (never downgrades Invoiced)</item>
/// </list>
/// Whatever order the two events arrive in, the end state is the same.
/// </summary>
internal sealed partial class InvoiceGeneratedStatusHandler(
    IDbContextFactory<OrderingDbContext> dbContexts,
    TimeProvider clock,
    ILogger<InvoiceGeneratedStatusHandler> logger) : IBatchMessageHandler<InvoiceGenerated>
{
    public async Task HandleAsync(IReadOnlyList<BatchItem<InvoiceGenerated>> batch, CancellationToken cancellationToken)
    {
        var orderIds = batch.Select(i => i.Message.OrderId).Distinct().ToArray();
        var now = clock.GetUtcNow();
        await using var db = await dbContexts.CreateDbContextAsync(cancellationToken);

        var updated = await db.OrderStatuses
            .Where(s => orderIds.Contains(s.OrderId)
                        && (s.Status == OrderStatus.Received || s.Status == OrderStatus.InvoiceFailed))
            .ExecuteUpdateAsync(
                set => set.SetProperty(s => s.Status, OrderStatus.Invoiced).SetProperty(s => s.UpdatedAt, now),
                cancellationToken);

        if (updated < orderIds.Length)
        {
            LogNoTransition(orderIds.Length - updated, orderIds.Length);
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "{Unchanged} of {Total} orders were already Invoiced; their InvoiceGenerated changed nothing")]
    private partial void LogNoTransition(int unchanged, int total);
}

internal sealed partial class InvoiceFailedStatusHandler(
    IDbContextFactory<OrderingDbContext> dbContexts,
    TimeProvider clock,
    ILogger<InvoiceFailedStatusHandler> logger) : IBatchMessageHandler<InvoiceFailed>
{
    public async Task HandleAsync(IReadOnlyList<BatchItem<InvoiceFailed>> batch, CancellationToken cancellationToken)
    {
        var orderIds = batch.Select(i => i.Message.OrderId).Distinct().ToArray();
        var now = clock.GetUtcNow();
        await using var db = await dbContexts.CreateDbContextAsync(cancellationToken);

        var updated = await db.OrderStatuses
            .Where(s => orderIds.Contains(s.OrderId) && s.Status == OrderStatus.Received)
            .ExecuteUpdateAsync(
                set => set.SetProperty(s => s.Status, OrderStatus.InvoiceFailed).SetProperty(s => s.UpdatedAt, now),
                cancellationToken);

        foreach (var item in batch)
        {
            LogFailed(item.Message.OrderId, item.Message.Reason);
        }

        if (updated < orderIds.Length)
        {
            LogNoTransition(orderIds.Length - updated, orderIds.Length);
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "{Unchanged} of {Total} orders were past Received; their InvoiceFailed changed nothing")]
    private partial void LogNoTransition(int unchanged, int total);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Invoicing reported that order {OrderId} could not be invoiced: {Reason}")]
    private partial void LogFailed(Guid orderId, string reason);
}
