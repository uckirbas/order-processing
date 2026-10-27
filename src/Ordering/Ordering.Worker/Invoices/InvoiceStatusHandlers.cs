using BuildingBlocks.Messaging.Consuming;
using Contracts;
using Microsoft.EntityFrameworkCore;
using Ordering.Infrastructure.Orders;
using Ordering.Infrastructure.Persistence;

namespace Ordering.Worker.Invoices;

/// <summary>
/// Keeps the order's status in step with Invoicing, which owns invoices and only reports outcomes through events.
/// Every transition is a single conditional UPDATE, so duplicates and out-of-order events are harmless:
/// <list type="bullet">
/// <item>InvoiceGenerated: Received or InvoiceFailed → Invoiced (InvoiceFailed covers a successful DLQ replay)</item>
/// <item>InvoiceFailed: Received → InvoiceFailed (never downgrades Invoiced)</item>
/// </list>
/// Whatever order the two events arrive in, the end state is the same.
/// </summary>
internal sealed partial class InvoiceGeneratedStatusHandler(
    IDbContextFactory<OrderingDbContext> dbContexts,
    TimeProvider clock,
    ILogger<InvoiceGeneratedStatusHandler> logger) : IMessageHandler<InvoiceGenerated>
{
    public async Task HandleAsync(InvoiceGenerated message, MessageContext context, CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow();
        await using var db = await dbContexts.CreateDbContextAsync(cancellationToken);

        var updated = await db.OrderStatuses
            .Where(s => s.OrderId == message.OrderId
                        && (s.Status == OrderStatus.Received || s.Status == OrderStatus.InvoiceFailed))
            .ExecuteUpdateAsync(
                set => set.SetProperty(s => s.Status, OrderStatus.Invoiced).SetProperty(s => s.UpdatedAt, now),
                cancellationToken);

        if (updated == 0)
        {
            LogNoTransition(message.OrderId, context.MessageId);
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Order {OrderId} already Invoiced; InvoiceGenerated {MessageId} changed nothing")]
    private partial void LogNoTransition(Guid orderId, string messageId);
}

internal sealed partial class InvoiceFailedStatusHandler(
    IDbContextFactory<OrderingDbContext> dbContexts,
    TimeProvider clock,
    ILogger<InvoiceFailedStatusHandler> logger) : IMessageHandler<InvoiceFailed>
{
    public async Task HandleAsync(InvoiceFailed message, MessageContext context, CancellationToken cancellationToken)
    {
        var now = clock.GetUtcNow();
        await using var db = await dbContexts.CreateDbContextAsync(cancellationToken);

        var updated = await db.OrderStatuses
            .Where(s => s.OrderId == message.OrderId && s.Status == OrderStatus.Received)
            .ExecuteUpdateAsync(
                set => set.SetProperty(s => s.Status, OrderStatus.InvoiceFailed).SetProperty(s => s.UpdatedAt, now),
                cancellationToken);

        if (updated == 0)
        {
            LogNoTransition(message.OrderId, context.MessageId);
        }
        else
        {
            LogFailed(message.OrderId, message.Reason);
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Order {OrderId} is past Received; InvoiceFailed {MessageId} changed nothing")]
    private partial void LogNoTransition(Guid orderId, string messageId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Order {OrderId} could not be invoiced: {Reason}")]
    private partial void LogFailed(Guid orderId, string reason);
}
