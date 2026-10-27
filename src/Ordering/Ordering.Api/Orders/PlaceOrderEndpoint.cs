using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text.Json;
using BuildingBlocks.Outbox;
using Contracts;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Ordering.Api.Infrastructure;
using Ordering.Infrastructure.Orders;
using Ordering.Infrastructure.Persistence;

namespace Ordering.Api.Orders;

internal static class PlaceOrderEndpoint
{
    public const string IdempotencyKeyHeader = "Idempotency-Key";
    public const string IdempotentReplayedHeader = "Idempotent-Replayed";
    public const int IdempotencyKeyMaxLength = 64;

    /// <summary>
    /// The whole critical path is one short transaction: order + items + status + outbox in a single
    /// <c>SaveChangesAsync</c> (one batched round-trip). No broker, SMTP or other remote call happens here,
    /// so a pooled connection is held only for the duration of that commit.
    /// </summary>
    public static async Task<Results<Created<OrderAccepted>, Ok<OrderAccepted>, ProblemHttpResult>> HandleAsync(
        [FromHeader(Name = IdempotencyKeyHeader)] string? idempotencyKey,
        PlaceOrderRequest request,
        OrderingDbContext db,
        TimeProvider clock,
        HttpContext httpContext,
        CancellationToken cancellationToken)
    {
        if (!IsValidIdempotencyKey(idempotencyKey))
        {
            return TypedResults.Problem(
                statusCode: StatusCodes.Status400BadRequest,
                title: "Missing or invalid Idempotency-Key header",
                detail: $"Send a client-generated key of 1-{IdempotencyKeyMaxLength} printable ASCII characters " +
                        "and reuse it only when retrying the same request.");
        }

        // Request validation has already run (endpoint filter), so required members are non-null here.
        var customerId = request.CustomerId!.Value;
        var requestHash = ComputeFingerprint(request);
        var now = clock.GetUtcNow();

        var order = Order.Place(
            Guid.CreateVersion7(now),
            customerId,
            request.CustomerEmail!,
            request.Currency!,
            request.Items!.Select(i => new NewOrderLine(i.ProductId!, i.ProductName!, i.Quantity!.Value, i.UnitPrice!.Value)),
            idempotencyKey,
            requestHash,
            now);

        db.Orders.Add(order);
        db.OrderStatuses.Add(OrderStatusRecord.Received(order.Id, now));
        db.OutboxMessages.Add(OutboxMessage.Create(
            ToOrderPlaced(order), OrderPlaced.EventType, OrderPlaced.RoutingKey, ContractsJsonContext.Default.OrderPlaced, now));

        try
        {
            await db.SaveChangesAsync(cancellationToken);
        }
        catch (DbUpdateException ex) when (IsIdempotencyConflict(ex))
        {
            // A request with the same key committed first; a concurrent one waits on the unique index until
            // that commit and then lands here. Our transaction rolled back as a whole, so no second outbox row exists.
            db.ChangeTracker.Clear();
            return await ReplayAsync(db, customerId, idempotencyKey, requestHash, httpContext, cancellationToken);
        }

        return TypedResults.Created($"/orders/{order.Id}", new OrderAccepted(order.Id, order.Reference, OrderStatus.Received));
    }

    private static async Task<Results<Created<OrderAccepted>, Ok<OrderAccepted>, ProblemHttpResult>> ReplayAsync(
        OrderingDbContext db,
        Guid customerId,
        string idempotencyKey,
        byte[] requestHash,
        HttpContext httpContext,
        CancellationToken cancellationToken)
    {
        var existing = await db.Orders.AsNoTracking()
            .Where(o => o.CustomerId == customerId && o.IdempotencyKey == idempotencyKey)
            .Join(db.OrderStatuses, o => o.Id, s => s.OrderId, (o, s) => new { o.Id, o.Reference, o.RequestHash, s.Status })
            .SingleAsync(cancellationToken);

        if (!existing.RequestHash.AsSpan().SequenceEqual(requestHash))
        {
            return TypedResults.Problem(
                statusCode: StatusCodes.Status422UnprocessableEntity,
                title: "Idempotency-Key reused with a different request",
                detail: "This key already belongs to another order. Use a new key for a new order.");
        }

        httpContext.Response.Headers[IdempotentReplayedHeader] = "true";
        return TypedResults.Ok(new OrderAccepted(existing.Id, existing.Reference, existing.Status));
    }

    private static bool IsValidIdempotencyKey([NotNullWhen(true)] string? key) =>
        key is { Length: > 0 and <= IdempotencyKeyMaxLength } && key.All(c => c is >= '!' and <= '~');

    private static bool IsIdempotencyConflict(DbUpdateException ex) =>
        ex.InnerException is PostgresException
        {
            SqlState: PostgresErrorCodes.UniqueViolation,
            ConstraintName: OrderingDbContext.IdempotencyIndexName,
        };

    /// <summary>Hash of the re-serialized body, so whitespace or property order in the raw JSON does not matter.</summary>
    private static byte[] ComputeFingerprint(PlaceOrderRequest request) =>
        SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(request, ApiJsonContext.Default.PlaceOrderRequest));

    private static OrderPlaced ToOrderPlaced(Order order) => new(
        order.Id,
        order.Reference,
        order.CustomerId,
        order.CustomerEmail,
        order.Currency,
        order.TotalAmount,
        order.Items.Select(i => new OrderPlacedItem(i.ProductId, i.ProductName, i.Quantity, i.UnitPrice)).ToArray(),
        order.CreatedAt);
}
