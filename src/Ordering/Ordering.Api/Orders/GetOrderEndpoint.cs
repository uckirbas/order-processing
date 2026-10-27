using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.EntityFrameworkCore;
using Ordering.Infrastructure.Persistence;

namespace Ordering.Api.Orders;

internal static class GetOrderEndpoint
{
    /// <summary>Reads only the Ordering database: status arrives here through events, never by calling another service.</summary>
    public static async Task<Results<Ok<OrderDetails>, NotFound>> HandleAsync(
        Guid id, OrderingDbContext db, CancellationToken cancellationToken)
    {
        var order = await (
                from o in db.Orders.AsNoTracking()
                join s in db.OrderStatuses on o.Id equals s.OrderId
                where o.Id == id
                select new OrderDetails(
                    o.Id,
                    o.Reference,
                    s.Status,
                    s.UpdatedAt,
                    o.CustomerId,
                    o.CustomerEmail,
                    o.Currency,
                    o.TotalAmount,
                    o.CreatedAt,
                    o.Items
                        .OrderBy(i => i.LineNo)
                        .Select(i => new OrderLineDetails(i.ProductId, i.ProductName, i.Quantity, i.UnitPrice))
                        .ToList()))
            .SingleOrDefaultAsync(cancellationToken);

        return order is null ? TypedResults.NotFound() : TypedResults.Ok(order);
    }
}
