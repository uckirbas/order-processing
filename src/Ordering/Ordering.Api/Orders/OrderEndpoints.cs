using Ordering.Api.Infrastructure;

namespace Ordering.Api.Orders;

internal static class OrderEndpoints
{
    public static IEndpointRouteBuilder MapOrderEndpoints(this IEndpointRouteBuilder app)
    {
        var orders = app.MapGroup("/orders")
            .WithTags("Orders")
            .RequireRateLimiting(LoadShedding.PolicyName)
            .ProducesProblem(StatusCodes.Status503ServiceUnavailable);

        orders.MapPost("", PlaceOrderEndpoint.HandleAsync)
            .WithName("PlaceOrder")
            .WithSummary("Place an order")
            .WithDescription(
                "Stores the order and returns immediately; confirmation e-mail and invoice are processed asynchronously. " +
                "Retrying with the same Idempotency-Key and body returns the original order (200, Idempotent-Replayed: true); " +
                "the same key with a different body is rejected with 422.")
            .ProducesValidationProblem()
            .ProducesProblem(StatusCodes.Status422UnprocessableEntity);

        orders.MapGet("/{id:guid}", GetOrderEndpoint.HandleAsync)
            .WithName("GetOrder")
            .WithSummary("Get order details and current status");

        return app;
    }
}
