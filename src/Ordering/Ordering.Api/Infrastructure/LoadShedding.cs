using System.Threading.RateLimiting;
using Microsoft.AspNetCore.RateLimiting;

namespace Ordering.Api.Infrastructure;

/// <summary>
/// Caps in-flight requests per instance. When the database slows down, latency (not throughput) grows first;
/// without a cap requests pile up, every one of them waits for a pooled connection and the instance degrades
/// for everybody. Over the cap a request waits in a short queue, and when that is full it gets 503 + Retry-After
/// immediately.
/// The cap follows the connection pool, not the request rate: an order holds a connection for most of its life, so a
/// few permits per pooled connection keep the pool busy, and everything beyond waits here, where waiting is cheap.
/// A cap far above the pool (k6 showed it at 512 for a pool of 20) moves the wait into the pool instead, where it
/// ends in a 2 s timeout and an exception: under overload that costs more CPU than the work itself and spirals.
/// </summary>
internal static class LoadShedding
{
    public const string PolicyName = "orders";

    public static IServiceCollection AddLoadShedding(this IServiceCollection services, IConfiguration configuration)
    {
        var options = configuration.GetSection("LoadShedding").Get<LoadSheddingOptions>() ?? new LoadSheddingOptions();

        return services.AddRateLimiter(limiter =>
        {
            limiter.RejectionStatusCode = StatusCodes.Status503ServiceUnavailable;
            limiter.OnRejected = (context, _) =>
            {
                context.HttpContext.Response.Headers.RetryAfter = "1";
                return ValueTask.CompletedTask;
            };

            // A single limiter instance shared by all requests of this policy (thread-safe by design).
            limiter.AddConcurrencyLimiter(PolicyName, concurrency =>
            {
                concurrency.PermitLimit = options.MaxConcurrentRequests;
                concurrency.QueueLimit = options.QueueLimit;
                concurrency.QueueProcessingOrder = QueueProcessingOrder.OldestFirst;
            });
        });
    }

    private sealed class LoadSheddingOptions
    {
        public int MaxConcurrentRequests { get; init; } = 64;

        public int QueueLimit { get; init; } = 256;
    }
}
