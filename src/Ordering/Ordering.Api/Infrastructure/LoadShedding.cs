using System.Threading.RateLimiting;
using Microsoft.AspNetCore.RateLimiting;

namespace Ordering.Api.Infrastructure;

/// <summary>
/// Caps in-flight requests per instance. When the database slows down, latency (not throughput) grows first;
/// without a cap requests pile up, every one of them waits for a pooled connection and the instance degrades
/// for everybody. Over the cap a request gets 503 + Retry-After immediately.
/// By Little's Law the cap ≈ per-instance RPS × acceptable latency (2,000/s × 0.25 s = 500); tuned with k6.
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
        public int MaxConcurrentRequests { get; init; } = 512;

        public int QueueLimit { get; init; } = 128;
    }
}
