using Microsoft.AspNetCore.Diagnostics;
using Npgsql;

namespace Ordering.Api.Infrastructure;

/// <summary>
/// Database unreachable, pool wait exceeded or command timed out: answer 503 + Retry-After at once.
/// There is no server-side retry on the hot path; it would multiply load during an outage (retry storm).
/// The client retries, and the Idempotency-Key makes that safe.
/// </summary>
internal sealed class TransientDatabaseExceptionHandler(
    IProblemDetailsService problemDetails,
    ILogger<TransientDatabaseExceptionHandler> logger) : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
    {
        if (!IsTransient(exception))
        {
            return false;
        }

        logger.LogWarning(exception, "Database temporarily unavailable; request rejected with 503");

        httpContext.Response.StatusCode = StatusCodes.Status503ServiceUnavailable;
        httpContext.Response.Headers.RetryAfter = "1";

        return await problemDetails.TryWriteAsync(new ProblemDetailsContext
        {
            HttpContext = httpContext,
            Exception = exception,
            ProblemDetails =
            {
                Status = StatusCodes.Status503ServiceUnavailable,
                Title = "Service temporarily unavailable",
                Detail = "Please retry with the same Idempotency-Key.",
            },
        });
    }

    private static bool IsTransient(Exception exception)
    {
        for (var e = exception; e is not null; e = e.InnerException)
        {
            if (e is NpgsqlException { IsTransient: true } or TimeoutException)
            {
                return true;
            }
        }

        return false;
    }
}
