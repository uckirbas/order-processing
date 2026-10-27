using System.Text.Json;
using Microsoft.AspNetCore.Diagnostics;

namespace Ordering.Api.Infrastructure;

/// <summary>
/// Malformed JSON, a wrong value type ("2" for a number) or a body over the Kestrel limit: answer with
/// the request's own status code (400 / 413) as ProblemDetails, pointing at the offending JSON path.
/// </summary>
internal sealed class BadRequestExceptionHandler(IProblemDetailsService problemDetails) : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
    {
        if (exception is not BadHttpRequestException badRequest)
        {
            return false;
        }

        httpContext.Response.StatusCode = badRequest.StatusCode;

        return await problemDetails.TryWriteAsync(new ProblemDetailsContext
        {
            HttpContext = httpContext,
            Exception = exception,
            ProblemDetails =
            {
                Status = badRequest.StatusCode,
                Title = badRequest.StatusCode == StatusCodes.Status413PayloadTooLarge
                    ? "Request body too large"
                    : "Malformed request body",
                Detail = badRequest.InnerException is JsonException { Path: { } path }
                    ? $"Invalid JSON or wrong value type at '{path}'."
                    : "The request body could not be read.",
            },
        });
    }
}
