using Contracts;
using Invoicing.Worker.Persistence;
using Microsoft.Extensions.Options;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;

namespace Invoicing.Worker.Invoices;

public sealed record InvoiceDocument(InvoiceLine[] Lines, decimal Total);

/// <summary>Produces the invoice document; in production this is where an e-invoice provider would be called.</summary>
public interface IInvoiceGenerator
{
    /// <exception cref="InvoiceRejectedException">A business rule makes this order impossible to invoice.</exception>
    Task<InvoiceDocument> GenerateAsync(OrderPlaced order, CancellationToken cancellationToken);
}

/// <summary>Permanent: retrying cannot change the outcome.</summary>
public sealed class InvoiceRejectedException(string message) : Exception(message);

/// <summary>Transient: the (simulated) provider timed out or was unavailable.</summary>
public sealed class InvoiceProviderUnavailableException(string message) : Exception(message);

public sealed class InvoiceGeneratorOptions
{
    public const string SectionName = "InvoiceGenerator";

    /// <summary>Simulated rendering + provider round-trip.</summary>
    public TimeSpan Latency { get; set; } = TimeSpan.FromMilliseconds(20);

    /// <summary>Share of calls failing transiently (0-1); lets the retry and circuit-breaker paths be exercised.</summary>
    public double TransientFailureRate { get; set; }

    /// <summary>Configured in appsettings (a default here would be merged with, not replaced by, configured values).</summary>
    public string[] SupportedCurrencies { get; set; } = [];

    public TimeSpan AttemptTimeout { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan TotalTimeout { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Calls in flight to the provider per process. Batches generate their invoices concurrently, so without this
    /// the provider would see up to Concurrency × MaxBatchSize simultaneous calls.
    /// </summary>
    public int MaxConcurrency { get; set; } = 32;
}

/// <summary>Stand-in for a real e-invoice provider, with configurable latency and failure injection.</summary>
internal sealed class SimulatedInvoiceGenerator(IOptions<InvoiceGeneratorOptions> options, TimeProvider clock) : IInvoiceGenerator
{
    private readonly InvoiceGeneratorOptions _options = options.Value;
    private readonly HashSet<string> _currencies = new(options.Value.SupportedCurrencies, StringComparer.Ordinal);

    public async Task<InvoiceDocument> GenerateAsync(OrderPlaced order, CancellationToken cancellationToken)
    {
        if (!_currencies.Contains(order.Currency))
        {
            throw new InvoiceRejectedException($"Currency '{order.Currency}' is not supported for invoicing.");
        }

        var lines = order.Items
            .Select(i => new InvoiceLine(i.ProductId, i.ProductName, i.Quantity, i.UnitPrice, i.Quantity * i.UnitPrice))
            .ToArray();
        var total = lines.Sum(l => l.LineTotal);
        if (total != order.TotalAmount)
        {
            throw new InvoiceRejectedException($"Line total {total} does not match order total {order.TotalAmount}.");
        }

        await Task.Delay(_options.Latency, clock, cancellationToken);

        // Random.Shared is thread-safe; a shared new Random() would not be.
        if (_options.TransientFailureRate > 0 && Random.Shared.NextDouble() < _options.TransientFailureRate)
        {
            throw new InvoiceProviderUnavailableException("Simulated e-invoice provider timeout.");
        }

        return new InvoiceDocument(lines, total);
    }
}

internal static class InvoiceGeneration
{
    public const string PipelineName = "invoice-generator";

    /// <summary>Rejections are business outcomes, not provider health: they neither retry nor trip the breaker.</summary>
    private static bool IsTransient(Exception exception) =>
        exception is not (InvoiceRejectedException or BrokenCircuitException or OperationCanceledException);

    public static IServiceCollection AddInvoiceGeneration(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<InvoiceGeneratorOptions>()
            .Bind(configuration.GetSection(InvoiceGeneratorOptions.SectionName))
            .Validate(o => o.SupportedCurrencies.Length > 0, "InvoiceGenerator:SupportedCurrencies must not be empty.")
            .Validate(o => o.TransientFailureRate is >= 0 and <= 1, "InvoiceGenerator:TransientFailureRate must be between 0 and 1.")
            .Validate(o => o.MaxConcurrency > 0, "InvoiceGenerator:MaxConcurrency must be positive.")
            .ValidateOnStart();
        services.AddSingleton<IInvoiceGenerator, SimulatedInvoiceGenerator>();

        // total timeout → retry (exponential + jitter) → breaker → concurrency limit → attempt timeout.
        // The limit sits inside the retry, so a call waiting out its backoff holds no permit. Callers queue rather than
        // being rejected (the consumer prefetch bounds the queue), and the wait counts against the total timeout only.
        services.AddResiliencePipeline(PipelineName, (pipeline, context) =>
        {
            var generator = context.GetOptions<InvoiceGeneratorOptions>();
            var transient = new PredicateBuilder().Handle<Exception>(IsTransient);
            pipeline
                .AddTimeout(generator.TotalTimeout)
                .AddRetry(new RetryStrategyOptions
                {
                    MaxRetryAttempts = 3,
                    Delay = TimeSpan.FromMilliseconds(200),
                    BackoffType = DelayBackoffType.Exponential,
                    UseJitter = true,
                    ShouldHandle = transient,
                })
                .AddCircuitBreaker(new CircuitBreakerStrategyOptions
                {
                    FailureRatio = 0.5,
                    MinimumThroughput = 20,
                    SamplingDuration = TimeSpan.FromSeconds(30),
                    BreakDuration = TimeSpan.FromSeconds(30),
                    ShouldHandle = transient,
                })
                .AddConcurrencyLimiter(generator.MaxConcurrency, queueLimit: int.MaxValue)
                .AddTimeout(generator.AttemptTimeout);
        });

        return services;
    }
}
