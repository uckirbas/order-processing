using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;

namespace Notification.Worker.Email;

internal static class EmailServiceCollectionExtensions
{
    public static IServiceCollection AddSmtpEmail(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<SmtpOptions>()
            .Bind(configuration.GetSection(SmtpOptions.SectionName))
            .Validate(o => o.MaxConnections > 0, "Smtp:MaxConnections must be positive.")
            .ValidateOnStart();

        services.AddSingleton<SmtpClientPool>();
        services.AddSingleton<IEmailSender, SmtpEmailSender>();

        // Outer to inner: total timeout → retry (exponential + jitter) → circuit breaker → per-attempt timeout.
        // Singleton pipeline: the breaker state is shared by every concurrent send, which is what lets it open.
        services.AddResiliencePipeline(SmtpResilience.PipelineName, (pipeline, context) =>
        {
            var smtp = context.GetOptions<SmtpOptions>();
            var transient = new PredicateBuilder().Handle<Exception>(SmtpResilience.IsTransient);

            pipeline
                .AddTimeout(smtp.TotalTimeout)
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
                .AddTimeout(smtp.AttemptTimeout);
        });

        return services;
    }
}
