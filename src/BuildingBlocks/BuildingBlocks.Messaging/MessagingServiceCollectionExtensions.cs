using System.Text.Json.Serialization.Metadata;
using BuildingBlocks.Messaging.Consuming;
using BuildingBlocks.Messaging.Outbox;
using BuildingBlocks.Outbox;
using Contracts;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Polly;
using Polly.CircuitBreaker;

namespace BuildingBlocks.Messaging;

public static class MessagingServiceCollectionExtensions
{
    /// <summary>Shared, process-wide broker plumbing: connections and topology. Every RabbitMQ-using service calls this.</summary>
    public static IServiceCollection AddRabbitMq(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<RabbitMqOptions>().Bind(configuration.GetSection(RabbitMqOptions.SectionName));
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<RabbitMqConnections>();
        services.TryAddSingleton<RabbitMqTopology>();
        return services;
    }

    /// <summary>
    /// Runs the outbox relay for the service's own database (an <c>NpgsqlDataSource</c> must be registered)
    /// and publishes to <paramref name="exchange"/>.
    /// </summary>
    public static IServiceCollection AddOutboxRelay(this IServiceCollection services, IConfiguration configuration, string exchange)
    {
        services.AddOptions<OutboxRelayOptions>()
            .Bind(configuration.GetSection(OutboxRelayOptions.SectionName))
            .Configure(options => options.Exchange = exchange)
            .Validate(o => o.BatchSize is > 0 and <= 5_000, "Outbox:BatchSize must be between 1 and 5000.")
            .Validate(o => o.LeaseDuration > o.PublishTimeout,
                "Outbox:LeaseDuration must exceed Outbox:PublishTimeout, or another relay could take over a batch still being published.")
            .ValidateOnStart();

        services.TryAddSingleton<OutboxStore>();
        services.TryAddSingleton<OutboxPublisher>();

        // Singleton pipeline: the breaker's state must be shared across batches, or it would never open.
        services.AddResiliencePipeline(OutboxRelayService.PipelineName, (pipeline, context) =>
        {
            var relay = context.GetOptions<OutboxRelayOptions>();
            pipeline
                .AddCircuitBreaker(new CircuitBreakerStrategyOptions
                {
                    FailureRatio = 0.5,
                    MinimumThroughput = 4,
                    SamplingDuration = TimeSpan.FromSeconds(30),
                    BreakDuration = TimeSpan.FromSeconds(15),
                })
                .AddTimeout(relay.PublishTimeout);
        });

        services.AddHostedService<OutboxRelayService>();
        return services;
    }

    /// <summary>
    /// Consumes <paramref name="queue"/> with <typeparamref name="THandler"/> (resolved from a new scope per batch).
    /// Only messages whose AMQP type equals <c>TMessage.EventType</c> are accepted; anything else is dead-lettered.
    /// </summary>
    public static IServiceCollection AddRabbitMqConsumer<TMessage, THandler>(
        this IServiceCollection services,
        ConsumerQueue queue,
        JsonTypeInfo<TMessage> typeInfo,
        Action<ConsumerOptions>? configure = null)
        where TMessage : IIntegrationEvent
        where THandler : class, IBatchMessageHandler<TMessage>
    {
        var options = new ConsumerOptions();
        configure?.Invoke(options);

        if (options.Concurrency < 1 || options.MaxBatchSize < 1 || options.Prefetch < options.MaxBatchSize)
        {
            throw new InvalidOperationException(
                $"Consumer of '{queue.Name}': Concurrency and MaxBatchSize must be at least 1, and Prefetch at least MaxBatchSize " +
                $"(got {options.Concurrency}, {options.MaxBatchSize}, {options.Prefetch}).");
        }

        services.TryAddSingleton<FailedMessagePublisher>();
        services.TryAddScoped<THandler>();
        services.AddSingleton(new ConsumerRegistration<TMessage>(queue, TMessage.EventType, typeInfo, options));
        services.AddHostedService<RabbitMqConsumerService<TMessage, THandler>>();
        return services;
    }
}
