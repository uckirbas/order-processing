using BuildingBlocks.Messaging;
using Contracts;
using Npgsql;
using Ordering.Infrastructure.Persistence;
using Ordering.Worker.Invoices;

var builder = Host.CreateApplicationBuilder(args);

// One data source (and so one connection pool) per process, shared by the relay and the EF contexts.
var orderingConnectionString = builder.Configuration.GetConnectionString("Ordering")
    ?? throw new InvalidOperationException("Connection string 'Ordering' is not configured.");
builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(orderingConnectionString));
builder.Services.AddOrderingDbContextFactory();
builder.Services.AddSingleton(TimeProvider.System);

builder.Services.AddRabbitMq(builder.Configuration);
builder.Services.AddOutboxRelay(builder.Configuration, exchange: MessagingTopology.OrderingExchange);
builder.Services.AddRabbitMqConsumer<InvoiceGenerated, InvoiceGeneratedStatusHandler>(
    MessagingTopology.OrderingInvoiceGenerated,
    ContractsJsonContext.Default.InvoiceGenerated,
    consumer => builder.Configuration.GetSection("Consumers:InvoiceGenerated").Bind(consumer));
builder.Services.AddRabbitMqConsumer<InvoiceFailed, InvoiceFailedStatusHandler>(
    MessagingTopology.OrderingInvoiceFailed,
    ContractsJsonContext.Default.InvoiceFailed,
    consumer => builder.Configuration.GetSection("Consumers:InvoiceFailed").Bind(consumer));

await builder.Build().RunAsync();
