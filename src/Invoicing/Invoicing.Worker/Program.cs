using BuildingBlocks.Messaging;
using Contracts;
using Invoicing.Worker.Invoices;
using Invoicing.Worker.Persistence;
using Npgsql;

var builder = Host.CreateApplicationBuilder(args);

// One data source (and so one connection pool) per process, shared by the relay and the EF contexts.
var invoicingConnectionString = builder.Configuration.GetConnectionString("Invoicing")
    ?? throw new InvalidOperationException("Connection string 'Invoicing' is not configured.");
builder.Services.AddSingleton(_ => NpgsqlDataSource.Create(invoicingConnectionString));
builder.Services.AddInvoicingPersistence();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddInvoiceGeneration(builder.Configuration);

builder.Services.AddRabbitMq(builder.Configuration);
builder.Services.AddOutboxRelay(builder.Configuration, exchange: MessagingTopology.InvoicingExchange);
builder.Services.AddRabbitMqConsumer<OrderPlaced, OrderPlacedInvoiceHandler>(
    MessagingTopology.InvoicingOrderPlaced,
    ContractsJsonContext.Default.OrderPlaced,
    consumer => builder.Configuration.GetSection("Consumers:OrderPlaced").Bind(consumer));

await builder.Build().RunAsync();
