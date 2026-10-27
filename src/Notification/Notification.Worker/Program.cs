using BuildingBlocks.Messaging;
using Contracts;
using Notification.Worker.Email;
using Notification.Worker.Notifications;
using Notification.Worker.Persistence;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddNotificationPersistence(
    builder.Configuration.GetConnectionString("Notification")
    ?? throw new InvalidOperationException("Connection string 'Notification' is not configured."));

builder.Services.AddOptions<NotificationOptions>()
    .Bind(builder.Configuration.GetSection(NotificationOptions.SectionName))
    .Validate(o => o.LeaseDuration > builder.Configuration.GetSection(SmtpOptions.SectionName).Get<SmtpOptions>()?.TotalTimeout,
        "Notification:LeaseDuration must exceed Smtp:TotalTimeout, or a slow send could be taken over and duplicated.")
    .ValidateOnStart();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSmtpEmail(builder.Configuration);

builder.Services.AddRabbitMq(builder.Configuration);
builder.Services.AddRabbitMqConsumer<OrderPlaced, OrderPlacedNotificationHandler>(
    MessagingTopology.NotificationOrderPlaced,
    ContractsJsonContext.Default.OrderPlaced,
    consumer => builder.Configuration.GetSection("Consumers:OrderPlaced").Bind(consumer));
builder.Services.AddRabbitMqConsumer<InvoiceGenerated, InvoiceGeneratedNotificationHandler>(
    MessagingTopology.NotificationInvoiceGenerated,
    ContractsJsonContext.Default.InvoiceGenerated,
    consumer => builder.Configuration.GetSection("Consumers:InvoiceGenerated").Bind(consumer));

await builder.Build().RunAsync();
