namespace BuildingBlocks.Messaging;

public sealed class RabbitMqOptions
{
    public const string SectionName = "RabbitMq";

    public string Host { get; set; } = "localhost";

    public int Port { get; set; } = 5672;

    public string UserName { get; set; } = "guest";

    public string Password { get; set; } = "guest";

    public string VirtualHost { get; set; } = "/";

    /// <summary>Shown in the management UI together with the machine name, so each replica is identifiable.</summary>
    public string ClientName { get; set; } = "order-processing";
}
