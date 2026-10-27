namespace Notification.Worker.Email;

public sealed class SmtpOptions
{
    public const string SectionName = "Smtp";

    public string Host { get; set; } = "localhost";

    public int Port { get; set; } = 1025;

    public string? UserName { get; set; }

    public string? Password { get; set; }

    /// <summary>Upper bound of concurrent SMTP sessions (and pooled connections) per process.</summary>
    public int MaxConnections { get; set; } = 16;

    public string FromAddress { get; set; } = "no-reply@order-processing.local";

    public string FromName { get; set; } = "Order Processing";

    public TimeSpan AttemptTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>All in-process attempts together; must stay well below the notification lease.</summary>
    public TimeSpan TotalTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
