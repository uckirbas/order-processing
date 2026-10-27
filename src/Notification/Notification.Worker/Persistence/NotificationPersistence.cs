using BuildingBlocks.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Notification.Worker.Persistence;

public static class NotificationPersistence
{
    /// <summary>Workers create one short-lived context per batch from a pool of reusable instances.</summary>
    public static IServiceCollection AddNotificationPersistence(this IServiceCollection services, string connectionString) =>
        services.AddPooledDbContextFactory<NotificationDbContext>(options => options.UseServiceDatabase(connectionString));
}

/// <summary>Used by <c>dotnet ef</c>; migrations connect straight to PostgreSQL, not PgBouncer.</summary>
internal sealed class NotificationDesignTimeDbContextFactory : IDesignTimeDbContextFactory<NotificationDbContext>
{
    private const string LocalDirectConnection =
        "Host=localhost;Port=15432;Database=notification;Username=notification_svc;Password=notification_svc_pwd";

    public NotificationDbContext CreateDbContext(string[] args)
    {
        var options = new DbContextOptionsBuilder<NotificationDbContext>();
        options.UseServiceDatabase(Environment.GetEnvironmentVariable("NOTIFICATION_MIGRATIONS_CONNECTION") ?? LocalDirectConnection);
        return new NotificationDbContext(options.Options);
    }
}
