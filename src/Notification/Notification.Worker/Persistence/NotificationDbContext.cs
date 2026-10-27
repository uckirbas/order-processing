using BuildingBlocks.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Notification.Worker.Persistence;

/// <summary>Not thread-safe: one instance per message, created from the pooled factory.</summary>
public sealed class NotificationDbContext(DbContextOptions<NotificationDbContext> options) : DbContext(options)
{
    public DbSet<Notification> Notifications => Set<Notification>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Notification>(notification =>
        {
            notification.ToTable("notifications");
            notification.HasKey(n => n.Id);
            notification.Property(n => n.Id).ValueGeneratedNever();
            notification.Property(n => n.Type).HasConversion<string>().HasMaxLength(32);
            notification.Property(n => n.Status).HasConversion<string>().HasMaxLength(32);
            notification.Property(n => n.Recipient).HasMaxLength(254);
            notification.Property(n => n.LastError).HasMaxLength(1000);
            notification.HasIndex(n => new { n.OrderId, n.Type }).IsUnique().HasDatabaseName("ux_notifications_order_id_type");

            // Each row is updated in place a few times (claim, outcome): leave room for HOT updates.
            notification.HasStorageParameter("fillfactor", 80);
        });
    }
}

public static class NotificationPersistence
{
    /// <summary>Workers create one short-lived context per message from a pool of reusable instances.</summary>
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
