using Microsoft.EntityFrameworkCore;

namespace Notification.Worker.Persistence;

/// <summary>Not thread-safe: one instance per batch, created from the pooled factory.</summary>
public sealed class NotificationDbContext(DbContextOptions<NotificationDbContext> options) : DbContext(options)
{
    public DbSet<Notification> Notifications => Set<Notification>();

    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.ApplyConfigurationsFromAssembly(typeof(NotificationDbContext).Assembly);
}
