using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Notification.Worker.Persistence;

internal sealed class NotificationConfiguration : IEntityTypeConfiguration<Notification>
{
    public void Configure(EntityTypeBuilder<Notification> notification)
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
    }
}
