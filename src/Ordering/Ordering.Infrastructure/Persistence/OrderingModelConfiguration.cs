using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Ordering.Infrastructure.Orders;

namespace Ordering.Infrastructure.Persistence;

internal sealed class OrderConfiguration : IEntityTypeConfiguration<Order>
{
    public void Configure(EntityTypeBuilder<Order> order)
    {
        order.ToTable("orders");
        order.HasKey(o => o.Id);
        order.Property(o => o.Id).ValueGeneratedNever();
        order.Property(o => o.Reference).HasMaxLength(30);
        order.Property(o => o.CustomerEmail).HasMaxLength(254);
        order.Property(o => o.Currency).HasMaxLength(3).IsFixedLength();
        order.Property(o => o.TotalAmount).HasPrecision(18, 2);
        order.Property(o => o.IdempotencyKey).HasMaxLength(64);

        order.HasIndex(o => new { o.CustomerId, o.IdempotencyKey })
            .IsUnique()
            .HasDatabaseName(OrderingDbContext.IdempotencyIndexName);

        order.HasMany(o => o.Items).WithOne().HasForeignKey(i => i.OrderId);
        order.Navigation(o => o.Items).UsePropertyAccessMode(PropertyAccessMode.Field);

        order.HasOne<OrderStatusRecord>().WithOne().HasForeignKey<OrderStatusRecord>(s => s.OrderId);
    }
}

internal sealed class OrderItemConfiguration : IEntityTypeConfiguration<OrderItem>
{
    public void Configure(EntityTypeBuilder<OrderItem> item)
    {
        item.ToTable("order_items");
        item.HasKey(i => new { i.OrderId, i.LineNo });
        item.Property(i => i.ProductId).HasMaxLength(64);
        item.Property(i => i.ProductName).HasMaxLength(200);
        item.Property(i => i.UnitPrice).HasPrecision(18, 2);
    }
}

internal sealed class OrderStatusConfiguration : IEntityTypeConfiguration<OrderStatusRecord>
{
    public void Configure(EntityTypeBuilder<OrderStatusRecord> status)
    {
        status.ToTable("order_status");
        status.HasKey(s => s.OrderId);
        status.Property(s => s.Status).HasConversion<string>().HasMaxLength(32);

        // Each row is updated in place a few times; free space on the page lets PostgreSQL do HOT updates
        // (no index maintenance) instead of moving the new row version to another page.
        status.HasStorageParameter("fillfactor", 80);
    }
}
