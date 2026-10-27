using BuildingBlocks.Outbox;
using Microsoft.EntityFrameworkCore;
using Ordering.Infrastructure.Orders;

namespace Ordering.Infrastructure.Persistence;

/// <summary>Not thread-safe: one instance per request (API) or per batch (worker), never shared.</summary>
public sealed class OrderingDbContext(DbContextOptions<OrderingDbContext> options) : DbContext(options)
{
    /// <summary>Unique index behind API idempotency; its name is how a duplicate request is recognised.</summary>
    public const string IdempotencyIndexName = "ux_orders_customer_id_idempotency_key";

    public DbSet<Order> Orders => Set<Order>();

    public DbSet<OrderItem> OrderItems => Set<OrderItem>();

    public DbSet<OrderStatusRecord> OrderStatuses => Set<OrderStatusRecord>();

    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ApplyConfigurationsFromAssembly(typeof(OrderingDbContext).Assembly);
        modelBuilder.AddOutbox();
    }
}
