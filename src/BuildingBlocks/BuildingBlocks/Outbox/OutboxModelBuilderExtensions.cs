using Microsoft.EntityFrameworkCore;

namespace BuildingBlocks.Outbox;

public static class OutboxModelBuilderExtensions
{
    /// <summary>Maps <see cref="OutboxMessage"/> to <c>outbox_messages</c> in the calling service's own database.</summary>
    public static ModelBuilder AddOutbox(this ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OutboxMessage>(outbox =>
        {
            outbox.ToTable("outbox_messages");
            outbox.HasKey(m => m.Id);
            outbox.Property(m => m.Id).ValueGeneratedNever();
            outbox.Property(m => m.Type).HasMaxLength(100);
            outbox.Property(m => m.RoutingKey).HasMaxLength(100);
            outbox.Property(m => m.Payload).HasColumnType("jsonb");

            // Every row is inserted once and deleted once: vacuum early so dead tuples do not pile up
            // in front of the relay's primary-key scan.
            // Passed as a string: a double is rendered with the machine culture (e.g. "0,01" on tr-TR) in migration SQL.
            outbox.HasStorageParameter("autovacuum_vacuum_scale_factor", "0.01");
        });

        return modelBuilder;
    }
}
