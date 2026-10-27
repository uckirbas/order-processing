using BuildingBlocks.Outbox;
using Microsoft.EntityFrameworkCore;

namespace Invoicing.Worker.Persistence;

/// <summary>Not thread-safe: one instance per batch, created from the pooled factory.</summary>
public sealed class InvoicingDbContext(DbContextOptions<InvoicingDbContext> options) : DbContext(options)
{
    public const string InvoiceNumberSequence = "invoice_number_seq";

    public DbSet<Invoice> Invoices => Set<Invoice>();

    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Gaps are possible (nextval is not rolled back and runs before the conflict check); see DESIGN.md.
        modelBuilder.HasSequence<long>(InvoiceNumberSequence);

        modelBuilder.ApplyConfigurationsFromAssembly(typeof(InvoicingDbContext).Assembly);
        modelBuilder.AddOutbox();
    }
}
