using BuildingBlocks.Outbox;
using BuildingBlocks.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Npgsql;

namespace Invoicing.Worker.Persistence;

/// <summary>Not thread-safe: one instance per message, created from the pooled factory.</summary>
public sealed class InvoicingDbContext(DbContextOptions<InvoicingDbContext> options) : DbContext(options)
{
    public const string InvoiceNumberSequence = "invoice_number_seq";

    public DbSet<Invoice> Invoices => Set<Invoice>();

    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Gaps are possible (nextval is not rolled back and runs before the conflict check); see DESIGN.md.
        modelBuilder.HasSequence<long>(InvoiceNumberSequence);

        modelBuilder.Entity<Invoice>(invoice =>
        {
            invoice.ToTable("invoices");
            invoice.HasKey(i => i.Id);
            invoice.Property(i => i.Id).ValueGeneratedNever();
            invoice.Property(i => i.OrderReference).HasMaxLength(30);
            invoice.Property(i => i.InvoiceNumber).HasMaxLength(20);
            invoice.Property(i => i.Status).HasConversion<string>().HasMaxLength(32);
            invoice.Property(i => i.CustomerEmail).HasMaxLength(254);
            invoice.Property(i => i.Currency).HasMaxLength(3).IsFixedLength();
            invoice.Property(i => i.TotalAmount).HasPrecision(18, 2);
            invoice.Property(i => i.Lines).HasColumnType("jsonb");
            invoice.Property(i => i.FailureReason).HasMaxLength(1000);
            invoice.HasIndex(i => i.OrderId).IsUnique().HasDatabaseName("ux_invoices_order_id");
            invoice.HasIndex(i => i.InvoiceNumber).IsUnique().HasDatabaseName("ux_invoices_invoice_number");
        });

        modelBuilder.AddOutbox();
    }
}

public static class InvoicingPersistence
{
    /// <summary>One short-lived context per message, on the process-wide data source the outbox relay also uses.</summary>
    public static IServiceCollection AddInvoicingPersistence(this IServiceCollection services) =>
        services.AddPooledDbContextFactory<InvoicingDbContext>((provider, options) =>
            options.UseServiceDatabase(provider.GetRequiredService<NpgsqlDataSource>()));
}

/// <summary>Used by <c>dotnet ef</c>; migrations connect straight to PostgreSQL, not PgBouncer.</summary>
internal sealed class InvoicingDesignTimeDbContextFactory : IDesignTimeDbContextFactory<InvoicingDbContext>
{
    private const string LocalDirectConnection =
        "Host=localhost;Port=15432;Database=invoicing;Username=invoicing_svc;Password=invoicing_svc_pwd";

    public InvoicingDbContext CreateDbContext(string[] args)
    {
        var options = new DbContextOptionsBuilder<InvoicingDbContext>();
        options.UseServiceDatabase(Environment.GetEnvironmentVariable("INVOICING_MIGRATIONS_CONNECTION") ?? LocalDirectConnection);
        return new InvoicingDbContext(options.Options);
    }
}
