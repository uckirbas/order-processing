using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Invoicing.Worker.Persistence;

internal sealed class InvoiceConfiguration : IEntityTypeConfiguration<Invoice>
{
    public void Configure(EntityTypeBuilder<Invoice> invoice)
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
    }
}
