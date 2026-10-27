using BuildingBlocks.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Npgsql;

namespace Invoicing.Worker.Persistence;

public static class InvoicingPersistence
{
    /// <summary>One short-lived context per batch, on the process-wide data source the outbox relay also uses.</summary>
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
