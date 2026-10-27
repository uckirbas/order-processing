using BuildingBlocks.Persistence;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.DependencyInjection;

namespace Ordering.Infrastructure.Persistence;

public static class OrderingPersistence
{
    public static DbContextOptionsBuilder UseOrderingDatabase(this DbContextOptionsBuilder options, string connectionString) =>
        options.UseServiceDatabase(connectionString);

    /// <summary>API: pooled, request-scoped contexts (resetting a pooled instance is cheaper than building one).</summary>
    public static IServiceCollection AddOrderingDbContextPool(this IServiceCollection services, string connectionString) =>
        services.AddDbContextPool<OrderingDbContext>(options => options.UseOrderingDatabase(connectionString));

    /// <summary>
    /// Worker: one short-lived context per batch from a pooled factory, on the process-wide
    /// <see cref="Npgsql.NpgsqlDataSource"/> the outbox relay also uses (a single connection pool).
    /// </summary>
    public static IServiceCollection AddOrderingDbContextFactory(this IServiceCollection services) =>
        services.AddPooledDbContextFactory<OrderingDbContext>((provider, options) =>
            options.UseServiceDatabase(provider.GetRequiredService<Npgsql.NpgsqlDataSource>()));
}

/// <summary>
/// Used by <c>dotnet ef</c>. Migrations connect straight to PostgreSQL (not PgBouncer):
/// they may rely on session-level features that transaction pooling cannot provide.
/// </summary>
internal sealed class OrderingDesignTimeDbContextFactory : IDesignTimeDbContextFactory<OrderingDbContext>
{
    private const string LocalDirectConnection =
        "Host=localhost;Port=15432;Database=ordering;Username=ordering_svc;Password=ordering_svc_pwd";

    public OrderingDbContext CreateDbContext(string[] args)
    {
        var connectionString = Environment.GetEnvironmentVariable("ORDERING_MIGRATIONS_CONNECTION") ?? LocalDirectConnection;
        var options = new DbContextOptionsBuilder<OrderingDbContext>();
        options.UseOrderingDatabase(connectionString);
        return new OrderingDbContext(options.Options);
    }
}
