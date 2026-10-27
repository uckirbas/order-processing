using Microsoft.EntityFrameworkCore;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure;

namespace BuildingBlocks.Persistence;

public static class ServiceDatabase
{
    /// <summary>
    /// Provider settings shared by every service's DbContext, used both at runtime and by <c>dotnet ef</c>
    /// so they build the same model. Deliberately no EF retry strategy: retries wrap a whole unit of work
    /// (the API's client, a message handler), never a single statement.
    /// </summary>
    public static DbContextOptionsBuilder UseServiceDatabase(this DbContextOptionsBuilder options, string connectionString) =>
        options.UseNpgsql(connectionString, Configure).UseSnakeCaseNamingConvention();

    /// <summary>
    /// Same settings on an existing data source: a worker whose outbox relay already owns an
    /// <see cref="NpgsqlDataSource"/> then has one connection pool per process instead of two.
    /// </summary>
    public static DbContextOptionsBuilder UseServiceDatabase(this DbContextOptionsBuilder options, NpgsqlDataSource dataSource) =>
        options.UseNpgsql(dataSource, Configure).UseSnakeCaseNamingConvention();

    private static void Configure(NpgsqlDbContextOptionsBuilder npgsql) =>
        npgsql.MigrationsHistoryTable("__ef_migrations_history");
}
