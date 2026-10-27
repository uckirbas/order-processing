using System.Net.Http.Json;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using Ordering.Infrastructure.Persistence;
using Testcontainers.PostgreSql;

namespace Ordering.Api.IntegrationTests;

/// <summary>One PostgreSQL container and one API host per test class; tests isolate themselves with fresh ids.</summary>
public sealed class OrderingApiFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder("postgres:18.6-alpine")
        .WithDatabase("ordering")
        .Build();

    private WebApplicationFactory<Program> _factory = null!;
    private NpgsqlDataSource _dataSource = null!;

    public HttpClient Client { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _postgres.StartAsync();
        var connectionString = _postgres.GetConnectionString();

        var options = new DbContextOptionsBuilder<OrderingDbContext>();
        options.UseOrderingDatabase(connectionString);
        await using (var db = new OrderingDbContext(options.Options))
        {
            await db.Database.MigrateAsync();
        }

        _dataSource = NpgsqlDataSource.Create(connectionString);
        _factory = new WebApplicationFactory<Program>()
            .WithWebHostBuilder(host => host.UseSetting("ConnectionStrings:Ordering", connectionString));
        Client = _factory.CreateClient();
    }

    public Task<HttpResponseMessage> PostOrderAsync(object body, string? idempotencyKey)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, "/orders") { Content = JsonContent.Create(body) };
        if (idempotencyKey is not null)
        {
            request.Headers.Add("Idempotency-Key", idempotencyKey);
        }

        return Client.SendAsync(request, TestContext.Current.CancellationToken);
    }

    public async Task<long> CountOutboxMessagesAsync(Guid orderId)
    {
        await using var command = _dataSource.CreateCommand(
            "SELECT count(*) FROM outbox_messages WHERE type = 'OrderPlaced.v1' AND (payload ->> 'orderId')::uuid = $1");
        command.Parameters.AddWithValue(orderId);
        return (long)(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await _factory.DisposeAsync();
        await _dataSource.DisposeAsync();
        await _postgres.DisposeAsync();
    }
}
