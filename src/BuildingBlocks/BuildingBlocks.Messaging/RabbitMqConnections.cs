using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace BuildingBlocks.Messaging;

/// <summary>
/// One long-lived connection for publishing and a separate one for consuming. When the broker applies flow control
/// to a publishing connection, acks travelling on the consuming connection are not blocked behind it.
/// Connections are thread-safe and shared by the whole process; channels are not, so every component owns its own.
/// Both connections recover automatically (network and topology) after a broker restart.
/// </summary>
public sealed class RabbitMqConnections : IAsyncDisposable
{
    private readonly LazyConnection _publish;
    private readonly LazyConnection _consume;

    public RabbitMqConnections(IOptions<RabbitMqOptions> options)
    {
        var settings = options.Value;
        var factory = new ConnectionFactory
        {
            HostName = settings.Host,
            Port = settings.Port,
            UserName = settings.UserName,
            Password = settings.Password,
            VirtualHost = settings.VirtualHost,
            AutomaticRecoveryEnabled = true,
            TopologyRecoveryEnabled = true,
            NetworkRecoveryInterval = TimeSpan.FromSeconds(5),
            RequestedHeartbeat = TimeSpan.FromSeconds(30),
        };

        var clientName = $"{settings.ClientName}@{Environment.MachineName}";
        _publish = new LazyConnection(factory, $"{clientName}:publish");
        _consume = new LazyConnection(factory, $"{clientName}:consume");
    }

    public ValueTask<IConnection> GetPublishConnectionAsync(CancellationToken cancellationToken) =>
        _publish.GetAsync(cancellationToken);

    public ValueTask<IConnection> GetConsumeConnectionAsync(CancellationToken cancellationToken) =>
        _consume.GetAsync(cancellationToken);

    public async ValueTask DisposeAsync()
    {
        await _publish.DisposeAsync();
        await _consume.DisposeAsync();
    }

    /// <summary>Created on first use (the broker may not be up when the process starts), exactly once.</summary>
    private sealed class LazyConnection(ConnectionFactory factory, string clientName) : IAsyncDisposable
    {
        private readonly SemaphoreSlim _gate = new(1, 1);
        private IConnection? _connection;

        public async ValueTask<IConnection> GetAsync(CancellationToken cancellationToken)
        {
            // Lock-free fast path. Volatile: the reference is published by another thread, and ARM64 (Apple Silicon,
            // Graviton) does not order plain reads and writes the way x64 does.
            if (Volatile.Read(ref _connection) is { } existing)
            {
                return existing;
            }

            await _gate.WaitAsync(cancellationToken);
            try
            {
                if (_connection is null)
                {
                    var connection = await factory.CreateConnectionAsync(clientName, cancellationToken);
                    Volatile.Write(ref _connection, connection);
                }

                return _connection!;
            }
            finally
            {
                _gate.Release();
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_connection is { } connection)
            {
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }

            _gate.Dispose();
        }
    }
}
