using Npgsql;

namespace BuildingBlocks.Outbox;

/// <summary>
/// Claims and deletes outbox rows with plain, parameterised Npgsql commands: the relay loop needs no DbContext.
/// Thread-safe: <see cref="NpgsqlDataSource"/> is, and every call uses its own pooled connection.
/// </summary>
public sealed class OutboxStore(NpgsqlDataSource dataSource)
{
    // MATERIALIZED guarantees the claiming sub-select runs exactly once. Written inline as
    // "WHERE id IN (SELECT … LIMIT … FOR UPDATE SKIP LOCKED)", PostgreSQL may re-evaluate it under
    // concurrent updates and claim more rows than the limit.
    private const string ClaimSql = """
        WITH claimable AS MATERIALIZED (
            SELECT id FROM outbox_messages
            WHERE locked_until IS NULL OR locked_until < now()
            ORDER BY id
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE outbox_messages o
        SET    locked_until = now() + $2, attempts = o.attempts + 1
        FROM   claimable c
        WHERE  o.id = c.id
        RETURNING o.id, o.type, o.routing_key, o.payload, o.attempts
        """;

    private const string DeleteSql = "DELETE FROM outbox_messages WHERE id = ANY($1)";

    private const string ReleaseSql = "UPDATE outbox_messages SET locked_until = NULL WHERE id = ANY($1)";

    /// <summary>
    /// A single auto-committed statement: the row locks last milliseconds, while the lease (<c>locked_until</c>,
    /// compared against the database clock) keeps other relays away while this one publishes outside any transaction.
    /// </summary>
    public async Task<IReadOnlyList<PendingOutboxMessage>> ClaimAsync(
        int batchSize, TimeSpan lease, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(ClaimSql);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = batchSize });
        command.Parameters.Add(new NpgsqlParameter<TimeSpan> { TypedValue = lease });

        var claimed = new List<PendingOutboxMessage>(batchSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            claimed.Add(new PendingOutboxMessage(
                reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetString(3), reader.GetInt32(4)));
        }

        return claimed;
    }

    /// <summary>No lease check needed: a broker-confirmed message is delivered, whichever relay deletes its row.</summary>
    public async Task DeleteAsync(IReadOnlyCollection<Guid> ids, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(DeleteSql);
        command.Parameters.Add(new NpgsqlParameter<Guid[]> { TypedValue = [.. ids] });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Ends the lease early so the rows are retried as soon as the broker is back, instead of after the lease expires.
    /// Best effort: if this fails too, the lease simply runs out.
    /// </summary>
    public async Task ReleaseAsync(IReadOnlyCollection<Guid> ids, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(ReleaseSql);
        command.Parameters.Add(new NpgsqlParameter<Guid[]> { TypedValue = [.. ids] });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}

public sealed record PendingOutboxMessage(Guid Id, string Type, string RoutingKey, string Payload, int Attempts);
