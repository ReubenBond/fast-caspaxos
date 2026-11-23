using System.Globalization;
using Microsoft.Data.Sqlite;
using Orleans.Consensus.CasPaxos.Server;

namespace Orleans.Consensus.CasPaxos.Sqlite;

/// <summary>
/// Binds an acceptor database to the voter identity it serves, persists its join floor and optional
/// bootstrap configuration digest, and records the admissions this voter grants other voters.
/// </summary>
public sealed class SqliteVoterRegistry
{
    private readonly SqliteAcceptorServer _acceptor;
    private volatile bool _admitted;

    private SqliteVoterRegistry(
        SqliteAcceptorServer acceptor,
        string voterId,
        Guid incarnation,
        bool admitted,
        long joinConfiguration,
        ByteString? bootstrapDigest)
    {
        _acceptor = acceptor;
        VoterId = voterId;
        Incarnation = incarnation;
        _admitted = admitted;
        JoinConfiguration = joinConfiguration;
        BootstrapDigest = bootstrapDigest;
    }

    /// <summary>The voter identity the database is bound to.</summary>
    public string VoterId { get; }

    /// <summary>The random identity of this database's binding, which a fresh database receives once.</summary>
    public Guid Incarnation { get; }

    /// <summary>Whether a majority of the other voters admitted this binding's incarnation, so the voter may answer.</summary>
    public bool IsAdmitted => _admitted;

    /// <summary>The 1-based configuration number in which this voter was first introduced.</summary>
    public long JoinConfiguration { get; }

    /// <summary>Optional SHA-256 digest of the canonical bootstrap configuration <c>V(1)</c>.</summary>
    public ByteString? BootstrapDigest { get; }

    /// <summary>
    /// Binds the database which <paramref name="acceptor"/> serves to <paramref name="voterId"/>.
    /// For a fresh voter joining in <paramref name="joinConfiguration"/> &gt; 1, atomically initializes
    /// <c>acceptor_floor</c> to <c>(joinConfiguration, 0, 0)</c> in the same transaction.
    /// </summary>
    /// <exception cref="InvalidDataException">
    /// The database is bound to another voter, holds acceptor state without a binding, or disagrees on
    /// <paramref name="bootstrapDigest"/>.
    /// </exception>
    public static async Task<SqliteVoterRegistry> OpenAsync(
        SqliteAcceptorServer acceptor,
        string voterId,
        long joinConfiguration = 1,
        ByteString? bootstrapDigest = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(acceptor);
        ArgumentException.ThrowIfNullOrEmpty(voterId);
        ArgumentOutOfRangeException.ThrowIfLessThan(joinConfiguration, 1);
        if (bootstrapDigest is { } digest && (digest.IsDefault || digest.Length != 32))
        {
            throw new ArgumentException("A bootstrap configuration digest must be a 32-byte SHA-256.", nameof(bootstrapDigest));
        }

        var candidate = Guid.NewGuid();
        var binding = await acceptor.ExecuteAsync(
            (connection, transaction) => Bind(connection, transaction, voterId, candidate, joinConfiguration, bootstrapDigest),
            cancellationToken).ConfigureAwait(false);
        return binding switch
        {
            { Voter: null } => throw new InvalidDataException(
                $"The acceptor database '{acceptor.DatabasePath}' holds acceptor state without a voter binding, so it cannot be shown to be voter '{voterId}''s."),
            { Voter: var bound } when !string.Equals(bound, voterId, StringComparison.Ordinal) => throw new InvalidDataException(
                $"The acceptor database '{acceptor.DatabasePath}' is bound to voter '{bound}', not '{voterId}'; a database serves the one voter it is bound to."),
            { BootstrapDigest: { } stored } when bootstrapDigest is { } expected && stored != expected => throw new InvalidDataException(
                $"The acceptor database '{acceptor.DatabasePath}' recorded a different bootstrap configuration digest than supplied."),
            _ => new SqliteVoterRegistry(acceptor, voterId, binding.Incarnation, binding.Admitted, binding.JoinConfiguration, binding.BootstrapDigest),
        };
    }

    /// <summary>Durably records that a majority of the other voters admitted this binding's incarnation.</summary>
    public async ValueTask MarkAdmittedAsync(CancellationToken cancellationToken = default)
    {
        if (_admitted) return;
        _ = await _acceptor.ExecuteAsync(static (connection, transaction) =>
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "UPDATE voter_binding SET admitted = 1 WHERE singleton = 1;";
            return command.ExecuteNonQuery();
        }, cancellationToken).ConfigureAwait(false);
        _admitted = true;
    }

    /// <summary>
    /// Admits <paramref name="incarnation"/> of the other voter <paramref name="voterId"/> unless this voter already
    /// admitted another incarnation of it, durably recording the first incarnation it admits. When both sides supply a
    /// bootstrap configuration digest, they must agree.
    /// </summary>
    /// <returns>The incarnation this voter admits for <paramref name="voterId"/>: the one asked for when admitted.</returns>
    public ValueTask<Guid> AdmitAsync(
        string voterId,
        Guid incarnation,
        ByteString? bootstrapDigest = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(voterId);
        if (string.Equals(voterId, VoterId, StringComparison.Ordinal))
        {
            throw new ArgumentException("A voter admits only other voters.", nameof(voterId));
        }

        if (bootstrapDigest is { } remote)
        {
            if (remote.IsDefault || remote.Length != 32)
            {
                throw new ArgumentException("A bootstrap configuration digest must be a 32-byte SHA-256.", nameof(bootstrapDigest));
            }

            if (BootstrapDigest is { } local && local != remote)
            {
                throw new InvalidDataException(
                    $"Bootstrap configuration digest mismatch during admission of voter '{voterId}'.");
            }
        }

        return _acceptor.ExecuteAsync((connection, transaction) =>
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                INSERT INTO voter_admissions VALUES ($voter, $incarnation) ON CONFLICT (voter) DO NOTHING;
                SELECT incarnation FROM voter_admissions WHERE voter = $voter;
                """;
            command.Parameters.AddWithValue("$voter", voterId);
            command.Parameters.AddWithValue("$incarnation", incarnation.ToByteArray());
            return new Guid((byte[])command.ExecuteScalar()!);
        }, cancellationToken);
    }

    /// <summary>
    /// Creates the registry's tables when absent and reads the binding, creating one for a database without acceptor state.
    /// A database holding acceptor state without a binding answers a binding without a voter: the operation answers as a
    /// value, since an exception would fail the writer's transaction.
    /// </summary>
    private static Binding Bind(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string voterId,
        Guid candidate,
        long joinConfiguration,
        ByteString? bootstrapDigest)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS voter_binding (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                voter TEXT NOT NULL,
                incarnation BLOB NOT NULL CHECK (length(incarnation) = 16),
                admitted INTEGER NOT NULL CHECK (admitted IN (0, 1)),
                join_configuration INTEGER NOT NULL DEFAULT 1 CHECK (join_configuration >= 1),
                bootstrap_digest BLOB CHECK (bootstrap_digest IS NULL OR length(bootstrap_digest) = 32)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS voter_admissions (
                voter TEXT PRIMARY KEY,
                incarnation BLOB NOT NULL CHECK (length(incarnation) = 16)
            ) STRICT, WITHOUT ROWID;
            """;
        command.ExecuteNonQuery();

        EnsureColumn(connection, transaction, "voter_binding", "join_configuration",
            "ALTER TABLE voter_binding ADD COLUMN join_configuration INTEGER NOT NULL DEFAULT 1 CHECK (join_configuration >= 1);");
        EnsureColumn(connection, transaction, "voter_binding", "bootstrap_digest",
            "ALTER TABLE voter_binding ADD COLUMN bootstrap_digest BLOB CHECK (bootstrap_digest IS NULL OR length(bootstrap_digest) = 32);");

        command.CommandText = "SELECT voter, incarnation, admitted, join_configuration, bootstrap_digest FROM voter_binding WHERE singleton = 1;";
        using (var reader = command.ExecuteReader())
        {
            if (reader.Read())
            {
                var storedDigest = reader.IsDBNull(4) ? (ByteString?)null : new ByteString(reader.GetFieldValue<byte[]>(4));
                return new(
                    reader.GetString(0),
                    new Guid(reader.GetFieldValue<byte[]>(1)),
                    reader.GetInt64(2) == 1,
                    reader.GetInt64(3),
                    storedDigest);
            }
        }

        command.CommandText = "SELECT EXISTS (SELECT 1 FROM acceptor_state);";
        if (Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture) != 0)
        {
            return new(null, Guid.Empty, false, 1, null);
        }

        // Atomically persist the new voter's binding and initialize acceptor_floor to (c_join, 0, 0).
        if (joinConfiguration > 1)
        {
            SqliteAcceptorServer.RaiseFloor(connection, transaction, joinConfiguration);
        }

        command.CommandText = """
            INSERT INTO voter_binding (singleton, voter, incarnation, admitted, join_configuration, bootstrap_digest)
            VALUES (1, $voter, $incarnation, 0, $joinConfig, $digest);
            """;
        command.Parameters.AddWithValue("$voter", voterId);
        command.Parameters.AddWithValue("$incarnation", candidate.ToByteArray());
        command.Parameters.AddWithValue("$joinConfig", joinConfiguration);
        command.Parameters.AddWithValue("$digest", bootstrapDigest is { } d ? d.ToArray() : DBNull.Value);
        command.ExecuteNonQuery();
        return new(voterId, candidate, false, joinConfiguration, bootstrapDigest);
    }

    private static void EnsureColumn(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string table,
        string column,
        string alterSql)
    {
        using var check = connection.CreateCommand();
        check.Transaction = transaction;
        check.CommandText = $"SELECT count(*) FROM pragma_table_info('{table}') WHERE name = $name;";
        check.Parameters.AddWithValue("$name", column);
        if (Convert.ToInt64(check.ExecuteScalar(), CultureInfo.InvariantCulture) == 0)
        {
            using var alter = connection.CreateCommand();
            alter.Transaction = transaction;
            alter.CommandText = alterSql;
            alter.ExecuteNonQuery();
        }
    }

    private sealed record Binding(
        string? Voter,
        Guid Incarnation,
        bool Admitted,
        long JoinConfiguration,
        ByteString? BootstrapDigest);
}
