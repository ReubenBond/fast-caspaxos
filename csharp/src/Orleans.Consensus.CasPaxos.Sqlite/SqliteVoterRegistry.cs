using System.Globalization;
using Microsoft.Data.Sqlite;

namespace Orleans.Consensus.CasPaxos.Sqlite;

/// <summary>
/// Binds an acceptor database to the fixed voter identity it serves, and records the admissions this voter grants the
/// other voters of its fixed membership.
/// </summary>
/// <remarks>
/// <para>
/// The binding and the admissions lie in tables beside the acceptor state, written by the acceptor's own writer in its
/// immediate transactions, so they share the database's fate and one connection writes the file. Opening a database
/// whose binding names another voter, or which holds acceptor state without a binding, fails: that state is not this
/// voter's. A database without acceptor state receives a new binding with a fresh random incarnation, unadmitted.
/// </para>
/// <para>
/// A voter whose state was lost starts again on a fresh database, which cannot remember the promises the lost state
/// gave. Admission detects it: before its first answer, a fresh voter asks the other voters to admit its incarnation,
/// and each records the first incarnation it admits for a voter and refuses any other. Once a majority of the other
/// voters admitted an incarnation, any later majority of them includes a voter which refuses a new one, so a voter
/// whose state was lost is refused rather than answering with forgotten promises.
/// </para>
/// <para>
/// Every change commits durably (WAL, <c>synchronous=FULL</c>) before its method returns. A failed change fails the
/// acceptor's writer, which reports it to its fatal-error handler and retires, since its outcome may be durable.
/// </para>
/// </remarks>
public sealed class SqliteVoterRegistry
{
    private readonly SqliteAcceptorServer _acceptor;
    private volatile bool _admitted;

    private SqliteVoterRegistry(SqliteAcceptorServer acceptor, string voterId, Guid incarnation, bool admitted)
    {
        _acceptor = acceptor;
        VoterId = voterId;
        Incarnation = incarnation;
        _admitted = admitted;
    }

    /// <summary>The voter identity the database is bound to.</summary>
    public string VoterId { get; }

    /// <summary>The random identity of this database's binding, which a fresh database receives once.</summary>
    public Guid Incarnation { get; }

    /// <summary>Whether a majority of the other voters admitted this binding's incarnation, so the voter may answer.</summary>
    public bool IsAdmitted => _admitted;

    /// <summary>Binds the database which <paramref name="acceptor"/> serves to <paramref name="voterId"/>.</summary>
    /// <exception cref="InvalidDataException">
    /// The database is bound to another voter, or holds acceptor state without a binding.
    /// </exception>
    public static async Task<SqliteVoterRegistry> OpenAsync(SqliteAcceptorServer acceptor, string voterId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(acceptor);
        ArgumentException.ThrowIfNullOrEmpty(voterId);
        var candidate = Guid.NewGuid();
        var binding = await acceptor.ExecuteAsync((connection, transaction) => Bind(connection, transaction, voterId, candidate), cancellationToken).ConfigureAwait(false);
        return binding switch
        {
            { Voter: null } => throw new InvalidDataException(
                $"The acceptor database '{acceptor.DatabasePath}' holds acceptor state without a voter binding, so it cannot be shown to be voter '{voterId}''s."),
            { Voter: var bound } when !string.Equals(bound, voterId, StringComparison.Ordinal) => throw new InvalidDataException(
                $"The acceptor database '{acceptor.DatabasePath}' is bound to voter '{bound}', not '{voterId}'; a database serves the one voter it is bound to."),
            _ => new SqliteVoterRegistry(acceptor, voterId, binding.Incarnation, binding.Admitted),
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
    /// admitted another incarnation of it, durably recording the first incarnation it admits.
    /// </summary>
    /// <returns>The incarnation this voter admits for <paramref name="voterId"/>: the one asked for when admitted.</returns>
    public ValueTask<Guid> AdmitAsync(string voterId, Guid incarnation, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(voterId);
        if (string.Equals(voterId, VoterId, StringComparison.Ordinal))
        {
            throw new ArgumentException("A voter admits only other voters.", nameof(voterId));
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
    private static Binding Bind(SqliteConnection connection, SqliteTransaction transaction, string voterId, Guid candidate)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS voter_binding (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                voter TEXT NOT NULL,
                incarnation BLOB NOT NULL CHECK (length(incarnation) = 16),
                admitted INTEGER NOT NULL CHECK (admitted IN (0, 1))
            ) STRICT;
            CREATE TABLE IF NOT EXISTS voter_admissions (
                voter TEXT PRIMARY KEY,
                incarnation BLOB NOT NULL CHECK (length(incarnation) = 16)
            ) STRICT, WITHOUT ROWID;
            """;
        command.ExecuteNonQuery();
        command.CommandText = "SELECT voter, incarnation, admitted FROM voter_binding WHERE singleton = 1;";
        using (var reader = command.ExecuteReader())
        {
            if (reader.Read())
            {
                return new(reader.GetString(0), new Guid(reader.GetFieldValue<byte[]>(1)), reader.GetInt64(2) == 1);
            }
        }

        command.CommandText = "SELECT EXISTS (SELECT 1 FROM acceptor_state);";
        if (Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture) != 0)
        {
            return new(null, Guid.Empty, false);
        }

        command.CommandText = "INSERT INTO voter_binding VALUES (1, $voter, $incarnation, 0);";
        command.Parameters.AddWithValue("$voter", voterId);
        command.Parameters.AddWithValue("$incarnation", candidate.ToByteArray());
        command.ExecuteNonQuery();
        return new(voterId, candidate, false);
    }

    private sealed record Binding(string? Voter, Guid Incarnation, bool Admitted);
}
