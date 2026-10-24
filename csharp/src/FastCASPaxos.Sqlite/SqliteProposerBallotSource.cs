using System.Globalization;
using FastCASPaxos.Model;
using FastCASPaxos.Server;
using Microsoft.Data.Sqlite;

namespace FastCASPaxos.Sqlite;

/// <summary>
/// Durably allocates classic ballot rounds and logical write identities. Configure a distinct
/// positive proposer id per allocator database and retain that database for the identity's lifetime.
/// Concurrent instances sharing this file serialize reservations with immediate transactions.
/// </summary>
public sealed class SqliteProposerBallotSource : IProposerBallotSource, IDisposable
{
    private readonly object _gate = new();
    private readonly SqliteConnection _connection;
    private bool _disposed;
    public int ProposerId { get; }

    public SqliteProposerBallotSource(string databasePath, int proposerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(proposerId);
        ProposerId = proposerId;
        _connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = Path.GetFullPath(databasePath),
            Mode = SqliteOpenMode.ReadWriteCreate,
            Pooling = false
        }.ToString());
        try
        {
            _connection.Open();
            using var command = _connection.CreateCommand();
            command.CommandText = """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS proposer_round (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    proposer INTEGER NOT NULL CHECK(proposer > 0),
                    reserved INTEGER NOT NULL CHECK(reserved >= 0)
                );
                INSERT OR IGNORE INTO proposer_round VALUES (1, $proposer, 0);
                SELECT proposer FROM proposer_round WHERE singleton = 1;
                """;
            command.Parameters.AddWithValue("$proposer", proposerId);
            // journal_mode also returns a row, so read the identity in its own command.
            command.ExecuteNonQuery();
            command.CommandText = "SELECT proposer FROM proposer_round WHERE singleton = 1;";
            if (Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture) != proposerId)
            {
                throw new InvalidOperationException("This allocator database belongs to a different proposer identity.");
            }
        }
        catch
        {
            _connection.Dispose();
            throw;
        }
    }

    public Ballot Reserve(int minimumRound)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minimumRound);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            using var transaction = _connection.BeginTransaction(deferred: false);
            using var command = _connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "SELECT reserved FROM proposer_round WHERE singleton = 1;";
            var previous = Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
            var round = Math.Max(checked(previous + 1), minimumRound);
            command.CommandText = "UPDATE proposer_round SET reserved = $round WHERE singleton = 1;";
            command.Parameters.AddWithValue("$round", round);
            command.ExecuteNonQuery();
            transaction.Commit();
            return new Ballot(round, ProposerId);
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed) return;
            _disposed = true;
            _connection.Dispose();
        }
    }
}
