using System.Diagnostics;
using System.Globalization;
using Orleans.Consensus.CasPaxos.Model;
using Orleans.Consensus.CasPaxos.Server;
using Microsoft.Data.Sqlite;

namespace Orleans.Consensus.CasPaxos.Sqlite;

/// <summary>
/// Durably allocates classic ballot rounds and register-service incarnations. Configure a distinct
/// positive proposer id per allocator database and retain that database for the identity's lifetime.
/// Concurrent instances sharing this file serialize reservations with immediate transactions.
/// Construction flushes the database's directory so the database and WAL names are durable; a failed
/// flush is reported to the fatal-error handler and fails construction.
/// </summary>
public sealed class SqliteProposerBallotSource : IProposerBallotSource, IDisposable
{
    private readonly Lock _gate = new();
    private readonly SqliteConnection _connection;
    private readonly IFatalErrorHandler _fatalErrors;
    private readonly string _path;
    private Exception? _failure;
    private bool _disposed;
    public int ProposerId { get; }

    public SqliteProposerBallotSource(string databasePath, int proposerId, IFatalErrorHandler fatalErrors)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(proposerId);
        ArgumentNullException.ThrowIfNull(fatalErrors);
        ProposerId = proposerId;
        _fatalErrors = fatalErrors;
        var fullPath = Path.GetFullPath(databasePath);
        _path = fullPath;
        _connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = fullPath,
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

            // This connection holds the WAL until disposal, so no file is created after the flush.
            Debug.Assert(File.Exists(fullPath + "-wal"), "Initialization opens the WAL.");
            DurableDirectory.FlushFiles(Path.GetDirectoryName(fullPath)!, fatalErrors);
        }
        catch
        {
            _connection.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Reserves a round of at least <paramref name="minimumRound"/>, returning it once its reservation commits. A failed
    /// reservation is reported to the fatal-error handler and retires the allocator: its outcome may be durable, and
    /// only a fresh process recovering from the database allocates again.
    /// </summary>
    public Ballot Reserve(int minimumRound)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minimumRound);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_failure is not null) throw new InvalidOperationException("The ballot allocator failed and allocates no more rounds.", _failure);
            try
            {
                using var transaction = _connection.BeginTransaction(deferred: false);
                using var command = _connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = "SELECT reserved FROM proposer_round WHERE singleton = 1;";
                var previous = Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
                var reserved = Math.Max(checked(previous + 1), minimumRound);
                command.CommandText = "UPDATE proposer_round SET reserved = $round WHERE singleton = 1;";
                command.Parameters.AddWithValue("$round", reserved);
                command.ExecuteNonQuery();
                transaction.Commit();
                return new Ballot(reserved, ProposerId);
            }
            catch (Exception exception) when (exception is SqliteException or IOException or InvalidCastException or FormatException)
            {
                _failure = exception;
                _fatalErrors.OnLocalStorageFailure("ballot reservation", _path, exception);
                throw;
            }
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
