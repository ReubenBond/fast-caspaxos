using System.Collections.Immutable;
using System.Globalization;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Threading.Channels;
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using Microsoft.Data.Sqlite;

namespace FastCASPaxos.Sqlite;

/// <summary>A file-backed keyed acceptor with a bounded, dedicated SQLite writer.</summary>
public sealed class SqliteAcceptorServer : IKeyedAcceptor, ILocalAcceptorInspection, IAcceptorDigestSource, IAsyncDisposable
{
    private const int ApplicationId = 0x46435058;
    private readonly Channel<WorkItem> _queue;
    private readonly TaskCompletionSource _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _stopped = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly string _databasePath;
    private readonly int _maxBatchSize;
    private readonly NodeEventJournal? _diagnostics;
    private readonly List<AcceptorStateObservation> _changes = [];
    private Exception? _failure;
    private int _disposeRequested;

    // Exposes the durability boundary to deterministic fault/cancellation tests.
    internal Action<SqliteConnection, SqliteTransaction, int>? BeforeCommit { get; set; }

    private SqliteAcceptorServer(string databasePath, int queueCapacity, int maxBatchSize, NodeEventJournal? diagnostics)
    {
        _databasePath = databasePath;
        _maxBatchSize = maxBatchSize;
        _diagnostics = diagnostics;
        _queue = Channel.CreateBounded<WorkItem>(new BoundedChannelOptions(queueCapacity)
        {
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait,
            AllowSynchronousContinuations = false
        });
    }

    /// <summary>
    /// Opens a database using WAL and synchronous=FULL. Initialization and all SQLite I/O run
    /// on a dedicated thread. Cancellation applies before initialization starts.
    /// </summary>
    public static async ValueTask<SqliteAcceptorServer> OpenAsync(
        string databasePath, int queueCapacity = 256, int maxBatchSize = 64,
        NodeEventJournal? diagnostics = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(queueCapacity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBatchSize);
        cancellationToken.ThrowIfCancellationRequested();
        var server = new SqliteAcceptorServer(Path.GetFullPath(databasePath), queueCapacity, maxBatchSize, diagnostics);
        new Thread(server.Run) { IsBackground = true, Name = "FastCASPaxos SQLite writer" }.Start();
        try
        {
            await server._ready.Task.ConfigureAwait(false);
            return server;
        }
        catch
        {
            await server._stopped.Task.ConfigureAwait(false);
            throw;
        }
    }

    public ValueTask<AcceptorPrepareResult<ByteString?>> PrepareAsync(
        string key, PrepareRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ValidateBallot(request.Ballot, nameof(request));
        return Enqueue((connection, transaction) =>
        {
            var encodedKey = EncodeKey(key);
            var state = Load(connection, transaction, encodedKey, out var exists);
            var original = state;
            var result = AcceptorEngine.Prepare(ref state, request);
            if (SaveIfChanged(connection, transaction, encodedKey, exists, original, state))
                ObserveChange(key, state);
            return result;
        }, cancellationToken);
    }

    public ValueTask<AcceptorAcceptResult> AcceptAsync(
        string key, AcceptRequest<ByteString> request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (request.Value.IsDefault) throw new ArgumentException("Initialized bytes are required.", nameof(request));
        ValidateBallot(request.Ballot, nameof(request));
        if (!request.NextBallotToPrepare.IsZero)
        {
            ValidateBallot(request.NextBallotToPrepare, nameof(request));
        }

        return Enqueue((connection, transaction) =>
        {
            var encodedKey = EncodeKey(key);
            var state = Load(connection, transaction, encodedKey, out var exists);
            var original = state;
            var result = AcceptorEngine.Accept(ref state,
                new AcceptRequest<ByteString?>(request.Ballot, request.Value, request.NextBallotToPrepare));
            if (SaveIfChanged(connection, transaction, encodedKey, exists, original, state))
                ObserveChange(key, state);
            return result;
        }, cancellationToken);
    }

    public ValueTask<KeyPage> ListKeysAsync(
        string prefix = "", string? afterKey = null, int limit = 100,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        return Enqueue((connection, transaction) =>
        {
            var encodedPrefix = EncodeKey(prefix);
            var prefixEnd = GetPrefixEnd(encodedPrefix);
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                SELECT key, promised_round, promised_proposer, accepted_round, accepted_proposer, value, value_hash
                FROM acceptor_state
                WHERE accepted_round IS NOT NULL
                    AND key >= $prefix
                """ + (prefixEnd is null ? "" : " AND key < $end")
                + (afterKey is null ? "" : " AND key > $after") + " ORDER BY key LIMIT $limit;";
            command.Parameters.AddWithValue("$prefix", encodedPrefix);
            if (prefixEnd is not null)
            {
                command.Parameters.AddWithValue("$end", prefixEnd);
            }

            if (afterKey is not null)
            {
                command.Parameters.AddWithValue("$after", EncodeKey(afterKey));
            }

            command.Parameters.AddWithValue("$limit", (long)limit + 1);
            using var reader = command.ExecuteReader();
            var entries = ImmutableArray.CreateBuilder<KeyedAcceptorState>();
            while (reader.Read())
            {
                if (entries.Count == limit)
                {
                    return new KeyPage(entries.ToImmutable(), entries[^1].Key);
                }

                entries.Add(new KeyedAcceptorState(
                    DecodeKey(reader.GetFieldValue<byte[]>(0)),
                    new Ballot(reader.GetInt32(1), reader.GetInt32(2)),
                    new Ballot(reader.GetInt32(3), reader.GetInt32(4)),
                    ReadAcceptedValue(reader, 5)));
            }

            return new KeyPage(entries.DrainToImmutable(), null);
        }, cancellationToken);
    }

    public ValueTask<AcceptorDigestPage> ListDigestsAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        return Enqueue((connection, transaction) =>
        {
            var encodedPrefix = EncodeKey(prefix);
            var prefixEnd = GetPrefixEnd(encodedPrefix);
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                SELECT key, accepted_round, accepted_proposer, value_hash
                FROM acceptor_state WHERE accepted_round IS NOT NULL AND key >= $prefix
                """ + (prefixEnd is null ? "" : " AND key < $end")
                + (afterKey is null ? "" : " AND key > $after") + " ORDER BY key LIMIT $limit;";
            command.Parameters.AddWithValue("$prefix", encodedPrefix);
            if (prefixEnd is not null) command.Parameters.AddWithValue("$end", prefixEnd);
            if (afterKey is not null) command.Parameters.AddWithValue("$after", EncodeKey(afterKey));
            command.Parameters.AddWithValue("$limit", (long)limit + 1);
            using var reader = command.ExecuteReader();
            var entries = ImmutableArray.CreateBuilder<KeyDigest>();
            while (reader.Read())
            {
                if (entries.Count == limit) return new AcceptorDigestPage(entries.ToImmutable(), entries[^1].Key);
                entries.Add(new(DecodeKey(reader.GetFieldValue<byte[]>(0)),
                    new(new(reader.GetInt32(1), reader.GetInt32(2)), new(reader.GetFieldValue<byte[]>(3)))));
            }
            return new AcceptorDigestPage(entries.DrainToImmutable(), null);
        }, cancellationToken);
    }

    public ValueTask<AcceptorReadResult> ReadAsync(
        string key, AcceptedDigest? known = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        known?.Validate();
        return Enqueue<AcceptorReadResult>((connection, transaction) =>
        {
            var encodedKey = EncodeKey(key);
            AcceptedDigest digest;
            using (var command = connection.CreateCommand())
            {
                command.Transaction = transaction;
                command.CommandText = """
                    SELECT accepted_round, accepted_proposer, value_hash FROM acceptor_state
                    WHERE key = $key AND accepted_round IS NOT NULL;
                    """;
                command.Parameters.AddWithValue("$key", encodedKey);
                using var reader = command.ExecuteReader();
                if (!reader.Read()) return new AcceptorMissing();
                digest = new(new(reader.GetInt32(0), reader.GetInt32(1)), new(reader.GetFieldValue<byte[]>(2)));
                if (digest == known) return new AcceptorUnchanged();
            }
            var state = Load(connection, transaction, encodedKey, out _);
            Debug.Assert(state.AcceptedValue is not null);
            return new KeyedAcceptorState(key, state.PromisedBallot, state.AcceptedBallot, state.AcceptedValue.Value);
        }, cancellationToken);
    }

    public ValueTask<LocalAcceptorState?> InspectAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        return Enqueue((connection, transaction) =>
        {
            var state = Load(connection, transaction, EncodeKey(key), out var exists);
            return exists ? new LocalAcceptorState(key, state.PromisedBallot, state.AcceptedBallot, state.AcceptedValue) : null;
        }, cancellationToken);
    }

    public ValueTask<LocalAcceptorPage> InspectKeysAsync(
        string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(prefix);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(limit);
        return Enqueue((connection, transaction) =>
        {
            var encodedPrefix = EncodeKey(prefix);
            var prefixEnd = GetPrefixEnd(encodedPrefix);
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = """
                SELECT key, promised_round, promised_proposer, accepted_round, accepted_proposer, value, value_hash
                FROM acceptor_state WHERE key >= $prefix
                """ + (prefixEnd is null ? "" : " AND key < $end")
                + (afterKey is null ? "" : " AND key > $after") + " ORDER BY key LIMIT $limit;";
            command.Parameters.AddWithValue("$prefix", encodedPrefix);
            if (prefixEnd is not null) command.Parameters.AddWithValue("$end", prefixEnd);
            if (afterKey is not null) command.Parameters.AddWithValue("$after", EncodeKey(afterKey));
            command.Parameters.AddWithValue("$limit", (long)limit + 1);
            using var reader = command.ExecuteReader();
            var entries = ImmutableArray.CreateBuilder<LocalAcceptorState>();
            while (reader.Read())
            {
                if (entries.Count == limit) return new LocalAcceptorPage(entries.ToImmutable(), entries[^1].Key);
                entries.Add(new(DecodeKey(reader.GetFieldValue<byte[]>(0)),
                    new Ballot(reader.GetInt32(1), reader.GetInt32(2)),
                    reader.IsDBNull(3) ? Ballot.Zero : new Ballot(reader.GetInt32(3), reader.GetInt32(4)),
                    reader.IsDBNull(5) ? null : ReadAcceptedValue(reader, 5)));
            }
            return new LocalAcceptorPage(entries.DrainToImmutable(), null);
        }, cancellationToken);
    }

    private void ObserveChange(string key, AcceptorState<ByteString?> state)
    {
        if (_diagnostics is not null)
            _changes.Add(AcceptorStateObservation.From(new(key, state.PromisedBallot, state.AcceptedBallot, state.AcceptedValue)));
    }

    /// <summary>Clears all accepted values and promises after the owner has fenced and drained protocol traffic.</summary>
    public async ValueTask ClearAsync(CancellationToken cancellationToken = default)
    {
        await Enqueue((connection, transaction) =>
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "DELETE FROM acceptor_state;";
            return command.ExecuteNonQuery();
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Stops admission and drains admitted work. Repeated calls await the same shutdown.
    /// A failed writer propagates its storage exception through disposal as well as its requests.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        Interlocked.Exchange(ref _disposeRequested, 1);
        _queue.Writer.TryComplete();
        return new ValueTask(_stopped.Task);
    }

    private async ValueTask<TResult> Enqueue<TResult>(
        Func<SqliteConnection, SqliteTransaction, TResult> operation, CancellationToken cancellationToken)
    {
        ThrowIfStopped();
        cancellationToken.ThrowIfCancellationRequested();
        var item = new WorkItem<TResult>(operation, cancellationToken);
        try
        {
            await _queue.Writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            ThrowIfStopped();
            throw;
        }

        return await item.Completion.Task.ConfigureAwait(false);
    }

    private void ThrowIfStopped()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposeRequested) != 0, this);
        if (Volatile.Read(ref _failure) is { } failure)
        {
            ExceptionDispatchInfo.Capture(failure).Throw();
        }
    }

    private void Run()
    {
        var batch = new List<WorkItem>();
        try
        {
            using (var connection = new SqliteConnection(new SqliteConnectionStringBuilder
            {
                DataSource = _databasePath,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Private,
                Pooling = false,
                ForeignKeys = true
            }.ToString()))
            {
                connection.Open();
                Initialize(connection);
                _diagnostics?.Publish("storage", new StorageObservation("ready"));
                _ready.SetResult();
                while (_queue.Reader.WaitToReadAsync().AsTask().GetAwaiter().GetResult())
                {
                    while (batch.Count < _maxBatchSize && _queue.Reader.TryRead(out var item))
                    {
                        if (item.TryStart())
                        {
                            batch.Add(item);
                        }
                    }

                    if (batch.Count == 0)
                    {
                        continue;
                    }

                    // Immediate transactions serialize read/modify/write even across instances.
                    using (var transaction = connection.BeginTransaction(deferred: false))
                    {
                        foreach (var item in batch)
                        {
                            item.Execute(connection, transaction);
                        }

                        BeforeCommit?.Invoke(connection, transaction, batch.Count);
                        transaction.Commit();
                    }

                    foreach (var change in _changes) _diagnostics?.Publish("acceptor-state", change);
                    _changes.Clear();
                    foreach (var item in batch)
                    {
                        item.Complete();
                    }

                    batch.Clear();
                }
            }

            _diagnostics?.Publish("storage", new StorageObservation("stopped"));
            _stopped.SetResult();
        }
        catch (Exception exception)
        {
            // A commit error can be ambiguous. Retire the connection and require recovery from disk.
            Volatile.Write(ref _failure, exception);
            _changes.Clear();
            _diagnostics?.Publish("storage", new StorageObservation("failed", exception.GetType().Name));
            _queue.Writer.TryComplete(exception);
            foreach (var item in batch)
            {
                item.Fail(exception);
            }

            while (_queue.Reader.TryRead(out var item))
            {
                item.Fail(exception);
            }

            _ready.TrySetException(exception);
            _stopped.SetException(exception);
        }
    }

    private static void Initialize(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA journal_mode=WAL;";
        if (!string.Equals((string?)command.ExecuteScalar(), "wal", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("The acceptor database requires SQLite WAL journal mode.");
        }

        command.CommandText = "PRAGMA synchronous=FULL;";
        command.ExecuteNonQuery();
        command.CommandText = "PRAGMA synchronous;";
        if (Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture) != 2)
        {
            throw new InvalidOperationException("The acceptor database requires SQLite synchronous=FULL.");
        }

        command.CommandText = "PRAGMA foreign_keys;";
        if (Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture) != 1)
        {
            throw new InvalidOperationException("The acceptor database requires SQLite foreign key enforcement.");
        }

        using var transaction = connection.BeginTransaction(deferred: false);
        command.Transaction = transaction;
        command.CommandText = "PRAGMA application_id;";
        var applicationId = Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        command.CommandText = "PRAGMA user_version;";
        var schemaVersion = Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        if (applicationId == ApplicationId && schemaVersion == 2)
        {
            transaction.Commit();
            return;
        }

        var migrate = applicationId == ApplicationId && schemaVersion == 1;
        command.CommandText = "SELECT count(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%';";
        if (!migrate && (applicationId != 0 || schemaVersion != 0 || Convert.ToInt64(command.ExecuteScalar(), CultureInfo.InvariantCulture) != 0))
        {
            throw new InvalidDataException("The file is not a supported FastCASPaxos acceptor database.");
        }

        if (migrate)
        {
            command.CommandText = "ALTER TABLE acceptor_state RENAME TO acceptor_state_v1;";
            command.ExecuteNonQuery();
        }
        command.CommandText = """
            CREATE TABLE acceptor_state (
                key BLOB PRIMARY KEY CHECK (length(key) % 2 = 0),
                promised_round INTEGER NOT NULL CHECK (promised_round BETWEEN 1 AND 2147483647),
                promised_proposer INTEGER NOT NULL CHECK (promised_proposer BETWEEN 0 AND 2147483647),
                accepted_round INTEGER CHECK (accepted_round BETWEEN 1 AND 2147483647),
                accepted_proposer INTEGER CHECK (accepted_proposer BETWEEN 0 AND 2147483647),
                value BLOB,
                value_hash BLOB CHECK (value_hash IS NULL OR length(value_hash) = 32),
                CHECK ((accepted_round IS NULL AND accepted_proposer IS NULL AND value IS NULL AND value_hash IS NULL)
                    OR (accepted_round IS NOT NULL AND accepted_proposer IS NOT NULL AND value IS NOT NULL AND value_hash IS NOT NULL))
            ) STRICT, WITHOUT ROWID;
            """ + $"PRAGMA application_id={ApplicationId}; PRAGMA user_version=2;";
        command.ExecuteNonQuery();
        if (migrate)
        {
            connection.CreateFunction<byte[]?, byte[]?>("fastcaspaxos_sha256",
                value => value is null ? null : SHA256.HashData(value), isDeterministic: true);
            command.CommandText = """
                INSERT INTO acceptor_state
                SELECT key, promised_round, promised_proposer, accepted_round, accepted_proposer, value,
                    fastcaspaxos_sha256(value) FROM acceptor_state_v1;
                DROP TABLE acceptor_state_v1;
                """;
            command.ExecuteNonQuery();
        }
        transaction.Commit();
    }

    private static AcceptorState<ByteString?> Load(
        SqliteConnection connection, SqliteTransaction transaction, byte[] key, out bool exists)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT promised_round, promised_proposer, accepted_round, accepted_proposer, value, value_hash
            FROM acceptor_state WHERE key = $key;
            """;
        command.Parameters.AddWithValue("$key", key);
        using var reader = command.ExecuteReader();
        exists = reader.Read();
        if (!exists)
        {
            return AcceptorState.CreateInitial<ByteString?>(null);
        }

        return new AcceptorState<ByteString?>
        {
            PromisedBallot = new Ballot(reader.GetInt32(0), reader.GetInt32(1)),
            AcceptedBallot = reader.IsDBNull(2) ? Ballot.Zero : new Ballot(reader.GetInt32(2), reader.GetInt32(3)),
            AcceptedValue = reader.IsDBNull(4) ? null : ReadAcceptedValue(reader, 4)
        };
    }

    private static ByteString ReadAcceptedValue(SqliteDataReader reader, int valueOrdinal)
    {
        var value = reader.GetFieldValue<byte[]>(valueOrdinal);
        var hash = reader.GetFieldValue<byte[]>(valueOrdinal + 1);
        if (!SHA256.HashData(value).AsSpan().SequenceEqual(hash))
            throw new InvalidDataException("Accepted bytes do not match their persisted SHA-256.");
        return new(value);
    }

    private static bool SaveIfChanged(
        SqliteConnection connection, SqliteTransaction transaction, byte[] key, bool exists,
        AcceptorState<ByteString?> original, AcceptorState<ByteString?> state)
    {
        if (exists && original.PromisedBallot == state.PromisedBallot
            && original.AcceptedBallot == state.AcceptedBallot
            && original.AcceptedValue == state.AcceptedValue)
        {
            return false;
        }

        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        if (exists && original.AcceptedBallot == state.AcceptedBallot
            && original.AcceptedValue == state.AcceptedValue)
        {
            command.CommandText = """
                UPDATE acceptor_state SET promised_round=$pr, promised_proposer=$pp WHERE key=$key;
                """;
            command.Parameters.AddWithValue("$key", key);
            command.Parameters.AddWithValue("$pr", state.PromisedBallot.Round);
            command.Parameters.AddWithValue("$pp", state.PromisedBallot.Proposer);
            command.ExecuteNonQuery();
            return true;
        }
        var value = state.AcceptedValue?.ToArray();
        command.CommandText = """
            INSERT INTO acceptor_state (key, promised_round, promised_proposer, accepted_round, accepted_proposer, value, value_hash)
            VALUES ($key, $pr, $pp, $ar, $ap, $value, $hash)
            ON CONFLICT(key) DO UPDATE SET
                promised_round = excluded.promised_round, promised_proposer = excluded.promised_proposer,
                accepted_round = excluded.accepted_round, accepted_proposer = excluded.accepted_proposer,
                value = excluded.value, value_hash = excluded.value_hash;
            """;
        command.Parameters.AddWithValue("$key", key);
        command.Parameters.AddWithValue("$pr", state.PromisedBallot.Round);
        command.Parameters.AddWithValue("$pp", state.PromisedBallot.Proposer);
        command.Parameters.AddWithValue("$ar", state.AcceptedBallot.IsZero ? DBNull.Value : state.AcceptedBallot.Round);
        command.Parameters.AddWithValue("$ap", state.AcceptedBallot.IsZero ? DBNull.Value : state.AcceptedBallot.Proposer);
        command.Parameters.AddWithValue("$value", value is null ? DBNull.Value : value);
        command.Parameters.AddWithValue("$hash", value is null ? DBNull.Value : SHA256.HashData(value));
        command.ExecuteNonQuery();
        return true;
    }

    private static void ValidateBallot(Ballot ballot, string parameterName)
    {
        if (ballot.Round <= 0 || ballot.Proposer < 0)
        {
            throw new ArgumentOutOfRangeException(parameterName, "Ballots require a positive round and nonnegative proposer.");
        }
    }

    // Big-endian UTF-16 code units make SQLite BLOB ordering exactly StringComparer.Ordinal,
    // preserving NUL and unpaired surrogates as well as ordinary Unicode keys.
    private static byte[] EncodeKey(string key)
    {
        var bytes = new byte[checked(key.Length * 2)];
        for (var i = 0; i < key.Length; i++)
        {
            BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(i * 2), key[i]);
        }

        return bytes;
    }

    private static string DecodeKey(byte[] key)
    {
        if (key.Length % 2 != 0)
        {
            throw new InvalidDataException("An acceptor key contains an incomplete UTF-16 code unit.");
        }

        return string.Create(key.Length / 2, key, static (chars, bytes) =>
        {
            for (var i = 0; i < chars.Length; i++)
            {
                chars[i] = (char)BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(i * 2));
            }
        });
    }

    private static byte[]? GetPrefixEnd(byte[] prefix)
    {
        // The exclusive byte successor bounds the index scan to this prefix, even with a cursor.
        for (var i = prefix.Length - 1; i >= 0; i--)
        {
            if (prefix[i] != byte.MaxValue)
            {
                var end = prefix[..(i + 1)];
                end[i]++;
                return end;
            }
        }

        return null;
    }

    private abstract class WorkItem(CancellationToken cancellationToken)
    {
        public bool TryStart()
        {
            if (!cancellationToken.IsCancellationRequested)
            {
                return true;
            }

            Cancel(cancellationToken);
            return false;
        }

        public abstract void Execute(SqliteConnection connection, SqliteTransaction transaction);
        public abstract void Complete();
        public abstract void Fail(Exception exception);
        protected abstract void Cancel(CancellationToken cancellationToken);
    }

    private sealed class WorkItem<TResult>(
        Func<SqliteConnection, SqliteTransaction, TResult> operation, CancellationToken cancellationToken)
        : WorkItem(cancellationToken)
    {
        private TResult _result = default!;

        public TaskCompletionSource<TResult> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public override void Execute(SqliteConnection connection, SqliteTransaction transaction) =>
            _result = operation(connection, transaction);

        public override void Complete() => Completion.SetResult(_result);
        public override void Fail(Exception exception) => Completion.SetException(exception);
        protected override void Cancel(CancellationToken token) => Completion.SetCanceled(token);
    }
}
