using System.Collections.Immutable;
using System.Globalization;
using System.Text.Json;
using Microsoft.Data.Sqlite;

namespace FastCASPaxos.Host;

public sealed record ResetRequest(string OperationId, string ExpectedEpoch)
{
    internal string NextEpoch => checked(ulong.Parse(ExpectedEpoch, CultureInfo.InvariantCulture) + 1).ToString(CultureInfo.InvariantCulture);

    public void Validate()
    {
        if (!Guid.TryParseExact(OperationId, "N", out var id) || id == Guid.Empty
            || id.ToString("N") != OperationId
            || !ulong.TryParse(ExpectedEpoch, NumberStyles.None, CultureInfo.InvariantCulture, out var epoch)
            || epoch == ulong.MaxValue || epoch.ToString(CultureInfo.InvariantCulture) != ExpectedEpoch)
            throw new ArgumentException("Reset requires a canonical nonzero operation GUID and the observed decimal epoch below UInt64.MaxValue.");
    }
}

public sealed record AdminNodeResult(string NodeId, string State, string? Error = null);
public sealed record ClusterAdminResult(string State, ImmutableArray<AdminNodeResult> Nodes, ResetRequest? Reset = null);
public sealed record ResetProgress(ResetRequest Request, string Phase, ImmutableArray<AdminNodeResult> Nodes);
public sealed record NodeAdministration(
    int SchemaVersion, string NodeId, string CoordinatorId, string Epoch, string State,
    ProposerSettings Settings, ResetRequest? PendingReset, ResetProgress? ClusterReset);

internal sealed record NodeAdminState(
    string Epoch, string Phase, ProposerSettings Settings, ResetRequest? PendingReset = null,
    ResetProgress? ClusterReset = null, ResetRequest? LastReset = null)
{
    public const string InitialEpoch = "0";
}

// Shares the retained proposer database, so reset progress and identity reservations survive together.
internal sealed class NodeAdminStateStore : IDisposable
{
    private readonly object _gate = new();
    private readonly SqliteConnection _connection;
    private NodeAdminState _state;

    public NodeAdminStateStore(string database, ProposerSettings initialSettings)
    {
        initialSettings = initialSettings with { Mode = initialSettings.CreatePolicy().Name };
        _connection = new(new SqliteConnectionStringBuilder
        {
            DataSource = database, Mode = SqliteOpenMode.ReadWrite, Pooling = false,
        }.ToString());
        try
        {
            _connection.Open();
            using var command = _connection.CreateCommand();
            command.CommandText = """
                PRAGMA synchronous=FULL;
                CREATE TABLE IF NOT EXISTS host_admin (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    state TEXT NOT NULL
                );
                INSERT OR IGNORE INTO host_admin VALUES (1, $state);
                """;
            command.Parameters.AddWithValue("$state", JsonSerializer.Serialize(
                new NodeAdminState(NodeAdminState.InitialEpoch, "active", initialSettings)));
            command.ExecuteNonQuery();
            command.CommandText = "SELECT state FROM host_admin WHERE singleton = 1;";
            _state = JsonSerializer.Deserialize<NodeAdminState>((string)command.ExecuteScalar()!)
                ?? throw new InvalidDataException("The durable administration state is empty.");
            _state.Settings.CreatePolicy();
            if (!ulong.TryParse(_state.Epoch, NumberStyles.None, CultureInfo.InvariantCulture, out _)
                || _state.Phase is not ("active" or "preparing" or "prepared" or "cleared")
                || (_state.Phase != "active" && _state.PendingReset is null))
                throw new InvalidDataException("The durable administration state is invalid.");
        }
        catch { _connection.Dispose(); throw; }
    }

    public NodeAdminState State => Volatile.Read(ref _state);

    public void Update(Func<NodeAdminState, NodeAdminState> update)
    {
        lock (_gate)
        {
            var next = update(_state);
            using var transaction = _connection.BeginTransaction(deferred: false);
            using var command = _connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "UPDATE host_admin SET state = $state WHERE singleton = 1;";
            command.Parameters.AddWithValue("$state", JsonSerializer.Serialize(next));
            command.ExecuteNonQuery();
            transaction.Commit();
            Volatile.Write(ref _state, next);
        }
    }

    public void Dispose() => _connection.Dispose();
}
