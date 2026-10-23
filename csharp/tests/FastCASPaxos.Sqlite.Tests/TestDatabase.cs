using Microsoft.Data.Sqlite;

namespace FastCASPaxos.Sqlite.Tests;

internal sealed class TestDatabase : IDisposable
{
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
    private readonly string _directory = System.IO.Path.Combine(
        System.IO.Path.GetTempPath(), "FastCASPaxos.Sqlite.Tests", Guid.NewGuid().ToString("N"));

    public TestDatabase() => Directory.CreateDirectory(_directory);

    public string Path => System.IO.Path.Combine(_directory, "acceptor.db");

    public async Task<SqliteAcceptorServer> OpenAsync(int queueCapacity = 256, int maxBatchSize = 64) =>
        await SqliteAcceptorServer.OpenAsync(Path, queueCapacity, maxBatchSize).AsTask().WaitAsync(Timeout);

    public SqliteConnection Connect()
    {
        var connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = Path,
            Pooling = false,
            DefaultTimeout = 5,
            ForeignKeys = true,
        }.ToString());
        connection.Open();
        return connection;
    }

    public static void Execute(SqliteConnection connection, string sql, SqliteTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    // Encoding.BigEndianUnicode replaces isolated surrogates. The persisted contract
    // is UTF16 code units, not Unicode scalar values, so encode each char explicitly.
    public static byte[] KeyBytes(string key)
    {
        var bytes = new byte[key.Length * 2];
        for (var index = 0; index < key.Length; index++)
        {
            bytes[index * 2] = (byte)(key[index] >> 8);
            bytes[index * 2 + 1] = (byte)key[index];
        }

        return bytes;
    }

    public void Dispose() => Directory.Delete(_directory, recursive: true);
}
