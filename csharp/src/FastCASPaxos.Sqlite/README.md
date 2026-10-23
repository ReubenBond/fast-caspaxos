# SQLite keyed acceptors

`SqliteAcceptorServer` implements the transport-independent
`FastCASPaxos.Server.IKeyedAcceptor` protocol API. One SQLite file holds the
promised ballot and optional accepted ballot/value for every key belonging to
one logical acceptor.

```csharp
using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;

await using var acceptor = await SqliteAcceptorServer.OpenAsync("acceptor.db");
var ballot = Ballot.InitialFast();
var result = await acceptor.AcceptAsync(
    "accounts/42",
    new AcceptRequest<ByteString>(ballot, new ByteString([1, 2, 3]), new Ballot(2, 0)));

var page = await acceptor.ListKeysAsync(prefix: "accounts/", limit: 100);
```

## Protocol contract

`PrepareAsync` and `AcceptAsync` use `AcceptorEngine` for every transition.
Prepare returns the accepted ballot/value and either a promise or the conflicting
ballot. Accept returns the current promised ballot on success, including the
piggybacked prepare, or the conflicting ballot on rejection. Exact retries of an
accepted ballot/value return the existing promise. Requests with a new value at
an already accepted ballot are rejected.

`ByteString` copies its input and provides structural equality. `ToArray` returns
a caller-owned copy, so both request and response bytes stay stable for the
lifetime of a protocol operation. A null prepare value and `Ballot.Zero` identify
an unaccepted key; `ByteString.Empty` is an accepted zero-length payload. Payload
encoding, logical versions, conditional writes, and quorum decisions belong to
the register layer.

Ballot rounds are positive `Int32` values; proposer ids are nonnegative `Int32`
values, with zero identifying shared fast rounds. The optional next ballot also
accepts `Ballot.Zero`. All ballot components are persisted exactly. Hosts assign
distinct classic proposer ids and recover above persisted ballot floors before
reusing a proposer identity after restart. Exhausting the round range requires
an explicit operational decision before another round is allocated.

## Durability and lifecycle

Opening validates effective `journal_mode=WAL`, `synchronous=FULL`, and foreign
key enforcement. The database has a versioned application/schema identity;
fresh initialization requires an empty database. Use a local filesystem with
SQLite WAL locking and durable-flush support, and retain the database and its
WAL together through crashes. File access and backups follow SQLite's
[WAL](https://www.sqlite.org/wal.html) and
[backup](https://www.sqlite.org/backup.html) requirements.

A dedicated thread owns each server's connection and performs all synchronous
SQLite work. A bounded queue applies backpressure (default capacity 256).
The writer groups up to 64 ready requests in one immediate transaction, with
each request reading the preceding request's state. This serializes same-key
operations and amortizes durable commits across unrelated keys. SQLite's writer
lock also serializes multiple server instances opening the same file. The file
represents one acceptor identity regardless of the number of open instances.

The completion order is **read durable state, apply the core transition, persist,
commit, release responses**. Promise-only rows survive restart. Every operation,
including local reads and enumeration, enters this boundary. Each request's
temporary state is owned by its transaction. Accepted and promised state is
retained for the lifetime of the acceptor identity.

Cancellation applies during queue admission and before the writer selects a
request. A canceled request at either boundary performs no transition. Once
selected, a request awaits the transaction outcome even if its token is canceled.
Initialization likewise checks cancellation before starting and then awaits
its outcome. Disposal closes admission, drains admitted work, and releases the
connection. Further calls fail with `ObjectDisposedException`; repeated disposal
awaits the same shutdown.

Any storage/transaction failure faults all affected requests and retires the
writer. Pending and future operations report that failure; disposal reports it
as well. Commit errors and process/transport failures can leave an unknown
outcome: reopen the same database and recover through the protocol, using exact
ballot/value retries where appropriate. Rejections remain protocol results;
storage errors remain exceptions.

## Local inventory

`ListKeysAsync(prefix, afterKey, limit)` returns locally accepted records,
including each record's promised ballot, accepted ballot, and immutable bytes.
Promise-only rows remain durable and are excluded from inventory.

Keys are .NET strings with case-sensitive ordinal prefix matching and
`StringComparer.Ordinal` ordering. SQLite stores keys as big-endian UTF-16 code
units in BLOB columns, so SQL ordering, prefix matching, and exclusive cursor
comparison agree for every string, including empty keys, NUL, and unpaired
surrogates. `%` and `_` are ordinary key characters.

The limit is a positive `Int32`. `ContinuationKey` is the final returned key
when further matching records exist; null means that page exhausted its
snapshot. Empty-string cursors are valid and distinct from null. Pass the
continuation unchanged with the same prefix to fetch the next page.

Each page observes one local transaction snapshot. Concurrent writes can affect
later pages; a key first accepted at or below the cursor appears on a subsequent
scan. An accepted record is evidence about this acceptor; cluster-level register
visibility and replicated enumeration are established by the register service.

## Validation

The solution includes `FastCASPaxos.Sqlite.Tests`, which exercises isolated real
SQLite files. The repository C# workflow runs these alongside the core and
simulation suites. A focused local run is:

```powershell
dotnet test csharp\tests\FastCASPaxos.Sqlite.Tests --configuration Release --verbosity minimal
```
