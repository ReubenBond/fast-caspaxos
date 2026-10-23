# gRPC transport and server

`FastCASPaxos.Grpc` exposes both the keyed acceptor protocol and the replicated
register API over HTTP/2. `FastCASPaxos.Host` is an ASP.NET Core executable which
owns a SQLite acceptor, a retained proposer allocator, and a register service
over a configured fixed peer set. The transport library targets .NET 8 and
.NET 10; the executable and smoke client target .NET 10.

## Build and run

From the repository root, using the .NET 10 SDK:

```powershell
dotnet build csharp\FastCASPaxos.slnx -c Release
dotnet run --project csharp\src\FastCASPaxos.Host -c Release --no-build
```

The default node listens on `http://127.0.0.1:5001`, serves both gRPC APIs,
and uses a single-member cluster. Its durable files live in the host project's
`data` directory. The same host exposes the
[HTTP/REST register API](../FastCASPaxos.Rest/README.md) on
`http://127.0.0.1:5000` over HTTP/1.1. `/health/live` returns HTTP 200 on either listener once the host is
serving requests. This is process liveness; a register operation establishes
quorum availability.

The same HTTP listener provides [per-node administrative diagnostics](../FastCASPaxos.Host/README.md):
explicit peer discovery, read-only local state, and directly subscribed SSE
streams for protocol traffic, durable acceptor changes, and register outcomes.

### Three-process example

[`just cluster`](../../../README.md#run-a-local-three-node-cluster) builds the
host and console once and runs all three members in one terminal with retained
data. For separate terminals, use the commands below.

Build once as above. In three terminals, run these commands respectively,
using an absolute durable directory appropriate for your machine:

```powershell
pwsh -File csharp\samples\Run-Node.ps1 -Node a -DataDirectory C:\fastcaspaxos-data
pwsh -File csharp\samples\Run-Node.ps1 -Node b -DataDirectory C:\fastcaspaxos-data
pwsh -File csharp\samples\Run-Node.ps1 -Node c -DataDirectory C:\fastcaspaxos-data
```

The script configures the same three identities on gRPC ports 5001, 5002, and 5003,
distinct acceptor files, and proposer identities 1, 2, and 3 with distinct
allocator files. REST uses ports 5004, 5005, and 5006. `-BasePort` changes all
six ports together: gRPC uses offsets 0-2 and REST uses offsets 3-5. On Linux, pass
an absolute path such as `/var/lib/fastcaspaxos` for `-DataDirectory`.

Run a fresh-key smoke operation through all three endpoints:

```powershell
$key = "smoke/$([Guid]::NewGuid().ToString('N'))"
dotnet run --project csharp\samples\FastCASPaxos.Grpc.Smoke -c Release --no-build -- write $key http://127.0.0.1:5001 http://127.0.0.1:5002 http://127.0.0.1:5003
```

The client creates an empty value, verifies a stale condition mismatches,
writes identical bytes with a fresh version, reads through every endpoint,
and verifies listing. Stop node `c` with Ctrl+C, then repeat `write` with a
fresh key and only the first two endpoints. The remaining majority continues
in classic mode. Restart nodes with their original identities and files;
`verify $key <endpoints...>` checks the retained empty value and key listing.

For an automated three-process run with temporary databases, including an
abrupt one-node outage and a full restart, run:

```powershell
pwsh -File csharp\scripts\Test-GrpcCluster.ps1
```

This script starts the built executables, checks HTTP/2 liveness, exercises
healthy and degraded register operations over HTTP/1.1 REST and HTTP/2 gRPC,
restarts all processes using the same files, and verifies retained values and
REST ETags. It owns and removes its temporary
database directory and stops only its own processes.

## Configuration

The executable uses standard ASP.NET Core JSON, environment-variable, and
command-line configuration. `appsettings.json` provides loopback-safe defaults.
For example, `FastCASPaxos__ProposerId=2` is the environment-variable form of
`--FastCASPaxos:ProposerId=2`. Configuration binds strictly: unknown
`FastCASPaxos` properties, invalid types, durations, membership, and identity
settings fail startup with diagnostics.

| Setting under `FastCASPaxos` | Contract / default |
| --- | --- |
| `AcceptorId` | Local member identity, appearing exactly once in `Peers`; default `a`. |
| `AcceptorDatabase` | Durable local acceptor file; default `data/acceptor-a.db`. |
| `ProposerId` | Cluster-unique positive proposer identity; default `1`. |
| `ProposerDatabase` | Dedicated retained allocator file; default `data/proposer-1.db`. |
| `Peers` | Array of `{ "Id": "a", "Endpoint": "https://host:grpc-port", "HttpEndpoint": "https://host:admin-port" }`; unique ordinal ids and unique origins per endpoint family. The advertised HTTP origin is browser-reachable. |
| `Mode` | `classic` (default), `fast`, `leader`, `fast+leader`, or `adaptive`. |
| `QueueCapacity`, `MaxBatchSize` | SQLite admission capacity `256` and transaction batch maximum `64`. |
| `Capacity` | Register active-plus-queued admission bound, `256`. |
| `OperationTimeout` | Register operation deadline including queueing, `00:00:10`. |
| `AttemptTimeout` | Register phase attempt deadline, `00:00:01`. |
| `RetryBackoff` | Base retry delay, `00:00:00.010`. |
| `PeerCallTimeout` | Outgoing protocol RPC deadline, `00:00:05`; attempts also pass cancellation. |
| `TargetReplicationFactor` | `0` selects a majority (F+1 for 2F+1 members); positive values select an explicit target up to membership size. |
| `RepairInterval`, `RepairRetryInterval` | Periodic scan interval `00:01:00` and pending shutdown-pass retry delay `00:00:01`. |
| `RepairPageSize` | Digest entries per member per inventory range, `128`. |
| `MaxReceiveMessageBytes`, `MaxSendMessageBytes` | Serialized gRPC message limits, each `4194304`, for servers and peer channels. Allow for protobuf and register-envelope overhead. |
| `MaxConcurrentConnections`, `MaxStreamsPerConnection` | Kestrel bounds, `256` and `128`. |
| `MaxRequestBodyBytes` | Per-request Kestrel body bound on REST PUT, `4194304`; gRPC retains its independent message bounds. Configure peer message limits for envelope/framing overhead. |
| `TrustedAdminOrigins` | Additional exact workbench origins for admin/REST CORS, default `[]`; advertised host origins are included. |
| `EventHistoryCapacity`, `MaxEventStreams`, `HeartbeatInterval` | Retained event count `1024`, concurrent SSE stream limit `64`, quiet-stream heartbeat `00:00:15`. |

Durations are positive and at most one day. File paths resolve relative to the
ASP.NET content root; parent directories are created on startup. Acceptor and
allocator files have distinct paths. All nodes must configure the same full
membership. Every peer id denotes one distinct durable acceptor: aliases,
proxies, DNS records, and multiple processes opening the same database retain
that single identity. Configuration validates duplicate origins; operators
maintain this physical-identity invariant across DNS and deployment changes.
The local peer calls the host's own durable acceptor directly.

Opening the host initializes local storage and creates remote channels.
Peers can join or restart in any order; a disconnected peer retains its quorum
slot. Fixed membership and unique proposer identities are deployment
invariants. Restart each node with its original files and configuration.

Configure listeners through `Kestrel:Endpoints`, including `Url`, `Protocols`,
and `Certificate`. The default plaintext gRPC endpoint uses `Http2`, and the
separate REST listener uses `Http1`. A TLS `Http1AndHttp2` listener negotiates
either protocol through ALPN; plaintext h2c uses the dedicated `Http2`
listener. Use standard Kestrel certificate configuration and
secret providers for certificate credentials. The host uses the platform's
normal HTTPS certificate validation for outgoing peer connections.

Both APIs grant trusted callers access to consensus state. Deploy them within
a trusted network boundary with TLS and firewall/proxy access controls. An
embedding application can apply standard ASP.NET authentication/authorization
to `MapGrpcService` endpoints. Public exposure requires those controls;
protocol access permits arbitrary ballot and opaque-value operations.

## C# clients

Reference `FastCASPaxos.Grpc` from the application. Applications own and dispose
their channels; adapters own individual calls. A call timeout defaults to
30 seconds and can be supplied to either adapter. Both constructors also accept
an optional `TimeProvider` (default `TimeProvider.System`), which supplies the
UTC instant used to calculate each call's absolute gRPC deadline. Deterministic
tests can supply a controlled provider and call invoker; the gRPC channel owns
transport deadline enforcement.

```csharp
using FastCASPaxos.Grpc;
using FastCASPaxos.Server;
using Grpc.Net.Client;

using var channel = GrpcChannel.ForAddress("http://127.0.0.1:5001");
IRegisterService registers = new GrpcRegisterService(
    channel.CreateCallInvoker(), callTimeout: TimeSpan.FromSeconds(15));

var created = await registers.CompareExchangeAsync("catalog/item", null, ByteString.Empty);
var current = await registers.ReadAsync("catalog/item");
var page = await registers.ListKeysAsync("catalog/", limit: 100);
```

An application can instead own its proposer and use protocol adapters:

```csharp
using FastCASPaxos.Grpc;
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;
using Grpc.Net.Client;

using var a = GrpcChannel.ForAddress("http://127.0.0.1:5001");
using var b = GrpcChannel.ForAddress("http://127.0.0.1:5002");
using var c = GrpcChannel.ForAddress("http://127.0.0.1:5003");
using var ballots = new SqliteProposerBallotSource("application-proposer.db", proposerId: 4);
await using var registers = new ReplicatedRegisterService(
    [
        new("a", new GrpcKeyedAcceptor(a.CreateCallInvoker())),
        new("b", new GrpcKeyedAcceptor(b.CreateCallInvoker())),
        new("c", new GrpcKeyedAcceptor(c.CreateCallInvoker())),
    ],
    ballots);
```

Reference `FastCASPaxos.Sqlite` for the second example. Reserve proposer `4`
uniquely and retain its allocator. Raw protocol callers sharing register
keys preserve the register layer's `FCR1` envelopes and conditional-update
semantics. Keep application-owned opaque protocol values in separate namespaces.

## Wire contract

[`Protos/register.proto`](Protos/register.proto) defines package
`fastcaspaxos.v1`, services `Acceptor` and `Register`, and C# generated types
under `FastCASPaxos.Grpc.Wire`. Domain interfaces continue to use domain types.
Prepare and accept outcomes are explicit oneofs. Prepare success echoes the
requested ballot; prepare rejection retains the accepted state and conflicting
ballot. Accept returns the promised or conflicting ballot. Optional next
ballots, including `(0, 0)`, preserve piggyback semantics exactly.

Message presence distinguishes unaccepted protocol state from accepted empty
bytes. Register state explicitly selects `absent` or `present`; a present
state carries both bytes and opaque version. CompareExchange selects `applied`,
`mismatch`, or `unknown`. Unknown optionally carries a later observation and a
typed diagnostic cause. An explicitly present empty expected-version field
reaches register validation as an empty token, distinct from create-if-absent.

Keys, prefixes, and cursors are ordinary protobuf Unicode strings. C# adapters
reject malformed UTF-16 before sending; NUL, empty strings, and valid
supplementary characters round-trip. Local APIs retain their full .NET-string
capacity. A local inventory containing a malformed UTF-16 key reports an
explicit transport error. Text transport clients use well-formed Unicode
namespaces.

Pages use server-defined .NET ordinal ordering, ordinal prefix matching, and
exclusive cursors. Empty-string cursors are present values. A continuation
identifies the final returned key; absence means exhaustion. Clients validate
ordering, bounds, prefix, and continuation shape. Preserve server order and
pass continuation strings unchanged, particularly in languages whose default
string ordering differs for supplementary characters. Each page/entry retains
the [underlying listing guarantees](../FastCASPaxos.Server/README.md#quorum-backed-key-listing).

## Errors, uncertainty, and cancellation

| Boundary result | C# adapter behavior |
| --- | --- |
| Local malformed key/ballot or nonpositive page size | `ArgumentException` before dispatch. |
| Cancellation already requested | `OperationCanceledException` before dispatch. |
| Server validation before mutation | `InvalidArgument` with a typed failure trailer; adapter throws `ArgumentException`. |
| Register admission full | `ResourceExhausted` with a typed failure trailer; adapter throws `RegisterBusyException`. |
| Register operation deadline before mutation | `Unavailable` with a typed failure trailer; adapter throws `RegisterUnavailableException`. |
| Server-confirmed cancellation before mutation | `Cancelled` with a typed failure trailer; adapter throws `OperationCanceledException`. |
| Read/protocol transport failure | `RemoteCallException` retaining gRPC status and cause, or caller cancellation as `OperationCanceledException`. |
| Invalid response contract | `RemoteCallException` with `DataLoss`. |
| CompareExchange interruption or invalid response after dispatch | `Unknown`, with the failure in `Error`. |
| Server-returned CompareExchange outcome | Exact `Applied`, `Mismatch`, or `Unknown`, with its established state or later observation. |

The `fastcaspaxos-failure` trailer identifies server-confirmed pre-mutation
failure. Bare infrastructure statuses remain ambiguous for a dispatched
CompareExchange, including `Unavailable`, `DeadlineExceeded`, and `Cancelled`.
Callers therefore receive `Unknown` when a server may have committed before a
response was lost. Preserve that distinction in application retry and
reconciliation policies. Use channels with the default retry configuration:
application-level automatic replay of CompareExchange changes the operation's
identity and can obscure an earlier success.

gRPC deadlines bound transport waiting and propagate cancellation to domain
calls. The SQLite writer continues selected transactions to a durable outcome;
disconnecting a client leaves that guarantee intact. A response can be lost
while its transaction commits. Successful protocol responses are constructed
only after the underlying durable operation completes.

## Lifecycle and durable operations

Each host stages bounded durability passes periodically, with an initial
membership-ordered offset to spread work. `POST /v1/admin/repair` stages a pass
on every configured host through `Repair.Stage`; repeated commands coalesce.
HTTP 202 confirms all hosts accepted staging. HTTP 503 reports partial staging,
and reachable hosts retain their work. `repair` diagnostic events expose staging,
running, completion, pending replication, cancellation, and failures.

The `AcceptorDigests` gRPC service exposes `ListDigests` and `FetchIfDifferent`.
`GrpcAcceptorDigestSource` implements the domain `IAcceptorDigestSource` client.
Digest pages contain ordinal keys, accepted ballots, and 32-byte SHA-256 values.
Conditional fetch has explicit `missing`, `unchanged`, and `different` outcomes;
only `different` transfers accepted bytes. These APIs use the same trusted
network and transport validation boundaries as the protocol APIs.

Ctrl+C or the normal ASP.NET host shutdown invokes the repair lifecycle barrier
before Kestrel stops. The node seals incoming prepare/accept admission, closes
the register service, and waits for admitted mutations to reach their durable
outcomes. Digest and inspection endpoints remain readable while the final
local-key scan restores the target replica count **on the remaining hosts**.
Internal repair retains local protocol access through this barrier.

For three hosts and target 2, both surviving hosts must durably acknowledge
each local key. The departing host's disk is excluded from this target.
Simultaneous shutdowns, a target equal to membership size, a populated
single-member cluster, or insufficient surviving peers can keep the barrier
pending. The `StopAsync` cancellation token ends replication waiting promptly;
ASP.NET's `HostOptions.ShutdownTimeout` supplies the normal host deadline
(30 seconds by default). Configure the generic-host deadline for the deployment
and retain node storage after an interrupted drain. Cancellation emits a warning
and a `repair` cancellation event with the incomplete-durability outcome.

After the barrier completes or is canceled, HTTP shuts down and the node
disposes channels, proposer allocation, and SQLite in dependency order.
SQLite drains already selected transactions. Embedders call `NodeRuntime.StopAsync`
before `DisposeAsync` to request the replication barrier; disposal releases
resources and retains its existing durable queue-drain semantics.

Keep acceptor databases and their WAL state for each acceptor identity's
lifetime. Keep the allocator for each proposer identity's lifetime, including
all processes sharing that identity. Use local filesystems with SQLite locking
and durable-flush support. Back up using SQLite's online backup facilities or
copy the complete durable state after a clean stop. Restore the latest durable
state when restarting an identity. Recovery from an older backup requires an
offline cluster recovery procedure which preserves every acknowledged promise
and allocation; fresh proposer storage receives a new unique proposer identity.
See the [SQLite durability guide](../FastCASPaxos.Sqlite/README.md#durability-and-lifecycle)
and [register restart guide](../FastCASPaxos.Server/README.md#membership-ballots-and-restart).

## Verification

The C# workflow runs the full solution, compiles the .NET 8 transports, and
executes the three-process REST/gRPC smoke/outage/restart script. Focused real HTTP/2
integration coverage is available with:

```powershell
dotnet test csharp\tests\FastCASPaxos.Grpc.Tests -c Release --verbosity minimal
```
