# Replicated registers

`ReplicatedRegisterService` implements `IRegisterService` over a fixed set of
`IKeyedAcceptor` endpoints. The same implementation serves local callers,
clients using remote acceptor adapters, and a transport-facing server.
Each key has an independent linearizable conditional register with immutable,
opaque payload bytes.

```csharp
using FastCASPaxos.Server;
using FastCASPaxos.Sqlite;

await using var a = await SqliteAcceptorServer.OpenAsync("acceptor-a.db");
await using var b = await SqliteAcceptorServer.OpenAsync("acceptor-b.db");
await using var c = await SqliteAcceptorServer.OpenAsync("acceptor-c.db");
using var ballots = new SqliteProposerBallotSource("proposer-1.db", proposerId: 1);
await using var registers = new ReplicatedRegisterService(
    [new("a", a), new("b", b), new("c", c)], ballots);

var created = await registers.CompareExchangeAsync("catalog/item", null, ByteString.Empty);
var current = await registers.ReadAsync("catalog/item");
var updated = await registers.CompareExchangeAsync(
    "catalog/item", current.Version, new ByteString("updated"u8));
var page = await registers.ListKeysAsync(prefix: "catalog/", limit: 100);
```

The caller owns the endpoints and ballot source. Dispose the register service
before disposing those dependencies.

## Values and conditional outcomes

`RegisterValue` uses null `Value` and `Version` for logical absence. A present
empty payload has a zero-length `ByteString` and a non-null version. ByteString
copies input bytes and returns owned copies through `ToArray()`.

Pass the exact opaque version from a read or successful write as
`expectedVersion`. Null creates only an absent register. Every applied write
receives a fresh version, including a write of identical payload bytes. Reads
and failed conditions establish the recovered state through an identity accept
quorum and preserve its logical version.

| Status | Established outcome |
| --- | --- |
| `Applied` | This operation's unique version reached an accept quorum. `Current` contains that version and payload. |
| `Mismatch` | The observed condition failed, and this operation's mutations are known to have been rejected or remained unsent. `Current` is the established state. |
| `Unknown` | A mutation may have applied. `Current`, when available, is a later established observation. `Error`, when available, preserves the interruption or failure. |

The operation identity stays fixed through internal retries. Fast-round
contenders with identical payloads have distinct identities, so only one can
return `Applied` for a given expected version. Recovery of the operation's own
version establishes success. A different later version after an uncertain
mutation preserves `Unknown`: another writer may already have superseded it.
Applications resolve this using their own business semantics.

Cancellation before mutation dispatch throws `OperationCanceledException`.
Deadline expiry before mutation dispatch throws `RegisterUnavailableException`,
with the latest peer failures as its inner aggregate. Interruption after
mutation dispatch returns `Unknown` with the cause in `Error`. A successful
quorum result remains authoritative even if a caller cancels immediately after
completion.

## Membership, ballots, and restart

All participants configure the same fixed identities, each identifying one
distinct durable acceptor. Endpoint adapters preserve that identity across
reconnection. The service counts at most one response per configured identity
and phase. Classic quorum is a majority of the original membership; fast
quorum is `ceil(3N/4)`. Failed and slow endpoints retain their membership slots.
Responses are correlated with their originating key, operation, and phase;
late responses remain isolated from subsequent attempts.

Assign each proposer a distinct positive integer identity and retain its
allocator database for that identity's entire lifetime. Every process using
the same identity shares the same allocator file. Move the allocator with the
identity when moving a proposer; allocate a new distinct identity when
provisioning fresh storage. Retain acceptor databases as well.

`SqliteProposerBallotSource` serializes durable round reservations through
immediate SQLite transactions, WAL, and `synchronous=FULL`. Each reservation
exceeds the previously reserved round and meets the requested minimum.
Reservations commit before an owned classic ballot, including a piggyback
promise, becomes observable. Logical versions consume the same monotonic
identity space. Restarted proposers therefore advance past all of their
pre-restart owned ballots and logical identities, including delayed traffic.
The source rejects opening an allocator under a different proposer identity.
Round exhaustion fails with `OverflowException` before a wrapped ballot can
be emitted; provision a new proposer identity before exhausting its range.

The default mode is classic. `RegisterServiceOptions.Mode` also accepts the
core fast, leader, fast+leader, and adaptive policies. Shared fast ballots keep
their protocol identity; recovery uses freshly reserved owned classic ballots.
Queued operations on a key share their engine and can reuse confirmed
piggyback promises. An idle key releases its engine; its next operation recovers
durable state. Adaptive policy controls local ballot selection.

## Quorum-backed key listing

`ListKeysAsync(prefix, afterKey, limit)` returns logical present keys in
case-sensitive `StringComparer.Ordinal` order. Prefix matching is ordinal,
`afterKey` is exclusive, and `limit` is positive. A non-null continuation is
the final returned key and can be passed unchanged as the next `afterKey`.
Local APIs support every .NET string, including NUL and isolated surrogates.
Text transports apply their own encoding validation at their boundary.

The service merges inventory pages from a reachable majority. The covered
range ends at the earliest continuation among those pages; exhausted pages
cover the remaining suffix. That quorum intersects every prior committed
write quorum, including keys distributed unevenly across replicas.
Each candidate then goes through a linearizable read before inclusion.
A read can finish a pending accepted write, making it present. Accepted logical
absence envelopes and promise-only records are excluded from the logical list.

Each key is observed independently during the scan. Concurrent insertions
ahead of the cursor may appear on the current scan; insertions at or before
the cursor appear when a later scan starts earlier. Continuation pages observe
the cluster when requested. Applications requiring a consistent multi-key
view can store that view as one register value.

## Scheduling and protocol boundary

`Capacity` bounds active plus queued API calls. A full service reports
`RegisterBusyException` immediately. A per-key queue serializes operations;
other keys progress independently. The operation deadline includes queueing,
inventory discovery, and candidate resolution. Attempt deadlines and
proposer-staggered retry backoff use the configured `TimeProvider`.
Per-endpoint in-flight limits also bound retained calls when a transport
ignores cancellation. Endpoint implementations return promptly from their
async entry points and cooperate with cancellation to release transport work.
Disposal closes admission, cancels active work, and waits for admitted calls
to report their outcomes.

The register layer stores a versioned `FCR1` envelope containing logical
presence, an operation identity, and payload bytes. Protocol endpoints store
these bytes opaquely. Protocol clients sharing register keys preserve this
envelope and the register's conditional-update rules. Malformed accepted
envelopes produce explicit errors. Application namespaces and peer identity
configuration belong to the hosting layer.

## Durability repair

`DurabilityRepair` merges bounded digest pages from reachable fixed members.
Matching accepted ballots and SHA-256 fingerprints establish which replicas
hold identical accepted bytes. A matching set must satisfy the ballot's commit
quorum as well as the configured replica target before the pass skips a key.
Reachable members with missing or divergent state trigger an identity proposal.

Repair uses the existing proposer's fast-ballot recovery rules and a freshly
reserved classic ballot, then sends the recovered bytes to every member.
This preserves logical register versions and supports opaque protocol values.
The scan's source bytes are a candidate only when prepare observes an entirely
unaccepted quorum; accepted quorum evidence determines recovery otherwise.
Higher promises cause another recovery above their ballot floor.

`DurabilityRepairOptions.TargetReplicas = 0` selects a majority, or F+1 for
2F+1 members. Each successful repair requires both a classic quorum and the
target count of durable acknowledgements. Periodic passes attempt all reachable
replicas, including those beyond the target, and revisit returning members.
Unavailable members retain their membership slots. A pass reports examined,
repaired, pending counts and bounded peer diagnostics.

`RunPassAsync(departing: true)` scans the local accepted keys and requires fresh
remote protocol acknowledgements for each key, including matching digests.
Peers accepting protocol traffic can satisfy the target; peers already draining
their own shutdown keep their readable inventory but reject new protocol work.
Its caller seals external mutation admission
and drains admitted work first, establishing the departure scan's finite key set.
Departure pagination follows that local set, so continuing inserts on other
members leave the departure scan's endpoint unchanged.
Each key is established independently; ongoing cluster writes use the same
consensus guarantees as application operations.

Pages bound inventory memory. One outstanding call slot per peer bounds retained
work even when a transport ignores cancellation; per-attempt and per-key deadlines
bound waiting. Attempt completion or timeout cancels its outstanding transport
call so cooperative peers release their call slots. Cancellation ends the scan promptly. Endpoint implementations
complete any already selected durable transaction according to their storage
contract. Local storage and integrity failures retain their original exceptions;
timeouts and capacity exhaustion are reported as temporary unavailability.
