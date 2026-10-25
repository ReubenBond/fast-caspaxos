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
empty payload has a zero-length `ByteString` and a non-null version. `ByteString`
is a readonly struct with content-based `Equals`, `GetHashCode`, `==`, and `!=`.
It copies input bytes and returns owned copies through `ToArray()`.
`default(ByteString)` has `IsDefault == true` and represents uninitialized
bytes. `ByteString.Empty` and `new ByteString([])` are initialized, empty values
with `IsEmpty == true`. Equality and hashing support default values, and default
compares unequal to initialized empty bytes. Reading `Length`, `IsEmpty`, or
`ToArray()` on a default value throws `InvalidOperationException`.

The class-to-struct API change requires rebuilding consumers. Use `ByteString?`
for optional bytes and unwrap present values through `.Value` or a property
pattern before passing them to APIs accepting `ByteString`. Nullable equality
preserves the distinction between absent bytes and present empty bytes.
Required payloads accept initialized values, including empty bytes, and reject
default values with `ArgumentException` before mutation or network dispatch.
Payloads remain opaque across SQLite and protobuf boundaries.

Pass the exact opaque version from a read or successful write as
`expectedVersion`. Null creates only an absent register. Every applied write
increments its per-register revision, including a write of identical payload bytes.
The opaque eight-byte version encodes a positive unsigned 64-bit revision in
big-endian order. A new register starts at one; the conditional state transition
computes the next revision from the accepted value. Exhaustion throws
`OverflowException` before a wrapped revision can be proposed. Reads
first gather durable observations using `IKeyedAcceptor.ReadAsync`. Matching
accepted ballots and complete versioned bytes establish the value at a classic
majority or `ceil(3N/4)` for a fast ballot. A majority of never-accepted records
establishes absence. These reads preserve acceptor state and consume no ballots.
Inconclusive observations enter identity prepare/accept recovery. Failed CAS
conditions also use identity acceptance. Both paths preserve the logical version.

`ReadAsync(key, known: null)` returns an `AcceptorReadResult` union containing
`AcceptorMissing` or `KeyedAcceptorState`. Supplying a matching `AcceptedDigest`
returns `AcceptorUnchanged`. Each result is a fresh durable local observation;
the register service combines distinct peer observations from one read attempt
to establish a linearizable value. Promised ballots may differ.

The projects target .NET 10 and .NET 11 using the .NET 11 RC2 SDK pinned in
`global.json` and C# `latest`. The .NET 10 assembly includes the compiler's union
metadata contract; .NET 11 uses the runtime-provided types.

| Status | Established outcome |
| --- | --- |
| `Applied` | This operation's complete envelope, including its request identity, reached an accept quorum. `Current` contains its revision and payload. |
| `Mismatch` | The observed condition failed. Mutations remained unsent, were explicitly rejected, or recovery established another envelope at the same or lower revision and fenced the earlier attempts. `Current` is the established state. |
| `Unknown` | A mutation may have applied. `Current`, when available, is a later established observation. `Error`, when available, preserves the interruption or failure. |

The operation identity stays fixed through internal retries. Fast-round
contenders with identical payloads have distinct identities, so only one can
return `Applied` for a given expected version. Recovery of the operation's own
envelope establishes success. Recovery of a different envelope at the same
revision establishes `Mismatch`, even with a missing peer response: every applied
write advances the revision, and the recovery quorum fences prior attempts.
A higher revision after an uncertain mutation preserves `Unknown`: another writer
may already have superseded it.
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
promise, becomes observable. On its first write a register-service instance
reserves one durable incarnation from this source. Requests then use the tuple
`(proposer, incarnation, in-memory sequence)` to distinguish identical-byte
contenders and recognize recovery of their own writes. The sequence is checked
for overflow and remains fixed across retries. Instances sharing a proposer
receive distinct incarnations. Register revisions advance within the register;
successive fast writes use only the acceptor commit, with no proposer-side
allocator transaction after incarnation initialization. Restarted proposers
advance past all previously reserved ballots and incarnations.
Classic prepared ballots remain owned by the engine that reserved them,
including when several service instances share a proposer identity and allocator.
Learning another engine's classic promise advances the ballot floor; that engine
retains exclusive use of its reservation. Shared fast promises can be reused by
other engines after their committed base is learned.
The source rejects opening an allocator under a different proposer identity.
Round exhaustion fails with `OverflowException` before a wrapped ballot can
be emitted; provision a new proposer identity before exhausting its range.

The default mode is **adaptive**, combining shared fast ballots with leader-owned
classic recovery and prepared leader rounds. Uncontended creation can use the
initial fast ballot directly. Successful accepts piggyback the next prepare;
subsequent writes reuse a quorum-confirmed promise in a single accept round.
The service publishes `Committed` conclusions containing the accepted ballot,
complete envelope, and the next promise actually confirmed by a quorum.
Subscribers run after the authoritative operation and key-gate cleanup. Each
subscriber has an independent delivery outcome; failures preserve the committed
result and the warm engine while subsequent subscribers still receive the
conclusion. `LastNotificationFailure` retains the most recent failure and its
commit. Supplying a `NodeEventJournal` also records every failure as a separate
`notification` event; the host wires this into its SSE diagnostics.
Trusted cluster peers deliver these conclusions through `IRegisterLearner.Learn`.
Learning uses the same bounded key cache and applies pending conclusions under
the key's operation gate. Fast-mode writers can consequently alternate between
nodes using the shared prepared ballot; leader promises remain proposer-owned.
Quorum reads also seed the committed value and any matching quorum-confirmed
next promise.
Fast rounds use the fast quorum; leader rounds use a majority. Contention or
timeouts trigger classic recovery, and successful leader rounds reduce conflict
debt so the policy can reopen fast mode. Before a write, an optional local
`ILocalAcceptorInspection` advances the proposer's ballot floor using both
durable accepted and promised ballots. A locally permitted, quorum-confirmed
prepared ballot remains reusable. Cold existing keys and inconclusive
prepared state start prepare above that floor. Explicit classic, fast, leader, and
fast+leader policies remain available.

Idle keys retain their engine, accepted base, prepared promise, and adaptive
history in a least-recently-used cache of at most `Capacity` idle keys. Active and
queued keys retain their state until their callers finish. Eviction releases the
engine; the next access establishes a fresh base. Quorum reads always gather fresh
acceptor observations. Cached state permits speculation only through the core
protocol's prepared-ballot checks and quorum acknowledgements.

Hosts deliver commit learning asynchronously, outside the write's accept RTT.
A receiver with a delivered value and usable prepared promise can issue a
one-RTT update. Delayed learning, eviction, restart, or an intervening higher
promise causes quorum recovery from the locally known ballot floor.
Local accepted bytes alone supply a candidate for recovery; quorum evidence
establishes the value used for a prepare-free operation.

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

The register layer writes an `FCR2` envelope containing logical presence, an
eight-byte revision, a sixteen-byte request identity, and payload bytes. Presence
occupies one byte after the four-byte header; present payloads begin at offset 29.
Request identities encode proposer and incarnation as positive big-endian Int32s,
followed by a positive big-endian Int64 request sequence. Existing `FCR1` envelopes
remain readable with their exact versions and payloads. Their next successful
write increments the existing eight-byte version interpreted as a revision and
writes `FCR2`; recovery preserves the original envelope.
Deploy the updated register layer to all writers together so every writer sharing
these keys follows the per-register revision rule.
Protocol endpoints store
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
