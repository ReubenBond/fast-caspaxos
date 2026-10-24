# Per-node administrative diagnostics, schema v1

The ASP.NET Minimal API host provides direct discovery, local acceptor snapshots,
and a live SSE stream on each node's HTTP listener. A workbench discovers the
configured peers and opens one stream per host. Register reads, listing, and
conditional writes use the [quorum REST API](../FastCASPaxos.Rest/README.md).
The [host deployment guide](../FastCASPaxos.Grpc/README.md) covers listeners,
durable identities, TLS, and startup.

The [React administration console](../../../admin/README.md) is served at `/admin/`
by the normal host build and publish path. Its workbench combines conditional
register editing with direct per-node streams, local snapshots, and message
diagnostics. Building the host with console assets requires Node.js 24+ and npm.

## Configuration and access

Every member in the authoritative `FastCASPaxos:Peers` array has:

| Property | Meaning |
| --- | --- |
| `Id` | Stable durable acceptor identity. |
| `Endpoint` | HTTP/2-capable gRPC origin used by proposers. |
| `HttpEndpoint` | Explicit browser-reachable HTTP(S) origin serving REST and `/v1/admin`. |

All three properties are required. Each endpoint family has unique origins.
HTTP origins have a host, optional port, and root path; credentials, query
strings, fragments, and wildcard/unspecified bind addresses fail validation.
Advertise the addresses reachable from the administrator's browser. Reverse
proxies forward these root-level routes to the corresponding node. Listener
addresses and advertised addresses are independent deployment settings.
`Run-Node.ps1` and `Test-GrpcCluster.ps1` advertise every member's HTTP endpoint.

| Additional `FastCASPaxos` setting | Default / meaning |
| --- | --- |
| `TrustedAdminOrigins` | `[]`; additional exact HTTP(S) origins hosting a trusted workbench. |
| `EventHistoryCapacity` | `1024`; retained observations per node. Positive integer. |
| `MaxEventStreams` | `64`; concurrent SSE responses per node. Positive integer. |
| `HeartbeatInterval` | `00:00:15`; positive duration, at most one day. |

For a local frontend at port 5173, set
`FastCASPaxos__TrustedAdminOrigins__0=http://localhost:5173`. CORS permits this
origin and the explicitly advertised peer HTTP origins. The named policy
`FastCASPaxos.Admin` covers both admin and REST groups: GET/PUT/POST, the
`Content-Type`, `If-Match`, `If-None-Match`, `Last-Event-ID`, and `Authorization`
request headers, and exposed `ETag`/`Location` response headers. Cross-origin
credentials are disabled by default.

Deployment requires a trusted network, TLS, and access controls protecting
diagnostics and both register/protocol APIs. Events contain keys and value
previews. Embedding applications use standard ASP.NET authentication and
`MapAdminApi(node).RequireAuthorization()` plus equivalent policies on REST and
gRPC. Applications using authenticated cookies can customize the named CORS
policy to allow credentials for their exact trusted origins. Native
`EventSource` sends cookies with `{ withCredentials: true }`; bearer-token
deployments use a fetch-based SSE client or a standard authenticated proxy.
Correlation metadata is diagnostic input with no authorization role.

## Routes

Every JSON response uses camelCase and `schemaVersion: 1`. Ballots are exactly
`{round: number, proposer: number}`, preserving both complete Int32 components.
UTC timestamps use the ISO 8601 JSON representation of `DateTimeOffset`.
Sequences are decimal **strings**, preserving precision in JavaScript.

| Route | Response |
| --- | --- |
| `GET /v1/admin/node` | Identity, boot identity, fixed peers, cursor, and observed health. |
| `GET /v1/admin/state?key=...` | One read-only local snapshot; `entry: null` means no persisted row. |
| `GET /v1/admin/states?prefix=&after=&limit=100` | Local inventory including promise-only records. |
| `GET /v1/admin/events` | `text/event-stream`, one host's observations. |
| `GET /v1/admin/repair` | Node/boot identity, `draining`, `shutdownComplete` for the configured shutdown mode, `targetReplicas`, and nullable timestamped `lastRepair`. |
| `POST /v1/admin/repair` | Stages coalesced durability passes across the configured hosts; 202 confirms staging, 503 identifies incomplete staging. |

Snapshots use `Cache-Control: no-store`. SSE uses `no-store, no-transform`,
`X-Accel-Buffering: no`, disabled server buffering, and a flush after each
frame. Configure reverse-proxy buffering and idle timeouts consistently.
Query names are case-sensitive and appear once; unsupported/duplicate names,
malformed percent escapes, and invalid UTF-8 return a 400 problem response.
The admin and REST routes share the same strict decoder. Use percent-encoded UTF-8 query values, with
`key=` and `after=` preserving empty strings.

`/node` fields:

```typescript
type NodeStatus = {
  schemaVersion: 1;
  nodeId: string;
  proposerId: number;
  bootId: string;                 // new 32-character GUID on each process/runtime start
  startedAt: string;
  cursor: string;
  peers: { id: string; grpcEndpoint: string; httpEndpoint: string; eventsUrl: string }[];
  storage: { status: "starting" | "ready" | "failed" | "stopped"; error: string | null };
  lastOperation: NodeEvent<OperationObservation> | null;
  heartbeatMilliseconds: number;
  historyCapacity: number;
};
```

`storage` describes the local SQLite writer lifecycle: initialization establishes
`ready`; a transaction failure retires it and establishes `failed`. The error is
an exception type; detailed errors remain in server logs. `lastOperation` is the
most recently completed quorum API observation, with its own timestamp and
sequence. Null means that this process has observed no completed operation.
Treat older outcomes as historical evidence and keep their age visible.

Process liveness (`/health/live`), browser stream reachability, local storage
health, and recent quorum operation outcomes are separate indicators. Startup
and liveness depend on the host itself. Peer outages affect observed RPC and
register results while reachable nodes continue serving their own streams.
The browser marks a host stale when its stream disconnects or exceeds its
chosen heartbeat deadline, for example twice `heartbeatMilliseconds`, using a
monotonic local timer. A heartbeat demonstrates that the host's stream is
reachable; its `storageStatus` independently describes local storage.

### Local snapshots

`/state` returns `{schemaVersion,nodeId,bootId,cursor,timestamp,entry}`.
`/states` returns `{schemaVersion,nodeId,bootId,cursor,timestamp,entries,continuation}`.
A local entry has:

```typescript
type Ballot = { round: number; proposer: number };
type PayloadPreview = { length: number; base64: string; truncated: boolean };
type RegisterPreview = {
  format: "register" | "opaque";
  isPresent: boolean | null;
  version: string | null;         // Base64 opaque register version, without ETag quotes
  value: PayloadPreview | null;
};
type LocalState = {
  key: string;
  promisedBallot: Ballot;
  acceptedBallot: Ballot;
  acceptedValue: PayloadPreview | null;
  register: RegisterPreview | null;
};
```

Promise-only rows have `acceptedBallot: {round:0,proposer:0}` and null
`acceptedValue`/`register`. A raw accepted empty value has a zero-length preview.
Valid register envelopes are decoded by the register layer, with explicit
logical absence (`isPresent:false`) or presence (`isPresent:true`, including
empty payloads). Other accepted bytes have `format:"opaque"` and null logical
fields. Logical state established by a quorum is obtained through REST.

Inventory is ordered by .NET ordinal key ordering with ordinal prefixes and
exclusive cursors. `limit` is 1-100, default 100. `continuation` is the last
returned key when more entries exist; null means this page exhausted its local
snapshot. Pages are independent observations. Inspection runs on the provider's
existing serialized queue and reads SQLite directly, preserving ballots and
accepted bytes. Protocol `IKeyedAcceptor.ListKeysAsync` retains its
accepted-only contract.
Local-only keys containing isolated UTF-16 surrogates remain accessible through
the .NET inspection interface; an HTTP inventory containing such a key returns
500 `invalid-text` so its identity remains explicit.

## Stream schema

Durable observations have strictly increasing local sequences within a boot.
The SSE `id` is `<bootId>:<sequence>`. Hosts have independent sequence spaces.
JSON data has this envelope:

```typescript
type NodeEvent<T> = {
  schemaVersion: 1;
  nodeId: string;
  bootId: string;
  sequence: string;
  timestamp: string;
  kind: "protocol" | "operation" | "acceptor-state" | "storage" | "repair" | "reset" | "heartbeat";
  data: T;
  id: string;
};
type KeyPreview = { text: string; length: number; truncated: boolean; lossy: boolean };
```

Frames have `id: ...`, `event: <kind>`, `data: <single-line JSON>`, and a blank
line. Heartbeats omit the SSE `id` field and carry the reader's existing cursor
in JSON. Reset/heartbeat control frames can repeat a sequence; retained
observations advance it. Consumers use `addEventListener` for the named events.

| Event | `data` fields and semantics |
| --- | --- |
| `protocol` | `operation` (`prepare`/`accept`), `direction` (`local`/`outbound`/`inbound`), `phase`, nullable `peerId`, `callId`, nullable `operationId`, `key:KeyPreview`, `ballot`, `nextBallot`, nullable `outcome`, `promisedBallot`, `acceptedBallot`, `conflictingBallot`, `value:PayloadPreview`, `error`. |
| `operation` | `operation` (`read`/`compare-exchange`/`list`), `operationId`, `key:KeyPreview` (list uses its prefix), `outcome`, nullable `isPresent`, `value:PayloadPreview`, `version` (Base64), and `error`. |
| `acceptor-state` | `key:KeyPreview`, `promisedBallot`, `acceptedBallot`, nullable `acceptedValue:PayloadPreview` and `register:RegisterPreview`. A transition in a successfully committed local SQLite batch. |
| `storage` | `status` (`ready`/`failed`/`stopped`), nullable exception-type `error`. |
| `repair` | `state` (`staged`/`draining`/`running`/`complete`/`pending`/`cancelled`/`failed`), `departing`, `targetReplicas`, `examined`, `repaired`, `pending`, nullable diagnostic `error`, `timestamp`. |
| `reset` | `reason`: `initial`, `invalid-cursor`, `restarted`, or `history-unavailable`. |
| `heartbeat` | `storageStatus`: current local writer lifecycle status. |

Protocol phases distinguish `request-initiated` (local/outgoing invocation),
`request-received` (incoming provider invocation), `response-observed`
(local/outgoing completion), `response-produced` (incoming provider completion),
and `fault-observed`. A received request has passed transport decoding.
An initiated request records an attempt at the client boundary; the destination's
received observation establishes delivery. Outcomes are `promised`/`accepted`/
`rejected`; faults include an exception type.

`callId` pairs each invocation and completion. Host gRPC interceptors propagate
it and `operationId` to remote observers via `fcp-node-bin`, `fcp-call`, and
`fcp-operation` metadata. Peer identity comes from configured outgoing peers or
incoming diagnostic metadata; callers without metadata have null incoming
`peerId`. Every quorum API invocation gets a fresh operation id.

Repair staging can be requested from any configured HTTP origin:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:5004/v1/admin/repair
```

The command schedules work and returns promptly; `repair` events report the
subsequent passes. Periodic scheduling, target replication, and graceful
departure are described in the
[durability lifecycle guide](../FastCASPaxos.Grpc/README.md#lifecycle-and-durable-operations).

Operation outcomes are `established` (read), `completed` (listing), `applied`,
`mismatch`, `unknown` (conditional write), or `fault`. An unknown result's
optional value/version is the service's later observation. The application
reconciles its write using the existing register outcome contract.
`isPresent` is true/false when the service supplies a current register
observation and null when it supplies none, preserving observed absence
separately from unavailable state.
Individual accepted-state and protocol observations describe individual
acceptors; operation observations describe the quorum API's result.

Previews contain at most 256 original bytes, encoded in Base64, the complete
original byte length, and explicit truncation. Raw accepted bytes and decoded
register payloads have separate previews. Event keys retain at most 1024 UTF-16
code units, preserving valid surrogate pairs at the cut, with original length
and explicit truncation. Isolated UTF-16 surrogates in local-only keys become
display replacement characters with `lossy:true`. A truncated or lossy key
invalidates the entire selected view; its text is a display preview.
Snapshot keys remain complete. Render keys/previews as text or bytes using
normal escaping.

## Replay, resynchronization, and lifecycle

Use `Last-Event-ID` to resume. A new `EventSource` can supply
`?after=<percent-encoded cursor>`; its automatic reconnect header takes
precedence over that query value. Retained observations strictly after the cursor
are replayed and then live observations follow through the same atomic ring
lookup. Subscription has a single sequence authority and no replay/live gap.

A fresh connection receives `reset:initial` at the current cursor. A malformed
cursor, another boot, an ahead-of-host sequence, or overwritten history produces
an explicit reset at the current cursor. A slow reader that falls behind the
bounded ring receives `reset:history-unavailable` when it resumes reading.
Heartbeats use the injected `TimeProvider`; quiet streams therefore remain
observable and testable with controlled time.

Use events as **invalidations**, with local snapshots as the state authority:

1. Open each host stream, record its boot/cursor, and start buffering bounded
   invalidations. Mark that host's view stale on reset.
2. Fetch local snapshots or inventory pages. Each response cursor is captured
   **before** its queued read. Preserve stream invalidations after that cursor.
3. Refetch invalidated selected keys/pages. Accept a snapshot only for the
   current boot; refetch again if subsequent buffered events invalidate it.
   Restart enumeration on insertion/key truncation where the visible inventory
   might change.
4. On reset, disconnect, or client-buffer overflow, discard incomplete
   reconciliation, mark the view stale, and fetch fresh state. Logical register
   views use the REST read/list endpoints after operation/acceptor invalidation.

A transaction may contain multiple transitions of one key; snapshots observe
the provider's serialized state, while events describe transitions in the
committed batch. This invalidation protocol handles delayed replies and
intermediate observations without applying an older event over a newer snapshot.
Consumers needing historical messages retain only their own bounded UI window.

Publication copies bounded immutable summaries and appends to one shared ring.
It performs no subscriber callbacks or network I/O. Slow sockets retain one
bounded event at a time; stream admission is bounded and returns 503
`stream-capacity` when full. State observations publish after successful COMMIT
and before protocol completion. Rolled-back batches publish storage failure and
fault observations. Exact retries and rejections emit their protocol outcomes
and preserve the existing durable state.

Disconnect/shutdown cancels the stream wait/write and releases its admission
slot. Node disposal drains the existing services and SQLite before disposing
the journal. A restarted process creates a fresh boot identity and fresh ring;
the retained SQLite state remains the authority for resynchronization.
