# Fast CASPaxos administration console

The React/TypeScript workbench runs at **`/admin/` on every host's HTTP listener**.
It discovers the fixed cluster through `/v1/admin/node`, then opens a native
`EventSource` and fetches local snapshots directly from every configured host.
The request-host selector chooses the host used for quorum register operations;
all node subscriptions remain active.

## Build and run

Prerequisites: .NET 11 RC2 SDK (pinned in `global.json`) and Node.js 24+ with npm.

From the repository root in PowerShell:

```powershell
dotnet build csharp\src\FastCASPaxos.Host -c Release
dotnet run --project csharp\src\FastCASPaxos.Host -f net11.0 -c Release --no-build
```

Open `http://127.0.0.1:5000/admin/`. For a cluster, use the existing
[host deployment guide](../csharp/src/FastCASPaxos.Grpc/README.md).

The host's normal build restores the lockfile with `npm ci` when needed, runs
TypeScript checking and the Vite production build, and copies assets into
`wwwroot/admin` beside the executable. Publish includes the same assets:

```powershell
dotnet publish csharp\src\FastCASPaxos.Host -f net11.0 -c Release
Set-Location csharp\src\FastCASPaxos.Host\bin\Release\net11.0\publish
dotnet .\FastCASPaxos.Host.dll
```

Run the published executable with its publish directory as the working directory,
or pass `--contentRoot` pointing to that directory, so the host reads its
configuration. `/admin` redirects to `/admin/`; the HTML uses `no-store`, and
content-hashed assets use immutable caching. `/v1` and gRPC retain their routes.

For backend-only builds, `-p:BuildAdminConsole=false` explicitly skips UI tooling.
A host without built assets responds to `/admin/` with a descriptive 503 problem
and build instructions. Generated assets, dependencies, and test artifacts stay
out of source control.

## Frontend development

Configure every host to trust the exact development origin:

```powershell
$env:FastCASPaxos__TrustedAdminOrigins__0 = 'http://127.0.0.1:5173'
# Start the hosts with this setting, then in another terminal:
$env:VITE_API_ORIGIN = 'http://127.0.0.1:5000'
npm --prefix admin ci
npm --prefix admin run dev
```

Open `http://127.0.0.1:5173/admin/`. Vite contacts the configured REST origin;
SSE and local-state requests go directly to advertised peer HTTP endpoints.
Match the origin exactly, including hostname and port. HTTPS deployments use
HTTPS browser-reachable peer endpoints and the existing access controls.

Protect the console, diagnostic APIs, register APIs, and gRPC listener together
using the deployment's trusted network and TLS/access-control boundary. Keys and
payload previews are operational data. This console uses the existing CORS
policy and native EventSource default credential behavior; deploy authentication
consistent with that boundary. The [admin contract](../csharp/src/FastCASPaxos.Host/README.md)
documents integration choices for authenticated applications.

## Operating the workbench

**Registers:** prefix search and exclusive cursor pagination show up to 50 logical
keys per page. Empty keys and empty continuation strings remain distinct from
absence. Stream resets and broad key invalidations restart enumeration at the
first page. Select a key for a quorum-established read and independent local
acceptor snapshots.
Quorum reads first observe accepted state through the read-only acceptor API.
Matching classic or fast quorums return the value in one round trip while
preserving ballots and stored state. Inconclusive observations use consensus
recovery. The same path serves inspector refreshes and key-list candidate reads.

**Values and edits:** the inspector shows complete bytes with their exact opaque
ETag. UTF-8 decoding is strict; binary values use Base64. Switching representations
preserves bytes, including a UTF-8 BOM and original line endings. The editor
captures its own expected ETag, keeping drafts stable when newer observations
arrive. Create uses `If-None-Match: *`; compare-and-swap uses the captured strong
`If-Match` value. Pending and unknown writes retain their editor; navigating away
from an unsaved draft requires explicit discard confirmation.

**Conflicts and uncertainty:** a 412 conflict preserves the draft and precondition.
A response declaring an unknown outcome, a transport interruption, or an
unclassified server failure after dispatch produces **Write outcome unknown**.
The write remains blocked until an explicit fresh read and adoption of the
observed version. Reconcile using application semantics: a later observed value
alone cannot identify which operation applied. Each submission issues one PUT.

**Node health:** connected/reconnecting/stale/unreachable describe the browser's
stream observation. Storage displays the host's independently reported lifecycle
status. Heartbeats keep quiet streams current. A monotonic deadline of three
advertised heartbeat intervals marks a quiet stream stale; a disconnected stream
is labeled reconnecting, then unreachable at that deadline. Native EventSource
owns reconnect and `Last-Event-ID` replay. Boot changes and reset/history-gap
events establish fresh observation baselines.

**Local state:** each row reports that node's persisted promise, accepted ballot,
accepted bytes, and server-decoded register preview. Its snapshot cursor belongs
to that node's boot. The quorum-established value remains separately labeled.
Truncated or lossy event keys invalidate broad views; exact lookup always uses
the complete selected key.

**Messages:** the timeline retains 1,000 events, renders the latest 200 matching
events, and shows eviction counts. Heartbeats update health. Node, event-kind,
and text/correlation filters help trace requests/replies and outcomes. Expand an
event for call/operation IDs, ballots, error details, and bounded Base64 previews.
Pausing freezes the displayed timeline while subscriptions and state refreshes
continue. Arrival order is observational; per-node sequence and call correlation
carry ordering evidence across independent host clocks.

Reads coalesce on a 300 ms deadline, with one in-flight request per resource and
at most one trailing refresh. Successful same-resource reads advance visible
observations even when events arrive during the read; those observations remain
explicitly stale while a trailing refresh is pending. Hard generation fences
discard responses after disconnect, abort, selection, query, host, or boot changes.
State rendering batches
every 100 ms; reads have a 15-second abort deadline. Disconnects fence queued and
in-flight reads, and reconnection resynchronizes observations. Teardown closes
streams and cancels requests and timers.

## Verification and preview

```powershell
npm --prefix admin test
dotnet publish csharp\src\FastCASPaxos.Host -f net11.0 -c Release
Set-Location admin
npx playwright install chromium
npm run test:browser
```

Vitest and React Testing Library use controlled clocks, deferred requests, and
explicit stream deliveries for lifecycle, race, byte, cursor, write, and UI
invariants. Playwright starts three real published SQLite-backed hosts in a
temporary directory, exercises browser SSE/CORS, conditional edits, an empty
binary key, competing-host changes, and one-node disconnect/restart. It captures
desktop/mobile evidence using real `demo/` keys labeled as isolated demo data,
then stops its owned processes and removes their temporary databases.

The **Administration console** GitHub workflow runs deterministic and real-host
browser tests, and uploads screenshots/traces as `admin-browser-evidence`.
Local screenshot files are under `admin/test-results/`. The production bundle
uses only React, native fetch, and native EventSource.
