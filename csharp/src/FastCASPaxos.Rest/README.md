# HTTP register API

`FastCASPaxos.Rest` maps ASP.NET Minimal APIs onto an application-owned
`IRegisterService`. `FastCASPaxos.Host` serves these routes using its existing
SQLite-backed replicated register service. Each key has a linearizable read
and conditional write, an opaque logical version, and quorum-backed listing.
The transport library targets .NET 10 and .NET 11 with the latest C# language version.

## Run and embed

From the repository root with the .NET 11 RC2 SDK pinned in `global.json`:

```powershell
dotnet build csharp\src\FastCASPaxos.Host -c Release
dotnet run --project csharp\src\FastCASPaxos.Host -f net11.0 -c Release --no-build
```

The default single-member host serves REST at `http://127.0.0.1:5000` over
HTTP/1.1 and gRPC at `http://127.0.0.1:5001` over HTTP/2. Both listeners serve
`/health/live`; a register operation establishes quorum availability.
The [gRPC host guide](../FastCASPaxos.Grpc/README.md) covers durable files,
fixed membership, proposer identities, shutdown, and a three-process example.
`Run-Node.ps1` assigns gRPC ports `BasePort` through `BasePort+2` and REST
ports `BasePort+3` through `BasePort+5` (defaults 5001-5003 and 5004-5006).
The host's [administrative API](../FastCASPaxos.Host/README.md) advertises these
origins and provides per-node SSE diagnostics. Its exact trusted-origin CORS
policy also covers REST and exposes `ETag`/`Location` to browser clients.

An embedding ASP.NET application registers its service and maps the route group:

```csharp
using FastCASPaxos.Rest;
using FastCASPaxos.Server;

builder.Services.AddSingleton<IRegisterService>(registers);
var app = builder.Build();
app.MapRegisterApi(); // Returns a RouteGroupBuilder for endpoint conventions.
```

The application owns the service's lifetime and its dependencies. The optional
mapping argument changes the group prefix, for example `MapRegisterApi("/api/registers")`.
`MapRegisterApi(maxRequestBodyBytes: 1024)` sets a positive per-PUT body limit
using the server's `IHttpMaxRequestBodySizeFeature`; omitting it retains the
server's configured limit.
Configure standard ASP.NET authentication/authorization and apply
`RequireAuthorization()` to the returned group when authenticating callers.
Deploy the host within a trusted network with TLS and firewall/proxy controls;
protect the gRPC protocol endpoints as well as these application endpoints.

### Listeners and bounds

Configure listeners with standard `Kestrel:Endpoints` settings. The two
plaintext defaults deliberately select `Http1` and `Http2` separately:
plaintext `Http1AndHttp2` selects HTTP/1.1, whereas gRPC h2c requires an
HTTP/2 listener. A TLS endpoint configured with `Protocols: Http1AndHttp2`
uses ALPN to serve both protocols on the same port. Set peer addresses to the
HTTP/2-capable endpoint and configure certificates through standard Kestrel
configuration and secret providers.

`FastCASPaxos:MaxRequestBodyBytes` is a positive host setting, default
`4194304`. The host passes it to the REST route mapping, which sets the
per-request limit before reading a PUT body. Kestrel enforces it for both
fixed-length and chunked uploads. gRPC keeps its independently configured
message limits. The adapter buffers the complete bounded body before
dispatching one write. The host also configures connection bounds; standard
Kestrel request-line/header limits, body data-rate enforcement, and reverse
proxy limits apply. Embedding applications configure these transport bounds
on their own servers or through the mapping's per-PUT limit.

These are deployment limits. The local register and storage APIs retain their
payload capacity. Configure gRPC peer message limits to allow the payload plus
register-envelope and protobuf overhead when raising the REST body limit or
using payloads near that limit in a multi-node deployment.

## Routes and representation

| Request | Result |
| --- | --- |
| `GET /v1/registers/value?key=...` | `200 application/octet-stream`, exact bytes, and a strong `ETag`; `404` for absence. |
| `PUT /v1/registers/value?key=...`, `If-None-Match: *` | Create if absent: `201`, new `ETag`, and `Location`. |
| `PUT /v1/registers/value?key=...`, `If-Match: "<token>"` | Update the exact version: `204` and new `ETag`. |
| Failed PUT comparison | `412` problem, plus the established current `ETag` when present. |
| `GET /v1/registers?prefix=...&after=...&limit=...` | `200` JSON page with `keys` and nullable `continuation`. |

Present empty payloads return `200`, zero bytes, and an ETag. Successful PUT
responses have empty bodies. An ETag is a quoted, canonical RFC 4648 Base64
encoding of opaque version bytes, including padding. Clients retain and resend
the complete quoted header. Each successful write receives a fresh version,
even for identical bytes; reads preserve the established logical version.

PUT requires `Content-Type: application/octet-stream`, a complete unencoded
body, and exactly one supported precondition. Missing preconditions return
`428`. Duplicate headers, lists of ETags, weak ETags, `If-Match: *`, an
`If-None-Match` value other than `*`, both headers together, and malformed or
noncanonical ETags return `400`. The register service validates the decoded
opaque token. Date conditions and `If-Range` return `400`. Conditional headers
on GET return `400`; GET performs a fresh linearizable read. Encoded content
returns `415`, and `Content-Range` returns `400`.

All adapter responses include `Cache-Control: no-store`, including errors and
listing. Clients and proxies must forward each linearizable read to the service.

### Key and query encoding

The complete key lives in the query, keeping path normalization separate from
register identity. Encode each query component as UTF-8 and percent-escape it
once: use `Uri.EscapeDataString` in .NET or curl `--data-urlencode`. For example,
the key `catalog/../a% b` becomes `key=catalog%2F..%2Fa%25%20b`.
`%2F` is slash, `%25` is percent, `%2B` is plus, `%00` is NUL, and
`%F0%9F%98%80` is a supplementary Unicode character. A literal `+` in a query
decodes to space. Values preserve ordinal case and Unicode code points.

`key=` addresses the empty key; omitting `key` is an error. Each supported,
case-sensitive query name appears once, in `name=value` form. Unknown or
duplicate names, malformed percent escapes, invalid UTF-8, and malformed
UTF-16 return `400` before service dispatch. HTTP callers use well-formed
Unicode namespaces; listing a local key with an isolated surrogate reports an
explicit server error. The local .NET APIs retain their string capacity.

Listing defaults to `prefix=""`, absent `after`, and `limit=100`. `after=` is
the empty exclusive cursor, distinct from omitting it. An explicit limit is
a positive decimal Int32. A page looks like:

```json
{"keys":["catalog/a","catalog/b"],"continuation":"catalog/b"}
```

Pass a non-null continuation unchanged as the next `after`; null indicates
exhaustion. Preserve server order (`StringComparer.Ordinal`) and ordinal
prefix matching. Each key is observed independently through a linearizable
read; continuation pages observe the cluster when requested. The
[register listing contract](../FastCASPaxos.Server/README.md#quorum-backed-key-listing)
defines quorum coverage and concurrent insertion behavior.

## Outcomes and failures

Errors use `application/problem+json` with standard ProblemDetails fields
(`type`, `title`, `status`, `detail`) and stable `code` and `outcome` extensions.

| Status / code | Outcome |
| --- | --- |
| `400 invalid`, `415 invalid`, `428 invalid` | `not-applied`; request or token validation failed. |
| `400 body-read`, `413 body-read` | `not-applied`; body reading failed before service dispatch. |
| `412 mismatch` | `not-applied`; the established condition failed. |
| `503 busy` | `not-applied`; register admission was full. |
| `503 unavailable` | `not-applied`; the register deadline expired before mutation. |
| `499 cancelled` | `not-applied`; cancellation occurred before mutation. A disconnected client may receive no response. |
| `503 unknown` | `unknown`; the write may have applied. |
| `500 internal` | A dispatched PUT has `unknown`; other operations have `not-applied`. Server logs retain diagnostics. |
| `404 missing` | `not-applied`; a read established logical absence. |

For example:

```json
{
  "type":"about:blank",
  "title":"unknown",
  "status":503,
  "detail":"The write may have applied. Reconcile the register using application semantics.",
  "code":"unknown",
  "outcome":"unknown",
  "currentEtag":"\"AAAAAQAAAAI=\"",
  "cause":"deadline"
}
```

`currentEtag`, when present on an unknown outcome, denotes a later established
observation that may have superseded this operation. It is a JSON extension,
separate from the response ETag used for definitive outcomes. `cause`, when
available, is `cancelled`, `deadline`, or `internal`. Exception messages and
storage paths stay in server logs.

`RequestAborted` flows to the register service. Its confirmed quorum result
remains authoritative; cancellation after possible mutation preserves the
service's explicit `Unknown`. Selected SQLite transactions reach their durable
outcomes even when clients disconnect.

A client-side disconnect, timeout, proxy failure, or response outside this
documented contract after sending PUT also leaves the outcome uncertain.
Reconcile using a fresh read and application operation identity/business
semantics: a later value can supersede an earlier successful write. Configure
clients and proxies to deliver each PUT once. A new PUT creates a new register
operation identity, even when its bytes and precondition match an earlier call.

## Examples

PowerShell 7, against the default running host:

```powershell
$root = 'http://127.0.0.1:5000/v1/registers'
$key = "example/$([Guid]::NewGuid().ToString('N'))"
$uri = "$root/value?key=$([Uri]::EscapeDataString($key))"
$created = Invoke-WebRequest $uri -Method Put -ContentType 'application/octet-stream' `
    -Headers @{'If-None-Match'='*'} -Body ([byte[]]@(0, 255, 10, 128))
$read = Invoke-WebRequest $uri
$etag = $read.Headers.ETag[0] # Keep the quotes.
$updated = Invoke-WebRequest $uri -Method Put -ContentType 'application/octet-stream' `
    -Headers @{'If-Match'=$etag} -Body ([byte[]]@())
$stale = Invoke-WebRequest $uri -Method Put -ContentType 'application/octet-stream' `
    -Headers @{'If-Match'=$etag} -Body ([byte[]]@(1)) -SkipHttpErrorCheck
$stale.StatusCode # 412; the update is a present empty value with a new ETag.
$page = Invoke-RestMethod "$root`?prefix=example%2F&limit=1"
if ($null -ne $page.continuation) {
    Invoke-RestMethod "$root`?prefix=example%2F&limit=1&after=$([Uri]::EscapeDataString($page.continuation))"
}
```

For a non-successful write, inspect the problem `outcome`. `unknown` requires
application reconciliation; `mismatch` establishes a failed precondition.

Curl in a POSIX shell (choose a fresh key each run):

```sh
root=http://127.0.0.1:5000/v1/registers
key="curl/$(date +%s)"
url="$root/value?key=curl%2F${key#curl/}"
printf '\000\377\012\200' > value.bin
curl --fail-with-body -i -X PUT "$url" -H 'Content-Type: application/octet-stream' \
  -H 'If-None-Match: *' --data-binary @value.bin
curl --fail-with-body --get "$root/value" --data-urlencode "key=$key" \
  -D headers.txt -o read.bin
etag=$(sed -n 's/^[Ee][Tt][Aa][Gg]: *//p' headers.txt | tr -d '\r')
curl --fail-with-body -i -X PUT "$url" -H 'Content-Type: application/octet-stream' \
  -H "If-Match: $etag" --data-binary '' # 204 and a new ETag
curl -i -X PUT "$url" -H 'Content-Type: application/octet-stream' \
  -H "If-Match: $etag" --data-binary @value.bin # 412, stale ETag
curl --get "$root" --data-urlencode 'prefix=curl/' --data-urlencode 'limit=1'
```

## Verification

```powershell
dotnet test csharp\tests\FastCASPaxos.Rest.Tests -c Release --verbosity minimal
dotnet build csharp\samples\FastCASPaxos.Grpc.Smoke -c Release
pwsh -File csharp\scripts\Test-GrpcCluster.ps1
```

Deterministic fake-service and phase-barrier cases establish HTTP validation,
cancellation, and outcome translation. Real HTTP/1.1 plus SQLite cases
establish transport, gRPC interoperability, and retained values/versions.
The shared executable smoke checks three processes, one-node outage, and a
full restart over both transports. CI also runs the existing protocol DST
suite for both .NET 10 and .NET 11.
