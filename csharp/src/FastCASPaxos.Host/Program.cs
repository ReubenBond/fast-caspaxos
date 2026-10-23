using FastCASPaxos.Grpc;
using FastCASPaxos.Host;
using FastCASPaxos.Rest;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);
var options = builder.Configuration.GetSection("FastCASPaxos").Get<NodeOptions>(
    binder => binder.ErrorOnUnknownConfiguration = true) ?? new NodeOptions();
options.AcceptorDatabase = Path.GetFullPath(options.AcceptorDatabase, builder.Environment.ContentRootPath);
options.ProposerDatabase = Path.GetFullPath(options.ProposerDatabase, builder.Environment.ContentRootPath);
options.Validate();
builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.ConfigureEndpointDefaults(endpoint => endpoint.Protocols = HttpProtocols.Http2);
    kestrel.Limits.MaxConcurrentConnections = options.MaxConcurrentConnections;
    kestrel.Limits.Http2.MaxStreamsPerConnection = options.MaxStreamsPerConnection;
});
builder.Services.AddGrpc(grpc =>
{
    grpc.MaxReceiveMessageSize = options.MaxReceiveMessageBytes;
    grpc.MaxSendMessageSize = options.MaxSendMessageBytes;
    grpc.EnableDetailedErrors = false;
    grpc.Interceptors.Add<DiagnosticServerInterceptor>();
});
builder.Services.AddAdminCors(options);

await using var node = await NodeRuntime.OpenAsync(options);
builder.Services.AddSingleton(node.Acceptor);
builder.Services.AddSingleton(node.Registers);
await using var app = builder.Build();
app.UseCors();
app.MapGrpcService<AcceptorGrpcService>();
app.MapGrpcService<RegisterGrpcService>();
app.MapRegisterApi(maxRequestBodyBytes: options.MaxRequestBodyBytes).RequireCors(AdminEndpoints.CorsPolicy);
app.MapAdminApi(node);
app.MapGet("/health/live", () => Results.Text("Serving register requests."));
await app.RunAsync();
