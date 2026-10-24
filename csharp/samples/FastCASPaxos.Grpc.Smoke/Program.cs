using FastCASPaxos.Grpc;
using FastCASPaxos.Server;
using Grpc.Net.Client;

if (args.Length < 3 || args[0] is not ("write" or "verify"))
    throw new ArgumentException("Usage: FastCASPaxos.Grpc.Smoke <write|verify> <key> <endpoint> [<endpoint> ...]");

var key = args[1];
var channels = args.Skip(2).Select(endpoint => GrpcChannel.ForAddress(endpoint)).ToArray();
try
{
    var clients = channels.Select(channel => new GrpcRegisterService(channel.CreateCallInvoker())).ToArray();
    if (args[0] == "write")
    {
        var initial = await clients[0].ReadAsync(key);
        Require(!initial.IsPresent && initial.Value is null, "The smoke key must start absent.");
        var created = await clients[0].CompareExchangeAsync(key, null, ByteString.Empty);
        Require(created.Status == CompareExchangeStatus.Applied && created.Current is { IsPresent: true },
            $"Create failed: {created.Status}, {created.Error}");
        var mismatch = await clients[^1].CompareExchangeAsync(key, null, new ByteString([1]));
        Require(mismatch.Status == CompareExchangeStatus.Mismatch
            && Equals(mismatch.Current?.Version, created.Current!.Version), "The stale condition must mismatch.");
        var updated = await clients[^1].CompareExchangeAsync(key, created.Current!.Version, ByteString.Empty);
        Require(updated.Status == CompareExchangeStatus.Applied
            && !Equals(updated.Current?.Version, created.Current.Version), "Identical bytes must receive a fresh version.");
    }

    foreach (var client in clients)
    {
        var value = await client.ReadAsync(key);
        Require(value.IsPresent && value.Value?.Length == 0, "The retained register must contain a present empty value.");
        var page = await client.ListKeysAsync(prefix: key, limit: 1);
        Require(page.Keys.Length == 1 && page.Keys[0] == key, "Listing must discover the written key.");
    }
    Console.WriteLine($"Smoke succeeded: {args[0]} '{key}' through {clients.Length} endpoint(s).");
}
finally
{
    foreach (var channel in channels) channel.Dispose();
}

static void Require(bool condition, string message)
{
    if (!condition) throw new InvalidOperationException(message);
}
