using System.Collections.Immutable;
using System.Net.Http.Json;

namespace FastCASPaxos.Host;

public sealed partial class NodeRuntime
{
    private readonly SemaphoreSlim _clusterAdministration = new(1);
    private readonly HttpClient _adminHttp = new() { Timeout = TimeSpan.FromSeconds(30) };

    public async Task<ClusterAdminResult> ApplyClusterSettingsAsync(
        ProposerSettings settings, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings.CreatePolicy();
        EnsureCoordinator();
        await _clusterAdministration.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_admin!.State.ClusterReset is { Phase: not "complete" })
                throw new InvalidOperationException("Resume the pending reset before changing cluster settings.");
            var results = await Task.WhenAll(Peers.Select(peer =>
                ContactAdminAsync(peer, "settings", settings,
                    () => ApplySettingsAsync(settings, cancellationToken), cancellationToken))).ConfigureAwait(false);
            return new(results.All(result => result.Error is null) ? "complete" : "incomplete", [.. results]);
        }
        finally { _clusterAdministration.Release(); }
    }

    public async Task<ClusterAdminResult> ResetClusterAsync(
        ResetRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        request.Validate();
        EnsureCoordinator();
        await _clusterAdministration.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var state = _admin!.State;
            var progress = state.ClusterReset;
            if (progress?.Request != request)
            {
                if (progress?.Request.OperationId == request.OperationId
                    || progress is { Phase: not "complete" } || state.Epoch != request.ExpectedEpoch
                    || state.Phase != "active")
                    throw new InvalidOperationException("Resume the pending reset or refresh the cluster epoch before starting another reset.");
                progress = new(request, "prepare", [.. Peers.Select(peer => new AdminNodeResult(peer.Id, "pending"))]);
                _admin.Update(current => current with { ClusterReset = progress });
            }
            while (progress.Phase != "complete")
            {
                var phase = progress.Phase;
                await Task.WhenAll(Peers.Select(async peer =>
                {
                    var result = await ContactAdminAsync(peer, "reset/" + phase, request,
                        () => ApplyResetPhaseAsync(request, phase, cancellationToken), cancellationToken).ConfigureAwait(false);
                    _admin.Update(current => current with
                    {
                        ClusterReset = current.ClusterReset! with
                        {
                            Nodes = [.. current.ClusterReset.Nodes.Select(node => node.NodeId == peer.Id ? result : node)],
                        },
                    });
                })).ConfigureAwait(false);
                progress = _admin.State.ClusterReset!;
                if (progress.Nodes.Any(node => node.Error is not null))
                    return new("incomplete", progress.Nodes, request);
                // Persist each all-member barrier before dispatching the next phase.
                progress = progress with
                {
                    Phase = phase switch { "prepare" => "clear", "clear" => "activate", _ => "complete" },
                    Nodes = phase == "activate" ? progress.Nodes : [.. Peers.Select(peer => new AdminNodeResult(peer.Id, "pending"))],
                };
                _admin.Update(current => current with { ClusterReset = progress });
            }
            return new("complete", progress.Nodes, request);
        }
        finally { _clusterAdministration.Release(); }
    }

    private void EnsureCoordinator()
    {
        EnsureAdministrable();
        if (Diagnostics.NodeId != CoordinatorId)
            throw new InvalidOperationException($"Send cluster administration commands to coordinator '{CoordinatorId}'.");
    }

    private async Task<AdminNodeResult> ContactAdminAsync<T>(
        AdminPeer peer, string path, T body, Func<Task<NodeAdministration>> local, CancellationToken cancellationToken)
    {
        try
        {
            NodeAdministration status;
            if (peer.Id == Diagnostics.NodeId)
                status = await local().WaitAsync(_adminHttp.Timeout, cancellationToken).ConfigureAwait(false);
            else
            {
                using var response = await _adminHttp.PostAsJsonAsync(
                    peer.HttpEndpoint + "/v1/admin/internal/" + path, body, cancellationToken).ConfigureAwait(false);
                if (!response.IsSuccessStatusCode)
                    throw new HttpRequestException($"HTTP {(int)response.StatusCode}: {await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false)}");
                status = await response.Content.ReadFromJsonAsync<NodeAdministration>(cancellationToken).ConfigureAwait(false)
                    ?? throw new InvalidDataException("The node returned an empty administration response.");
            }
            if (status.NodeId != peer.Id || status.SchemaVersion != 1)
                throw new InvalidDataException($"Expected administration status from node '{peer.Id}'.");
            return new(peer.Id, status.State);
        }
        catch (Exception error) when (error is HttpRequestException or OperationCanceledException
            or TimeoutException or InvalidOperationException or IOException or UnauthorizedAccessException
            or Microsoft.Data.Sqlite.SqliteException or System.Text.Json.JsonException)
        {
            Diagnostics.Publish("administration", new { nodeId = peer.Id, command = path, error = error.Message });
            return new(peer.Id, "unconfirmed", error.Message + " Refresh status and retry the same operation.");
        }
    }
}
