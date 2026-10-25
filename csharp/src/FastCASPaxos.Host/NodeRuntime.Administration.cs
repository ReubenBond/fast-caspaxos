using FastCASPaxos.Messages;
using FastCASPaxos.Protocol;
using FastCASPaxos.Server;
using FastCASPaxos.Server.Diagnostics;
using System.Buffers.Binary;
using System.Globalization;

namespace FastCASPaxos.Host;

public sealed partial class NodeRuntime
{
    public string CoordinatorId => Peers.MinBy(peer => peer.Id, StringComparer.Ordinal)!.Id;
    public NodeAdministration Administration
    {
        get
        {
            var state = _admin!.State;
            return new(1, Diagnostics.NodeId, CoordinatorId, state.Epoch, state.Phase,
                state.Settings, state.PendingReset, state.ClusterReset);
        }
    }

    public IDisposable EnterProtocol(string epoch)
    {
        var acceptor = _epochAcceptor ?? throw new InvalidOperationException("The node is not open.");
        if (acceptor.Epoch != epoch)
            throw new RegisterUnavailableException(new InvalidOperationException("The peer epoch is stale."));
        return acceptor.Admission.Enter();
    }

    public async Task<NodeAdministration> ApplySettingsAsync(ProposerSettings settings, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(settings);
        settings = settings with { Mode = settings.CreatePolicy().Name };
        await _administration.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureAdministrable();
            if (_admin!.State.Phase != "active")
                throw new InvalidOperationException("Complete the pending cluster reset before changing settings.");
            // Once admitted, finish the durable swap even if the HTTP caller disconnects.
            var previous = GetRegisters();
            _admin.Update(state => state with { Settings = settings });
            await previous.Admission.CloseAsync().ConfigureAwait(false);
            await previous.Core.DisposeAsync().ConfigureAwait(false);
            OpenRegisters(_admin.State.Epoch, settings);
            Diagnostics.Publish("reset", new ResetObservation("proposer-settings-changed"));
            return Administration;
        }
        finally { _administration.Release(); }
    }

    public async Task<NodeAdministration> ApplyResetPhaseAsync(
        ResetRequest request, string phase, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        request.Validate();
        if (phase is not ("prepare" or "clear" or "activate"))
            throw new ArgumentException("Expected prepare, clear, or activate.", nameof(phase));
        await _administration.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            EnsureAdministrable();
            var state = _admin!.State;
            if (state.LastReset == request && state.Phase == "active")
                return Administration; // A lost activation acknowledgement is safe to replay.
            if (state.LastReset?.OperationId == request.OperationId
                || state.Epoch != request.ExpectedEpoch || (state.PendingReset is not null && state.PendingReset != request))
                throw new InvalidOperationException("The reset conflicts with this node's durable epoch or pending reset.");
            switch (phase)
            {
                case "prepare":
                    if (state.Phase is "prepared" or "cleared") return Administration;
                    _admin.Update(current => current with { Phase = "preparing", PendingReset = request });
                    await QuiesceGenerationAsync().ConfigureAwait(false);
                    _admin.Update(current => current with { Phase = "prepared" });
                    break;
                case "clear":
                    if (state.Phase == "cleared") return Administration;
                    if (state.Phase != "prepared") throw new InvalidOperationException("Prepare this node before clearing.");
                    await _acceptor!.ClearAsync(CancellationToken.None).ConfigureAwait(false);
                    _admin.Update(current => current with { Phase = "cleared" });
                    break;
                case "activate":
                    if (state.Phase != "cleared") throw new InvalidOperationException("Clear this node before activation.");
                    RetireGeneration();
                    _admin.Update(current => current with
                    {
                        Epoch = request.NextEpoch, Phase = "active", PendingReset = null, LastReset = request,
                    });
                    OpenGeneration(request.NextEpoch);
                    break;
            }
            Diagnostics.Publish("reset", new ResetObservation("cluster-" + phase));
            return Administration;
        }
        finally { _administration.Release(); }
    }

    private void EnsureAdministrable()
    {
        if (_stopping || _disposal is not null)
            throw new InvalidOperationException("The node is stopping.");
    }

    private async Task QuiesceGenerationAsync()
    {
        var registers = GetRegisters();
        var serving = registers.Admission.CloseAsync();
        var protocol = _epochAcceptor!.Admission.CloseAsync();
        _generationStopping.Cancel();
        _learningStopping.Cancel();
        await registers.Core.DisposeAsync().ConfigureAwait(false);
        await Task.WhenAll(serving, protocol, Task.WhenAll(_learners.Select(peer => peer.Worker))).ConfigureAwait(false);
    }

    private void RetireGeneration()
    {
        _repair!.Dispose();
        foreach (var channel in _channels) channel.Dispose();
        _channels.Clear();
        _remoteSchedulers.Clear();
        _learners.Clear();
        _learningStopping.Dispose();
        _generationStopping.Dispose();
        _learningStopping = new();
        _generationStopping = new();
        while (_staged.Reader.TryRead(out _)) { }
    }

    private RegisterGeneration GetRegisters()
    {
        return Volatile.Read(ref _registerGeneration) ?? throw new InvalidOperationException("The node is not open.");
    }

    private RegisterGeneration GetServingRegisters()
    {
        ObjectDisposedException.ThrowIf(IsDraining || _disposal is not null, this);
        return GetRegisters();
    }

    private sealed record RegisterGeneration(string Epoch, IRegisterService Service, ReplicatedRegisterService Core)
    {
        public NodeAdmission Admission { get; } = new();
        private readonly byte[] _prefix = VersionPrefix(Epoch);

        private static byte[] VersionPrefix(string epoch)
        {
            if (epoch == NodeAdminState.InitialEpoch) return [];
            var bytes = new byte[8];
            BinaryPrimitives.WriteUInt64BigEndian(bytes, ulong.Parse(epoch, CultureInfo.InvariantCulture));
            return bytes;
        }

        public RegisterValue Export(RegisterValue value) => value.Version is { } version && _prefix.Length > 0
            ? value with { Version = new ByteString([.. _prefix, .. version.ToArray()]) } : value;

        public bool TryImport(ByteString? version, out ByteString? imported)
        {
            imported = version;
            if (version is null || _prefix.Length == 0) return true;
            if (version.Value.IsDefault) throw new ArgumentException("Initialized version bytes are required.", nameof(version));
            var bytes = version.Value.ToArray();
            if (bytes.Length <= _prefix.Length || !bytes.AsSpan(0, _prefix.Length).SequenceEqual(_prefix)) return false;
            imported = new ByteString(bytes.AsSpan(_prefix.Length));
            return true;
        }
    }

    private sealed class RuntimeRegisters(NodeRuntime node) : IRegisterService
    {
        public async ValueTask<RegisterValue> ReadAsync(string key, CancellationToken cancellationToken = default)
        {
            var generation = node.GetServingRegisters();
            using var lease = generation.Admission.Enter();
            return generation.Export(await generation.Service.ReadAsync(key, cancellationToken).ConfigureAwait(false));
        }

        public async ValueTask<CompareExchangeResult> CompareExchangeAsync(
            string key, ByteString? expectedVersion, ByteString value, CancellationToken cancellationToken = default)
        {
            if (value.IsDefault) throw new ArgumentException("Initialized value bytes are required.", nameof(value));
            var generation = node.GetServingRegisters();
            using var lease = generation.Admission.Enter();
            if (!generation.TryImport(expectedVersion, out var imported))
                return new(CompareExchangeStatus.Mismatch,
                    generation.Export(await generation.Service.ReadAsync(key, cancellationToken).ConfigureAwait(false)));
            var result = await generation.Service.CompareExchangeAsync(key, imported, value, cancellationToken).ConfigureAwait(false);
            return result.Current is { } current ? result with { Current = generation.Export(current) } : result;
        }

        public async ValueTask<RegisterKeyPage> ListKeysAsync(
            string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default)
        {
            var generation = node.GetServingRegisters();
            using var lease = generation.Admission.Enter();
            return await generation.Service.ListKeysAsync(prefix, afterKey, limit, cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class RuntimeAcceptor(NodeRuntime node) : IKeyedAcceptor
    {
        private IKeyedAcceptor Current => node._observedAcceptor ?? throw new InvalidOperationException("The node is not open.");
        public ValueTask<AcceptorReadResult> ReadAsync(string key, AcceptedDigest? known = null, CancellationToken cancellationToken = default) =>
            Current.ReadAsync(key, known, cancellationToken);
        public ValueTask<AcceptorPrepareResult<ByteString?>> PrepareAsync(string key, PrepareRequest request, CancellationToken cancellationToken = default) =>
            Current.PrepareAsync(key, request, cancellationToken);
        public ValueTask<AcceptorAcceptResult> AcceptAsync(string key, AcceptRequest<ByteString> request, CancellationToken cancellationToken = default) =>
            Current.AcceptAsync(key, request, cancellationToken);
        public ValueTask<KeyPage> ListKeysAsync(string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default) =>
            Current.ListKeysAsync(prefix, afterKey, limit, cancellationToken);
    }

    private sealed class RuntimeDigests(NodeRuntime node) : IAcceptorDigestSource
    {
        public ValueTask<AcceptorDigestPage> ListDigestsAsync(
            string prefix = "", string? afterKey = null, int limit = 100, CancellationToken cancellationToken = default) =>
            node._epochAcceptor!.ListDigestsAsync(prefix, afterKey, limit, cancellationToken);
    }
}
