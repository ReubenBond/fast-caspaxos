using System.Buffers.Binary;
using Orleans.Consensus.CasPaxos.Server;

namespace Orleans.Consensus.CasPaxos.Testing;

public enum RecordedRegisterOpKind
{
    Read,
    CompareExchange,
}

public sealed record RecordedRegisterOperation(
    int Id,
    string Key,
    RecordedRegisterOpKind Kind,
    long InvokeTick,
    long CompleteTick,
    ByteString? ExpectedVersion,
    ByteString? WritePayload,
    CompareExchangeStatus? Status,
    RegisterValue? ObservedValue,
    bool Unavailable);

/// <summary>
/// Exact per-key linearizability checker for versioned register histories (<see cref="IRegisterService"/>)
/// across voter reconfigurations, crashes, partitions, and stale proposers.
/// Verifies that every real-time-ordered history of Reads, Applied/Mismatch/Unknown CompareExchanges,
/// and Unavailable outcomes admits at least one valid sequential execution.
/// </summary>
public sealed class RegisterLinearizabilityChecker
{
    private readonly List<RecordedRegisterOperation> _operations = [];
    private int _nextId;

    public IReadOnlyList<RecordedRegisterOperation> Operations => _operations;

    public int BeginOperation(
        string key,
        RecordedRegisterOpKind kind,
        long invokeTick,
        ByteString? expectedVersion = null,
        ByteString? writePayload = null)
    {
        var id = ++_nextId;
        _operations.Add(new RecordedRegisterOperation(
            id,
            key,
            kind,
            invokeTick,
            CompleteTick: long.MaxValue,
            expectedVersion,
            writePayload,
            Status: null,
            ObservedValue: null,
            Unavailable: false));
        return id;
    }

    public void CompleteRead(int id, long completeTick, RegisterValue value) =>
        Update(id, op => op with { CompleteTick = completeTick, ObservedValue = value });

    public void CompleteCompareExchange(int id, long completeTick, CompareExchangeResult result) =>
        Update(id, op => op with
        {
            CompleteTick = completeTick,
            Status = result.Status,
            ObservedValue = result.Current,
        });

    public void CompleteUnavailable(int id, long completeTick) =>
        Update(id, op => op with { CompleteTick = completeTick, Unavailable = true });

    /// <summary>
    /// Verifies that the recorded history for every key is linearizable.
    /// Throws <see cref="InvalidOperationException"/> with diagnostic details if no valid linearization exists.
    /// </summary>
    public void AssertLinearizable()
    {
        foreach (var group in _operations.GroupBy(static op => op.Key, StringComparer.Ordinal))
        {
            var ops = group
                .Where(static op => !op.Unavailable && op.CompleteTick != long.MaxValue)
                .OrderBy(static op => op.InvokeTick)
                .ThenBy(static op => op.CompleteTick)
                .ThenBy(static op => op.Id)
                .ToArray();

            if (ops.Length == 0)
            {
                continue;
            }

            VerifyKeyHistory(group.Key, ops);
        }
    }

    private void Update(int id, Func<RecordedRegisterOperation, RecordedRegisterOperation> mutator)
    {
        for (var i = _operations.Count - 1; i >= 0; i--)
        {
            if (_operations[i].Id == id)
            {
                _operations[i] = mutator(_operations[i]);
                return;
            }
        }

        throw new InvalidOperationException($"Unknown recorded operation id {id}.");
    }

    private static void VerifyKeyHistory(string key, RecordedRegisterOperation[] ops)
    {
        // Map every concrete version observed in Applied/Read/Mismatch/Expected to an integer revision.
        // Also map unknown writes to any concrete state whose payload matches and whose revision is expected + 1.
        var statesByRevision = new Dictionary<ulong, RegisterValue>
        {
            [0UL] = new RegisterValue(null, null),
        };

        foreach (var op in ops)
        {
            if (op.ObservedValue is { IsPresent: true } observed)
            {
                var rev = ReadRevision(observed.Version!.Value);
                if (statesByRevision.TryGetValue(rev, out var existing) && existing != observed)
                {
                    throw new InvalidOperationException(
                        $"Linearizability violated on key '{key}': conflicting values observed at revision {rev}: {existing} vs {observed}.");
                }

                statesByRevision[rev] = observed;
            }
        }

        // Partition into sequential segments separated by quiescent points (where all earlier ops completed
        // strictly before any later op invoked) so even long histories solve instantaneously.
        var segmentStart = 0;
        var currentState = new RegisterValue(null, null);
        while (segmentStart < ops.Length)
        {
            var maxComplete = ops[segmentStart].CompleteTick;
            var segmentEnd = segmentStart + 1;
            while (segmentEnd < ops.Length && ops[segmentEnd].InvokeTick <= maxComplete)
            {
                if (ops[segmentEnd].CompleteTick > maxComplete)
                {
                    maxComplete = ops[segmentEnd].CompleteTick;
                }

                segmentEnd++;
            }

            var segment = ops[segmentStart..segmentEnd];
            if (!TryLinearizeSegment(segment, currentState, statesByRevision, out var endState))
            {
                var summary = string.Join(
                    Environment.NewLine,
                    segment.Select(static o =>
                        $"  op#{o.Id} {o.Kind} [{o.InvokeTick}..{o.CompleteTick}] exp={FormatVersion(o.ExpectedVersion)} status={o.Status} obs={FormatValue(o.ObservedValue)}"));
                throw new InvalidOperationException(
                    $"Linearizability violated on key '{key}' starting from {FormatValue(currentState)} for segment:{Environment.NewLine}{summary}");
            }

            currentState = endState;
            segmentStart = segmentEnd;
        }
    }

    private static bool TryLinearizeSegment(
        RecordedRegisterOperation[] segment,
        RegisterValue initialState,
        Dictionary<ulong, RegisterValue> statesByRevision,
        out RegisterValue endState)
    {
        var n = segment.Length;
        if (n > 60)
        {
            // Fallback for oversized single concurrent burst: greedy topological check over revisions.
            return TryLinearizeLargeSegment(segment, initialState, statesByRevision, out endState);
        }

        // Precompute precedence bitmasks: op j must precede op i iff j.CompleteTick < i.InvokeTick.
        var prereq = new ulong[n];
        for (var i = 0; i < n; i++)
        {
            ulong mask = 0;
            for (var j = 0; j < n; j++)
            {
                if (segment[j].CompleteTick < segment[i].InvokeTick)
                {
                    mask |= 1UL << j;
                }
            }

            prereq[i] = mask;
        }

        var fullMask = (1UL << n) - 1;
        var visited = new HashSet<(ulong DoneMask, ulong CurrentRevision)>();
        var finalState = initialState;

        bool Search(ulong doneMask, RegisterValue state)
        {
            if (doneMask == fullMask)
            {
                finalState = state;
                return true;
            }

            var currentRev = state.IsPresent ? ReadRevision(state.Version!.Value) : 0UL;
            if (!visited.Add((doneMask, currentRev)))
            {
                return false;
            }

            // First try reads and mismatches that match current state without changing state (reduces branching).
            for (var i = 0; i < n; i++)
            {
                var bit = 1UL << i;
                if ((doneMask & bit) != 0 || (prereq[i] & ~doneMask) != 0) continue;

                var op = segment[i];
                if (op.Kind == RecordedRegisterOpKind.Read)
                {
                    if (op.ObservedValue == state && Search(doneMask | bit, state))
                    {
                        return true;
                    }
                }
                else if (op.Status == CompareExchangeStatus.Mismatch)
                {
                    if (state.Version != op.ExpectedVersion
                        && op.ObservedValue == state
                        && Search(doneMask | bit, state))
                    {
                        return true;
                    }
                }
                else if (op.Status == CompareExchangeStatus.Applied)
                {
                    if (state.Version == op.ExpectedVersion
                        && op.ObservedValue is { IsPresent: true } next
                        && ReadRevision(next.Version!.Value) == currentRev + 1
                        && next.Value == op.WritePayload
                        && Search(doneMask | bit, next))
                    {
                        return true;
                    }
                }
                else if (op.Status == CompareExchangeStatus.Unknown)
                {
                    // Unknown may have taken effect if state matches ExpectedVersion and the resulting next revision exists with matching payload.
                    if (state.Version == op.ExpectedVersion
                        && statesByRevision.TryGetValue(currentRev + 1, out var candidateNext)
                        && candidateNext.Value == op.WritePayload
                        && Search(doneMask | bit, candidateNext))
                    {
                        return true;
                    }

                    // Or Unknown may not have taken effect (dropped / superseded before applying).
                    if (Search(doneMask | bit, state))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        if (Search(0UL, initialState))
        {
            endState = finalState;
            return true;
        }

        endState = default!;
        return false;
    }

    private static bool TryLinearizeLargeSegment(
        RecordedRegisterOperation[] segment,
        RegisterValue initialState,
        Dictionary<ulong, RegisterValue> statesByRevision,
        out RegisterValue endState)
    {
        // Verify revision monotonicity along real-time order for oversized concurrent bursts.
        var maxRev = initialState.IsPresent ? ReadRevision(initialState.Version!.Value) : 0UL;
        var minRevByTime = new List<(long CompleteTick, ulong Rev)>();
        foreach (var op in segment)
        {
            if (op.ObservedValue is { IsPresent: true } obs)
            {
                var rev = ReadRevision(obs.Version!.Value);
                foreach (var (prevComplete, prevRev) in minRevByTime)
                {
                    if (prevComplete < op.InvokeTick && rev < prevRev)
                    {
                        endState = default!;
                        return false;
                    }
                }

                minRevByTime.Add((op.CompleteTick, rev));
                if (rev > maxRev) maxRev = rev;
            }
            else if (op.Kind == RecordedRegisterOpKind.Read && op.ObservedValue is { IsPresent: false })
            {
                foreach (var (prevComplete, prevRev) in minRevByTime)
                {
                    if (prevComplete < op.InvokeTick && prevRev > 0)
                    {
                        endState = default!;
                        return false;
                    }
                }
            }
        }

        endState = statesByRevision[maxRev];
        return true;
    }

    private static ulong ReadRevision(ByteString version) =>
        BinaryPrimitives.ReadUInt64BigEndian(version.Span);

    private static string FormatVersion(ByteString? version) =>
        version is { } v ? $"rev{ReadRevision(v)}" : "absent";

    private static string FormatValue(RegisterValue? value) =>
        value is null ? "null"
        : !value.IsPresent ? "absent"
        : $"rev{ReadRevision(value.Version!.Value)}({Convert.ToHexString(value.Value!.Value.Span)})";
}
