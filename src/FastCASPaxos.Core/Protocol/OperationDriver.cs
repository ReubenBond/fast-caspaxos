using FastCASPaxos.Messages;
using FastCASPaxos.Model;

namespace FastCASPaxos.Protocol;

/// <summary>
/// Orchestrates a client-side work queue across multiple proposers while keeping at most one in-flight request per proposer.
/// </summary>
public abstract class OperationDriver<TValue, TRoute>
    where TRoute : notnull
{
    private readonly TRoute _caller;
    private readonly List<TRoute> _proposerOrder;
    private readonly Dictionary<TRoute, ProposerWork> _proposers;
    private readonly Dictionary<TRoute, LatestResponse> _latestResponses = [];

    public OperationDriver(TRoute caller, IEnumerable<TRoute> proposers)
    {
        ArgumentNullException.ThrowIfNull(proposers);

        _caller = caller;
        _proposerOrder = [];
        _proposers = [];
        foreach (var proposer in proposers)
        {
            if (!_proposers.TryAdd(proposer, new ProposerWork()))
            {
                throw new InvalidOperationException($"Duplicate proposer route '{proposer}' is not allowed.");
            }

            _proposerOrder.Add(proposer);
        }
    }

    public IReadOnlyList<TRoute> Proposers => _proposerOrder;

    public bool HasOutstandingWork => _proposers.Values.Any(state => state.Pending.Count > 0 || state.HasScheduledRequest);

    public void Enqueue(TRoute proposer, IOperation<TValue> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        GetWork(proposer).Pending.Enqueue(operation);
    }

    public void EnqueueRange(TRoute proposer, IEnumerable<IOperation<TValue>> operations)
    {
        ArgumentNullException.ThrowIfNull(operations);
        foreach (var operation in operations)
        {
            Enqueue(proposer, operation);
        }
    }

    public IReadOnlyList<IOperation<TValue>> GetPendingOperations(TRoute proposer) =>
        [.. GetWork(proposer).Pending];

    /// <summary>
    /// Starts every proposer that is idle and has queued work.
    /// </summary>
    public void StartReadyRequests()
    {
        foreach (var proposal in StartReadyRequestsCore())
        {
            OnSendProposal(proposal);
        }
    }

    /// <summary>
    /// Records a proposer response and schedules any newly unblocked work.
    /// </summary>
    public void HandleResponse(RoutedProposeResponse<TValue, TRoute> response)
    {
        var work = GetWork(response.Proposer);
        if (!work.HasScheduledRequest)
        {
            throw new InvalidOperationException($"Unable to handle response for round {response.Round} from proposer '{response.Proposer}' with no scheduled request.");
        }

        RecordLatestResponse(response);

        foreach (var proposal in StartReadyRequestsCore())
        {
            OnSendProposal(proposal);
        }

        if (!HasOutstandingWork)
        {
            OnCompleted(CreateCompletion());
        }
    }

    protected abstract void OnSendProposal(ScheduledProposal<TValue, TRoute> proposal);
    protected abstract void OnCompleted(OperationDriverCompletion<TValue, TRoute> completion);

    private List<ScheduledProposal<TValue, TRoute>> StartReadyRequestsCore()
    {
        List<ScheduledProposal<TValue, TRoute>> requests = [];
        foreach (var proposer in _proposerOrder)
        {
            var work = _proposers[proposer];
            if (work.HasScheduledRequest || !work.Pending.TryDequeue(out var operation))
            {
                continue;
            }

            requests.Add(Schedule(proposer, operation));
        }

        return requests;
    }

    private ScheduledProposal<TValue, TRoute> Schedule(TRoute proposer, IOperation<TValue> operation)
    {
        var scheduledOperation = CreateRequestOperation(operation);
        GetWork(proposer).HasScheduledRequest = true;
        return new ScheduledProposal<TValue, TRoute>(proposer, scheduledOperation);
    }

    private IOperation<TValue> CreateRequestOperation(IOperation<TValue> operation)
    {
        if (operation is IRoutedOperation<TRoute> routedOperation)
        {
            if (!EqualityComparer<TRoute>.Default.Equals(routedOperation.Caller, _caller))
            {
                throw new InvalidOperationException(
                    $"Operation '{operation}' is already routed to caller '{routedOperation.Caller}', but this driver schedules work for caller '{_caller}'.");
            }

            return operation;
        }

        return new RoutedOperation<TValue, TRoute>(_caller, operation);
    }

    private void RecordLatestResponse(RoutedProposeResponse<TValue, TRoute> response)
    {
        GetWork(response.Proposer).HasScheduledRequest = false;
        
        if (!_latestResponses.TryGetValue(response.Proposer, out var existing))
        {
            _latestResponses[response.Proposer] = new LatestResponse(response.Round, response.CommittedValue);
            return;
        }

        // Keep the most recent response per proposer (highest round wins).
        if (existing.Round < response.Round)
        {
            _latestResponses[response.Proposer] = new LatestResponse(response.Round, response.CommittedValue);
        }
        // Else: keep existing (older response with same or higher round)
    }

    private OperationDriverCompletion<TValue, TRoute> CreateCompletion()
    {
        Dictionary<TRoute, TValue> finalValues = [];
        var latestValue = default(TValue)!;
        var firstValue = default(TValue)!;
        var firstValueSet = false;
        var finalValuesAgree = true;

        foreach (var (proposer, response) in _latestResponses)
        {
            var finalValue = response.CommittedValue;
            finalValues[proposer] = finalValue;

            if (firstValueSet)
            {
                if (!EqualityComparer<TValue>.Default.Equals(firstValue, finalValue))
                {
                    finalValuesAgree = false;
                }
            }
            else
            {
                firstValue = finalValue;
                firstValueSet = true;
            }

            latestValue = finalValue;
        }

        return new OperationDriverCompletion<TValue, TRoute>
        {
            FinalValues = finalValues,
            LatestValue = latestValue,
            FinalValuesAgree = finalValuesAgree,
        };
    }

    private ProposerWork GetWork(TRoute proposer)
    {
        if (!_proposers.TryGetValue(proposer, out var work))
        {
            throw new InvalidOperationException($"Unknown proposer route '{proposer}'.");
        }

        return work;
    }

    private sealed class ProposerWork
    {
        public Queue<IOperation<TValue>> Pending { get; } = new();

        public bool HasScheduledRequest { get; set; }
    }

    private readonly record struct LatestResponse(int Round, TValue CommittedValue);
}
