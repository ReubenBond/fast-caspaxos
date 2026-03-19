using FastCASPaxos.Messages;
using FastCASPaxos.Model;
using FastCASPaxos.Protocol;
using FastCASPaxos.Simulation;
using FastCASPaxos.Simulation.Contracts;

namespace FastCASPaxos.Simulation.Scenarios;

public static class SimulationScenarioRunner
{
    public static OperationDriverCompletion<TValue, FastCasAddress> RunPlannedWork<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        IEnumerable<KeyValuePair<FastCasAddress, List<IOperation<TValue>>>> plannedWork,
        Func<IOperation<TValue>>? createReadOperation = null,
        int maxIterationsPerResponse = 1000,
        int maxReadReconciliationRounds = 5)
        where TValue : IVersionedValue<TValue>
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(plannedWork);

        var workItems = plannedWork.ToList();
        var driver = new ScenarioOperationDriver<TValue>(cluster, cluster.ClientAddress, workItems.Select(item => item.Key));
        var reconciliationRounds = 0;
        foreach (var (proposer, operations) in workItems)
        {
            driver.EnqueueRange(proposer, operations);
        }

        var handledResponses = new HashSet<int>();
        driver.StartReadyRequests();

        while (true)
        {
            var progressed = cluster.RunUntil(
                () => cluster.ClientResponses.Keys.Any(requestId => !handledResponses.Contains(requestId)),
                maxIterationsPerResponse);

            if (!progressed)
            {
                throw new InvalidOperationException(cluster.GetFailureDiagnostics(nameof(RunPlannedWork)));
            }

            var readyResponses = cluster.ClientResponses.Values
                .Where(response => handledResponses.Add(response.Round))
                .OrderBy(response => response.Round)
                .ToList();

            foreach (var response in readyResponses)
            {
                driver.ResetRecording();
                driver.HandleResponse(response);
                cluster.AssertSafetyInvariants();

                if (driver.LastCompletion is not { } completion)
                {
                    continue;
                }

                if (completion.FinalValuesAgree || createReadOperation is null)
                {
                    return completion;
                }

                if (++reconciliationRounds > maxReadReconciliationRounds)
                {
                    throw new InvalidOperationException(
                        $"Scenario failed to converge after {maxReadReconciliationRounds} reconciliation read rounds.{Environment.NewLine}" +
                        $"{DescribeCompletion(completion)}{Environment.NewLine}" +
                        cluster.GetFailureDiagnostics(nameof(RunPlannedWork)));
                }

                foreach (var proposer in driver.Proposers)
                {
                    driver.Enqueue(proposer, createReadOperation());
                }

                driver.StartReadyRequests();
            }
        }
    }

    public static Dictionary<FastCasAddress, List<IOperation<TValue>>> DistributeRoundRobin<TValue>(
        IReadOnlyList<FastCasAddress> proposers,
        IEnumerable<IOperation<TValue>> operations)
    {
        ArgumentNullException.ThrowIfNull(proposers);
        ArgumentNullException.ThrowIfNull(operations);

        Dictionary<FastCasAddress, List<IOperation<TValue>>> plan = [];
        foreach (var proposer in proposers)
        {
            plan[proposer] = [];
        }

        var index = 0;
        foreach (var operation in operations)
        {
            plan[proposers[index % proposers.Count]].Add(operation);
            index++;
        }

        return plan;
    }

    public static Dictionary<FastCasAddress, List<IOperation<TValue>>> DistributeRandom<TValue>(
        IReadOnlyList<FastCasAddress> proposers,
        IEnumerable<IOperation<TValue>> operations,
        Random random)
    {
        ArgumentNullException.ThrowIfNull(proposers);
        ArgumentNullException.ThrowIfNull(operations);
        ArgumentNullException.ThrowIfNull(random);

        Dictionary<FastCasAddress, List<IOperation<TValue>>> plan = [];
        foreach (var proposer in proposers)
        {
            plan[proposer] = [];
        }

        foreach (var operation in operations)
        {
            plan[proposers[random.Next(proposers.Count)]].Add(operation);
        }

        return plan;
    }

    private static string DescribeCompletion<TValue>(
        OperationDriverCompletion<TValue, FastCasAddress> completion)
    {
        var lines = new List<string>
        {
            $"latest={completion.LatestValue}",
        };

        foreach (var proposer in completion.FinalValues.OrderBy(entry => entry.Key.NetworkAddress))
        {
            lines.Add(
                $"{proposer.Key}: final={proposer.Value?.ToString() ?? "<missing>"}");
        }

        return string.Join(Environment.NewLine, lines);
    }

    private sealed class ScenarioOperationDriver<TValue>(
        FastCasSimulationCluster<TValue> cluster,
        FastCasAddress caller,
        IEnumerable<FastCasAddress> proposers) : OperationDriver<TValue, FastCasAddress>(caller, proposers)
        where TValue : IVersionedValue<TValue>
    {
        private readonly FastCasSimulationCluster<TValue> _cluster = cluster;

        public OperationDriverCompletion<TValue, FastCasAddress>? LastCompletion { get; private set; }

        public void ResetRecording()
        {
            LastCompletion = null;
        }

        protected override void OnSendProposal(ScheduledProposal<TValue, FastCasAddress> proposal) =>
            _cluster.SendProposal(
                proposal.Proposer,
                proposal.Operation);

        protected override void OnCompleted(OperationDriverCompletion<TValue, FastCasAddress> completion) =>
            LastCompletion = completion;
    }
}
