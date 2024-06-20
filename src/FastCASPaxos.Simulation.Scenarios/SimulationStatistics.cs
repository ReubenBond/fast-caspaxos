using System.Globalization;
using FastCASPaxos.Diagnostics;

namespace FastCASPaxos.Simulation.Scenarios;

internal static class SimulationMetricNames
{
    public const string TransportAttemptedMessages = "fast_caspaxos.simulation.transport.attempted_messages";
    public const string TransportDeliveredMessages = "fast_caspaxos.simulation.transport.delivered_messages";
    public const string TransportDroppedMessages = "fast_caspaxos.simulation.transport.dropped_messages";
    public const string TransportPartitionedMessages = "fast_caspaxos.simulation.transport.partitioned_messages";
    public const string LocalProposalSubmissions = "fast_caspaxos.simulation.local_proposal_submissions";
    public const string DeliveredPrepareRequests = "fast_caspaxos.simulation.transport.delivered_prepare_requests";
    public const string DeliveredPreparePromises = "fast_caspaxos.simulation.transport.delivered_prepare_promises";
    public const string DeliveredPrepareRejections = "fast_caspaxos.simulation.transport.delivered_prepare_rejections";
    public const string DeliveredAcceptRequests = "fast_caspaxos.simulation.transport.delivered_accept_requests";
    public const string DeliveredAcceptAccepted = "fast_caspaxos.simulation.transport.delivered_accept_accepted";
    public const string DeliveredAcceptRejected = "fast_caspaxos.simulation.transport.delivered_accept_rejected";
    public const string LocalProposalCompletions = "fast_caspaxos.simulation.local_proposal_completions";
}

internal sealed record RoundTripDistribution(
    IReadOnlyDictionary<int, int> Histogram,
    long Total,
    int Recorded,
    int Min,
    int Max)
{
    public double PerRecordedProposal => Recorded == 0 ? 0 : (double)Total / Recorded;
}

internal sealed class RoundTripDistributionBuilder
{
    private readonly Dictionary<int, int> _histogram = [];
    private long _total;
    private int _recorded;
    private int _min = int.MaxValue;
    private int _max;

    public void Add(int roundTrips)
    {
        _total += roundTrips;
        _recorded++;
        _min = Math.Min(_min, roundTrips);
        _max = Math.Max(_max, roundTrips);
        _histogram.TryGetValue(roundTrips, out var count);
        _histogram[roundTrips] = count + 1;
    }

    public RoundTripDistribution Build() =>
        new(
            new Dictionary<int, int>(_histogram),
            _total,
            _recorded,
            _recorded == 0 ? 0 : _min,
            _max);
}

public sealed class SimulationStatisticsSummary
{
    private readonly Dictionary<string, long> _rawTotals;
    private readonly RoundTripDistribution _acceptorRoundTrips;
    private readonly RoundTripDistribution _readAcceptorRoundTrips;
    private readonly RoundTripDistribution _writeAcceptorRoundTrips;

    private SimulationStatisticsSummary(
        int runCount,
        int successfulRunCount,
        int failedRunCount,
        Dictionary<string, long> rawTotals,
        RoundTripDistribution acceptorRoundTrips,
        RoundTripDistribution readAcceptorRoundTrips,
        RoundTripDistribution writeAcceptorRoundTrips)
    {
        RunCount = runCount;
        SuccessfulRunCount = successfulRunCount;
        FailedRunCount = failedRunCount;
        _rawTotals = rawTotals;
        _acceptorRoundTrips = acceptorRoundTrips;
        _readAcceptorRoundTrips = readAcceptorRoundTrips;
        _writeAcceptorRoundTrips = writeAcceptorRoundTrips;
    }

    public int RunCount { get; }

    public int SuccessfulRunCount { get; }

    public int FailedRunCount { get; }

    public IReadOnlyDictionary<string, long> RawTotals => _rawTotals;

    public IReadOnlyDictionary<int, int> AcceptorRoundTripHistogram => _acceptorRoundTrips.Histogram;

    public IReadOnlyDictionary<int, int> ReadAcceptorRoundTripHistogram => _readAcceptorRoundTrips.Histogram;

    public IReadOnlyDictionary<int, int> WriteAcceptorRoundTripHistogram => _writeAcceptorRoundTrips.Histogram;

    public long DeliveredMessages => GetTotal(SimulationMetricNames.TransportDeliveredMessages);

    public long DroppedMessages => GetTotal(SimulationMetricNames.TransportDroppedMessages);

    public long PartitionedMessages => GetTotal(SimulationMetricNames.TransportPartitionedMessages);

    public long ProposalAttempts => GetTotal(ProposerDiagnostics.AttemptsMetricName);

    public long ProposalSuccesses => GetTotal(ProposerDiagnostics.SuccessesMetricName);

    public long PrepareAttempts => GetTotal(ProposerDiagnostics.PrepareAttemptsMetricName);

    public long PrepareSuccesses => GetTotal(ProposerDiagnostics.PrepareSuccessesMetricName);

    public long AcceptAttempts => GetTotal(ProposerDiagnostics.AcceptAttemptsMetricName);

    public long AcceptSuccesses => GetTotal(ProposerDiagnostics.AcceptSuccessesMetricName);

    public long Conflicts => GetTotal(ProposerDiagnostics.ConflictsMetricName);

    public long FastRoundAttempts => GetTotal(ProposerDiagnostics.FastRoundAttemptsMetricName);

    public long FastRoundSuccesses => GetTotal(ProposerDiagnostics.FastRoundSuccessesMetricName);

    public double ProposalSuccessRate => Divide(ProposalSuccesses, ProposalAttempts);

    public double PrepareSuccessRate => Divide(PrepareSuccesses, PrepareAttempts);

    public double AcceptSuccessRate => Divide(AcceptSuccesses, AcceptAttempts);

    public double ConflictRate => Divide(Conflicts, ProposalAttempts);

    public double FastRoundUsageRate => Divide(FastRoundAttempts, ProposalAttempts);

    public double FastRoundSuccessRate => Divide(FastRoundSuccesses, FastRoundAttempts);

    public long AcceptorRoundTripsTotal => _acceptorRoundTrips.Total;

    public int AcceptorRoundTripsRecorded => _acceptorRoundTrips.Recorded;

    public int AcceptorRoundTripsMin => _acceptorRoundTrips.Min;

    public int AcceptorRoundTripsMax => _acceptorRoundTrips.Max;

    public long ReadAcceptorRoundTripsTotal => _readAcceptorRoundTrips.Total;

    public int ReadAcceptorRoundTripsRecorded => _readAcceptorRoundTrips.Recorded;

    public int ReadAcceptorRoundTripsMin => _readAcceptorRoundTrips.Min;

    public int ReadAcceptorRoundTripsMax => _readAcceptorRoundTrips.Max;

    public long WriteAcceptorRoundTripsTotal => _writeAcceptorRoundTrips.Total;

    public int WriteAcceptorRoundTripsRecorded => _writeAcceptorRoundTrips.Recorded;

    public int WriteAcceptorRoundTripsMin => _writeAcceptorRoundTrips.Min;

    public int WriteAcceptorRoundTripsMax => _writeAcceptorRoundTrips.Max;

    public double AcceptorRoundTripsPerSuccessfulProposal =>
        _acceptorRoundTrips.PerRecordedProposal;

    public double ReadAcceptorRoundTripsPerSuccessfulRead =>
        _readAcceptorRoundTrips.PerRecordedProposal;

    public double WriteAcceptorRoundTripsPerSuccessfulWrite =>
        _writeAcceptorRoundTrips.PerRecordedProposal;

    public double ProtocolPhasesPerSuccessfulProposal =>
        Divide(PrepareAttempts + AcceptAttempts, ProposalSuccesses);

    public double NetworkMessagesPerSuccessfulProposal =>
        Divide(DeliveredMessages, ProposalSuccesses);

    public static SimulationStatisticsSummary FromRun(SimulationRunResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return FromRuns([result]);
    }

    public static SimulationStatisticsSummary FromBatch(SimulationBatchResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return FromRuns(result.Runs);
    }

    public static SimulationStatisticsSummary FromRuns(IEnumerable<SimulationRunResult> runs)
    {
        ArgumentNullException.ThrowIfNull(runs);

        Dictionary<string, long> totals = new(StringComparer.Ordinal);
        var acceptorRoundTrips = new RoundTripDistributionBuilder();
        var readAcceptorRoundTrips = new RoundTripDistributionBuilder();
        var writeAcceptorRoundTrips = new RoundTripDistributionBuilder();
        var runCount = 0;
        var successfulRunCount = 0;
        var failedRunCount = 0;
        foreach (var run in runs)
        {
            runCount++;
            if (run.Success)
            {
                successfulRunCount++;
            }
            else
            {
                failedRunCount++;
            }

            foreach (var (name, value) in run.Statistics)
            {
                totals.TryGetValue(name, out var current);
                totals[name] = current + value;
            }

            foreach (var roundTripSample in run.ProposalRoundTrips)
            {
                acceptorRoundTrips.Add(roundTripSample.RoundTrips);
                switch (roundTripSample.Kind)
                {
                    case SimulationProposalKind.Read:
                        readAcceptorRoundTrips.Add(roundTripSample.RoundTrips);
                        break;
                    case SimulationProposalKind.Write:
                        writeAcceptorRoundTrips.Add(roundTripSample.RoundTrips);
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(
                            nameof(roundTripSample.Kind),
                            roundTripSample.Kind,
                            null);
                }
            }
        }

        return new SimulationStatisticsSummary(
            runCount,
            successfulRunCount,
            failedRunCount,
            totals,
            acceptorRoundTrips.Build(),
            readAcceptorRoundTrips.Build(),
            writeAcceptorRoundTrips.Build());
    }

    private long GetTotal(string name) =>
        _rawTotals.TryGetValue(name, out var value) ? value : 0;

    private static double Divide(long numerator, long denominator) =>
        denominator == 0 ? 0 : (double)numerator / denominator;
}

public static class SimulationStatisticsFormatter
{
    public static string FormatRunConsoleSummary(SimulationRunResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        var summary = result.CreateStatisticsSummary();
        return string.Create(
            CultureInfo.InvariantCulture,
            $"  stats: messages.delivered={summary.DeliveredMessages}, proposals={summary.ProposalAttempts}, prepares={summary.PrepareAttempts}, accepts={summary.AcceptAttempts}, conflicts={summary.Conflicts}, fast-rounds={summary.FastRoundAttempts}, proposal-success={summary.ProposalSuccessRate:P1}, fast-round-success={summary.FastRoundSuccessRate:P1}, acceptor-rtts/proposal={summary.AcceptorRoundTripsPerSuccessfulProposal:F2}, network-messages/proposal={summary.NetworkMessagesPerSuccessfulProposal:F2}");
    }

    public static string FormatBatchConsoleSummary(SimulationBatchResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        var summary = result.CreateStatisticsSummary();
        List<string> lines =
        [
            "aggregate statistics:",
        ];
        AppendCommonSummary(lines, summary);
        return string.Join(Environment.NewLine, lines);
    }

    public static string FormatRunArtifact(SimulationRunResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        var summary = result.CreateStatisticsSummary();
        List<string> lines =
        [
            "statistics:",
        ];
        foreach (var stat in result.Statistics.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            AppendLong(lines, stat.Key, stat.Value);
        }

        lines.Add(string.Empty);
        lines.Add("derived:");
        AppendCommonSummary(lines, summary);

        lines.Add(string.Empty);
        lines.Add("details:");
        foreach (var detail in result.Details.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            lines.Add($"  {detail.Key}={detail.Value}");
        }

        return string.Join(Environment.NewLine, lines);
    }

    public static string FormatBatchArtifact(SimulationBatchResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        var summary = result.CreateStatisticsSummary();
        List<string> lines =
        [
            "aggregate statistics:",
        ];
        AppendCommonSummary(lines, summary);

        lines.Add(string.Empty);
        lines.Add("raw totals:");
        foreach (var stat in summary.RawTotals.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            AppendLong(lines, stat.Key, stat.Value);
        }

        return string.Join(Environment.NewLine, lines);
    }

    private static void AppendCommonSummary(List<string> lines, SimulationStatisticsSummary summary)
    {
        AppendLong(lines, "runs.total", summary.RunCount);
        AppendLong(lines, "runs.succeeded", summary.SuccessfulRunCount);
        AppendLong(lines, "runs.failed", summary.FailedRunCount);
        AppendLong(lines, "messages.delivered", summary.DeliveredMessages);
        AppendLong(lines, "messages.dropped", summary.DroppedMessages);
        AppendLong(lines, "messages.partitioned", summary.PartitionedMessages);
        AppendLong(lines, "proposals.total", summary.ProposalAttempts);
        AppendLong(lines, "proposals.succeeded", summary.ProposalSuccesses);
        AppendDouble(lines, "proposals.success_rate", summary.ProposalSuccessRate);
        AppendLong(lines, "prepares.total", summary.PrepareAttempts);
        AppendLong(lines, "prepares.succeeded", summary.PrepareSuccesses);
        AppendDouble(lines, "prepares.success_rate", summary.PrepareSuccessRate);
        AppendLong(lines, "accepts.total", summary.AcceptAttempts);
        AppendLong(lines, "accepts.succeeded", summary.AcceptSuccesses);
        AppendDouble(lines, "accepts.success_rate", summary.AcceptSuccessRate);
        AppendLong(lines, "conflicts.total", summary.Conflicts);
        AppendDouble(lines, "conflicts.rate", summary.ConflictRate);
        AppendLong(lines, "fast_rounds.total", summary.FastRoundAttempts);
        AppendLong(lines, "fast_rounds.succeeded", summary.FastRoundSuccesses);
        AppendDouble(lines, "fast_rounds.usage_rate", summary.FastRoundUsageRate);
        AppendDouble(lines, "fast_rounds.success_rate", summary.FastRoundSuccessRate);
        AppendDouble(
            lines,
            "messages.network_per_successful_proposal",
            summary.NetworkMessagesPerSuccessfulProposal);
        AppendLong(lines, "round_trips.acceptor_rtts_total", summary.AcceptorRoundTripsTotal);
        AppendLong(lines, "round_trips.acceptor_rtts_recorded", summary.AcceptorRoundTripsRecorded);
        AppendDouble(
            lines,
            "round_trips.acceptor_rtts_per_successful_proposal",
            summary.AcceptorRoundTripsPerSuccessfulProposal);
        AppendLong(lines, "round_trips.acceptor_rtts_min", summary.AcceptorRoundTripsMin);
        AppendLong(lines, "round_trips.acceptor_rtts_max", summary.AcceptorRoundTripsMax);
        AppendString(
            lines,
            "round_trips.acceptor_rtts_histogram",
            FormatHistogram(summary.AcceptorRoundTripHistogram));
        AppendLong(lines, "round_trips.read_acceptor_rtts_total", summary.ReadAcceptorRoundTripsTotal);
        AppendLong(lines, "round_trips.read_acceptor_rtts_recorded", summary.ReadAcceptorRoundTripsRecorded);
        AppendDouble(
            lines,
            "round_trips.read_acceptor_rtts_per_successful_read",
            summary.ReadAcceptorRoundTripsPerSuccessfulRead);
        AppendLong(lines, "round_trips.read_acceptor_rtts_min", summary.ReadAcceptorRoundTripsMin);
        AppendLong(lines, "round_trips.read_acceptor_rtts_max", summary.ReadAcceptorRoundTripsMax);
        AppendString(
            lines,
            "round_trips.read_acceptor_rtts_histogram",
            FormatHistogram(summary.ReadAcceptorRoundTripHistogram));
        AppendLong(lines, "round_trips.write_acceptor_rtts_total", summary.WriteAcceptorRoundTripsTotal);
        AppendLong(lines, "round_trips.write_acceptor_rtts_recorded", summary.WriteAcceptorRoundTripsRecorded);
        AppendDouble(
            lines,
            "round_trips.write_acceptor_rtts_per_successful_write",
            summary.WriteAcceptorRoundTripsPerSuccessfulWrite);
        AppendLong(lines, "round_trips.write_acceptor_rtts_min", summary.WriteAcceptorRoundTripsMin);
        AppendLong(lines, "round_trips.write_acceptor_rtts_max", summary.WriteAcceptorRoundTripsMax);
        AppendString(
            lines,
            "round_trips.write_acceptor_rtts_histogram",
            FormatHistogram(summary.WriteAcceptorRoundTripHistogram));
    }

    private static void AppendLong(List<string> lines, string name, long value) =>
        lines.Add(
            string.Create(
                CultureInfo.InvariantCulture,
                $"  {name}={value}"));

    private static void AppendDouble(List<string> lines, string name, double value) =>
        lines.Add(
            string.Create(
                CultureInfo.InvariantCulture,
                $"  {name}={value:F4}"));

    private static void AppendString(List<string> lines, string name, string value) =>
        lines.Add(
            string.Create(
                CultureInfo.InvariantCulture,
                $"  {name}={value}"));

    private static string FormatHistogram(IReadOnlyDictionary<int, int> histogram)
    {
        if (histogram.Count == 0)
        {
            return "none";
        }

        return string.Join(
            ",",
            histogram
                .OrderBy(entry => entry.Key)
                .Select(entry => string.Create(CultureInfo.InvariantCulture, $"{entry.Key}x{entry.Value}")));
    }
}
