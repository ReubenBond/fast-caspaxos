using System.Diagnostics;
using System.Diagnostics.Metrics;
using FastCASPaxos.Model;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace FastCASPaxos.Diagnostics;

/// <summary>
/// Combines structured logging, <see cref="DiagnosticSource"/> events, and <see cref="Meter"/>-based
/// metrics for <see cref="Protocol.ProposerEngine{TValue,TRoute}"/>.
/// Inject via constructor to keep the engine free of observability state.
/// </summary>
public sealed partial class ProposerDiagnostics
{
    public const string MeterName = "FastCASPaxos.ProposerEngine";
    public const string AttemptsMetricName = "fast_caspaxos.proposer.attempts";
    public const string SuccessesMetricName = "fast_caspaxos.proposer.successes";
    public const string ConflictsMetricName = "fast_caspaxos.proposer.conflicts";
    public const string PrepareRetriesMetricName = "fast_caspaxos.proposer.prepare_retries";
    public const string BallotBumpsMetricName = "fast_caspaxos.proposer.ballot_bumps";
    public const string RecoveryAdoptionsMetricName = "fast_caspaxos.proposer.recovery_adoptions";
    public const string PrepareAttemptsMetricName = "fast_caspaxos.proposer.prepare_attempts";
    public const string PrepareSuccessesMetricName = "fast_caspaxos.proposer.prepare_successes";
    public const string PrepareFailuresMetricName = "fast_caspaxos.proposer.prepare_failures";
    public const string AcceptAttemptsMetricName = "fast_caspaxos.proposer.accept_attempts";
    public const string AcceptSuccessesMetricName = "fast_caspaxos.proposer.accept_successes";
    public const string AcceptFailuresMetricName = "fast_caspaxos.proposer.accept_failures";
    public const string FastRoundAttemptsMetricName = "fast_caspaxos.proposer.fast_round_attempts";
    public const string FastRoundSuccessesMetricName = "fast_caspaxos.proposer.fast_round_successes";

    private readonly ILogger _logger;
    private readonly DiagnosticSource _diagnosticSource;
    private readonly Counter<long> _attempts;
    private readonly Counter<long> _successes;
    private readonly Counter<long> _conflicts;
    private readonly Counter<long> _prepareRetries;
    private readonly Counter<long> _ballotBumps;
    private readonly Counter<long> _recoveryAdoptions;
    private readonly Counter<long> _prepareAttempts;
    private readonly Counter<long> _prepareSuccesses;
    private readonly Counter<long> _prepareFailures;
    private readonly Counter<long> _acceptAttempts;
    private readonly Counter<long> _acceptSuccesses;
    private readonly Counter<long> _acceptFailures;
    private readonly Counter<long> _fastRoundAttempts;
    private readonly Counter<long> _fastRoundSuccesses;

    /// <summary>A no-op instance for code paths (and tests) that do not need observability.</summary>
    public static ProposerDiagnostics Nop { get; } = new(NullLogger.Instance);

    public ProposerDiagnostics(ILogger logger)
        : this(logger, new DiagnosticListener("FastCASPaxos.ProposerEngine"), new Meter(MeterName))
    {
    }

    public ProposerDiagnostics(ILogger logger, DiagnosticSource diagnosticSource, Meter meter)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(diagnosticSource);
        ArgumentNullException.ThrowIfNull(meter);

        _logger = logger;
        _diagnosticSource = diagnosticSource;
        _attempts = meter.CreateCounter<long>(AttemptsMetricName, description: "Number of proposals started");
        _successes = meter.CreateCounter<long>(SuccessesMetricName, description: "Number of proposals successfully completed");
        _conflicts = meter.CreateCounter<long>(ConflictsMetricName, description: "Number of ballot conflicts detected");
        _prepareRetries = meter.CreateCounter<long>(PrepareRetriesMetricName, description: "Number of prepare retries");
        _ballotBumps = meter.CreateCounter<long>(BallotBumpsMetricName, description: "Number of ballot bumps above a conflict");
        _recoveryAdoptions = meter.CreateCounter<long>(RecoveryAdoptionsMetricName, description: "Number of values adopted during recovery");
        _prepareAttempts = meter.CreateCounter<long>(PrepareAttemptsMetricName, description: "Number of prepare phases started");
        _prepareSuccesses = meter.CreateCounter<long>(PrepareSuccessesMetricName, description: "Number of prepare phases that reached quorum");
        _prepareFailures = meter.CreateCounter<long>(PrepareFailuresMetricName, description: "Number of prepare phases that retried without reaching a usable quorum");
        _acceptAttempts = meter.CreateCounter<long>(AcceptAttemptsMetricName, description: "Number of accept phases started");
        _acceptSuccesses = meter.CreateCounter<long>(AcceptSuccessesMetricName, description: "Number of accept phases that reached quorum");
        _acceptFailures = meter.CreateCounter<long>(AcceptFailuresMetricName, description: "Number of accept phases that lost quorum and retried");
        _fastRoundAttempts = meter.CreateCounter<long>(FastRoundAttemptsMetricName, description: "Number of accept phases attempted on a fast ballot");
        _fastRoundSuccesses = meter.CreateCounter<long>(FastRoundSuccessesMetricName, description: "Number of accept phases that committed on a fast ballot");
    }

    /// <summary>The underlying <see cref="DiagnosticSource"/> used to emit events.</summary>
    public DiagnosticSource DiagnosticSource => _diagnosticSource;

    public void OnProposalStarted(object engine, int proposerId, Ballot ballot)
    {
        _attempts.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
        LogProposalStarted(proposerId, ballot);

        if (_diagnosticSource.IsEnabled(ProposerEventNames.ProposalStarted))
        {
            _diagnosticSource.Write(ProposerEventNames.ProposalStarted, new ProposalStartedEvent(engine));
        }
    }

    public void OnPrepareStarted(object engine, int proposerId, Ballot ballot)
    {
        _prepareAttempts.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
    }

    public void OnPrepareSucceeded(object engine, int proposerId, Ballot ballot)
    {
        _prepareSuccesses.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
    }

    public void OnPrepareFailed(object engine, int proposerId, Ballot ballot, Ballot conflictBallot)
    {
        _prepareFailures.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
    }

    public void OnAcceptStarted(object engine, int proposerId, Ballot ballot)
    {
        _acceptAttempts.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
        if (ballot.IsFastRoundBallot)
        {
            _fastRoundAttempts.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
        }
    }

    public void OnAcceptSucceeded(object engine, int proposerId, Ballot ballot)
    {
        _acceptSuccesses.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
        if (ballot.IsFastRoundBallot)
        {
            _fastRoundSuccesses.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
        }
    }

    public void OnAcceptFailed(object engine, int proposerId, Ballot ballot, Ballot conflictBallot)
    {
        _acceptFailures.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
    }

    public void OnValueCommitted(object engine, object committedValue, int proposerId, Ballot ballot)
    {
        LogValueCommitted(proposerId, ballot);

        if (_diagnosticSource.IsEnabled(ProposerEventNames.ValueCommitted))
        {
            _diagnosticSource.Write(ProposerEventNames.ValueCommitted, new ValueCommittedEvent(engine, committedValue, ballot));
        }
    }

    public void OnProposalCompleted(object engine, object committedValue, int proposerId)
    {
        _successes.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
        LogProposalCompleted(proposerId);

        if (_diagnosticSource.IsEnabled(ProposerEventNames.ProposalCompleted))
        {
            _diagnosticSource.Write(ProposerEventNames.ProposalCompleted, new ProposalCompletedEvent(engine, committedValue));
        }
    }

    public void OnConflictDetected(object engine, int proposerId, Ballot conflictBallot)
    {
        _conflicts.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
        LogConflictDetected(proposerId, conflictBallot);

        if (_diagnosticSource.IsEnabled(ProposerEventNames.ConflictDetected))
        {
            _diagnosticSource.Write(ProposerEventNames.ConflictDetected, new ConflictDetectedEvent(engine, conflictBallot));
        }
    }

    public void OnPrepareRetried(object engine, int proposerId, Ballot oldBallot, Ballot newBallot, Ballot conflictBallot)
    {
        _prepareRetries.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));

        if (!conflictBallot.IsZero && newBallot > oldBallot.NextRound(proposerId))
        {
            _ballotBumps.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
        }

        LogPrepareRetried(proposerId, oldBallot, newBallot);

        if (_diagnosticSource.IsEnabled(ProposerEventNames.PrepareRetried))
        {
            _diagnosticSource.Write(ProposerEventNames.PrepareRetried, new PrepareRetriedEvent(engine, oldBallot, newBallot, conflictBallot));
        }
    }

    public void OnValueAdopted(object engine, object adoptedValue, int proposerId)
    {
        _recoveryAdoptions.Add(1, new KeyValuePair<string, object?>("proposer_id", proposerId));
        LogValueAdopted(proposerId);

        if (_diagnosticSource.IsEnabled(ProposerEventNames.ValueAdopted))
        {
            _diagnosticSource.Write(ProposerEventNames.ValueAdopted, new ValueAdoptedEvent(engine, adoptedValue));
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Proposer {ProposerId} starting proposal at ballot {Ballot}")]
    private partial void LogProposalStarted(int proposerId, Ballot ballot);

    [LoggerMessage(Level = LogLevel.Information, Message = "Proposer {ProposerId} committed value at ballot {Ballot}")]
    private partial void LogValueCommitted(int proposerId, Ballot ballot);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Proposer {ProposerId} completed proposal")]
    private partial void LogProposalCompleted(int proposerId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Proposer {ProposerId} detected conflict at ballot {ConflictBallot}")]
    private partial void LogConflictDetected(int proposerId, Ballot conflictBallot);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Proposer {ProposerId} retrying prepare: {OldBallot} -> {NewBallot}")]
    private partial void LogPrepareRetried(int proposerId, Ballot oldBallot, Ballot newBallot);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Proposer {ProposerId} adopted recovered value")]
    private partial void LogValueAdopted(int proposerId);
}
