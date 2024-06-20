using System.Globalization;

namespace FastCASPaxos.Simulation.Scenarios;

internal sealed record ParameterizedAppendSequenceOptions(
    int ProposerCount,
    int AcceptorCount,
    int ValueCount,
    double ConflictRate,
    int ConflictFanout,
    bool EnableFastCommit,
    bool EnableDistinguishedLeader)
{
    public const string ScenarioName = "parameterized-append-sequence";

    private static readonly HashSet<string> AllowedParameters =
    [
        "proposer-count",
        "acceptor-count",
        "value-count",
        "conflict-rate",
        "conflict-fanout",
        "fast",
        "leader",
    ];

    public static ParameterizedAppendSequenceOptions Parse(IReadOnlyDictionary<string, string> parameters)
    {
        ArgumentNullException.ThrowIfNull(parameters);
        ValidateKnownParameters(parameters);

        var proposerCount = GetPositiveInt(parameters, "proposer-count", defaultValue: 1);
        var acceptorCount = GetPositiveInt(parameters, "acceptor-count", defaultValue: 5);
        var valueCount = GetPositiveInt(parameters, "value-count", defaultValue: 10);
        var conflictRate = GetDouble(parameters, "conflict-rate", defaultValue: 0d, minValue: 0d, maxValue: 1d);
        var conflictFanout = GetPositiveInt(parameters, "conflict-fanout", defaultValue: 2);
        var enableFastCommit = GetBool(parameters, "fast", defaultValue: false);
        var enableDistinguishedLeader = GetBool(parameters, "leader", defaultValue: false);

        if (conflictRate > 0 && proposerCount < 2)
        {
            throw new ArgumentException(
                $"Scenario '{ScenarioName}' requires proposer-count >= 2 when conflict-rate is greater than zero.");
        }

        if (conflictRate > 0 && conflictFanout > proposerCount)
        {
            throw new ArgumentException(
                $"Scenario '{ScenarioName}' requires conflict-fanout <= proposer-count when conflict-rate is greater than zero.");
        }

        if (conflictRate > 0 && conflictFanout < 2)
        {
            throw new ArgumentException(
                $"Scenario '{ScenarioName}' requires conflict-fanout >= 2 when conflict-rate is greater than zero.");
        }

        return new(
            proposerCount,
            acceptorCount,
            valueCount,
            conflictRate,
            conflictFanout,
            enableFastCommit,
            enableDistinguishedLeader);
    }

    public IReadOnlyList<KeyValuePair<string, string>> DescribeParameters() =>
    [
        new("proposer-count", ProposerCount.ToString(CultureInfo.InvariantCulture)),
        new("acceptor-count", AcceptorCount.ToString(CultureInfo.InvariantCulture)),
        new("value-count", ValueCount.ToString(CultureInfo.InvariantCulture)),
        new("conflict-rate", ConflictRate.ToString("0.###", CultureInfo.InvariantCulture)),
        new("conflict-fanout", ConflictFanout.ToString(CultureInfo.InvariantCulture)),
        new("fast", EnableFastCommit.ToString().ToLowerInvariant()),
        new("leader", EnableDistinguishedLeader.ToString().ToLowerInvariant()),
    ];

    private static void ValidateKnownParameters(IReadOnlyDictionary<string, string> parameters)
    {
        var unknown = parameters.Keys
            .Where(key => !AllowedParameters.Contains(key))
            .OrderBy(key => key, StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (unknown.Length == 0)
        {
            return;
        }

        throw new ArgumentException(
            $"Scenario '{ScenarioName}' does not recognize parameter(s): {string.Join(", ", unknown)}.");
    }

    private static int GetPositiveInt(
        IReadOnlyDictionary<string, string> parameters,
        string name,
        int defaultValue)
    {
        if (!parameters.TryGetValue(name, out var value))
        {
            return defaultValue;
        }

        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            throw new ArgumentException(
                $"Scenario '{ScenarioName}' expects parameter '{name}' to be an integer.");
        }

        if (parsed <= 0)
        {
            throw new ArgumentException(
                $"Scenario '{ScenarioName}' expects parameter '{name}' to be positive.");
        }

        return parsed;
    }

    private static double GetDouble(
        IReadOnlyDictionary<string, string> parameters,
        string name,
        double defaultValue,
        double minValue,
        double maxValue)
    {
        if (!parameters.TryGetValue(name, out var value))
        {
            return defaultValue;
        }

        if (!double.TryParse(value, NumberStyles.Float | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var parsed))
        {
            throw new ArgumentException(
                $"Scenario '{ScenarioName}' expects parameter '{name}' to be a floating-point value.");
        }

        if (parsed < minValue || parsed > maxValue)
        {
            throw new ArgumentException(
                $"Scenario '{ScenarioName}' expects parameter '{name}' to be between {minValue.ToString(CultureInfo.InvariantCulture)} and {maxValue.ToString(CultureInfo.InvariantCulture)}.");
        }

        return parsed;
    }

    private static bool GetBool(
        IReadOnlyDictionary<string, string> parameters,
        string name,
        bool defaultValue)
    {
        if (!parameters.TryGetValue(name, out var value))
        {
            return defaultValue;
        }

        if (!bool.TryParse(value, out var parsed))
        {
            throw new ArgumentException(
                $"Scenario '{ScenarioName}' expects parameter '{name}' to be 'true' or 'false'.");
        }

        return parsed;
    }
}
