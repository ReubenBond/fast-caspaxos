using FastCASPaxos.Model;

namespace FastCASPaxos.Simulation.Scenarios;

public static class CoyoteParityCorpus
{
    private const string CharacterSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public static string LegacyStringExpectedValue => "Hello, World!";

    public static IReadOnlyList<IOperation<StringValue>> BuildLegacyStringOperations() =>
        LegacyStringExpectedValue
            .Select((character, index) => StringScenarioOperations.AppendAtVersion(index + 1, character.ToString()))
            .ToList();

    public static (IReadOnlyList<IOperation<SetValue>> Operations, HashSet<string> ExpectedValue) BuildLegacySetOperations(
        int seed = 1234,
        int count = 20)
    {
        var random = new Random(seed);
        var expected = new HashSet<string>(StringComparer.Ordinal);
        List<IOperation<SetValue>> operations = [];

        for (var index = 0; index < count; index++)
        {
            var value = CharacterSet[random.Next(CharacterSet.Length)].ToString();
            operations.Add(SetScenarioOperations.Add(value));
            expected.Add(value);
        }

        return (operations, expected);
    }

    public static (IReadOnlyList<IOperation<StringValue>> Operations, string ExpectedValue) BuildLegacyRandomStringOperations(
        int seed = 2345,
        int versions = 5,
        int operationsPerVersion = 3)
    {
        var random = new Random(seed);
        List<IOperation<StringValue>> operations = [];
        List<char> expected = [];

        foreach (var _ in Enumerable.Range(0, versions))
        {
            foreach (var __ in Enumerable.Range(0, operationsPerVersion))
            {
                var next = CharacterSet[random.Next(CharacterSet.Length)];
                operations.Add(StringScenarioOperations.AppendCharacter(next));
                expected.Add(next);
            }
        }

        return (operations, new string([.. expected]));
    }

    public static (IReadOnlyList<IOperation<StringValue>> Operations, string ExpectedValue) BuildLegacyForkingStringOperations(
        int seed = 3456,
        int versions = 5,
        int operationsPerVersion = 3)
    {
        var random = new Random(seed);
        List<IOperation<StringValue>> operations = [];
        List<char> expected = [];

        for (var version = 0; version < versions; version++)
        {
            for (var index = 0; index < operationsPerVersion; index++)
            {
                var next = CharacterSet[random.Next(CharacterSet.Length)].ToString();
                operations.Add(StringScenarioOperations.AppendAtVersion(version, next));
                if (version > 0 && index == 0)
                {
                    expected.Add(next[0]);
                }
            }
        }

        return (operations, new string([.. expected]));
    }
}
