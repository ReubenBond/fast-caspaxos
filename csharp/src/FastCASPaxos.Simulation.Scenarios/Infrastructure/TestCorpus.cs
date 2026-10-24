using System.Collections.Immutable;
using FastCASPaxos.Model;

namespace FastCASPaxos.Simulation.Scenarios;

public static class TestCorpus
{
    private const string CharacterSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    public static string StringExpectedValue => "Hello, World!";

    public static ImmutableArray<IOperation<StringValue>> BuildStringOperations() =>
    [
        .. StringExpectedValue
            .Select((character, index) => StringScenarioOperations.AppendAtVersion(index + 1, character.ToString())),
    ];

    public static (ImmutableArray<IOperation<SetValue>> Operations, HashSet<string> ExpectedValue) BuildSetOperations(
        int seed = 1234,
        int count = 20)
    {
        var random = new Random(seed);
        var expected = new HashSet<string>(StringComparer.Ordinal);
        var operations = ImmutableArray.CreateBuilder<IOperation<SetValue>>(count);

        for (var index = 0; index < count; index++)
        {
            var value = CharacterSet[random.Next(CharacterSet.Length)].ToString();
            operations.Add(SetScenarioOperations.Add(value));
            expected.Add(value);
        }

        return (operations.MoveToImmutable(), expected);
    }

    public static (ImmutableArray<IOperation<StringValue>> Operations, string ExpectedValue) BuildRandomStringOperations(
        int seed = 2345,
        int versions = 5,
        int operationsPerVersion = 3)
    {
        var random = new Random(seed);
        var operations = ImmutableArray.CreateBuilder<IOperation<StringValue>>(versions * operationsPerVersion);
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

        return (operations.MoveToImmutable(), new string([.. expected]));
    }

    public static (ImmutableArray<IOperation<StringValue>> Operations, string ExpectedValue) BuildForkingStringOperations(
        int seed = 3456,
        int versions = 5,
        int operationsPerVersion = 3)
    {
        var random = new Random(seed);
        var operations = ImmutableArray.CreateBuilder<IOperation<StringValue>>(versions * operationsPerVersion);
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

        return (operations.MoveToImmutable(), new string([.. expected]));
    }
}
