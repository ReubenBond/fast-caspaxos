using System.Diagnostics;
using System.Globalization;
using FastCASPaxos.Simulation.Runner;
using FastCASPaxos.Simulation.Scenarios;

internal static class Program
{
    public static async Task<int> Main(string[] args)
    {
        try
        {
            var options = ParseArguments(args);
            if (options.ListOnly)
            {
                PrintAvailableScenarios();
                return 0;
            }

            if (options.ShowHelp)
            {
                PrintUsage();
                return 0;
            }

            WaitForDebuggerIfRequested(options);

            var batchResult = await SimulationBatchRunner.RunAsync(
                new SimulationBatchOptions
                {
                    ScenarioNames = options.Scenarios,
                    ScenarioParameters = new Dictionary<string, string>(options.ScenarioParameters, StringComparer.OrdinalIgnoreCase),
                    Seed = options.Seed,
                    Rounds = options.Rounds,
                    OutputDirectory = options.OutputDirectory,
                    StopOnFailure = options.StopOnFailure,
                });

            Console.WriteLine(
                SimulationConsoleReportFormatter.FormatBatchReport(
                    batchResult,
                    Path.GetFullPath(options.OutputDirectory),
                    options.ScenarioParameters));
            return batchResult.FailedCount == 0 ? 0 : 1;
        }
        catch (ArgumentException ex)
        {
            Console.Error.WriteLine(ex.Message);
            Console.Error.WriteLine();
            PrintUsage();
            return 2;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex);
            return 1;
        }
    }

    private static RunnerCliOptions ParseArguments(string[] args)
    {
        var options = new RunnerCliOptions();
        for (var index = 0; index < args.Length; index++)
        {
            switch (args[index])
            {
                case "--help":
                case "-h":
                    options.ShowHelp = true;
                    break;
                case "--list":
                    options.ListOnly = true;
                    break;
                case "--scenario":
                case "-s":
                    options.Scenarios.AddRange(
                        SplitScenarioList(ReadRequiredValue(args, ref index, "--scenario")));
                    break;
                case "--seed":
                    options.Seed = ParseInt(ReadRequiredValue(args, ref index, "--seed"), "--seed");
                    break;
                case "--rounds":
                    options.Rounds = ParsePositiveInt(ReadRequiredValue(args, ref index, "--rounds"), "--rounds");
                    break;
                case "--wait-for-debugger":
                    options.WaitForDebugger = true;
                    break;
                case "--param":
                case "--scenario-param":
                    var (name, value) = ParseKeyValueOption(ReadRequiredValue(args, ref index, "--param"));
                    SetScenarioParameter(options.ScenarioParameters, name, value);
                    break;
                case "--proposer-count":
                    SetScenarioParameter(
                        options.ScenarioParameters,
                        "proposer-count",
                        ParsePositiveInt(
                            ReadRequiredValue(args, ref index, "--proposer-count"),
                            "--proposer-count").ToString(CultureInfo.InvariantCulture));
                    break;
                case "--acceptor-count":
                    SetScenarioParameter(
                        options.ScenarioParameters,
                        "acceptor-count",
                        ParsePositiveInt(
                            ReadRequiredValue(args, ref index, "--acceptor-count"),
                            "--acceptor-count").ToString(CultureInfo.InvariantCulture));
                    break;
                case "--value-count":
                    SetScenarioParameter(
                        options.ScenarioParameters,
                        "value-count",
                        ParsePositiveInt(
                            ReadRequiredValue(args, ref index, "--value-count"),
                            "--value-count").ToString(CultureInfo.InvariantCulture));
                    break;
                case "--conflict-rate":
                    SetScenarioParameter(
                        options.ScenarioParameters,
                        "conflict-rate",
                        ParseDouble(
                            ReadRequiredValue(args, ref index, "--conflict-rate"),
                            "--conflict-rate",
                            minValue: 0,
                            maxValue: 1).ToString("0.###", CultureInfo.InvariantCulture));
                    break;
                case "--conflict-fanout":
                    SetScenarioParameter(
                        options.ScenarioParameters,
                        "conflict-fanout",
                        ParsePositiveInt(
                            ReadRequiredValue(args, ref index, "--conflict-fanout"),
                            "--conflict-fanout").ToString(CultureInfo.InvariantCulture));
                    break;
                case "--fast":
                    SetScenarioParameter(
                        options.ScenarioParameters,
                        "fast",
                        ParseBool(ReadRequiredValue(args, ref index, "--fast"), "--fast")
                            .ToString()
                            .ToLowerInvariant());
                    break;
                case "--leader":
                    SetScenarioParameter(
                        options.ScenarioParameters,
                        "leader",
                        ParseBool(ReadRequiredValue(args, ref index, "--leader"), "--leader")
                            .ToString()
                            .ToLowerInvariant());
                    break;
                case "--output":
                case "-o":
                    options.OutputDirectory = ReadRequiredValue(args, ref index, "--output");
                    break;
                case "--continue-on-failure":
                    options.StopOnFailure = false;
                    break;
                default:
                    throw new ArgumentException($"Unknown argument '{args[index]}'.");
            }
        }

        if (!options.ListOnly && !options.ShowHelp && options.Scenarios.Count == 0)
        {
            options.Scenarios.Add("all");
        }

        return options;
    }

    private static void PrintAvailableScenarios()
    {
        Console.WriteLine("Available scenarios:");
        foreach (var scenario in SimulationBatchRunner.AvailableScenarios)
        {
            Console.WriteLine($"  {scenario.Name} - {scenario.Description}");
        }
    }

    private static void PrintUsage()
    {
        Console.WriteLine("FastCASPaxos simulation runner");
        Console.WriteLine();
        Console.WriteLine("Usage:");
        Console.WriteLine("  dotnet run --project src\\FastCASPaxos.Simulation.Runner -- [options]");
        Console.WriteLine("  dotnet run --project src\\FastCASPaxos.Simulation.Runner -- --wait-for-debugger --scenario parameterized-append-sequence --seed 9301 --rounds 1");
        Console.WriteLine("  dotnet run --project src\\FastCASPaxos.Simulation.Runner -- --scenario parameterized-append-sequence --seed 9301 --rounds 1 --proposer-count 1 --acceptor-count 5 --value-count 10 --conflict-rate 0 --fast false --leader false");
        Console.WriteLine();
        Console.WriteLine("Options:");
        Console.WriteLine("  --list                        List the available scenarios.");
        Console.WriteLine("  --scenario, -s <name[,name]>  Scenario name(s) to run. Defaults to 'all'.");
        Console.WriteLine("  --seed <int>                  Base seed for round 1. Defaults to 7001.");
        Console.WriteLine("  --rounds <int>                Number of rounds to execute. Each round uses seed + roundIndex and runs in parallel up to the default processor-count limit.");
        Console.WriteLine("  --wait-for-debugger           Wait for a debugger to attach before starting scenario execution.");
        Console.WriteLine("  --param <key=value>           Scenario-specific parameter (repeatable).");
        Console.WriteLine("  --proposer-count <int>        Convenience alias for --param proposer-count=<int>.");
        Console.WriteLine("  --acceptor-count <int>        Convenience alias for --param acceptor-count=<int>.");
        Console.WriteLine("  --value-count <int>           Convenience alias for --param value-count=<int>.");
        Console.WriteLine("  --conflict-rate <0..1>        Convenience alias for --param conflict-rate=<double>.");
        Console.WriteLine("  --conflict-fanout <int>       Convenience alias for --param conflict-fanout=<int>.");
        Console.WriteLine("  --fast <bool>                 Convenience alias for --param fast=<true|false>.");
        Console.WriteLine("  --leader <bool>               Convenience alias for --param leader=<true|false>.");
        Console.WriteLine("  --output, -o <path>           Artifact output directory. Defaults to artifacts\\simulation-runner.");
        Console.WriteLine("  --continue-on-failure         Keep running additional scenarios/rounds after a failure.");
        Console.WriteLine("  --help, -h                    Show this help text.");
    }

    private static string ReadRequiredValue(string[] args, ref int index, string optionName)
    {
        if (index + 1 >= args.Length)
        {
            throw new ArgumentException($"Missing value for '{optionName}'.");
        }

        index++;
        return args[index];
    }

    private static int ParseInt(string value, string optionName)
    {
        if (!int.TryParse(value, out var parsed))
        {
            throw new ArgumentException($"Option '{optionName}' expects an integer value.");
        }

        return parsed;
    }

    private static int ParsePositiveInt(string value, string optionName)
    {
        var parsed = ParseInt(value, optionName);
        if (parsed <= 0)
        {
            throw new ArgumentException($"Option '{optionName}' expects a positive integer value.");
        }

        return parsed;
    }

    private static double ParseDouble(
        string value,
        string optionName,
        double minValue,
        double maxValue)
    {
        if (!double.TryParse(value, NumberStyles.Float | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var parsed))
        {
            throw new ArgumentException($"Option '{optionName}' expects a floating-point value.");
        }

        if (parsed < minValue || parsed > maxValue)
        {
            throw new ArgumentException(
                $"Option '{optionName}' expects a value between {minValue.ToString(CultureInfo.InvariantCulture)} and {maxValue.ToString(CultureInfo.InvariantCulture)}.");
        }

        return parsed;
    }

    private static bool ParseBool(string value, string optionName)
    {
        if (!bool.TryParse(value, out var parsed))
        {
            throw new ArgumentException($"Option '{optionName}' expects 'true' or 'false'.");
        }

        return parsed;
    }

    private static (string Name, string Value) ParseKeyValueOption(string value)
    {
        var separatorIndex = value.IndexOf('=');
        if (separatorIndex <= 0 || separatorIndex == value.Length - 1)
        {
            throw new ArgumentException("Scenario parameters must use the format 'key=value'.");
        }

        var name = value[..separatorIndex].Trim();
        var parameterValue = value[(separatorIndex + 1)..].Trim();
        if (string.IsNullOrWhiteSpace(name) || string.IsNullOrWhiteSpace(parameterValue))
        {
            throw new ArgumentException("Scenario parameters must use the format 'key=value'.");
        }

        return (name, parameterValue);
    }

    private static void SetScenarioParameter(
        IDictionary<string, string> parameters,
        string name,
        string value) =>
        parameters[name] = value;

    private static void WaitForDebuggerIfRequested(RunnerCliOptions options)
    {
        if (!options.WaitForDebugger)
        {
            return;
        }

        Console.WriteLine(
            $"Waiting for debugger to attach to PID {Environment.ProcessId}. Attach now to continue execution.");

        while (!Debugger.IsAttached)
        {
            Debugger.Launch();
            if (!Debugger.IsAttached)
            {
                Thread.Sleep(10_000);
            }
        }

        Debugger.Break();
        Console.WriteLine("Debugger attached. Continuing.");
    }

    private static string[] SplitScenarioList(string value) =>
        value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    private sealed class RunnerCliOptions
    {
        public List<string> Scenarios { get; } = [];

        public Dictionary<string, string> ScenarioParameters { get; } =
            new(StringComparer.OrdinalIgnoreCase);

        public int Seed { get; set; } = Random.Shared.Next();

        public int Rounds { get; set; } = 100;

        public string OutputDirectory { get; set; } =
            Path.Combine(Environment.CurrentDirectory, "artifacts", "simulation-runner");

        public bool StopOnFailure { get; set; } = true;

        public bool ListOnly { get; set; }

        public bool ShowHelp { get; set; }

        public bool WaitForDebugger { get; set; }
    }
}
