using FastCASPaxos.Events;
using FastCASPaxos.Model;
using FastCASPaxos.Monitors;
using Microsoft.Coyote;
using Microsoft.Coyote.Actors;
using Microsoft.Coyote.IO;
using System;
using System.Collections.Generic;
using System.Linq;

namespace FastCASPaxos;

class Program
{
    public static void Main()
    {
        var config = Configuration.Create();
        IActorRuntime runtime = RuntimeFactory.Create(config);
        runtime.OnFailure += OnRuntimeFailure;
        runtime.Logger = new ConsoleLogger()
        {
            // Avoid logging runtime messages - we mark application messages as 'Important', so they are still logged.
            LogLevel = LogSeverity.Warning
        };
        RandomString(runtime);
        Console.ReadLine();
        Console.WriteLine("User cancelled the test by pressing ENTER");
    }

    private static void OnRuntimeFailure(Exception ex)
    {
        Console.WriteLine("Unhandled exception: {0}", ex.Message);
    }

    public readonly struct StringValue : IVersionedValue<string, StringValue>
    {
        public StringValue(int version, string value)
        {
            Version = version;
            Value = value;
        }

        public string Value { get; }
        public int Version { get; }

        public override bool Equals(object obj) => obj is StringValue other && Equals(other);

        public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS": $"{Value}@{Version}")})";

        public bool Equals(StringValue other) => Version == other.Version && string.Equals(Value, other.Value, StringComparison.Ordinal);

        public override int GetHashCode() => HashCode.Combine(Version, Value);

        public bool IsValidSuccessorTo(StringValue predecessor) => predecessor.Value is null || (Value is not null && Value.StartsWith(predecessor.Value));

        public static bool operator ==(StringValue left, StringValue right) => left.Equals(right);

        public static bool operator !=(StringValue left, StringValue right) => !(left == right);
    }

    [Microsoft.Coyote.SystematicTesting.Test]
    public static void String(IActorRuntime runtime)
    {
        var operations = new List<IOperation<string, StringValue>>();
        var version = 0;
        var expectedValue = "Hello, World!";
        foreach (var c in expectedValue)
        {
            operations.Add(new Operation<StringValue, string, StringValue>
            {
                Apply = AppendValues,
                Input = new StringValue(++version, c.ToString()),
                Name = $"Append '{c}' at version {version}"
            });
        }

        runtime.RegisterMonitor<LivenessMonitor<string, StringValue>>();
        runtime.RegisterMonitor<SafetyMonitor<string, StringValue>>();
        var driver = runtime.CreateActor(typeof(Client<string, StringValue>), new TestConfigEventHolder
        {
            Value = new TestConfigEvent<string, StringValue>
            {
                NumAcceptors = 5,
                NumProposers = 3,
                Operations = operations,
                EnableDistinguishedProposerOptimization = runtime.RandomBoolean(),
                EnableFastCommitOptimization = runtime.RandomBoolean(),
                ExpectedValue = expectedValue,
                EqualityComparer = (left, right) => string.Equals(left, right, StringComparison.Ordinal),
            }
        });

        static (OperationStatus, StringValue) AppendValues(StringValue current, StringValue input)
        {
            if (input.Version == current.Version + 1)
            {
                var newValue = (current.Value ?? string.Empty) + input.Value;
                return (OperationStatus.Success, new(input.Version, newValue));
            }

            if (current.Version >= input.Version)
            {
                // The value may have already been committed, but even if not, we should not retry
                return (OperationStatus.NotApplicable, current);
            }

            return (OperationStatus.Failed, current);
        }
    }

    public readonly struct SetValue : IVersionedValue<HashSet<string>, SetValue>
    {
        public SetValue(int version, HashSet<string> value)
        {
            Version = version;
            Value = value;
        }

        public HashSet<string> Value { get; }
        public int Version { get; }

        public override bool Equals(object obj) => obj is SetValue other && Equals(other);

        public override string ToString() => $"Val({((Value == default && Version == default) ? "GENESIS" : $"[{string.Join(", ", Value.OrderBy(s => s))}]@{Version}")})";

        public bool Equals(SetValue other) {
            if (Version != other.Version) return false;

            if (Value is null ^ other.Value is null) return false;

            // Both values are null
            if (Value is null) return true;

            if (!Value.SetEquals(other.Value)) return false;

            return true;
        }

        public override int GetHashCode() => HashCode.Combine(Version, Value);

        public bool IsValidSuccessorTo(SetValue predecessor)
        {
            if (predecessor.Value is null) return true;
            return Value.IsProperSupersetOf(predecessor.Value);
        }

        public static bool operator ==(SetValue left, SetValue right) => left.Equals(right);

        public static bool operator !=(SetValue left, SetValue right) => !(left == right);
    }

    public struct SetOperation
    {
        public bool Add { get; set; }
        public string Value { get; set; }
    }

    [Microsoft.Coyote.SystematicTesting.Test]
    public static void Set(IActorRuntime runtime)
    {
        var operations = new List<IOperation<HashSet<string>, SetValue>>();
        var version = 0;
        const string charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var chars = charset.Select(c => c.ToString()).ToArray();
        for (var i = 0; i < 20; i++)
        {
            var isAdd = true;// runtime.RandomBoolean(3);
            var val = chars[runtime.RandomInteger(chars.Length)];
            var op = new SetOperation { Add = isAdd, Value = val };
            operations.Add(new Operation<SetOperation, HashSet<string>, SetValue>
            {
                Apply = ApplyOperation,
                Input = op,
                Name = $"Append '{(isAdd ? "Add" : "Remove")} {val}' at version {version}"
            });
        }

        runtime.RegisterMonitor<LivenessMonitor<HashSet<string>, SetValue>>();
        runtime.RegisterMonitor<SafetyMonitor<HashSet<string>, SetValue>>();
        var driver = runtime.CreateActor(typeof(Client<HashSet<string>, SetValue>), new TestConfigEventHolder
        {
            Value = new TestConfigEvent<HashSet<string>, SetValue>
            {
                NumAcceptors = 5,
                NumProposers = 3,
                Operations = operations,
                EnableDistinguishedProposerOptimization = runtime.RandomBoolean(),
                EnableFastCommitOptimization = runtime.RandomBoolean(),
            }
        });

        static (OperationStatus, SetValue) ApplyOperation(SetValue current, SetOperation input)
        {
            if (current.Value is null)
            {
                current = new SetValue(current.Version, new HashSet<string>(StringComparer.Ordinal));
            }

            if (input.Add)
            {
                if (!current.Value.Contains(input.Value))
                {
                    var result = new SetValue(current.Version + 1, new HashSet<string>(current.Value, StringComparer.Ordinal)
                    {
                        input.Value
                    });
                    return (OperationStatus.Success, result);
                }
                else
                {
                    return (OperationStatus.NotApplicable, current);
                }
            }
            else
            {
                if (current.Value.Contains(input.Value))
                {
                    var result = new SetValue(current.Version + 1, new HashSet<string>(current.Value, StringComparer.Ordinal));
                    _ = result.Value.Remove(input.Value);
                    return (OperationStatus.Success, result);
                }
                else
                {
                    return (OperationStatus.NotApplicable, current);
                }
            }
        }
    }

    [Microsoft.Coyote.SystematicTesting.Test]
    public static void RandomString(IActorRuntime runtime)
    {
        const string charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var chars = charset.ToArray();
        var operations = new List<IOperation<string, StringValue>>();
        for (int version = 0; version < 5; version++)
        {
            // Add some random number of operations at that expected version.
            for (int i = 0; i < 3; i++)
            {
                var value = chars[runtime.RandomInteger(chars.Length)];
                operations.Add(new Operation<char, string, StringValue>
                {
                    Apply = AppendValues,
                    Input = value,
                    Name = $"Append '{value}' at version {version}"
                });
            }
        }

        operations.Add(new Operation<char, string, StringValue>
        {
            Apply = AppendValues,
            Input = '0',
            Name = "Read value"
        });

        runtime.RegisterMonitor<LivenessMonitor<string, StringValue>>();
        runtime.RegisterMonitor<SafetyMonitor<string, StringValue>>();
        var driver = runtime.CreateActor(typeof(Client<string, StringValue>), new TestConfigEventHolder
        {
            Value = new TestConfigEvent<string, StringValue>
            {
                NumAcceptors = 5,
                NumProposers = 3,
                Operations = operations,
                EnableDistinguishedProposerOptimization = runtime.RandomBoolean(),
                EnableFastCommitOptimization = runtime.RandomBoolean(),
            }
        });

        static (OperationStatus, StringValue) AppendValues(StringValue current, char input)
        {
            if (input != '0')
            {
                var newValue = (current.Value ?? string.Empty) + input;
                return (OperationStatus.Success, new(current.Version + 1, newValue));
            }

            return (OperationStatus.NotApplicable, current);
        }
    }

    [Microsoft.Coyote.SystematicTesting.Test]
    public static void ForkingString(IActorRuntime runtime)
    {
        const string charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        var chars = charset.Select(c => c.ToString()).ToArray();
        var operations = new List<IOperation<string, StringValue>>();
        for (int version = 0; version < 5; version++)
        {
            // Add some random number of operations at that expected version.
            for (int i = 0; i < 3; i++)
            {
                var value = chars[runtime.RandomInteger(chars.Length)];
                operations.Add(new Operation<StringValue, string, StringValue>
                {
                    Apply = AppendValues,
                    Input = new (version, value),
                    Name = $"Append '{value}' at version {version}"
                });
            }
        }

        runtime.RegisterMonitor<LivenessMonitor<string, StringValue>>();
        runtime.RegisterMonitor<SafetyMonitor<string, StringValue>>();
        var driver = runtime.CreateActor(typeof(Client<string, StringValue>), new TestConfigEventHolder
        {
            Value = new TestConfigEvent<string, StringValue>
            {
                NumAcceptors = 5,
                NumProposers = 3,
                Operations = operations,
                EnableDistinguishedProposerOptimization = runtime.RandomBoolean(),
                EnableFastCommitOptimization = runtime.RandomBoolean(),
            }
        });

        static (OperationStatus, StringValue) AppendValues(StringValue current, StringValue input)
        {
            if (input.Version == current.Version + 1)
            {
                var newValue = (current.Value ?? string.Empty) + input.Value;
                return (OperationStatus.Success, new(input.Version, newValue));
            }

            if (current.Version >= input.Version)
            {
                // The value may have already been committed, but even if not, we should not retry
                return (OperationStatus.NotApplicable, current);
            }

            return (OperationStatus.Failed, current);
        }
    }
}
