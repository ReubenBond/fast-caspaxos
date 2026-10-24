using FastCASPaxos.Server;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

public sealed class ByteStringTests
{
    [Fact]
    public void ConstructorAndToArrayOwnTheirBuffers()
    {
        byte[] input = [0, 42, 255];
        var value = new ByteString(input);
        input[1] = 99;
        var exported = value.ToArray();
        exported[2] = 0;

        Assert.Equal(3, value.Length);
        Assert.False(value.IsDefault);
        Assert.False(value.IsEmpty);
        Assert.Equal(new byte[] { 0, 42, 255 }, value.ToArray());
        Assert.NotSame(exported, value.ToArray());
        Assert.Empty(ByteString.Empty.ToArray());
        Assert.Equal(0, ByteString.Empty.Length);
    }

    [Fact]
    public void EqualityAndHashCodeUseByteContents()
    {
        var left = new ByteString([0, 42, 255]);
        var right = new ByteString([0, 42, 255]);

        Assert.True(left.Equals(right));
        Assert.True(left.Equals((object)right));
        Assert.True(right.Equals(left));
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
        Assert.Contains(right, new HashSet<ByteString> { left });
        Assert.Equal(ByteString.Empty, new ByteString([]));
        Assert.Equal(ByteString.Empty.GetHashCode(), new ByteString([]).GetHashCode());
        Assert.False(left.Equals(new ByteString([0, 42])));
        Assert.False(left.Equals(new ByteString([0, 42, 254])));
        Assert.False(left.Equals((ByteString?)null));
        Assert.False(left.Equals((object?)null));
        Assert.False(left.Equals((object)new byte[] { 0, 42, 255 }));
    }

    [Fact]
    public void DefaultValueRepresentsMissingBytes()
    {
        ByteString[] missingValues = [default, new()];
        foreach (var value in missingValues)
        {
            Assert.True(value.IsDefault);
            Assert.Throws<InvalidOperationException>(() => value.Length);
            Assert.Throws<InvalidOperationException>(() => value.IsEmpty);
            Assert.Throws<InvalidOperationException>(() => value.ToArray());
            Assert.True(value.Equals(default(ByteString)));
            Assert.True(value.Equals((object)default(ByteString)));
            Assert.Equal(default(ByteString).GetHashCode(), value.GetHashCode());
            Assert.True(value == default(ByteString));
            Assert.False(value != default(ByteString));
            Assert.False(value == ByteString.Empty);
            Assert.False(ByteString.Empty == value);
            Assert.True(value != ByteString.Empty);
            Assert.True(ByteString.Empty != value);
            Assert.False(value.Equals(ByteString.Empty));
            Assert.False(ByteString.Empty.Equals(value));
            Assert.False(value.Equals((object?)null));
        }
        Assert.Single(missingValues.ToHashSet());
        var values = new Dictionary<ByteString, string> { [default] = "missing", [ByteString.Empty] = "empty" };
        Assert.Equal("missing", values[new()]);
        Assert.Equal("empty", values[new([])]);
    }

    [Fact]
    public void ConstructedEmptyValuesAreInitializedAndEqual()
    {
        ByteString[] emptyValues = [ByteString.Empty, new([]), new(Array.Empty<byte>()), new(default(ReadOnlySpan<byte>))];
        foreach (var value in emptyValues)
        {
            Assert.False(value.IsDefault);
            Assert.True(value.IsEmpty);
            Assert.Equal(0, value.Length);
            Assert.Empty(value.ToArray());
            foreach (var other in emptyValues)
            {
                Assert.True(value.Equals(other));
                Assert.True(value.Equals((object)other));
                Assert.True(value == other);
                Assert.False(value != other);
                Assert.Equal(value.GetHashCode(), other.GetHashCode());
            }
        }
        Assert.Single(emptyValues.ToHashSet());
        Assert.Equal("empty", new Dictionary<ByteString, string> { [ByteString.Empty] = "empty" }[new([])]);
    }

    [Theory]
    [InlineData("same", true)]
    [InlineData("different-length", false)]
    [InlineData("different-content", false)]
    [InlineData("different-order", false)]
    [InlineData("empty", false)]
    [InlineData("missing", false)]
    public void EqualityOperatorsUseByteContents(string caseId, bool equal)
    {
        var left = new ByteString([0, 42, 255]);
        var right = caseId switch
        {
            "same" => new ByteString([0, 42, 255]),
            "different-length" => new ByteString([0, 42]),
            "different-content" => new ByteString([0, 42, 254]),
            "different-order" => new ByteString([255, 42, 0]),
            "empty" => ByteString.Empty,
            "missing" => default,
            _ => throw new ArgumentOutOfRangeException(nameof(caseId))
        };

        Assert.Equal(equal, left == right);
        Assert.Equal(equal, right == left);
        Assert.Equal(!equal, left != right);
        Assert.Equal(!equal, right != left);
        Assert.Equal(equal, left.Equals(right));
        Assert.Equal(equal, EqualityComparer<ByteString>.Default.Equals(left, right));
    }

    [Fact]
    public void NullableEqualityDistinguishesNullDefaultAndEmptyBytes()
    {
        ByteString? absent = null;
        ByteString? alsoAbsent = null;
        ByteString? missing = default(ByteString);
        ByteString? alsoMissing = new ByteString();
        ByteString? empty = ByteString.Empty;
        ByteString? constructedEmpty = new ByteString([]);
        ByteString? value = new ByteString([42]);
        ByteString? sameValue = new ByteString([42]);

        Assert.True(absent == alsoAbsent);
        Assert.False(absent != alsoAbsent);
        Assert.False(absent == empty);
        Assert.False(empty == absent);
        Assert.True(absent != empty);
        Assert.True(empty != absent);
        Assert.True(empty == constructedEmpty);
        Assert.False(empty != constructedEmpty);
        Assert.True(value == sameValue);
        Assert.False(value != sameValue);
        Assert.True(value != empty);
        Assert.True(value != absent);
        Assert.True(missing == alsoMissing);
        Assert.False(missing != alsoMissing);
        Assert.False(missing == absent);
        Assert.False(absent == missing);
        Assert.True(missing != absent);
        Assert.False(missing == empty);
        Assert.False(empty == missing);
        Assert.True(missing != empty);
        Assert.Equal(4, new HashSet<ByteString?>
            { absent, missing, alsoMissing, empty, constructedEmpty, value, sameValue }.Count);
    }

    [Fact]
    public void StructCopiesPreserveOwnedImmutableBytes()
    {
        byte[] input = [0, 42, 255];
        var original = new ByteString(input);
        var copy = original;
        var values = new Dictionary<ByteString, string> { [copy] = "retained" };
        input[0] = 99;
        copy.ToArray()[1] = 99;
        original = new ByteString([1]);

        Assert.Equal(new byte[] { 0, 42, 255 }, copy.ToArray());
        Assert.Equal("retained", values[new ByteString([0, 42, 255])]);
        Assert.Equal(new byte[] { 1 }, original.ToArray());
    }
}
