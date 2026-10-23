using FastCASPaxos.Server;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

public sealed class ByteStringTests
{
    [Fact]
    public void ConstructorAndToArray_OwnTheirBuffers()
    {
        byte[] input = [0, 42, 255];
        var value = new ByteString(input);
        input[1] = 99;
        var exported = value.ToArray();
        exported[2] = 0;

        Assert.Equal(3, value.Length);
        Assert.Equal(new byte[] { 0, 42, 255 }, value.ToArray());
        Assert.NotSame(exported, value.ToArray());
        Assert.Empty(ByteString.Empty.ToArray());
        Assert.Equal(0, ByteString.Empty.Length);
    }

    [Fact]
    public void EqualityAndHashCode_UseByteContents()
    {
        var left = new ByteString([0, 42, 255]);
        var right = new ByteString([0, 42, 255]);

        Assert.NotSame(left, right);
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
        Assert.False(left.Equals((object)new byte[] { 0, 42, 255 }));
    }
}
