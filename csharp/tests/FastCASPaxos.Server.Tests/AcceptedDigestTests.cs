using FastCASPaxos.Model;
using Xunit;

namespace FastCASPaxos.Server.Tests;

public sealed class AcceptedDigestTests
{
    [Theory]
    [InlineData("empty")]
    [InlineData("abc")]
    public void Hash_KnownVector_ReturnsSha256(string caseId)
    {
        var value = caseId == "empty" ? ByteString.Empty : new ByteString("abc"u8);
        var expected = caseId == "empty"
            ? "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"
            : "BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD";

        Assert.Equal(Convert.FromHexString(expected), AcceptedDigest.Hash(value).ToArray());
    }

    [Fact]
    public void Hash_NullValue_Throws() =>
        Assert.Throws<ArgumentNullException>(() => AcceptedDigest.Hash(null!));

    [Theory]
    [InlineData(0, 1)]
    [InlineData(-1, 1)]
    [InlineData(1, -1)]
    public void Validate_InvalidAcceptedBallot_Throws(int round, int proposer) =>
        Assert.Throws<ArgumentException>(() => new AcceptedDigest(new(round, proposer), new(new byte[32])).Validate());

    [Theory]
    [InlineData(0)]
    [InlineData(31)]
    [InlineData(33)]
    public void Validate_InvalidHashLength_Throws(int length) =>
        Assert.Throws<ArgumentException>(() => new AcceptedDigest(new(1, 1), new(new byte[length])).Validate());

    [Fact]
    public void Validate_NullHash_Throws() =>
        Assert.Throws<ArgumentNullException>(() => new AcceptedDigest(new(1, 1), null!).Validate());

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void Validate_FastOrClassicDigest_AcceptsAndComparesBothFields(int proposer)
    {
        var digest = new AcceptedDigest(new(1, proposer), new(new byte[32]));
        digest.Validate();

        Assert.Equal(digest, new AcceptedDigest(new(1, proposer), new(new byte[32])));
        Assert.NotEqual(digest, digest with { AcceptedBallot = new Ballot(2, proposer) });
        Assert.NotEqual(digest, digest with { ValueHash = new ByteString(Enumerable.Repeat((byte)1, 32).ToArray()) });
    }
}
