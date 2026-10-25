using FastCASPaxos.Model;
using FastCASPaxos.Server.Diagnostics;
using Xunit;
using static FastCASPaxos.Server.Tests.TestSupport;

namespace FastCASPaxos.Server.Tests;

public sealed class DiagnosticPreviewTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(255)]
    [InlineData(256)]
    [InlineData(257)]
    [InlineData(4096)]
    public void PayloadPreviewBoundsBinaryBytesWithoutMutatingSource(int length)
    {
        var bytes = Enumerable.Range(0, length).Select(i => (byte)i).ToArray();
        var value = new ByteString(bytes);
        var preview = Assert.IsType<PayloadPreview>(PayloadPreview.From(value));
        Assert.Equal(length, preview.Length);
        Assert.Equal(length > 256, preview.Truncated);
        Assert.Equal(bytes.Take(256), Convert.FromBase64String(preview.Base64));
        Assert.Equal(bytes, value.ToArray());
        Array.Fill(bytes, (byte)99);
        Assert.Equal(Enumerable.Range(0, Math.Min(length, 256)).Select(i => (byte)i),
            Convert.FromBase64String(preview.Base64));
        Assert.Null(PayloadPreview.From(null));
    }

    [Theory]
    [InlineData(1023)]
    [InlineData(1024)]
    [InlineData(1025)]
    public void KeyPreviewBoundsUtf16WithoutNormalizingUnicodeOrBinaryCharacters(int length)
    {
        var key = "\0é中" + new string('x', length - 3);
        var preview = Assert.IsType<KeyPreview>(KeyPreview.From(key));
        Assert.Equal(length, preview.Length);
        Assert.Equal(length > 1024, preview.Truncated);
        Assert.False(preview.Lossy);
        Assert.Equal(key[..Math.Min(length, 1024)], preview.Text);
        Assert.Null(KeyPreview.From(null));
        Assert.Equal(new KeyPreview("", 0, false), KeyPreview.From(""));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RegisterPreviewDistinguishesAbsentPresentEmptyAndOpaque(bool revisionEnvelope)
    {
        Assert.Equal(new RegisterPreview("register", false, null, null),
            RegisterPreview.FromAccepted(revisionEnvelope ? new("FCR2\0"u8) : Absent));
        var version = revisionEnvelope ? Revision(1) : Version(7, 11);
        var empty = RegisterPreview.FromAccepted(revisionEnvelope
            ? EnvelopeV2(1, ByteString.Empty) : Envelope(version, ByteString.Empty));
        Assert.Equal("register", empty.Format);
        Assert.True(empty.IsPresent);
        Assert.Equal(Convert.ToBase64String(version.ToArray()), empty.Version);
        Assert.Equal(new PayloadPreview(0, "", false), empty.Value);
        ByteString[] opaque =
        [
            new([]), new([0, 255]), new("FCR1"u8), new("FCR1\0extra"u8),
            new("FCR1\u0002"u8), new("FCR1\u0001"u8),
            Envelope(new(new byte[8]), new([7])),
            EnvelopeV2(0, Payload), EnvelopeV2(1, Payload, sequence: 0),
            new(EnvelopeV2(1, ByteString.Empty).Span[..28])
        ];
        foreach (var bytes in opaque)
            Assert.Equal(new RegisterPreview("opaque", null, null, null), RegisterPreview.FromAccepted(bytes));
    }

    [Fact]
    public void KeyPreviewTruncationDoesNotSplitValidSurrogatePair()
    {
        var key = new string('a', 1023) + "\U0001F600" + "tail";
        var preview = Assert.IsType<KeyPreview>(KeyPreview.From(key));
        Assert.Equal(1029, preview.Length);
        Assert.True(preview.Truncated);
        Assert.False(preview.Lossy);
        Assert.Equal(new string('a', 1023), preview.Text);
        var fits = new string('a', 1022) + "\U0001F600";
        Assert.Equal(new KeyPreview(fits, 1024, false), KeyPreview.From(fits));
    }

    [Fact]
    public void KeyPreviewMalformedUtf16IsExplicitlyLossyAndNeverAnExactAlias()
    {
        foreach (var malformed in new[] { "a\ud800b", "a\udc00b" })
        {
            var preview = Assert.IsType<KeyPreview>(KeyPreview.From(malformed));
            Assert.Equal("a\uFFFDb", preview.Text);
            Assert.Equal(3, preview.Length);
            Assert.True(preview.Lossy);
            Assert.False(preview.Truncated);
            Assert.NotEqual(KeyPreview.From("a\uFFFDb"), preview);
        }
        var longKey = "\ud800" + new string('x', 1024);
        var bounded = Assert.IsType<KeyPreview>(KeyPreview.From(longKey));
        Assert.Equal("\uFFFD" + new string('x', 1023), bounded.Text);
        Assert.Equal(1025, bounded.Length);
        Assert.True(bounded.Lossy);
        Assert.True(bounded.Truncated);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AcceptorStatePreviewPreservesFullBallotsAndTruncatesEnvelopePayloadIndependently(bool revisionEnvelope)
    {
        var payload = new ByteString(Enumerable.Range(0, 300).Select(i => (byte)i).ToArray());
        var accepted = revisionEnvelope ? EnvelopeV2(1, payload) : Envelope(Version(7, 11), payload);
        var preview = AcceptorStateObservation.From(new("中", new(21, 8), new(20, 7), accepted));
        Assert.Equal(new Ballot(21, 8), preview.PromisedBallot);
        Assert.Equal(new Ballot(20, 7), preview.AcceptedBallot);
        Assert.Equal(new KeyPreview("中", 1, false), preview.Key);
        Assert.Equal(revisionEnvelope ? 329 : 313, preview.AcceptedValue!.Length);
        Assert.True(preview.AcceptedValue.Truncated);
        Assert.Equal(accepted.ToArray().Take(256), Convert.FromBase64String(preview.AcceptedValue.Base64));
        Assert.Equal(300, preview.Register!.Value!.Length);
        Assert.True(preview.Register.Value.Truncated);
        Assert.Equal(payload.ToArray().Take(256), Convert.FromBase64String(preview.Register.Value.Base64));
        var promised = AcceptorStateObservation.From(new("promise", new(8, 3), Ballot.Zero, null));
        Assert.Null(promised.AcceptedValue);
        Assert.Null(promised.Register);
    }
}
