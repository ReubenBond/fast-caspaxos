using FastCASPaxos.Model;
using FastCASPaxos.Server;
using Xunit;

namespace FastCASPaxos.Sqlite.Tests;

public sealed class SqliteListingTests
{
    [Fact]
    public async Task Listing_OrdersEdgeKeysOrdinallyAcrossPages()
    {
        using var database = new TestDatabase();
        // Already in ordinal UTF16 code-unit order, independently of SQLite's ordering.
        // Keep malformed Unicode and control characters in data, never theory labels.
        string[] expected = ["", "\0", "\0a", "%", "%_", "A", "_", "a", "a\0", "\ud800", "\ud800x",
            "\ud83d\ude00", "\udc00", "\uffff"];
        var accepted = new Ballot(2, 3);
        var promised = new Ballot(3, 4);
        await using (var server = await database.OpenAsync())
        {
            for (var index = expected.Length - 1; index >= 0; index--)
            {
                Assert.True((await server.AcceptAsync(expected[index],
                    new(accepted, new([(byte)index]), promised))).IsAccepted);
            }

            Assert.True((await server.PrepareAsync("promise-only", new(promised))).IsPromised);
        }

        await using var reopened = await database.OpenAsync();
        var entries = new List<KeyedAcceptorState>();
        string? cursor = null;
        for (var pageNumber = 0; pageNumber < expected.Length; pageNumber++)
        {
            var page = await reopened.ListKeysAsync(afterKey: cursor, limit: 3);
            entries.AddRange(page.Entries);
            if (page.ContinuationKey is null)
            {
                break;
            }

            Assert.Equal(3, page.Entries.Count);
            Assert.Equal(page.Entries[^1].Key, page.ContinuationKey);
            Assert.NotEqual(cursor, page.ContinuationKey);
            cursor = page.ContinuationKey;
        }

        Assert.Equal(expected, entries.Select(entry => entry.Key));
        for (var index = 0; index < expected.Length; index++)
        {
            Assert.Equal(accepted, entries[index].AcceptedBallot);
            Assert.Equal(promised, entries[index].PromisedBallot);
            Assert.Equal(new ByteString([(byte)index]), entries[index].AcceptedValue);
        }

        // A caller modifying its exported bytes cannot mutate subsequent reads.
        entries[0].AcceptedValue.ToArray()[0] = 255;
        Assert.Equal(new ByteString([0]), (await reopened.ListKeysAsync(limit: 1)).Entries[0].AcceptedValue);
    }

    [Fact]
    public async Task Listing_PrefixIsLiteralOrdinalAndCursorIsExclusive()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        string[] keys = ["%_", "%_A", "%_a", "%x", "A", "a", "a\0", "a\0x",
            "a\uffff", "a\uffffx", "b", "\ud800", "\ud800x", "\ud800y", "\ud801",
            "\uffff", "\uffffx", "\uffff\uffff", "\uffff\uffffx", ""];
        foreach (var key in keys)
        {
            Assert.True((await server.AcceptAsync(key,
                new(Ballot.InitialFast(), ByteString.Empty, Ballot.Zero))).IsAccepted);
        }

        var literal = await server.ListKeysAsync("%_", limit: 2);
        Assert.Equal(new[] { "%_", "%_A" }, literal.Entries.Select(entry => entry.Key));
        Assert.Equal("%_A", literal.ContinuationKey);
        var remainder = await server.ListKeysAsync("%_", literal.ContinuationKey, limit: 2);
        Assert.Equal(new[] { "%_a" }, remainder.Entries.Select(entry => entry.Key));
        Assert.Null(remainder.ContinuationKey);
        Assert.Equal(new[] { "A" }, (await server.ListKeysAsync("A")).Entries.Select(entry => entry.Key));
        Assert.Equal(new[] { "a\0", "a\0x" },
            (await server.ListKeysAsync("a\0")).Entries.Select(entry => entry.Key));
        Assert.Equal(new[] { "\ud800", "\ud800x", "\ud800y" },
            (await server.ListKeysAsync("\ud800")).Entries.Select(entry => entry.Key));
        Assert.Equal(new[] { "a\uffff", "a\uffffx" },
            (await server.ListKeysAsync("a\uffff")).Entries.Select(entry => entry.Key));
        Assert.Equal(new[] { "\uffff", "\uffffx", "\uffff\uffff", "\uffff\uffffx" },
            (await server.ListKeysAsync("\uffff")).Entries.Select(entry => entry.Key));
        Assert.Equal(new[] { "\uffff\uffff", "\uffff\uffffx" },
            (await server.ListKeysAsync("\uffff\uffff")).Entries.Select(entry => entry.Key));
        Assert.Equal(new[] { "%_a" },
            (await server.ListKeysAsync("%_", "%_Z")).Entries.Select(entry => entry.Key));
        Assert.Empty((await server.ListKeysAsync("%_", "z")).Entries);
        Assert.Empty((await server.ListKeysAsync("a\uffff", "b")).Entries);
        Assert.Empty((await server.ListKeysAsync("absent")).Entries);
        Assert.DoesNotContain((await server.ListKeysAsync(afterKey: "")).Entries, entry => entry.Key == "");
    }

    [Fact]
    public async Task Listing_ContinuationDistinguishesMoreRowsFromExactLimit()
    {
        using var database = new TestDatabase();
        await using var server = await database.OpenAsync();
        var empty = await server.ListKeysAsync(limit: 1);
        Assert.Empty(empty.Entries);
        Assert.Null(empty.ContinuationKey);
        foreach (var key in new[] { "", "b", "c", "d" })
        {
            Assert.True((await server.AcceptAsync(key,
                new(Ballot.InitialFast(), ByteString.Empty, Ballot.Zero))).IsAccepted);
        }

        var singleton = await server.ListKeysAsync(limit: 1);
        Assert.Equal("", Assert.Single(singleton.Entries).Key);
        Assert.Equal("", singleton.ContinuationKey); // Empty key is not end-of-list.
        var first = await server.ListKeysAsync(limit: 2);
        Assert.Equal(new[] { "", "b" }, first.Entries.Select(entry => entry.Key));
        Assert.Equal("b", first.ContinuationKey);
        var exactRemainder = await server.ListKeysAsync(afterKey: first.ContinuationKey, limit: 2);
        Assert.Equal(new[] { "c", "d" }, exactRemainder.Entries.Select(entry => entry.Key));
        Assert.Null(exactRemainder.ContinuationKey);
        var exactAll = await server.ListKeysAsync(limit: 4);
        Assert.Equal(4, exactAll.Entries.Count);
        Assert.Null(exactAll.ContinuationKey);
        var beyond = await server.ListKeysAsync(afterKey: "d", limit: 1);
        Assert.Empty(beyond.Entries);
        Assert.Null(beyond.ContinuationKey);
        Assert.Equal(4, (await server.ListKeysAsync(limit: int.MaxValue)).Entries.Count);
    }
}
