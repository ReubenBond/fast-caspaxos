using System.Text;
using Microsoft.AspNetCore.Http;

namespace FastCASPaxos.Rest;

/// <summary>Exact, case-sensitive UTF-8 query decoding shared by register and administrative HTTP APIs.</summary>
public static class HttpQuery
{
    private static readonly UTF8Encoding Utf8 = new(false, true);

    public static Dictionary<string, string> Parse(HttpRequest request, params string[] allowedNames)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        var raw = request.QueryString.Value;
        if (string.IsNullOrEmpty(raw)) return result;
        foreach (var pair in raw.AsSpan(1).ToString().Split('&'))
        {
            var separator = pair.IndexOf('=');
            if (separator < 0) throw new ArgumentException("Query parameters require name=value.");
            var name = DecodeComponent(pair[..separator]);
            var value = DecodeComponent(pair[(separator + 1)..]);
            if (!allowedNames.Contains(name, StringComparer.Ordinal) || !result.TryAdd(name, value))
                throw new ArgumentException("Query parameter names must be supported and appear once.");
        }
        return result;
    }

    public static void ValidateResponseText(string value)
    {
        try { Utf8.GetByteCount(value); }
        catch (EncoderFallbackException error)
        {
            throw new InvalidOperationException("A listed key cannot be represented as HTTP Unicode text.", error);
        }
    }

    private static string DecodeComponent(string encoded)
    {
        try
        {
            var bytes = Utf8.GetBytes(encoded);
            var length = 0;
            for (var i = 0; i < bytes.Length; i++)
            {
                var value = bytes[i];
                if (value == '%')
                {
                    if (i + 2 >= bytes.Length || Hex(bytes[i + 1]) < 0 || Hex(bytes[i + 2]) < 0)
                        throw new ArgumentException("Query parameters require valid percent-encoded UTF-8.");
                    value = (byte)((Hex(bytes[i + 1]) << 4) | Hex(bytes[i + 2]));
                    i += 2;
                }
                else if (value == '+') value = (byte)' ';
                bytes[length++] = value;
            }
            return Utf8.GetString(bytes, 0, length);
        }
        catch (EncoderFallbackException) { throw new ArgumentException("Query parameters require well-formed Unicode."); }
        catch (DecoderFallbackException) { throw new ArgumentException("Query parameters require valid percent-encoded UTF-8."); }
    }

    private static int Hex(byte value) => value switch
    {
        >= (byte)'0' and <= (byte)'9' => value - '0',
        >= (byte)'A' and <= (byte)'F' => value - 'A' + 10,
        >= (byte)'a' and <= (byte)'f' => value - 'a' + 10,
        _ => -1,
    };
}
