using System.Buffers.Binary;

namespace BuildingBlocks.Identifiers;

/// <summary>Crockford Base32 (no I, L, O, U): unambiguous when read aloud or typed back by a customer.</summary>
public static class CrockfordBase32
{
    private const string Alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

    /// <summary>
    /// Encodes all 128 bits of <paramref name="value"/> in RFC 9562 (big-endian) byte order as 26 characters.
    /// For a UUIDv7 the text therefore sorts in creation order, like a ULID. Allocation: the result string only.
    /// </summary>
    public static string Encode(Guid value)
    {
        Span<byte> bytes = stackalloc byte[16];
        value.TryWriteBytes(bytes, bigEndian: true, out _);
        var number = BinaryPrimitives.ReadUInt128BigEndian(bytes);

        return string.Create(26, number, static (chars, remaining) =>
        {
            for (var i = chars.Length - 1; i >= 0; i--)
            {
                chars[i] = Alphabet[(int)(remaining & 0x1Fu)];
                remaining >>= 5;
            }
        });
    }
}
