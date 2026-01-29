using System.Buffers;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

#pragma warning disable IDE1006 // Naming Styles
namespace program;
#pragma warning restore IDE1006 // Naming Styles

public static class OneBrc
{
    private const int CacheLineSize = 64;

    public static void Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine("Usage: 1brc <path> [workers]");
            return;
        }

        string path = args[0];
        int workers = (args.Length >= 2 && int.TryParse(args[1], out int w) && w > 0)
            ? w : Environment.ProcessorCount;

        Console.OutputEncoding = Encoding.UTF8;

        Stopwatch sw = Stopwatch.StartNew();
        string output = ProcessFile(path, workers);
        sw.Stop();

        long bytes = new FileInfo(path).Length;
        double seconds = sw.Elapsed.TotalSeconds;
        double mibPerSec = bytes / (1024.0 * 1024.0) / seconds;

        Console.WriteLine(output);
        Console.WriteLine($"Processed in {seconds:n2}s  |  {mibPerSec:n1} MiB/s");
    }

    public static unsafe string ProcessFile(string filePath, int workers)
    {
        using MemoryMappedFile mmf = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
        using MemoryMappedViewAccessor accessor = mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);

        long fileLength = accessor.Capacity;
        if (fileLength == 0) return "{}";

        byte* basePtr = null;
        accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref basePtr);

        try
        {
            int taskCount = workers;
            DictionaryGroup[] results = new DictionaryGroup[taskCount];
            (long Start, long End)[] ranges = new (long Start, long End)[taskCount];

            long chunkSize = fileLength / taskCount;
            long startOffset = 0;

            // newline align WITHOUT syscalls
            for (int i = 0; i < taskCount; i++)
            {
                long endOffset;

                if (i + 1 < taskCount)
                {
                    endOffset = (i + 1) * chunkSize;

                    byte* p = basePtr + endOffset;
                    while (*p != (byte)'\n') p++;
                    endOffset = p - basePtr + 1;
                }
                else
                {
                    endOffset = fileLength;
                }

                ranges[i] = (startOffset, endOffset);
                startOffset = endOffset;
            }

            Parallel.For(0, taskCount, i =>
            {
                (long start, long end) = ranges[i];
                results[i] = ProcessChunk(basePtr, start, end);
            });

            Dictionary<string, EntryItem> result = new(11000);
            for (int i = 0; i < results.Length; i++)
            {
                DictionaryGroup dict = results[i];
                dict.AggregateTo(result);
                dict.Dispose();
            }

            string[] keys = [.. result.Keys];
            Array.Sort(keys, StringComparer.Ordinal);

            int estimated = result.Count * 32;
            char[] rented = ArrayPool<char>.Shared.Rent(estimated);
            Span<char> span = rented;
            int pos = 0;

            span[pos++] = '{';

            for (int i = 0; i < keys.Length; i++)
            {
                if (i > 0)
                {
                    span[pos++] = ',';
                    span[pos++] = ' ';
                }

                string key = keys[i];
                key.AsSpan().CopyTo(span[pos..]);
                pos += key.Length;

                span[pos++] = '=';

                WriteTemp(span, ref pos, result[key].MinTemp);
                span[pos++] = '/';

                long sum = result[key].SumTemp;
                long count = result[key].Count;
                long avg = (sum + (count / 2)) / count;
                WriteTemp(span, ref pos, avg);
                span[pos++] = '/';

                WriteTemp(span, ref pos, result[key].MaxTemp);
            }

            span[pos++] = '}';

            string output = new(span[..pos]);
            ArrayPool<char>.Shared.Return(rented);
            return output;
        }
        finally
        {
            accessor.SafeMemoryMappedViewHandle.ReleasePointer();
        }
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    static void WriteTemp(Span<char> span, ref int pos, long value)
    {
        if (value < 0)
        {
            span[pos++] = '-';
            value = -value;
        }

        long whole = value / 10;
        long frac = value % 10;

        if (whole >= 10)
        {
            span[pos++] = (char)('0' + (whole / 10));
            span[pos++] = (char)('0' + (whole % 10));
        }
        else
        {
            span[pos++] = (char)('0' + whole);
        }

        span[pos++] = '.';
        span[pos++] = (char)('0' + frac);
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static unsafe DictionaryGroup ProcessChunk(
        byte* basePtr,
        long startOffset,
        long endOffset)
    {
        DictionaryGroup entries = new();
        ProcessBuffer256(entries, basePtr + startOffset, (nint)(endOffset - startOffset));
        return entries;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static unsafe void ProcessBuffer256(
        DictionaryGroup entries,
        byte* buffer,
        nint bufferLength)
    {
        const ulong c = 11400714819323198485UL;

        nint index = 0;

        while (index < bufferLength)
        {
            byte* nameStartPtr = buffer + index;
            ulong hash = 14695981039346656037UL;

            // -------- Name scan + hash --------
            while (true)
            {
                nint remaining = bufferLength - index;
                if (remaining <= 0)
                    return;

                byte* p = buffer + index;

                int off = FindSemicolon(p, remaining);
                if (off < 0)
                {
                    int take = (int)Math.Min(32, remaining);

                    int i = 0;
                    for (; i + 8 <= take; i += 8)
                    {
                        ulong x = *(ulong*)(p + i);
                        hash = (hash * c) ^ x;
                    }
                    for (; i < take; i++)
                    {
                        hash = (hash * c) ^ p[i];
                    }

                    index += take;
                    continue;
                }

                int j = 0;
                for (; j + 8 <= off; j += 8)
                {
                    ulong x = *(ulong*)(p + j);
                    hash = (hash * c) ^ x;
                }
                for (; j < off; j++)
                {
                    hash = (hash * c) ^ p[j];
                }

                index += off + 1;
                break;
            }

            int nameLength = (int)(buffer + index - 1 - nameStartPtr);

            // -------- Ultra-fast temperature parse --------

            byte* t = buffer + index;

            int neg = (*t == (byte)'-') ? 1 : 0;
            t += neg;

            int temp;

            if (t[1] == (byte)'.')
            {
                // d.d
                int d1 = t[0] - (byte)'0';
                int d2 = t[2] - (byte)'0';
                temp = d1 * 10 + d2;
                index += neg + 4; // "d.d\n"
            }
            else
            {
                // dd.d
                int d1 = t[0] - (byte)'0';
                int d2 = t[1] - (byte)'0';
                int d3 = t[3] - (byte)'0';
                temp = d1 * 100 + d2 * 10 + d3;
                index += neg + 5; // "dd.d\n"
            }

            if (neg != 0)
                temp = -temp;

            // -------- Dictionary --------
            ref EntryItem entry = ref entries.GetOrAdd(hash, nameStartPtr, nameLength);

            if (entry.Count == 0)
            {
                entry.MinTemp = int.MaxValue;
                entry.MaxTemp = int.MinValue;
            }

            entry.Count++;
            entry.SumTemp += temp;
            entry.MinTemp = Math.Min(entry.MinTemp, temp);
            entry.MaxTemp = Math.Max(entry.MaxTemp, temp);
        }
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe int FindSemicolon(byte* ptr, nint remaining)
    {
        if (remaining < 32)
        {
            for (int i = 0; i < remaining; i++)
                if (ptr[i] == (byte)';')
                    return i;
            return -1;
        }

        ref byte r0 = ref *ptr;
        Vector256<byte> v = Vector256.LoadUnsafe(ref r0);
        Vector256<byte> semi = Vector256.Create((byte)';');
        Vector256<byte> eq = Vector256.Equals(v, semi);

        uint mask = eq.ExtractMostSignificantBits();
        return mask == 0 ? -1 : BitOperations.TrailingZeroCount(mask);
    }

    private sealed unsafe class DictionaryGroup : IDisposable
    {
        private const int Capacity = 1 << 14;

        private readonly Entry* _entries;
        private readonly byte* _nameArena;
        private int _arenaOffset;

        public DictionaryGroup()
        {
            _entries = (Entry*)NativeMemory.AlignedAlloc((nuint)(Capacity * sizeof(Entry)), 64);
            new Span<Entry>(_entries, Capacity).Clear();

            _nameArena = (byte*)NativeMemory.AlignedAlloc(4_000_000, 64);
            _arenaOffset = 0;
        }

        public void Dispose()
        {
            NativeMemory.AlignedFree(_entries);
            NativeMemory.AlignedFree(_nameArena);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref EntryItem GetOrAdd(ulong hash, byte* namePtr, int len)
        {
            if (_arenaOffset + len > 4_000_000)
                throw new InvalidOperationException("Name arena overflow.");

            int idx = (int)(hash & (Capacity - 1));

            while (true)
            {
                ref Entry e = ref _entries[idx];
                ulong h = e.Hash;

                // Match first (better branch behavior)
                if (h == hash && e.Length == len)
                {
                    if (len >= 8)
                    {
                        if (Unsafe.ReadUnaligned<long>(e.NamePtr) ==
                            Unsafe.ReadUnaligned<long>(namePtr) &&
                            MemoryCompare(e.NamePtr, namePtr, len))
                            return ref e.Value;
                    }
                    else
                    {
                        if (SmallCompare(e.NamePtr, namePtr, len))
                            return ref e.Value;
                    }
                }

                if (h == 0)
                {
                    byte* dest = _nameArena + _arenaOffset;
                    Buffer.MemoryCopy(namePtr, dest, len, len);

                    e.Hash = hash;
                    e.NamePtr = dest;
                    e.Length = len;
                    _arenaOffset += len;

                    return ref e.Value;
                }

                idx = (idx + 1) & (Capacity - 1);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool MemoryCompare(byte* a, byte* b, int len)
        {
            for (int i = 8; i < len; i++)
                if (a[i] != b[i]) return false;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool SmallCompare(byte* a, byte* b, int len)
        {
            for (int i = 0; i < len; i++)
                if (a[i] != b[i]) return false;
            return true;
        }

        public void AggregateTo(Dictionary<string, EntryItem> result)
        {
            for (int i = 0; i < Capacity; i++)
            {
                ref Entry e = ref _entries[i];
                if (e.Hash == 0) continue;

                string name = Encoding.UTF8.GetString(new ReadOnlySpan<byte>(e.NamePtr, e.Length));
                ref EntryItem dst = ref CollectionsMarshal.GetValueRefOrAddDefault(result, name, out _);
                dst.AggregateFrom(in e.Value);
            }
        }

        private struct Entry
        {
            public ulong Hash;
            public byte* NamePtr;
            public int Length;
            public EntryItem Value;
        }
    }


    public struct EntryItem
    {
        public long SumTemp;
        public long Count;
        public int MinTemp;
        public int MaxTemp;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AggregateFrom(in EntryItem other)
        {
            SumTemp += other.SumTemp;
            Count += other.Count;

            if (Count == other.Count)
            {
                MinTemp = other.MinTemp;
                MaxTemp = other.MaxTemp;
            }
            else
            {
                MinTemp = Math.Min(MinTemp, other.MinTemp);
                MaxTemp = Math.Max(MaxTemp, other.MaxTemp);
            }
        }
    }
}
