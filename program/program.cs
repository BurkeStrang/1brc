// Adapted from xoofx's Fast1BRC implementation
// https://github.com/xoofx/Fast1BRC
// Simplified for Linux only

using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace program;

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
        int workers = (args.Length >= 2 && int.TryParse(args[1], out var w) && w > 0)
            ? w : Environment.ProcessorCount;

        Console.OutputEncoding = Encoding.UTF8;

        var sw = Stopwatch.StartNew();
        var output = ProcessFile(path, workers);
        sw.Stop();

        long bytes = new FileInfo(path).Length;
        double seconds = sw.Elapsed.TotalSeconds;
        double mibPerSec = bytes / (1024.0 * 1024.0) / seconds;

        Console.WriteLine(output);
        Console.WriteLine($"Processed in {seconds:n2}s  |  {mibPerSec:n1} MiB/s");
    }

    public static string ProcessFile(string filePath, int workers)
    {
        using var fileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var fileLength = RandomAccess.GetLength(fileHandle);
        if (fileLength == 0) return "{}";

        // Split the file by chunks and process them in parallel
        Span<byte> localBuffer = stackalloc byte[256];
        var taskCount = Math.Max((int)Math.Max(fileLength / (int.MaxValue - Environment.SystemPageSize), 1), workers);
        var threads = new List<Thread>(taskCount);
        var results = new DictionaryGroup[taskCount];
        var chunkSize = fileLength / taskCount;

        long startOffset = 0;
        for (int i = 0; i < taskCount; i++)
        {
            long endOffset;
            if (i + 1 < taskCount)
            {
                endOffset = (i + 1) * chunkSize;
                RandomAccess.Read(fileHandle, localBuffer, endOffset);
                var indexOfEndOfLine = localBuffer.IndexOf((byte)'\n');
                Debug.Assert(indexOfEndOfLine >= 0);
                endOffset += indexOfEndOfLine + 1;
            }
            else
            {
                endOffset = fileLength;
            }

            long localStartOffset = startOffset;
            if (i + 1 == taskCount)
            {
                // Last chunk on current thread
                results[i] = ProcessChunkMemoryMapped(filePath, localStartOffset, endOffset);
            }
            else
            {
                var localIndex = i;
                var thread = new Thread(() =>
                {
                    results[localIndex] = ProcessChunkMemoryMapped(filePath, localStartOffset, endOffset);
                });
                thread.Start();
                threads.Add(thread);
            }

            startOffset = endOffset;
        }

        // Aggregate the results
        var result = new Dictionary<string, EntryItem>(11000);
        if (results.Length > 0)
        {
            var dict = results[^1];
            dict.AggregateTo(result);
            dict.Dispose();

            for (int i = 0; i < results.Length - 1; i++)
            {
                if (i < threads.Count)
                {
                    threads[i].Join();
                }
                dict = results[i];
                dict.AggregateTo(result);
                dict.Dispose();
            }
        }

        // Format the results
        var builder = new StringBuilder(result.Count * 100);
        builder.Append('{');
        bool isFirst = true;
        foreach (var pair in result.OrderBy(x => x.Key, StringComparer.Ordinal))
        {
            if (!isFirst) builder.Append(", ");
            builder.Append($"{pair.Key}={pair.Value.MinTemp / 10.0:0.0}/{pair.Value.SumTemp / (10.0 * pair.Value.Count):0.0}/{pair.Value.MaxTemp / 10.0:0.0}");
            isFirst = false;
        }
        builder.Append('}');

        return builder.ToString();
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static unsafe DictionaryGroup ProcessChunkMemoryMapped(string filePath, long startOffset, long endOffset)
    {
        var entries = new DictionaryGroup();

        using var fileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.RandomAccess);
        using var mappedFile = MemoryMappedFile.CreateFromFile(fileHandle, null, 0, MemoryMappedFileAccess.Read, HandleInheritability.None, true);

        var bufferLength = endOffset - startOffset;
        using var viewAccessor = mappedFile.CreateViewAccessor(startOffset, bufferLength, MemoryMappedFileAccess.Read);
        var handle = viewAccessor.SafeMemoryMappedViewHandle;
        byte* buffer = null;
        handle.AcquirePointer(ref buffer);
        ProcessBuffer256(entries, buffer + viewAccessor.PointerOffset, (nint)bufferLength);
        handle.ReleasePointer();

        return entries;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static unsafe void ProcessBuffer256(DictionaryGroup entries, byte* buffer, nint bufferLength)
    {
        // Use scalar fallback for small buffers (SIMD requires at least 32 byte reads)
        if (bufferLength < 64)
        {
            ProcessBufferScalar(entries, buffer, bufferLength);
            return;
        }

        nint index = 0;
        nint safeEnd = bufferLength - 32; // Leave room for SIMD reads

        while (index < bufferLength)
        {
            nint startLineIndex = index;

            var destName = entries.NamePointer;
            var last = Vector256<byte>.Zero;
            int nameLength;

            // SIMD search for semicolon (only when safe)
            while (index <= safeEnd)
            {
                var mask = Vector256.Create((byte)';');
                var v = Vector256.Load(buffer + index);
                var eq = Vector256.Equals(v, mask);

                if (eq == Vector256<byte>.Zero)
                {
                    v.Store(destName);
                    last = v;
                    destName += Vector256<byte>.Count;
                    index += Vector256<byte>.Count;
                }
                else
                {
                    var offset = BitOperations.TrailingZeroCount(eq.ExtractMostSignificantBits());
                    var val = Vector256.Create((byte)offset);
                    var indices = Vector256.Create((byte)0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31);
                    v = Vector256.GreaterThan(val, indices) & v;
                    index += offset;
                    nameLength = (int)(index - startLineIndex);
                    if (nameLength < 32)
                    {
                        last = v;
                    }
                    else if (nameLength > 32)
                    {
                        v.Store(destName);
                    }
                    goto found_semicolon;
                }
            }

            // Scalar fallback for remaining bytes near end of buffer
            while (index < bufferLength && *(buffer + index) != ';')
            {
                *destName++ = *(buffer + index);
                index++;
            }
            if (index >= bufferLength) break;
            nameLength = (int)(index - startLineIndex);
            if (nameLength <= 32)
            {
                // Copy name to vector for hash lookup
                var span = new ReadOnlySpan<byte>(buffer + startLineIndex, nameLength);
#pragma warning disable CA2014 // Do not use stackalloc in loops
                Span<byte> tempSpan = stackalloc byte[32];
#pragma warning restore CA2014 // Do not use stackalloc in loops
                tempSpan.Clear();
                span.CopyTo(tempSpan);
                last = Vector256.Create(tempSpan);
            }

        found_semicolon:
            index++; // Skip semicolon

            // Process the temperature
            int sign = 1;
            int temp = 0;
            while (index < bufferLength)
            {
                var c = *(buffer + index++);
                if (c == (byte)'-')
                {
                    sign = -1;
                }
                else if (c == (byte)'\n')
                {
                    temp *= sign;
                    break;
                }
                else if (c != '.')
                {
                    temp = temp * 10 + (c - '0');
                }
            }

            // Add the entry
            ref var entry = ref entries.GetOrAdd(last, nameLength);
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

    // Scalar fallback for small buffers
    private static unsafe void ProcessBufferScalar(DictionaryGroup entries, byte* buffer, nint bufferLength)
    {
        nint index = 0;
        while (index < bufferLength)
        {
            nint startLineIndex = index;

            // Find semicolon
            while (index < bufferLength && *(buffer + index) != ';') index++;
            if (index >= bufferLength) break;

            int nameLength = (int)(index - startLineIndex);
            index++; // Skip semicolon

            // Parse temperature
            int sign = 1;
            int temp = 0;
            while (index < bufferLength)
            {
                var c = *(buffer + index++);
                if (c == (byte)'-')
                {
                    sign = -1;
                }
                else if (c == (byte)'\n')
                {
                    temp *= sign;
                    break;
                }
                else if (c != '.')
                {
                    temp = temp * 10 + (c - '0');
                }
            }

            // Build vector key for lookup
#pragma warning disable CA2014 // Do not use stackalloc in loops
            Span<byte> nameSpan = stackalloc byte[32];
#pragma warning restore CA2014 // Do not use stackalloc in loops
            nameSpan.Clear();
            new ReadOnlySpan<byte>(buffer + startLineIndex, Math.Min(nameLength, 32)).CopyTo(nameSpan);
            var keyVector = Vector256.Create(nameSpan);

            // Copy to entries buffer for longer names
            if (nameLength > 32)
            {
                var dest = entries.NamePointer;
                new ReadOnlySpan<byte>(buffer + startLineIndex, nameLength).CopyTo(new Span<byte>(dest, nameLength));
            }

            // Add the entry
            ref var entry = ref entries.GetOrAdd(keyVector, nameLength);
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

    private sealed unsafe class DictionaryGroup : IDisposable
    {
        private FastDictionary<KeyName32, EntryItem> _entries32 = new(6000);
        private FastDictionary<KeyName128, EntryItem> _entriesAny = new(2000);
        private readonly KeyName128* _name128;

        public DictionaryGroup()
        {
            _name128 = (KeyName128*)NativeMemory.AlignedAlloc((nuint)sizeof(KeyName128), CacheLineSize);
        }

        public void Dispose()
        {
            NativeMemory.AlignedFree(_name128);
            _entries32.Dispose();
            _entriesAny.Dispose();
        }

        public byte* NamePointer => (byte*)_name128;

        public void AggregateTo(Dictionary<string, EntryItem> result)
        {
            foreach (var item in GetValues())
            {
                string name = item.Item1;
                ref var existingValue = ref CollectionsMarshal.GetValueRefOrAddDefault(result, name, out _);
                existingValue.AggregateFrom(in item.Item2);
            }
        }

        public IEnumerable<(string, EntryItem)> GetValues()
        {
            foreach (var entry in _entries32)
            {
                yield return (entry.Key.ToString(), entry.Value);
            }
            foreach (var entry in _entriesAny)
            {
                yield return (entry.Key.ToString(), entry.Value);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref EntryItem GetOrAdd(Vector256<byte> value, int length)
        {
            if (length <= 32)
            {
                return ref _entries32.GetValueRefOrAddDefault(value);
            }

            ClearName(length);
            return ref _entriesAny.GetValueRefOrAddDefault(in *_name128);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ClearName(int length)
        {
            Unsafe.InitBlockUnaligned(NamePointer + length, 0, (uint)(sizeof(KeyName128) - length));
        }
    }

    [StructLayout(LayoutKind.Sequential, Size = 32)]
    private readonly struct KeyName32 : IEquatable<KeyName32>
    {
        private readonly long _name1;
        private readonly long _name2;
        private readonly long _name3;
        private readonly long _name4;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(KeyName32 other)
        {
            if (Vector256.IsHardwareAccelerated)
            {
                return ToVector256().Equals(other.ToVector256());
            }
            return _name1 == other._name1 && _name2 == other._name2 && _name3 == other._name3 && _name4 == other._name4;
        }

        public override bool Equals(object? obj) => obj is KeyName32 other && Equals(other);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode() => ((_name1 * 397) ^ _name2).GetHashCode();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetHashCode(Vector256<byte> key) => ((key.AsInt64().GetElement(0) * 397) ^ key.AsInt64().GetElement(1)).GetHashCode();

        public override unsafe string ToString()
        {
            fixed (void* name = &_name1)
            {
                var span = new Span<byte>(name, 32);
                var indexOf0 = span.IndexOf((byte)0);
                if (indexOf0 >= 0) span = span[..indexOf0];
                return Encoding.UTF8.GetString(span);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Vector256<byte> ToVector256() => Unsafe.BitCast<KeyName32, Vector256<byte>>(this);
    }

    [StructLayout(LayoutKind.Sequential, Size = 128)]
    private readonly struct KeyName128 : IEquatable<KeyName128>
    {
        private readonly Vector256<byte> _name1;
        private readonly Vector256<byte> _name2;
        private readonly Vector256<byte> _name3;
        private readonly Vector256<byte> _name4;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(KeyName128 other)
        {
            return _name1 == other._name1 && _name2 == other._name2 && _name3 == other._name3 && _name4 == other._name4;
        }

        public override bool Equals(object? obj) => obj is KeyName128 other && Equals(other);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override int GetHashCode() => ((_name1.AsInt64().GetElement(0) * 397) ^ _name1.AsInt64().GetElement(1)).GetHashCode();

        public override unsafe string ToString()
        {
            fixed (void* name = &_name1)
            {
                var span = new Span<byte>(name, 128);
                var indexOf0 = span.IndexOf((byte)0);
                if (indexOf0 >= 0) span = span[..indexOf0];
                return Encoding.UTF8.GetString(span);
            }
        }
    }

    public struct EntryItem
    {
        public long SumTemp;
        public long Count;
        public int MinTemp;
        public int MaxTemp;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AggregateFrom(in EntryItem item)
        {
            SumTemp += item.SumTemp;
            Count += item.Count;
            if (Count == item.Count)
            {
                MinTemp = item.MinTemp;
                MaxTemp = item.MaxTemp;
            }
            else
            {
                MinTemp = Math.Min(item.MinTemp, MinTemp);
                MaxTemp = Math.Max(item.MaxTemp, MaxTemp);
            }
        }
    }

    private unsafe struct FastDictionary<TKey, TValue> : IDisposable, IEnumerable<KeyValuePair<TKey, TValue>>
        where TKey : unmanaged, IEquatable<TKey>
        where TValue : unmanaged
    {
        private Entry** _buckets;
        private Entry* _entries;
        private int _capacity;
        private int _count;

        public FastDictionary(int capacity)
        {
            Initialize(Math.Max(capacity, 4));
        }

        public readonly int Count => _count;

        public readonly void Dispose()
        {
            NativeMemory.AlignedFree(_buckets);
            NativeMemory.AlignedFree(_entries);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref TValue GetValueRefOrAddDefault(Vector256<byte> key)
        {
            uint hashCode = (uint)KeyName32.GetHashCode(key);

            ref Entry* bucket = ref GetBucket(hashCode);
            for (Entry* entry = bucket; entry != null; entry = entry->next)
            {
                if (Vector256.LoadAligned((byte*)&entry->key) == key)
                {
                    return ref entry->value;
                }
            }

            int count = _count;
            if (count == _capacity)
            {
                Resize();
                bucket = ref GetBucket(hashCode);
            }
            int index = count;
            _count = count + 1;

            var entries = _entries;
            var newEntry = entries + index;
            newEntry->key = Unsafe.BitCast<Vector256<byte>, TKey>(key);
            newEntry->value = default;
            newEntry->next = bucket;
            bucket = newEntry;

            return ref newEntry->value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref TValue GetValueRefOrAddDefault(in TKey key)
        {
            uint hashCode = (uint)key.GetHashCode();
            ref Entry* bucket = ref GetBucket(hashCode);
            for (Entry* entry = bucket; entry != null; entry = entry->next)
            {
                if (entry->key.Equals(key))
                {
                    return ref entry->value;
                }
            }

            int count = _count;
            if (count == _capacity)
            {
                Resize();
                bucket = ref GetBucket(hashCode);
            }
            int index = count;
            _count = count + 1;

            var entries = _entries;
            var newEntry = entries + index;
            newEntry->key = key;
            newEntry->value = default;
            newEntry->next = bucket;
            bucket = newEntry;

            return ref newEntry->value;
        }

        public readonly Enumerator GetEnumerator() => new(this);
        readonly IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator() => GetEnumerator();
        readonly System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

        private void Initialize(int capacity)
        {
            int size = GetPrime(capacity);
            _buckets = (Entry**)NativeMemory.AlignedAlloc((nuint)(size * sizeof(Entry*)), CacheLineSize);
            new Span<nint>(_buckets, size).Clear();
            _entries = (Entry*)NativeMemory.AlignedAlloc((nuint)(size * sizeof(Entry)), CacheLineSize);
            _capacity = size;
        }

        private void Resize()
        {
            int newSize = GetPrime(_count * 2);

            Entry* entries = (Entry*)NativeMemory.AlignedAlloc((nuint)(newSize * sizeof(Entry)), CacheLineSize);
            new Span<Entry>(_entries, _count).CopyTo(new Span<Entry>(entries, newSize));
            NativeMemory.AlignedFree(_entries);

            NativeMemory.AlignedFree(_buckets);
            _buckets = (Entry**)NativeMemory.AlignedAlloc((nuint)(newSize * sizeof(Entry*)), CacheLineSize);
            new Span<nint>(_buckets, newSize).Clear();
            _capacity = newSize;

            for (int i = 0; i < _count; i++)
            {
                ref Entry* bucket = ref GetBucket((uint)entries[i].key.GetHashCode());
                entries[i].next = bucket;
                bucket = entries + i;
            }

            _entries = entries;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ref Entry* GetBucket(uint hashCode)
        {
            return ref _buckets[hashCode % (uint)_capacity];
        }

        private static int GetPrime(int min)
        {
            int[] primes = [3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
        1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
        17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
        187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
        1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369];
            foreach (var prime in primes)
                if (prime >= min) return prime;
            return min | 1;
        }

        [StructLayout(LayoutKind.Sequential, Size = CacheLineSize)]
        private struct Entry
        {
            public TKey key;
            public TValue value;
            public Entry* next;
        }

        public struct Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>
        {
            private readonly FastDictionary<TKey, TValue> _dictionary;
            private int _index;
            private KeyValuePair<TKey, TValue> _current;

            internal Enumerator(FastDictionary<TKey, TValue> dictionary)
            {
                _dictionary = dictionary;
                _index = 0;
                _current = default;
            }

            public bool MoveNext()
            {
                while ((uint)_index < (uint)_dictionary._count)
                {
                    ref Entry entry = ref _dictionary._entries[_index++];
                    _current = new KeyValuePair<TKey, TValue>(entry.key, entry.value);
                    return true;
                }
                _current = default;
                return false;
            }

            public readonly KeyValuePair<TKey, TValue> Current => _current;
            readonly object System.Collections.IEnumerator.Current => _current;
            public readonly void Dispose() { }
            public void Reset() { _index = 0; _current = default; }
        }
    }
}