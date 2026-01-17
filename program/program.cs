using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Text;

namespace program;

public static class OneBrc
{
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

    var sw = System.Diagnostics.Stopwatch.StartNew();
    var (output, totalCount) = ProcessFile(path, workers);
    sw.Stop();

    long bytes = new FileInfo(path).Length;
    double seconds = sw.Elapsed.TotalSeconds;
    double mibPerSec = bytes / (1024.0 * 1024.0) / seconds;
    double linesPerSec = totalCount / seconds;

    Console.WriteLine(output);
    Console.WriteLine($"Processed {totalCount:n0} lines in {seconds:n2}s  |  {linesPerSec:n0} lines/s  |  {mibPerSec:n1} MiB/s");
  }

  public static (string Output, long TotalCount) ProcessFile(string path, int workers)
  {
    long fileLength = new FileInfo(path).Length;
    if (fileLength == 0) return ("{}", 0);

    using var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
    using var accessor = mmf.CreateViewAccessor(0, fileLength, MemoryMappedFileAccess.Read);

    unsafe
    {
      byte* ptr = null;
      accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref ptr);
      try
      {
        return ProcessFileCore(ptr, fileLength, workers);
      }
      finally
      {
        accessor.SafeMemoryMappedViewHandle.ReleasePointer();
      }
    }
  }

  private static unsafe (string Output, long TotalCount) ProcessFileCore(byte* filePtr, long fileLength, int workers)
  {
    var ranges = MakeRanges(filePtr, fileLength, workers);
    var partials = new StationMap[ranges.Length];

    // Process ranges in parallel
    Parallel.For(0, ranges.Length, new ParallelOptions { MaxDegreeOfParallelism = workers }, i =>
    {
      var map = new StationMap();
      ProcessRange(filePtr, ranges[i].start, ranges[i].end, map);
      partials[i] = map;
    });

    // Merge all maps
    var final = partials[0];
    for (int i = 1; i < partials.Length; i++)
    {
      final.Merge(partials[i]);
    }

    return final.FormatOutput();
  }

  private static unsafe (long start, long end)[] MakeRanges(byte* filePtr, long fileLength, int n)
  {
    if (n <= 1) return [(0, fileLength)];

    var result = new (long start, long end)[n];
    long baseSize = fileLength / n;
    long pos = 0;

    for (int i = 0; i < n; i++)
    {
      long start = pos;
      long end = (i == n - 1) ? fileLength : start + baseSize;

      // Align end to newline boundary (except for last chunk)
      if (i < n - 1 && end < fileLength)
      {
        while (end < fileLength && filePtr[end] != (byte)'\n') end++;
        if (end < fileLength) end++; // Include the newline in this chunk
      }

      result[i] = (start, end);
      pos = end;
    }

    return result;
  }

  private static unsafe void ProcessRange(byte* filePtr, long start, long end, StationMap map)
  {
    long pos = start;

    while (pos < end)
    {
      // Find newline using SIMD-optimized IndexOf
      long newline = FindByte(filePtr, pos, end, (byte)'\n');
      if (newline < 0) newline = end;

      // Fixed-offset semicolon check: temp is always 3-5 chars (e.g., "1.2", "-45.6")
      // So semicolon is at newline-4, newline-5, or newline-6
      long semicolon;
      if (filePtr[newline - 4] == ';') semicolon = newline - 4;
      else if (filePtr[newline - 5] == ';') semicolon = newline - 5;
      else semicolon = newline - 6;

      int stationLen = (int)(semicolon - pos);
      int tempLen = (int)(newline - semicolon - 1);

      var stationBytes = new ReadOnlySpan<byte>(filePtr + pos, stationLen);
      var tempBytes = new ReadOnlySpan<byte>(filePtr + semicolon + 1, tempLen);

      int temp = ParseTempBranchless(tempBytes);
      map.AddMeasurement(stationBytes, temp);

      pos = newline + 1;
    }
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  private static unsafe long FindByte(byte* data, long start, long end, byte target)
  {
    // Use .NET's built-in IndexOf - already SIMD optimized
    int len = (int)Math.Min(end - start, int.MaxValue);
    var span = new ReadOnlySpan<byte>(data + start, len);
    int idx = span.IndexOf(target);
    return idx >= 0 ? start + idx : -1;
  }

  // Branchless temperature parsing
  // Format: [-]D[D].D (e.g., "1.2", "-3.4", "12.3", "-45.6")
  // Returns temperature in tenths (e.g., "12.3" -> 123)
  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  public static int ParseTempBranchless(ReadOnlySpan<byte> s)
  {
    int isNeg = (s[0] == (byte)'-') ? 1 : 0;
    int len = s.Length;
    int idx = isNeg;

    int whole, frac;
    int twoDigitWhole = ((len - isNeg) == 4) ? 1 : 0;

    if (twoDigitWhole == 1)
    {
      whole = (s[idx] - '0') * 10 + (s[idx + 1] - '0');
      frac = s[idx + 3] - '0';
    }
    else
    {
      whole = s[idx] - '0';
      frac = s[idx + 2] - '0';
    }

    int value = whole * 10 + frac;
    return (value ^ -isNeg) + isNeg;
  }

  public static string FormatTenth(int t10)
  {
    bool neg = t10 < 0;
    int x = neg ? -t10 : t10;
    int whole = x / 10;
    int frac = x % 10;

    int digits = whole == 0 ? 1 : (int)Math.Log10(whole) + 1;
    int len = (neg ? 1 : 0) + digits + 2;

    return string.Create(len, (neg, whole, frac), static (span, s) =>
    {
      int pos = 0;
      if (s.neg) span[pos++] = '-';

      int start = pos;
      int w = s.whole;
      do { span[pos++] = (char)('0' + (w % 10)); w /= 10; } while (w > 0);
      span[start..pos].Reverse();

      span[pos++] = '.';
      span[pos++] = (char)('0' + s.frac);
    });
  }
}

// Custom hash table for station aggregation
public sealed class StationMap
{
  private const int InitialCapacity = 65536; // Larger to reduce collisions
  private const float LoadFactor = 0.5f;

  private Entry[] _entries;
  private int _count;
  private int _mask;

  public StationMap()
  {
    _entries = new Entry[InitialCapacity];
    _mask = InitialCapacity - 1;
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  public void AddMeasurement(ReadOnlySpan<byte> station, int temp)
  {
    uint hash = ComputeHash(station);
    int idx = (int)(hash & (uint)_mask);

    while (true)
    {
      ref var entry = ref _entries[idx];

      if (entry.Key == null)
      {
        entry.Key = station.ToArray();
        entry.Hash = hash;
        entry.Min = temp;
        entry.Max = temp;
        entry.Sum = temp;
        entry.Count = 1;
        _count++;

        if (_count > _entries.Length * LoadFactor)
        {
          Resize();
        }
        return;
      }

      if (entry.Hash == hash && station.Length == entry.Key.Length && station.SequenceEqual(entry.Key))
      {
        if (temp < entry.Min) entry.Min = temp;
        if (temp > entry.Max) entry.Max = temp;
        entry.Sum += temp;
        entry.Count++;
        return;
      }

      idx = (idx + 1) & _mask;
    }
  }

  public void Merge(StationMap other)
  {
    foreach (ref var entry in other._entries.AsSpan())
    {
      if (entry.Key == null) continue;

      uint hash = entry.Hash;
      int idx = (int)(hash & (uint)_mask);

      while (true)
      {
        ref var slot = ref _entries[idx];

        if (slot.Key == null)
        {
          slot = entry;
          _count++;
          if (_count > _entries.Length * LoadFactor) Resize();
          break;
        }

        if (slot.Hash == hash && entry.Key.Length == slot.Key.Length && entry.Key.AsSpan().SequenceEqual(slot.Key))
        {
          if (entry.Min < slot.Min) slot.Min = entry.Min;
          if (entry.Max > slot.Max) slot.Max = entry.Max;
          slot.Sum += entry.Sum;
          slot.Count += entry.Count;
          break;
        }

        idx = (idx + 1) & _mask;
      }
    }
  }

  public (string Output, long TotalCount) FormatOutput()
  {
    var stations = new List<(string Name, int Min, int Max, long Sum, int Count)>();
    long totalCount = 0;

    foreach (ref var entry in _entries.AsSpan())
    {
      if (entry.Key == null) continue;
      string name = Encoding.UTF8.GetString(entry.Key);
      stations.Add((name, entry.Min, entry.Max, entry.Sum, entry.Count));
      totalCount += entry.Count;
    }

    stations.Sort((a, b) => string.CompareOrdinal(a.Name, b.Name));

    var sb = new StringBuilder(stations.Count * 32);
    sb.Append('{');

    for (int i = 0; i < stations.Count; i++)
    {
      if (i > 0) sb.Append(", ");

      var (name, min, max, sum, count) = stations[i];
      int avg = sum >= 0
          ? (int)((sum + count / 2) / count)
          : (int)((sum - count / 2) / count);

      sb.Append(name).Append('=')
        .Append(OneBrc.FormatTenth(min)).Append('/')
        .Append(OneBrc.FormatTenth(avg)).Append('/')
        .Append(OneBrc.FormatTenth(max));
    }

    sb.Append('}');
    return (sb.ToString(), totalCount);
  }

  private void Resize()
  {
    var oldEntries = _entries;
    int newCapacity = _entries.Length * 2;
    _entries = new Entry[newCapacity];
    _mask = newCapacity - 1;
    _count = 0;

    foreach (ref var entry in oldEntries.AsSpan())
    {
      if (entry.Key == null) continue;

      int idx = (int)(entry.Hash & (uint)_mask);
      while (_entries[idx].Key != null)
      {
        idx = (idx + 1) & _mask;
      }
      _entries[idx] = entry;
      _count++;
    }
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  private static uint ComputeHash(ReadOnlySpan<byte> bytes)
  {
    // Read first bytes using MemoryMarshal (no fixed block needed)
    int len = bytes.Length;
    ulong v;
    if (len >= 8)
      v = System.Runtime.InteropServices.MemoryMarshal.Read<ulong>(bytes);
    else if (len >= 4)
      v = System.Runtime.InteropServices.MemoryMarshal.Read<uint>(bytes) | ((ulong)len << 32);
    else
      v = bytes[0] | ((ulong)len << 8);

    // Mix the bits (simplified xxHash-style finalizer)
    v ^= v >> 33;
    v *= 0xff51afd7ed558ccdUL;
    v ^= v >> 33;
    return (uint)v ^ (uint)(v >> 32);
  }

  private struct Entry
  {
    public byte[]? Key;
    public uint Hash;
    public int Min;
    public int Max;
    public long Sum;
    public int Count;
  }
}
