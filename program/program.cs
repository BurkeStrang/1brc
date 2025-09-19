using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace program;

public static class OneBrc
{
  // Entry point for normal runs (Stopwatch macro-bench friendly)
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
    using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.SequentialScan);
    long length = RandomAccess.GetLength(handle);

    var ranges = MakeRanges(length, workers);
    var partials = new ConcurrentBag<Dictionary<string, Stats>>();

    Parallel.ForEach(ranges, range =>
    {
      var local = new Dictionary<string, Stats>(1024, StringComparer.Ordinal);
      ProcessRange(handle, range.start, range.end, local);
      partials.Add(local);
    });

    var final = new Dictionary<string, Stats>(1 << 16, StringComparer.Ordinal);
    long totalCount = 0;

    foreach (var map in partials)
    {
      foreach (var kvp in map)
      {
        ref var slot = ref CollectionsMarshal.GetValueRefOrAddDefault(final, kvp.Key, out bool exists);
        if (!exists) slot = kvp.Value;
        else slot.Merge(kvp.Value);
      }
    }
    foreach (var s in final.Values) totalCount += s.Count;

    var names = final.Keys.ToList();
    names.Sort(StringComparer.Ordinal);

    var sb = new StringBuilder(1 << 20).Append('{');
    for (int i = 0; i < names.Count; i++)
    {
      var n = names[i];
      var st = final[n];
      if (i > 0) sb.Append(", ");
      sb.Append(n).Append('=')
        .Append(FormatTenth(st.Min)).Append('/')
        .Append(FormatTenth((int)Math.Round((double)st.Sum / st.Count))).Append('/')
        .Append(FormatTenth(st.Max));
    }
    sb.Append('}');

    return (sb.ToString(), totalCount);
  }

  internal static (long start, long end)[] MakeRanges(long length, int n)
  {
    var res = new (long, long)[n];
    long baseSize = length / n;
    long pos = 0;
    for (int i = 0; i < n; i++)
    {
      long s = pos;
      long e = (i == n - 1) ? length : s + baseSize;
      res[i] = (s, e);
      pos = e;
    }
    return res;
  }

  internal static void ProcessRange(
      SafeFileHandle handle, long start, long end,
      Dictionary<string, Stats> map, int blockSize = 1 << 20)
  {
    byte[] buffer = ArrayPool<byte>.Shared.Rent(blockSize + 256);
    var utf8 = Encoding.UTF8;

    var pool = new StationPool();

    long pos = start;
    bool firstBlock = true;
    int carryLen = 0;
    Span<byte> carry = stackalloc byte[256];

    while (pos < end)
    {
      int toRead = (int)Math.Min(blockSize, end - pos);
      int read = RandomAccess.Read(handle, buffer.AsSpan(0, toRead), pos);
      if (read == 0) break;

      var span = buffer.AsSpan(0, read);
      int idx = 0;

      if (firstBlock && start != 0)
      {
        while (idx < span.Length && span[idx] != (byte)'\n') idx++;
        if (idx < span.Length) idx++;
      }
      firstBlock = false;

      while (idx < span.Length)
      {
        int lineStart = idx;
        while (idx < span.Length && span[idx] != (byte)'\n') idx++;
        bool eol = idx < span.Length && span[idx] == (byte)'\n';

        if (!eol)
        {
          int tailLen = span.Length - lineStart;
          if (carryLen + tailLen > carry.Length)
            carry = GrowStackSpan(carry, carryLen + tailLen);
          span[lineStart..].CopyTo(carry[carryLen..]);
          carryLen += tailLen;
          break;
        }

        int lineLen = idx - lineStart;

        if (carryLen > 0)
        {
          if (carryLen + lineLen > carry.Length)
            carry = GrowStackSpan(carry, carryLen + lineLen);
          span.Slice(lineStart, lineLen).CopyTo(carry[carryLen..]);

          ParseLine(carry[..(carryLen + lineLen)], map, utf8, pool);
          carryLen = 0;
        }
        else
        {
          ParseLine(span.Slice(lineStart, lineLen), map, utf8, pool);
        }
        idx++;
      }
      pos += read;
    }
    ArrayPool<byte>.Shared.Return(buffer);
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  internal static Span<byte> GrowStackSpan(Span<byte> span, int needed)
  {
    var arr = new byte[Math.Max(needed, span.Length * 2)];
    span.CopyTo(arr);
    return arr;
  }

  internal static void ParseLine(
      ReadOnlySpan<byte> line,
      Dictionary<string, Stats> map,
      Encoding utf8,
      StationPool pool)
  {
    int sep = line.IndexOf((byte)';');
    if (sep <= 0) return;

    var stationBytes = line[..sep];
    var tempBytes = line[(sep + 1)..];

    string station = pool.GetStation(stationBytes, utf8);
    int temp10 = ParseTempTenths(tempBytes);

    ref var stats = ref CollectionsMarshal.GetValueRefOrAddDefault(map, station, out bool exists);
    if (!exists) stats = new Stats(temp10);
    else stats.Add(temp10);
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  internal static int ParseTempTenths(ReadOnlySpan<byte> s)
  {
    int sign = 1, i = 0;
    if (s[0] == (byte)'-') { sign = -1; i = 1; }

    int val = 0, frac = 0, fracDigits = 0;
    for (; i < s.Length; i++)
    {
      byte c = s[i];
      if (c == (byte)'.') { i++; break; }
      val = val * 10 + (c - (byte)'0');
    }
    for (; i < s.Length; i++)
    {
      byte c = s[i];
      frac = frac * 10 + (c - (byte)'0');
      fracDigits++;
    }
    if (fracDigits == 0) return sign * val * 10;
    if (fracDigits == 1) return sign * (val * 10 + frac);
    int rounded = (int)Math.Round(frac / Math.Pow(10, fracDigits - 1));
    return sign * (val * 10 + rounded);
  }

  internal struct Stats(int t)
  {
    public int Min = t, Max = t;
    public long Sum = t;
    public int Count = 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(int t)
    {
      if (t < Min) Min = t;
      if (t > Max) Max = t;
      Sum += t; Count++;
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Merge(Stats other)
    {
      if (other.Min < Min) Min = other.Min;
      if (other.Max > Max) Max = other.Max;
      Sum += other.Sum; Count += other.Count;
    }
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  internal static string FormatTenth(int t10)
  {
    bool neg = t10 < 0;
    int x = Math.Abs(t10);
    int whole = x / 10;
    int frac = x % 10;
    return neg ? $"-{whole}.{frac}" : $"{whole}.{frac}";
  }

  internal class StationPool
  {
    private readonly Dictionary<int, List<(byte[] bytes, string station)>> _buckets = new();

    public string GetStation(ReadOnlySpan<byte> stationBytes, Encoding utf8)
    {
      int hash = ComputeHash(stationBytes);
      
      if (_buckets.TryGetValue(hash, out var bucket))
      {
        foreach (var (bytes, station) in bucket)
        {
          if (stationBytes.SequenceEqual(bytes))
            return station;
        }
        
        // Hash collision - add new entry to bucket
        string newStation = utf8.GetString(stationBytes);
        bucket.Add((stationBytes.ToArray(), newStation));
        return newStation;
      }
      else
      {
        // New hash - create bucket and add entry
        string newStation = utf8.GetString(stationBytes);
        _buckets[hash] = new List<(byte[], string)> { (stationBytes.ToArray(), newStation) };
        return newStation;
      }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ComputeHash(ReadOnlySpan<byte> bytes)
    {
      // Simple FNV-1a hash
      const int fnvPrime = 16777619;
      const int fnvOffsetBasis = unchecked((int)2166136261);
      
      int hash = fnvOffsetBasis;
      foreach (byte b in bytes)
      {
        hash ^= b;
        hash *= fnvPrime;
      }
      return hash;
    }
  }
}
