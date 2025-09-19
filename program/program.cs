using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace program;

[SkipLocalsInit]
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
    var partials = new Dictionary<string, Stats>[ranges.Length];

    Parallel.For(0, ranges.Length, new ParallelOptions { MaxDegreeOfParallelism = workers }, i =>
    {
      var (s, e) = ranges[i];
      var local = new Dictionary<string, Stats>(2048, StringComparer.Ordinal);
      ProcessRange(handle, s, e, local);
      partials[i] = local;
    });

    var final = new Dictionary<string, Stats>(65536, StringComparer.Ordinal);
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

    var sb = new StringBuilder(Math.Max(2, names.Count * 32)).Append('{');
    for (int i = 0; i < names.Count; i++)
    {
      var n = names[i];
      var st = final[n];
      if (i > 0) sb.Append(", ");

      // integer nearest rounding without FP
      long sum = st.Sum;
      int c = st.Count;
      int avg = sum >= 0 ? (int)((sum + (c / 2)) / c) : (int)((sum - (c / 2)) / c);

      sb.Append(n).Append('=')
        .Append(FormatTenth(st.Min)).Append('/')
        .Append(FormatTenth(avg)).Append('/')
        .Append(FormatTenth(st.Max));
    }
    sb.Append('}');

    return (sb.ToString(), totalCount);
  }

  internal static (long start, long end)[] MakeRanges(long length, int n)
  {
    if (n <= 1) return [(0L, length)];
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
      Dictionary<string, Stats> map, int blockSize = 1 << 25 /* 32 MiB */)
  {
    byte[] buffer = ArrayPool<byte>.Shared.Rent(blockSize + 512);
    var utf8 = Encoding.UTF8;
    var pool = StationPool.TlsGetOrCreate();

    long pos = start;
    bool firstBlock = true;

    // Carry buffer for partial line across block boundaries
    Span<byte> carry = stackalloc byte[512];
    int carryLen = 0;

    while (pos < end)
    {
      int toRead = (int)Math.Min(blockSize, end - pos);
      int read = RandomAccess.Read(handle, buffer.AsSpan(0, toRead), pos);
      if (read == 0) break;

      var span = buffer.AsSpan(0, read);
      int idx = 0;

      // Skip partial first line if we didn’t start at 0
      if (firstBlock && start != 0)
      {
        int nl = span.IndexOf((byte)'\n');
        if (nl >= 0) idx = nl + 1; else { pos += read; firstBlock = false; continue; }
      }
      firstBlock = false;

      while (idx < span.Length)
      {
        int rel = span[idx..].IndexOf((byte)'\n');
        if (rel < 0)
        {
          // tail (partial line)
          int tailLen = span.Length - idx;
          EnsureCarry(ref carry, carryLen + tailLen, carryLen);
          span.Slice(idx, tailLen).CopyTo(carry[carryLen..]);
          carryLen += tailLen;
          break;
        }

        int lineLen = rel;

        if (carryLen > 0)
        {
          EnsureCarry(ref carry, carryLen + lineLen, carryLen);
          span.Slice(idx, lineLen).CopyTo(carry[carryLen..]);
          ParseLineOnce(carry[..(carryLen + lineLen)], map, utf8, pool);
          carryLen = 0;
        }
        else
        {
          ParseLineOnce(span.Slice(idx, lineLen), map, utf8, pool);
        }

        idx += lineLen + 1; // skip '\n'
      }

      pos += read;
    }

    ArrayPool<byte>.Shared.Return(buffer);
  }

  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  private static void EnsureCarry(ref Span<byte> carry, int needed, int used)
  {
    if (needed <= carry.Length) return;
    int newSize = Math.Max(needed, carry.Length * 2);
    var arr = new byte[newSize];
    carry[..used].CopyTo(arr);
    carry = arr; // switch to array-backed span
  }

  // Single-pass parse for one line slice: find ';' once, decode station (ASCII fast path),
  // parse temperature tenths with rounding, and aggregate into the map.
  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  internal static void ParseLineOnce(
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

  // Integer parser for tenths with rounding from subsequent digits, handles negatives
  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  internal static int ParseTempTenths(ReadOnlySpan<byte> s)
  {
    int i = 0, sign = 1;
    if (s.Length > 0 && s[0] == (byte)'-') { sign = -1; i = 1; }

    int whole = 0;
    for (; i < s.Length; i++)
    {
      byte c = s[i];
      if (c == (byte)'.') { i++; break; }
      whole = (whole * 10) + (c - (byte)'0');
    }

    int tenths = 0;
    if (i < s.Length)
    {
      tenths = s[i] - (byte)'0';
      i++;
    }

    // Round by the next digit (hundredths) if present (away from zero)
    int roundUp = 0;
    if (i < s.Length)
    {
      byte d = s[i];
      if (d >= (byte)'5') roundUp = 1;
    }

    int t10 = whole * 10 + tenths + roundUp;
    return sign * t10;
  }

  internal struct Stats(int t)
  {
    public int Min = t, Max = t;
    public long Sum = t;
    public int Count = 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(int v)
    {
      if (v < Min) Min = v;
      if (v > Max) Max = v;
      Sum += v; Count++;
    }
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Merge(Stats other)
    {
      if (other.Min < Min) Min = other.Min;
      if (other.Max > Max) Max = other.Max;
      Sum += other.Sum; Count += other.Count;
    }
  }

  // Fast writer for tenths (e.g., 12.3 / -7.0)
  [MethodImpl(MethodImplOptions.AggressiveInlining)]
  internal static string FormatTenth(int t10)
  {
    bool neg = t10 < 0;
    int x = neg ? -t10 : t10;
    int whole = x / 10;
    int frac = x - whole * 10;

    // digit count for whole
    int digits = 1;
    for (int tmp = whole; tmp >= 10; tmp /= 10) digits++;
    int len = (neg ? 1 : 0) + digits + 2; // sign + digits + '.' + frac

    return string.Create(len, (neg, whole, frac), static (span, s) =>
    {
      int pos = 0;
      if (s.neg) span[pos++] = '-';

      // write whole in reverse then reverse that segment
      int start = pos;
      int w = s.whole;
      do { span[pos++] = (char)('0' + (w % 10)); w /= 10; } while (w > 0);
      span[start..pos].Reverse();

      span[pos++] = '.';
      span[pos++] = (char)('0' + s.frac);
    });
  }

  // Bucketized station interner with ASCII fast path; one UTF-8 decode per unique station
  internal sealed class StationPool
  {
    private const int BucketCount = 1 << 12; // 4096
    private readonly List<(byte[] bytes, string str)>[] _buckets = new List<(byte[], string)>[BucketCount];

    [ThreadStatic] private static StationPool? _tls;
    public static StationPool TlsGetOrCreate() => _tls ??= new StationPool();

    public string GetStation(ReadOnlySpan<byte> stationBytes, Encoding utf8)
    {
      int h = ComputeHash(stationBytes);
      int b = h & (BucketCount - 1);

      var list = _buckets[b];
      if (list is null)
      {
        list = new List<(byte[], string)>(4);
        string s = IsAscii(stationBytes) ? CreateAsciiString(stationBytes) : utf8.GetString(stationBytes);
        list.Add((stationBytes.ToArray(), s));
        _buckets[b] = list;
        return s;
      }

      for (int i = 0; i < list.Count; i++)
      {
        var (bytes, s) = list[i];
        if (stationBytes.SequenceEqual(bytes)) return s;
      }

      string ns = IsAscii(stationBytes) ? CreateAsciiString(stationBytes) : utf8.GetString(stationBytes);
      list.Add((stationBytes.ToArray(), ns));
      return ns;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsAscii(ReadOnlySpan<byte> s)
    {
      // simple, branch-friendly check; JIT may vectorize
      for (int i = 0; i < s.Length; i++) if (s[i] >= 0x80) return false;
      return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string CreateAsciiString(ReadOnlySpan<byte> s)
      => string.Create(s.Length, s, static (span, src) =>
      {
        for (int i = 0; i < src.Length; i++) span[i] = (char)src[i];
      });

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ComputeHash(ReadOnlySpan<byte> bytes)
    {
      const uint FnvPrime = 16777619;
      uint hash = 2166136261;
      foreach (byte b in bytes) { hash ^= b; hash *= FnvPrime; }
      return (int)hash;
    }
  }
}
