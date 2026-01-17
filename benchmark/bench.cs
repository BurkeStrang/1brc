using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using program;

namespace benchmark;

// Custom column to show throughput in lines/sec
public class ThroughputColumn(long lineCount) : IColumn
{
  private readonly long _lineCount = lineCount;
  public string Id => "Throughput";
  public string ColumnName => "Lines/sec";
  public bool AlwaysShow => true;
  public ColumnCategory Category => ColumnCategory.Custom;
  public int PriorityInCategory => 0;
  public bool IsNumeric => true;
  public UnitType UnitType => UnitType.Dimensionless;
  public string Legend => "Lines processed per second";
  public string GetValue(Summary summary, BenchmarkCase benchmarkCase)
  {
    var report = summary[benchmarkCase];
    if (report?.ResultStatistics == null) return "N/A";
    var meanNs = report.ResultStatistics.Mean;
    var linesPerSec = _lineCount / (meanNs / 1_000_000_000.0);
    return $"{linesPerSec:N0}";
  }
  public string GetValue(Summary summary, BenchmarkCase benchmarkCase, SummaryStyle style) =>
    GetValue(summary, benchmarkCase);
  public bool IsDefault(Summary summary, BenchmarkCase benchmarkCase) => false;
  public bool IsAvailable(Summary summary) => true;
}

// Shared base class to reduce duplication
public abstract class BenchBase
{
  protected static string GetRepoRoot()
  {
    var dir = new DirectoryInfo(Directory.GetCurrentDirectory());
    while (dir != null && !File.Exists(Path.Combine(dir.FullName, "1brc.slnx")))
    {
      dir = dir.Parent;
    }
    return dir?.FullName ?? throw new DirectoryNotFoundException("Could not find repository root");
  }

  // Test both half and full processor count - fewer workers often performs better
  public static IEnumerable<int> WorkerOptions =>
    [
      Math.Max(1, Environment.ProcessorCount / 2),
      Environment.ProcessorCount
    ];
}

[MemoryDiagnoser]
[GcServer(true)]
[WarmupCount(1)]
[IterationCount(3)]  // Multiple iterations for better statistics on small file
[SimpleJob(RuntimeMoniker.Net10_0)]
[Config(typeof(SmallBenchConfig))]
public class SmallBench : BenchBase
{
  private class SmallBenchConfig : ManualConfig
  {
    public SmallBenchConfig() => AddColumn(new ThroughputColumn(10_000_000));
  }

  private const string DataFile = "measurements-10000000.txt";
  private string? _fullPath;

  [GlobalSetup]
  public void Setup()
  {
    _fullPath = Path.Combine(GetRepoRoot(), DataFile);
    if (!File.Exists(_fullPath))
      throw new FileNotFoundException($"Benchmark data file not found: {_fullPath}");
  }

  [ParamsSource(nameof(WorkerOptions))]
  public int Workers { get; set; }

  [Benchmark(Description = "Parse+Aggregate 10M")]
  public (string Output, long Count) Run() => OneBrc.ProcessFile(_fullPath!, Workers);
}

[MemoryDiagnoser]
[GcServer(true)]
[WarmupCount(0)]     // Skip warmup for huge file
[IterationCount(1)]  // Single iteration for 1B rows
[SimpleJob(RuntimeMoniker.Net10_0)]
[Config(typeof(LargeBenchConfig))]
public class LargeBench : BenchBase
{
  private class LargeBenchConfig : ManualConfig
  {
    public LargeBenchConfig() => AddColumn(new ThroughputColumn(1_000_000_000));
  }

  private const string DataFile = "measurements-1000000000.txt";
  private string? _fullPath;

  [GlobalSetup]
  public void Setup()
  {
    _fullPath = Path.Combine(GetRepoRoot(), DataFile);
    if (!File.Exists(_fullPath))
      throw new FileNotFoundException($"Benchmark data file not found: {_fullPath}");
  }

  [ParamsSource(nameof(WorkerOptions))]
  public int Workers { get; set; }

  [Benchmark(Description = "Parse+Aggregate 1B")]
  public (string Output, long Count) Run() => OneBrc.ProcessFile(_fullPath!, Workers);
}

public static class ProgramBench
{
  public static void Main(string[] args)
  {
    if (args.Length > 0 && args[0] == "large")
      BenchmarkRunner.Run<LargeBench>();
    else
      BenchmarkRunner.Run<SmallBench>();
  }
}
