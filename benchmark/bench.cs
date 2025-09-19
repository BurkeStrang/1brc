using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Running;
using program;

namespace benchmark;

[MemoryDiagnoser]
[WarmupCount(0)]                 // full-file runs: skip warmup
[IterationCount(1)]              // one iteration for the huge file
[SimpleJob(RuntimeMoniker.Net90)]
public class SmallBench
{
    [Params("measurements-10000000.txt")]
    public string Path { get; set; } = default!;

    private string GetFullPath() => System.IO.Path.Combine(GetRepoRoot(), Path);

    private static string GetRepoRoot()
    {
        var dir = new DirectoryInfo(Directory.GetCurrentDirectory());
        while (dir != null && !File.Exists(System.IO.Path.Combine(dir.FullName, "1brc.sln")))
        {
            dir = dir.Parent;
        }
        return dir?.FullName ?? throw new DirectoryNotFoundException("Could not find repository root");
    }

    [ParamsSource(nameof(WorkerOptions))]
    public int Workers { get; set; }

    public static IEnumerable<int> WorkerOptions =>
        [Environment.ProcessorCount, Environment.ProcessorCount * 2];

    [Benchmark(Description = "Parse+Aggregate 10M")]
    public (string Output, long Count) Run() =>
        OneBrc.ProcessFile(GetFullPath(), Workers);
}

[MemoryDiagnoser]
[WarmupCount(0)]                 // full-file runs: skip warmup
[IterationCount(1)]              // one iteration for the huge file
[SimpleJob(RuntimeMoniker.Net90)]
public class LargeBench
{
    [Params("measurements-1000000000.txt")]
    public string Path { get; set; } = default!;

    private string GetFullPath() => System.IO.Path.Combine(GetRepoRoot(), Path);

    private static string GetRepoRoot()
    {
        var dir = new DirectoryInfo(Directory.GetCurrentDirectory());
        while (dir != null && !File.Exists(System.IO.Path.Combine(dir.FullName, "1brc.sln")))
        {
            dir = dir.Parent;
        }
        return dir?.FullName ?? throw new DirectoryNotFoundException("Could not find repository root");
    }

    [ParamsSource(nameof(WorkerOptions))]
    public int Workers { get; set; }

    public static IEnumerable<int> WorkerOptions =>
        [Environment.ProcessorCount, Environment.ProcessorCount * 2];

    [Benchmark(Description = "Parse+Aggregate 1B")]
    public (string Output, long Count) Run() =>
        OneBrc.ProcessFile(GetFullPath(), Workers);
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
