# One Billion Row Challenge - C# Implementation

A high-performance C# solution
for the [One Billion Row Challenge](https://1brc.dev/),
designed to process massive
CSV files containing weather station measurements as quickly as possible.

## What is the One Billion Row Challenge?

The 1BRC is a fun exploration of how quickly you can process a text file
with one billion rows. The challenge involves:

- Reading a CSV file with weather station measurements
- Calculating min, mean, and max temperatures for each weather station
- Outputting results sorted alphabetically by station name

Input format: `<station_name>;<temperature>`

```text
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
Hamburg;-2.3
```

Output format: `{<station_name>=<min>/<mean>/<max>, ...}`

```text
{Bulawayo=8.9/8.9/8.9, Hamburg=-2.3/4.8/12.0, Palembang=38.8/38.8/38.8}
```

## Performance Features

This implementation achieves high performance through several optimizations:

- **Parallel Processing**: Splits file into chunks processed by multiple
  worker threads using `Parallel.For`
- **Memory-Mapped I/O**: Uses `MemoryMappedFile` with unsafe pointers for
  zero-copy file access
- **Custom Hash Table**: Open-addressing hash table with xxHash-style mixing
  for fast station lookups
- **Fixed-Offset Parsing**: Exploits known temperature format (3-5 chars) to
  find semicolons without scanning
- **Fixed-Point Arithmetic**: Processes temperatures as integers (tenths) to
  avoid floating-point operations
- **SIMD Newline Search**: Uses .NET's SIMD-optimized `IndexOf` for finding
  line boundaries
- **AOT Compilation**: Native ahead-of-time compilation eliminates JIT
  overhead and reduces startup time

## Getting Started

### Prerequisites

- .NET 10.0 SDK
- 7-Zip (for extracting measurement data files)

### Initial Setup

**Download measurement data files:**

Get the measurement data from Hugging Face:

```bash
# 1) Install Git LFS (once per machine)
sudo apt-get update && sudo apt-get install -y git-lfs
git lfs install

# 2) Clone the data repository
git clone https://huggingface.co/datasets/nietras/1brc.data

# 3) Fetch the real files for this repo
cd 1brc.data
git lfs fetch --all
git lfs pull

# 4) Extract measurement files to your project root
cd ..
# Unzip the files from 1brc.data/ to your project root
```

**Note:** The measurement files are gitignored to avoid committing large
datasets to the repository.

### Building

```bash
# Build the entire solution
dotnet build

# Build for maximum performance
dotnet build -c Release

# Publish with AOT compilation for ultimate performance
dotnet publish program -c Release -r linux-x64 \
  -p:PublishAot=true -p:StripSymbols=true -p:SelfContained=true
```

### Running

```bash
# Run with a measurements file
dotnet run --project program -- path/to/measurements.csv

# Run with custom worker count
dotnet run --project program -- path/to/measurements.csv 16

# For best performance, use Release build
dotnet run --project program -c Release -- path/to/measurements.csv

# Run AOT-compiled executable (after publishing)
./program/bin/Release/net10.0/linux-x64/publish/1brc path/to/measurements.csv
```

### Benchmarking

The project includes BenchmarkDotNet integration for precise performance
measurement:

```bash
# Run benchmarks with 10 million row dataset (default)
dotnet run --project benchmark -c Release

# Run benchmarks with 1 billion row dataset (build first for accurate results)
dotnet build benchmark -c Release
sudo dotnet benchmark/bin/Release/net10.0/benchmark.dll large
```

The benchmark tests different worker configurations (ProcessorCount/2 and
ProcessorCount) and provides detailed performance metrics including execution
time, memory allocation, and GC statistics.

### Performance Results

**1 Billion Rows Dataset (13GB):**

| Workers | Execution Time | Throughput   | Memory Allocated |
| ------- | -------------- | ------------ | ---------------- |
| 6       | ~24 s          | ~41M lines/s | ~12 MB           |
| 12      | ~30 s          | ~33M lines/s | ~24 MB           |

**Key Optimizations Applied:**

- Fixed-offset semicolon detection (temperature is always 3-5 chars)
- xxHash-style hash function with `MemoryMarshal.Read` for efficient byte
  reading
- Custom open-addressing hash table tuned for ~400 unique station names

## Project Structure

```text
├── 1brc.slnx                   # Visual Studio solution file
├── program/
│   ├── program.csproj          # Main executable project
│   └── program.cs              # Core processing logic (OneBrc + StationMap)
├── benchmark/
│   ├── benchmark.csproj        # BenchmarkDotNet project
│   └── bench.cs                # Performance benchmarks
├── tests/
│   ├── tests.csproj            # xUnit test project
│   ├── OneBrcTests.cs          # Unit tests
│   └── IntegrationTests.cs     # Integration tests
└── BenchmarkDotNet.Artifacts/  # Benchmark results and logs
```

## How It Works

### 1. Memory-Mapped File Access

The file is memory-mapped using `MemoryMappedFile`, providing a direct pointer
to the file contents without explicit I/O calls. This lets the OS handle
paging efficiently.

### 2. File Chunking

`MakeRanges` divides the file into equal chunks for parallel processing.
Chunk boundaries are adjusted to align with newline characters to ensure
complete records.

### 3. Parallel Processing

Each worker processes its chunk independently using `Parallel.For`:

- Scans for newlines using SIMD-optimized `IndexOf`
- Uses fixed-offset checks to find semicolons (temperature is always 3-5
  chars from newline)
- Maintains a local `StationMap` hash table for station statistics

### 4. Line Parsing

For each line:

- Finds newline with `span.IndexOf('\n')` (SIMD accelerated)
- Checks offsets -4, -5, -6 from newline to find semicolon
- Parses temperature as fixed-point integer using branchless arithmetic

### 5. Statistics Aggregation

Each `StationMap` maintains running statistics (min, max, sum, count) per
station. After all workers complete, maps are merged sequentially into the
first worker's map.

### 6. Output Formatting

Results are sorted alphabetically by station name and formatted as the
required output string with temperatures displayed to one decimal place.

## Performance Tuning

### Worker Count

- Default: `Environment.ProcessorCount`
- Can be overridden via command line argument
- **Fewer workers often perform better** due to memory bandwidth saturation
  (6 workers outperforms 12 on many systems)

### Memory Settings

- Server GC enabled for better throughput on large datasets
- Hash table sized at 65536 entries with 0.5 load factor
- Memory-mapped files let the OS manage page caching

### Temperature Parsing

Temperatures are parsed as integers representing tenths of degrees:

- `12.3°C` → `123` (tenths)
- `-5.7°C` → `-57` (tenths)

This avoids floating-point arithmetic and maintains precision for the
required output format.

## Example Usage

```bash
# Process a 1 billion row file with JIT compilation
dotnet run --project program -c Release -- measurements-1b.txt

# Process with AOT-compiled executable for maximum performance
./program/bin/Release/net10.0/linux-x64/publish/1brc measurements-1b.txt

# Output:
# {Abha=-31.1/18.0/66.5, Abidjan=-25.9/26.0/67.0, ...}
# Processed 10,000,000 lines in 0.40s  |  25,100,000 lines/s  |  950.4 MiB/s
```

## AOT Performance Benefits

AOT (Ahead-of-Time) compilation provides several performance advantages:

- **Faster Startup**: No JIT compilation overhead at runtime
- **Smaller Memory Footprint**: No need to store IL code or JIT compiler in
  memory
- **Predictable Performance**: Eliminates JIT compilation pauses during
  execution
- **Native Code Optimization**: Direct machine code generation optimized for
  the target platform

To publish with AOT, use:

```bash
dotnet publish program -c Release -r linux-x64 \
  -p:PublishAot=true -p:StripSymbols=true -p:SelfContained=true
```

The resulting executable will be a single native binary with no .NET runtime
dependencies.

## Contributing

This implementation prioritizes performance and serves as a reference for
high-performance file processing techniques in C#. Feel free to experiment
with different optimizations or adapt the techniques for similar challenges.
