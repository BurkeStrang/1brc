# One Billion Row Challenge - C# Implementation

A high-performance C# solution for the [One Billion Row Challenge](https://1brc.dev/), designed to process massive CSV files containing weather station measurements as quickly as possible.

## What is the One Billion Row Challenge?

The 1BRC is a fun exploration of how quickly you can process a text file with one billion rows. The challenge involves:

- Reading a CSV file with weather station measurements
- Calculating min, mean, and max temperatures for each weather station
- Outputting results sorted alphabetically by station name

Input format: `<station_name>;<temperature>`
```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
Hamburg;-2.3
```

Output format: `{<station_name>=<min>/<mean>/<max>, ...}`
```
{Bulawayo=8.9/8.9/8.9, Hamburg=-2.3/4.8/12.0, Palembang=38.8/38.8/38.8}
```

## Performance Features

This implementation achieves high performance through several optimizations:

- **Parallel Processing**: Splits file into chunks processed by multiple worker threads
- **Memory-Mapped I/O**: Uses `RandomAccess` APIs for efficient file reading
- **Hash-Based Station Lookup**: UTF-8 byte hashing with string interning eliminates per-line string allocations
- **Fixed-Point Arithmetic**: Processes temperatures as integers (tenths) to avoid floating-point operations
- **Zero-Copy Parsing**: Parses directly from byte spans without intermediate string allocations
- **Efficient Aggregation**: Uses `CollectionsMarshal` for high-performance dictionary operations

## Getting Started

### Prerequisites

- .NET 9.0 SDK
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

**Note:** The measurement files are gitignored to avoid committing large datasets to the repository.

### Building

```bash
# Build the entire solution
dotnet build

# Build for maximum performance
dotnet build -c Release
```

### Running

```bash
# Run with a measurements file
dotnet run --project program -- path/to/measurements.csv

# Run with custom worker count
dotnet run --project program -- path/to/measurements.csv 16

# For best performance, use Release build
dotnet run --project program -c Release -- path/to/measurements.csv
```

### Benchmarking

The project includes BenchmarkDotNet integration for precise performance measurement:

```bash
# Run benchmarks with 10 million row dataset (default)
dotnet run --project benchmark -c Release

# Run benchmarks with 1 billion row dataset
dotnet run --project benchmark -- large

# For release benchmarks with 1 billion dataset, build first then run
dotnet build benchmark -c Release
dotnet benchmark/bin/Release/net9.0/benchmark.dll large
```

The benchmark tests different worker configurations and provides detailed performance metrics including:
- Execution time
- Memory allocation
- Throughput (lines/second and MiB/second)

### Performance Results

On a 12th Gen Intel Core i7-1265U:

**10 Million Rows Dataset:**
| Workers | Execution Time | Throughput | Memory Allocated |
|---------|----------------|------------|------------------|
| 6       | 414-437 ms     | 22.9-24.2M lines/s | 6.11 MB         |
| 12      | 436-440 ms     | 22.7-22.9M lines/s | 8.98 MB         |

**1 Billion Rows Dataset:**
| Workers | Execution Time | Throughput | Memory Allocated |
|---------|----------------|------------|------------------|
| 6       | 57.11 s        | 17.5M lines/s | 6.11 MB          |
| 12      | 55.11 s        | 18.1M lines/s | 6.98 MB          |

**Key Achievement**: 98%+ reduction in memory allocations through hash-based UTF-8 byte lookup for station names, eliminating unnecessary string creation on every line.

## Project Structure

```
├── 1brc.sln                    # Visual Studio solution file
├── program/
│   ├── program.csproj          # Main executable project
│   └── program.cs              # Core processing logic
├── benchmark/
│   ├── benchmark.csproj        # BenchmarkDotNet project
│   └── bench.cs                # Performance benchmarks
└── BenchmarkDotNet.Artifacts/  # Benchmark results and logs
```

## How It Works

### 1. File Chunking
The file is divided into approximately equal chunks, with each chunk assigned to a worker thread. Chunk boundaries are adjusted to align with line endings to ensure complete records.

### 2. Parallel Processing
Each worker processes its chunk independently:
- Reads data in blocks using `RandomAccess.Read`
- Handles partial lines at chunk boundaries
- Maintains a local dictionary of station statistics

### 3. Line Parsing
For each line:
- Finds the semicolon separator
- Extracts station name (UTF-8 decoded and interned)
- Parses temperature as fixed-point integer (avoiding floating-point)

### 4. Statistics Aggregation
Each worker maintains running statistics (min, max, sum, count) per station. After all workers complete, their results are merged into a final dictionary.

### 5. Output Formatting
Results are sorted alphabetically by station name and formatted as the required output string.

## Performance Tuning

### Worker Count
- Default: `Environment.ProcessorCount`
- Can be overridden via command line
- Optimal count depends on file size and system characteristics

### Memory Settings
- Server GC enabled for better throughput on large datasets
- ArrayPool used for buffer reuse
- Configurable block size (default: 1MB)

### Temperature Parsing
Temperatures are parsed as integers representing tenths of degrees:
- `12.3°C` → `123` (tenths)
- `-5.7°C` → `-57` (tenths)

This avoids floating-point arithmetic and maintains precision for the required output format.

## Example Usage

```bash
# Process a 1 billion row file
dotnet run --project program -c Release -- measurements-1b.txt

# Output:
# {Abha=-31.1/18.0/66.5, Abidjan=-25.9/26.0/67.0, ...}
# Processed 10,000,000 lines in 0.40s  |  25,100,000 lines/s  |  950.4 MiB/s
```

## Contributing

This implementation prioritizes performance and serves as a reference for high-performance file processing techniques in C#. Feel free to experiment with different optimizations or adapt the techniques for similar challenges.