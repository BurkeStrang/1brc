# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a C# implementation of the One Billion Row Challenge (1BRC) - a performance-oriented file processing challenge. The project processes massive CSV files containing weather station measurements and calculates min/mean/max temperatures per station.

Input format: `<station_name>;<temperature>`
Output format: `{<station_name>=<min>/<mean>/<max>, ...}` sorted alphabetically by station name.

## Build and Run Commands

```bash
# Build the solution
dotnet build

# Build for release (recommended for performance testing)
dotnet build -c Release

# Run the main program (requires path to measurements file)
dotnet run --project program -- <path-to-measurements.csv> [workers]

# Run with release build for best performance
dotnet run --project program -c Release -- <path-to-measurements.csv>

# Quick verification with smaller dataset (after extracting data files)
dotnet run --project program -- measurements-10000000.txt

# AOT compilation for ultimate performance
dotnet publish program -c Release -r linux-x64 -p:PublishAot=true -p:StripSymbols=true -p:SelfContained=true

# Run AOT-compiled executable
./program/bin/Release/net10.0/linux-x64/publish/1brc measurements-10000000.txt

# Run benchmarks with 10 million row dataset (default)
dotnet run --project benchmark -c Release

# Run benchmarks with 1 billion row dataset (build first, then run directly)
dotnet build benchmark -c Release
sudo dotnet benchmark/bin/Release/net10.0/benchmark.dll large

# Run tests to verify correctness
dotnet test

# Run tests with detailed output
dotnet test --verbosity normal
```

## Architecture

### Core Components

- **program/program.cs**: Contains `OneBrc` class (file processing) and `StationMap` class (hash table for aggregation)
- **benchmark/bench.cs**: BenchmarkDotNet setup for performance measurement using configurable worker counts
- **tests/**: xUnit test project with unit tests and integration tests to verify correctness

### Key Performance Optimizations

The `OneBrc.ProcessFile` method implements several performance optimizations:

1. **Memory-Mapped Files**: Uses `MemoryMappedFile` with unsafe pointers for zero-copy file access
2. **Parallel Processing**: Uses `Parallel.For` with configurable worker threads to process file chunks concurrently
3. **Fixed-Offset Semicolon Detection**: Exploits known temperature format (3-5 chars) to find semicolons without scanning
4. **SIMD Newline Search**: Uses .NET's SIMD-optimized `IndexOf` for finding line boundaries
5. **Custom Hash Table**: `StationMap` with open addressing and xxHash-style mixing using `MemoryMarshal.Read`
6. **Fixed-Point Arithmetic**: Temperatures stored as tenths (integers) to avoid floating-point operations

### File Processing Flow

1. **Memory Mapping**: `ProcessFile` maps the entire file into memory using `MemoryMappedFile`
2. **Range Calculation**: `MakeRanges` divides the file into equal chunks aligned to newline boundaries
3. **Chunk Processing**: `ProcessRange` processes each chunk, finding lines and parsing them
4. **Line Parsing**: Uses SIMD `IndexOf` for newlines, fixed-offset checks for semicolons, branchless temp parsing
5. **Aggregation**: Results merged from all worker `StationMap`s into the first map
6. **Output Formatting**: Sorted alphabetically with temperatures formatted to one decimal place

## Testing Data

### Initial Setup Required
Before running the project, download the measurement data from Hugging Face:
```bash
# 1) Install Git LFS (once per machine)
sudo apt-get update && sudo apt-get install -y git-lfs
git lfs install

# 2) Clone the data repository
git clone https://huggingface.co/datasets/nietras/1brc.data

# 3) Fetch the real files
cd 1brc.data
git lfs fetch --all
git lfs pull

# 4) Extract measurement files to your project root
cd ..
# Unzip the files from 1brc.data/ to your project root
```

The measurement files needed for testing:
- `measurements-10000000.txt` (132MB) - 10 million rows for development/testing
- `measurements-1000000000.txt` (13GB) - 1 billion rows for full benchmarking

**Note:** The measurement files are gitignored and must be downloaded locally for each development environment.

The data format is:
```
<station_name>;<temperature>
```

Example:
```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
```

## Performance Tuning

- **Worker Count**: Default is `Environment.ProcessorCount`, can be overridden via command line
- **Benchmark Configurations**: Tests both `ProcessorCount` and `ProcessorCount * 2` workers
- **Memory Optimizations**: Server GC enabled, ArrayPool for buffer reuse, block size 32MB for file reading
- **AOT Benefits**: Faster startup, smaller memory footprint, predictable performance, native code optimization

## Project Configuration

### Solution Structure
- **1brc.sln**: Visual Studio solution file with three projects
- **program/**: Main executable with performance-optimized project settings
- **benchmark/**: BenchmarkDotNet project for precise performance measurement
- **tests/**: xUnit test project for verifying correctness of the implementation

### Main Project Optimizations (program.csproj)
- **ServerGarbageCollection**: Enabled for better throughput on large datasets
- **TieredPGO**: Profile-guided optimization for better JIT quality
- **OptimizationPreference**: Speed-focused optimizations
- **InvariantGlobalization**: Reduces AOT size by removing globalization support
- **PublishAot**: Can be enabled at publish time for native compilation

## Dependencies

- **.NET 10.0**: Target framework
- **BenchmarkDotNet 0.15.3**: Performance measurement framework (benchmark project only)
- **7-Zip**: Required for extracting measurement data files

## Key Implementation Details

### StationMap Class (lines 202-380)
Custom open-addressing hash table optimized for the ~400 unique station names in the dataset:
- **Initial capacity**: 65536 entries with 0.5 load factor to minimize collisions
- **Hash function**: xxHash-style mixing using `MemoryMarshal.Read<ulong>` for efficient byte reading
- **Collision resolution**: Linear probing with hash + length + SequenceEqual comparison
- **Entry struct**: Stores Key (byte[]), Hash, Min, Max, Sum, Count

### Key Methods in OneBrc Class
- **ProcessFile** (lines 34-55): Entry point that memory-maps the file and acquires unsafe pointer
- **MakeRanges** (lines 80-105): Divides file into equal chunks aligned to newline boundaries
- **ProcessRange** (lines 107-135): Main processing loop using SIMD IndexOf for newlines and fixed-offset semicolon detection
- **ParseTempBranchless** (lines 150-173): Branchless temperature parsing returning tenths as integer
- **FindByte** (lines 137-145): Wrapper around SIMD-optimized span.IndexOf

## Development Workflow

When modifying the core processing logic:
1. Run tests to ensure correctness: `dotnet test`
2. Run benchmarks before changes: `dotnet run --project benchmark -c Release`
3. Make targeted changes to maintain performance characteristics
4. Verify results with smaller dataset: `dotnet run --project program -- measurements-10000000.txt`
5. Re-run tests to verify correctness is maintained: `dotnet test`
6. Run full benchmarks to validate performance improvements

## Testing

The test suite includes comprehensive unit and integration tests to verify correctness:

### Unit Tests (OneBrcTests.cs)
- **ParseTempBranchless**: Tests temperature parsing with various formats (positive, negative, single/double digit)
- **FormatTenth**: Tests output formatting for temperature display
- **ComputeHash**: Tests hash function distribution and collision handling
- **StationMap**: Tests hash table operations (add, update, merge)

### Integration Tests (IntegrationTests.cs)
- **ProcessFile with sample data**: Tests end-to-end processing with known expected output
- **Multi-worker consistency**: Verifies same results regardless of worker count
- **Edge cases**: Tests boundary conditions and rounding behavior
- **Real data subset**: Optional test with actual measurement data if available

### Running Tests
```bash
# Run all tests
dotnet test

# Run with detailed output
dotnet test --verbosity normal

# Run specific test class
dotnet test --filter "FullyQualifiedName~OneBrcTests"

# Run integration tests only
dotnet test --filter "FullyQualifiedName~IntegrationTests"
```
