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
./program/bin/Release/net9.0/linux-x64/publish/1brc measurements-10000000.txt

# Run benchmarks with 10 million row dataset (default)
dotnet run --project benchmark -c Release

# Run benchmarks with 1 billion row dataset
dotnet run --project benchmark -c Release -- large

# Run tests to verify correctness
dotnet test

# Run tests with detailed output
dotnet test --verbosity normal
```

## Architecture

### Core Components

- **program/program.cs**: Main entry point containing the `OneBrc` class with high-performance file processing logic
- **benchmark/bench.cs**: BenchmarkDotNet setup for performance measurement using configurable worker counts
- **tests/**: xUnit test project with unit tests and integration tests to verify correctness

### Key Performance Optimizations

The `OneBrc.ProcessFile` method implements several performance optimizations:

1. **Parallel Processing**: Uses `Parallel.ForEach` with configurable worker threads to process file chunks concurrently
2. **Memory-Mapped File Access**: Uses `RandomAccess` APIs for efficient file reading without loading entire file into memory
3. **String Interning**: Per-worker string pools to avoid repeated UTF-8 decoding of station names
4. **Fixed-Point Arithmetic**: Temperatures stored as tenths (integers) to avoid floating-point operations
5. **ArrayPool**: Reuses byte buffers to minimize garbage collection
6. **Unsafe Operations**: Uses `CollectionsMarshal.GetValueRefOrAddDefault` for efficient dictionary operations

### File Processing Flow

1. **Range Calculation**: `MakeRanges` divides the file into equal chunks for parallel processing
2. **Chunk Processing**: `ProcessRange` reads file chunks with line boundary handling
3. **Line Parsing**: `ParseLine` extracts station names and temperatures from semicolon-delimited format
4. **Aggregation**: Results merged from all workers using `Stats.Merge`
5. **Output Formatting**: Sorted alphabetically with temperatures formatted to one decimal place

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

- **.NET 9.0**: Target framework
- **BenchmarkDotNet 0.15.3**: Performance measurement framework (benchmark project only)
- **7-Zip**: Required for extracting measurement data files

## Key Implementation Details

### StationPool Class (lines 297-354)
The `StationPool` class implements a hash-based UTF-8 byte lookup system that eliminates per-line string allocations. Each worker thread maintains its own pool using ThreadStatic, using FNV-1a hashing with collision handling via bucket lists. Includes ASCII fast path optimization for common station names.

### Stats Struct (lines 244-264)
The `Stats` struct stores temperature statistics as integers (tenths) to avoid floating-point arithmetic. Uses aggressive inlining for performance-critical operations and includes merge functionality for parallel aggregation.

### File Processing Architecture
- **MakeRanges** (lines 93-107): Divides files into equal-sized chunks for parallel processing
- **ProcessRange** (lines 109-175): Reads file chunks with line boundary handling using ArrayPool for buffer management
- **ParseLineOnce** (lines 189-208): Single-pass parsing with station name interning and temperature parsing

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

### Unit Tests
- **ParseTempTenths**: Tests temperature parsing with various formats and edge cases including rounding
- **ParseLineOnce**: Tests line parsing and aggregation logic
- **MakeRanges**: Tests file chunking logic for parallel processing
- **FormatTenth**: Tests output formatting
- **StationPool**: Tests string interning and station name handling
- **Stats.Merge**: Tests statistics aggregation across workers

### Integration Tests
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