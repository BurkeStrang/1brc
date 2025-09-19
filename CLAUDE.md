# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a C# implementation of the One Billion Row Challenge (1BRC) - a performance-oriented file processing challenge. The project processes massive CSV files containing weather station measurements and calculates min/mean/max temperatures per station.

## Build and Run Commands

```bash
# Build the solution
dotnet build

# Run the main program (requires path to measurements file)
dotnet run --project program -- <path-to-measurements.csv> [workers]

# Quick verification with smaller dataset (after extracting data files)
dotnet run --project program -- measurements-10000000.txt

# Run benchmarks
dotnet run --project benchmark -c Release

# Build for release (recommended for performance testing)
dotnet build -c Release
```

## Architecture

### Core Components

- **program/program.cs**: Main entry point containing the `OneBrc` class with high-performance file processing logic
- **benchmark/bench.cs**: BenchmarkDotNet setup for performance measurement using configurable worker counts

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

- Default worker count is `Environment.ProcessorCount`
- Benchmark tests both `ProcessorCount` and `ProcessorCount * 2` workers
- Server GC is enabled in the main program project for better throughput
- Block size for file reading is configurable (default 1MB)

## Dependencies

- **.NET 9.0**: Target framework
- **BenchmarkDotNet 0.15.3**: Performance measurement framework (benchmark project only)

## Key Implementation Details

### StationPool Class
The `StationPool` class (`program/program.cs:254-299`) implements a hash-based UTF-8 byte lookup system that eliminates per-line string allocations. Each worker thread maintains its own pool, using FNV-1a hashing with collision handling via bucket lists.

### Stats Struct
The `Stats` struct (`program/program.cs:222-242`) stores temperature statistics as integers (tenths) to avoid floating-point arithmetic. Uses aggressive inlining for performance-critical operations.

### File Chunking Strategy
The `MakeRanges` method divides files into equal-sized chunks for parallel processing, with chunk boundaries aligned to line endings to ensure complete records are processed by single workers.

## Development Workflow

When modifying the core processing logic:
1. Run benchmarks before changes: `dotnet run --project benchmark -c Release`
2. Make targeted changes to maintain performance characteristics
3. Verify results with smaller dataset: `dotnet run --project program -- measurements-10000000.txt`
4. Run full benchmarks to validate performance improvements