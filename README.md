# libvroom

<!-- badges: start -->
[![CI](https://github.com/jimhester/libvroom/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/jimhester/libvroom/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/jimhester/libvroom/branch/main/graph/badge.svg)](https://codecov.io/gh/jimhester/libvroom)
<!-- badges: end -->

High-performance CSV parser using SIMD instructions. Uses multi-threaded speculative parsing to process large files in parallel.

## Installation

```bash
git clone https://github.com/jimhester/libvroom.git
cd libvroom
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

## Usage

### Command Line

The build produces a `vroom` command line tool:

```bash
vroom count data.csv              # Count rows
vroom head data.csv               # Display first 10 rows
vroom select -c name,age data.csv # Select columns
vroom pretty data.csv             # Pretty-print with aligned columns
vroom info data.csv               # Get file info (rows, columns, dialect)
```

### C++ Library

```cpp
#include <libvroom.h>

libvroom::FileBuffer buffer = libvroom::load_file("data.csv");
libvroom::Parser parser;
libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &errors});
// result.num_columns(), result.dialect.delimiter, etc.
```

### CMake Integration

```cmake
include(FetchContent)
FetchContent_Declare(libvroom
  GIT_REPOSITORY https://github.com/jimhester/libvroom.git
  GIT_TAG main)
FetchContent_MakeAvailable(libvroom)

target_link_libraries(your_target PRIVATE vroom)
```

## Features

- **SIMD-accelerated parsing** via [Google Highway](https://github.com/google/highway) (x86-64 SSE4.2/AVX2, ARM NEON)
- **Multi-threaded** speculative chunking for large files
- **Automatic dialect detection** (delimiter, quoting, line endings)
- **Index caching** for instant re-reads of previously parsed files
- **Three error modes**: `STRICT` (stop on first error), `PERMISSIVE` (collect all errors), `BEST_EFFORT` (ignore errors)
- **Cross-platform** support for Linux and macOS

## Documentation

- [Getting Started](https://jimhester.github.io/libvroom/getting-started.html) - Build instructions and basic usage
- [CLI Reference](https://jimhester.github.io/libvroom/cli.html) - Command line tool options
- [Index Caching](https://jimhester.github.io/libvroom/caching.html) - Speed up repeated file reads
- [Architecture](https://jimhester.github.io/libvroom/architecture.html) - Two-pass algorithm details
- [API Reference](https://jimhester.github.io/libvroom/api/) - Full API documentation

## How It Works

libvroom uses a two-pass algorithm based on [Chang et al. (SIGMOD 2019)](https://www.microsoft.com/en-us/research/uploads/prod/2019/04/chunker-sigmod19.pdf):

1. **First pass**: Scan for line boundaries while tracking quote parity to find safe split points
2. **Second pass**: SIMD-accelerated field indexing, processing 64 bytes at a time

This approach, combined with SIMD techniques from [Langdale & Lemire's simdjson](https://arxiv.org/abs/1902.08318), enables parallel parsing while correctly handling quoted fields that span chunk boundaries.
