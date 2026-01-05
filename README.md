# simdcsv

<!-- badges: start -->
[![CI](https://github.com/jimhester/simdcsv/workflows/CI/badge.svg?branch=main)](https://github.com/jimhester/simdcsv/actions?query=branch%3Amain)
[![codecov](https://codecov.io/gh/jimhester/simdcsv/branch/main/graph/badge.svg)](https://codecov.io/gh/jimhester/simdcsv)
<!-- badges: end -->

High-performance CSV parser using SIMD instructions, designed for integration with R's [vroom](https://github.com/tidyverse/vroom) package. Uses multi-threaded speculative parsing to process large files in parallel.

## Installation

```bash
git clone https://github.com/jimhester/simdcsv.git
cd simdcsv
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
```

## Usage

### Command Line

The build produces an `scsv` command line tool:

```bash
scsv count data.csv              # Count rows
scsv head data.csv               # Display first 10 rows
scsv select -c name,age data.csv # Select columns
scsv pretty data.csv             # Pretty-print with aligned columns
scsv info data.csv               # Get file info (rows, columns, dialect)
```

### C++ Library

```cpp
#include <simdcsv.h>

simdcsv::FileBuffer buffer = simdcsv::load_file("data.csv");
simdcsv::Parser parser;
simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
auto result = parser.parse_auto(buffer.data(), buffer.size(), errors);
// result.num_columns(), result.dialect.delimiter, etc.
```

### CMake Integration

```cmake
include(FetchContent)
FetchContent_Declare(simdcsv
  GIT_REPOSITORY https://github.com/jimhester/simdcsv.git
  GIT_TAG main)
FetchContent_MakeAvailable(simdcsv)

target_link_libraries(your_target PRIVATE simdcsv_lib)
```

## Features

- **SIMD-accelerated parsing** via [Google Highway](https://github.com/google/highway) (x86-64 SSE4.2/AVX2, ARM NEON)
- **Multi-threaded** speculative chunking for large files
- **Automatic dialect detection** (delimiter, quoting, line endings)
- **Three error modes**: `STRICT` (stop on first error), `PERMISSIVE` (collect all errors), `BEST_EFFORT` (ignore errors)
- **Cross-platform** support for Linux and macOS

## Documentation

- [Getting Started](https://jimhester.github.io/simdcsv/getting-started.html) - Build instructions and basic usage
- [CLI Reference](https://jimhester.github.io/simdcsv/cli.html) - Command line tool options
- [Architecture](https://jimhester.github.io/simdcsv/architecture.html) - Two-pass algorithm details
- [API Reference](https://jimhester.github.io/simdcsv/api/) - Full API documentation

## How It Works

simdcsv uses a two-pass algorithm based on [Chang et al. (SIGMOD 2019)](https://www.microsoft.com/en-us/research/uploads/prod/2019/04/chunker-sigmod19.pdf):

1. **First pass**: Scan for line boundaries while tracking quote parity to find safe split points
2. **Second pass**: SIMD-accelerated field indexing, processing 64 bytes at a time

This approach, combined with SIMD techniques from [Langdale & Lemire's simdjson](https://arxiv.org/abs/1902.08318), enables parallel parsing while correctly handling quoted fields that span chunk boundaries.
