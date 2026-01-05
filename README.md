# simdcsv

<!-- badges: start -->
[![CI](https://github.com/jimhester/simdcsv/workflows/CI/badge.svg?branch=main)](https://github.com/jimhester/simdcsv/actions?query=branch%3Amain)
[![codecov](https://codecov.io/gh/jimhester/simdcsv/branch/main/graph/badge.svg)](https://codecov.io/gh/jimhester/simdcsv)
<!-- badges: end -->

High-performance CSV parser using SIMD instructions, designed for integration with R's [vroom](https://github.com/tidyverse/vroom) package.

## How does it work?

simdcsv uses a speculative multi-threaded two-pass algorithm to parse CSV files at high speed:

1. **First Pass**: Scans for line boundaries while tracking quote parity to find safe chunk split points for parallel processing
2. **Second Pass**: SIMD-accelerated field indexing using a state machine, processing 64 bytes at a time

The parser uses [Google Highway](https://github.com/google/highway) for portable SIMD operations across x86-64 (SSE4.2, AVX2) and ARM (NEON) architectures.

This approach is based on research from [Chang et al. (SIGMOD 2019)](https://www.microsoft.com/en-us/research/uploads/prod/2019/04/chunker-sigmod19.pdf) and SIMD techniques from [Langdale & Lemire's simdjson](https://arxiv.org/abs/1902.08318).

## Features

- **SIMD-accelerated parsing** via Google Highway for portable vectorization
- **Multi-threaded** speculative parsing for large files
- **Comprehensive error handling** with three modes:
  - `STRICT` - Stop on first error
  - `PERMISSIVE` - Collect all errors, try to recover
  - `BEST_EFFORT` - Ignore errors, parse what's possible
- **Cross-platform** support for Linux and macOS
- **Extensive test suite** - 79 tests covering well-formed and malformed CSVs

## Installation

```bash
# Clone the repository
git clone https://github.com/jimhester/simdcsv.git
cd simdcsv

# Configure and build (Release)
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build

# Run tests
cd build && ctest --output-on-failure
```

## Command Line Tool

The build produces an `scsv` command line tool for working with CSV files:

```bash
# Count rows in a CSV file
./build/scsv count data.csv

# Display first 10 rows
./build/scsv head data.csv

# Select specific columns
./build/scsv select -c name,age data.csv

# Pretty-print with aligned columns
./build/scsv pretty data.csv

# Get file info (rows, columns, dialect)
./build/scsv info data.csv
```

See the [CLI Documentation](https://jimhester.github.io/simdcsv/cli.html) for all available options.

## Quick Start

Include the single header `<simdcsv.h>` to access the full public API:

```cpp
#include <simdcsv.h>
#include <iostream>

int main() {
    // Load file with automatic aligned memory management
    simdcsv::FileBuffer buffer = simdcsv::load_file("data.csv");

    // Parse with automatic dialect detection
    simdcsv::Parser parser;
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    auto result = parser.parse_auto(buffer.data(), buffer.size(), errors);

    if (result.success()) {
        std::cout << "Parsed " << result.num_columns() << " columns\n";
        std::cout << "Detected delimiter: '" << result.dialect.delimiter << "'\n";
    }

    if (errors.has_errors()) {
        std::cerr << errors.summary() << std::endl;
    }

    return 0;
}
```

## CMake Integration

Add simdcsv to your project using CMake's FetchContent:

```cmake
include(FetchContent)
FetchContent_Declare(simdcsv
  GIT_REPOSITORY https://github.com/jimhester/simdcsv.git
  GIT_TAG main)
FetchContent_MakeAvailable(simdcsv)

target_link_libraries(your_target PRIVATE simdcsv_lib)
```

## API Overview

| Header | Description |
|--------|-------------|
| `<simdcsv.h>` | **Main header** - includes everything, provides simplified API |
| `<two_pass.h>` | Low-level parser with full control |
| `<error.h>` | Error types, codes, and ErrorCollector |
| `<dialect.h>` | Dialect configuration and detection |

## Learning more

- [Getting Started](https://jimhester.github.io/simdcsv/getting-started.html) - Build instructions and basic usage
- [Architecture](https://jimhester.github.io/simdcsv/architecture.html) - How the two-pass algorithm works
- [Error Handling](https://jimhester.github.io/simdcsv/error-handling.html) - Error types and handling modes
- [API Reference](https://jimhester.github.io/simdcsv/api/) - Full API documentation

## Dependencies

All dependencies are fetched automatically via CMake's FetchContent:

| Dependency | Version | Purpose |
|------------|---------|---------|
| [Google Highway](https://github.com/google/highway) | 1.3.0 | Portable SIMD abstraction |
| [Google Test](https://github.com/google/googletest) | 1.14.0 | Unit testing |
| [Google Benchmark](https://github.com/google/benchmark) | 1.8.3 | Performance benchmarking |

## References

- Ge, Chang and Li, Yinan and Eilebrecht, Eric and Chandramouli, Badrish and Kossmann, Donald. [Speculative Distributed CSV Data Parsing for Big Data Analytics](https://www.microsoft.com/en-us/research/uploads/prod/2019/04/chunker-sigmod19.pdf). SIGMOD 2019.

- Geoff Langdale, Daniel Lemire. [Parsing Gigabytes of JSON per Second](https://arxiv.org/abs/1902.08318). VLDB Journal 28 (6), 2019.
