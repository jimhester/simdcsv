# simdcsv

<!-- badges: start -->
[![CI](https://github.com/jimhester/simdcsv/workflows/CI/badge.svg)](https://github.com/jimhester/simdcsv/actions)
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

## Usage

```cpp
#include "two_pass.h"
#include "io_util.h"
#include "error.h"

int main() {
    // Load CSV file
    auto [buf, len] = simdcsv::load_file("data.csv");

    // Initialize parser
    simdcsv::two_pass parser;
    auto idx = parser.init(len, 4);  // 4 threads

    // Parse with error handling
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    parser.parse_with_errors(buf, idx, len, errors);

    if (errors.has_errors()) {
        std::cerr << errors.summary() << std::endl;
    }

    return 0;
}
```

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
