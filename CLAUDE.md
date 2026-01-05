# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

simdcsv is a high-performance CSV parser library using portable SIMD instructions (via Google Highway), designed for future integration with R's [vroom](https://github.com/tidyverse/vroom) package. The parser uses a speculative multi-threaded two-pass algorithm based on research by Chang et al. (SIGMOD 2019) and SIMD techniques from Langdale & Lemire (simdjson).

## Build Commands

```bash
# Configure and build (Release)
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build

# Minimal release build (library and CLI only, no tests/benchmarks)
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=OFF
cmake --build build

# Build shared library instead of static
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON
cmake --build build

# Run all tests
cd build && ctest --output-on-failure

# Run specific test binary
./build/simdcsv_test               # 42 well-formed CSV tests
./build/error_handling_test        # 37 error handling tests
./build/csv_parsing_test           # Integration tests

# Run benchmarks
./build/simdcsv_benchmark

# Build with code coverage
cmake -B build -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON
cmake --build build
```

## Architecture

### Two-Pass Parsing Algorithm

The core algorithm in `include/two_pass.h` uses speculative multi-threaded parsing:

1. **First Pass** (`first_pass_simd`): Scans for line boundaries while tracking quote parity. Finds the first newline at an even quote count (safe split point) and first newline at odd quote count.

2. **Speculative Chunking**: File is divided into chunks based on quote parity analysis. Multiple threads speculatively parse chunks, assuming quotation state at chunk boundaries.

3. **Second Pass** (`find_indexes_simd`): SIMD-based field indexing using a state machine (RECORD_START, FIELD_START, UNQUOTED_FIELD, QUOTED_FIELD, QUOTED_END). Processes 64 bytes at a time via Highway intrinsics.

### Key Components

| File | Purpose |
|------|---------|
| `include/two_pass.h` | Core two-pass parsing algorithm with multi-threading |
| `include/simd_highway.h` | Portable SIMD operations using Google Highway |
| `include/error.h` | Error codes, severity levels, ErrorCollector class |
| `include/io_util.h` | File loading with SIMD-aligned padding (32+ bytes) |
| `include/mem_util.h` | Aligned memory allocation for SIMD |
| `src/cli.cpp` | User-friendly CSV tool (scsv) |

### Error Handling Framework

The parser supports three error modes (see `include/error.h`):
- **STRICT**: Stop on first error
- **PERMISSIVE**: Collect all errors, try to recover
- **BEST_EFFORT**: Ignore errors, parse what's possible

16 error types with severity levels (WARNING, ERROR, FATAL) covering quotes, field structure, line endings, encoding, and I/O issues.

### SIMD Implementation

Uses Google Highway 1.3.0 for portable SIMD abstraction:
- x86-64: SSE4.2, AVX2
- ARM: NEON
- 64-byte processing lanes with scalar fallback for remainders

## Test Data Organization

Test CSV files are in `test/data/` organized by category:
- `basic/` - Simple CSV files
- `quoted/` - Quoted fields and escapes
- `separators/` - Different delimiter types
- `edge_cases/` - Empty fields, whitespace
- `line_endings/` - CRLF, LF, CR variations
- `real_world/` - Financial, contacts, unicode data
- `malformed/` - 16+ files for error detection testing

## Dependencies (fetched via CMake FetchContent)

- Google Highway 1.3.0 - Portable SIMD
- Google Test 1.14.0 - Unit testing
- Google Benchmark 1.8.3 - Performance benchmarking
