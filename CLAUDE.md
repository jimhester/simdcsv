# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

libvroom is a high-performance CSV parser library using portable SIMD instructions (via Google Highway), designed for future integration with R's [vroom](https://github.com/tidyverse/vroom) package. The parser uses a speculative multi-threaded two-pass algorithm based on research by Chang et al. (SIGMOD 2019) and SIMD techniques from Langdale & Lemire (simdjson).

## Naming and Authorship

This project is authored by Jim Hester, the original author of [vroom](https://github.com/tidyverse/vroom). The project was renamed from simdcsv to **libvroom** to:

1. Clearly indicate its relationship to vroom as the native SIMD parsing engine
2. Avoid confusion with abandoned simdjson-adjacent projects (e.g., geofflangdale/simdcsv)
3. Use conventional `lib*` naming for C/C++ libraries

## Build Commands

```bash
# Configure and build (Release) - use -j for parallel compilation
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)

# Minimal release build (library and CLI only, no tests/benchmarks)
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=OFF
cmake --build build -j$(nproc)

# Build shared library instead of static
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=ON
cmake --build build -j$(nproc)

# Run all tests
cd build && ctest --output-on-failure -j$(nproc)

# Run specific test binary
./build/libvroom_test              # 42 well-formed CSV tests
./build/error_handling_test        # 37 error handling tests
./build/csv_parsing_test           # Integration tests

# Run benchmarks
./build/libvroom_benchmark

# Build with code coverage
cmake -B build -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON
cmake --build build -j$(nproc)
```

## Build Acceleration

**ccache** is automatically detected and used if installed. Install with:
```bash
# Ubuntu/Debian
sudo apt install ccache

# macOS
brew install ccache
```

ccache dramatically speeds up rebuilds by caching compilation results. View stats with `ccache -s`.

## Language Server

clangd is available for code intelligence (go-to-definition, find references, diagnostics). To enable it in a fresh checkout or worktree:

```bash
cmake -B build -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
ln -sf build/compile_commands.json .
```

## Code Formatting

A `.clang-format` config is provided. Install the pre-commit hook to auto-format staged files:

```bash
ln -sf ../../scripts/pre-commit .git/hooks/pre-commit
```

To manually format a file: `clang-format -i <file>`

## Git Workflow

- **No force pushing**: Avoid `git push --force` on branches
- **Update branches via merge**: When a branch needs updates from main, use `git merge main` instead of rebasing
- **Squash on final merge**: Use squash merge when merging PRs into main to keep history clean
- **Check for merge conflicts**: When opening a branch, check for merge conflicts with main. If CI status checks aren't appearing on a PR, merge conflicts are often the cause

## Key Files

| File | Purpose |
|------|---------|
| `include/libvroom.h` | **Main public API** - Parser class, unified interface |
| `include/libvroom_c.h` | C API wrapper for FFI bindings |
| `include/two_pass.h` | Core two-pass parsing algorithm with multi-threading |
| `include/dialect.h` | CSV dialect detection (delimiter, quoting, line endings) |
| `include/error.h` | Error codes, ErrorCollector, three error modes |
| `include/streaming.h` | Streaming parser for large files |
| `include/simd_highway.h` | Portable SIMD operations (Highway) |
| `src/cli.cpp` | CLI tool (`vroom count/head/select/pretty/info`) |

## Architecture

Two-pass speculative parsing algorithm (see `include/two_pass.h`):
1. **First pass**: Scan for line boundaries tracking quote parity to find safe split points
2. **Speculative chunking**: Divide file for parallel processing based on quote analysis
3. **Second pass**: SIMD field indexing (64 bytes/iteration) with state machine

SIMD via Google Highway 1.3.0: x86-64 (SSE4.2, AVX2), ARM (NEON), scalar fallback.

## Documentation

| Topic | Location |
|-------|----------|
| Error handling (modes, types, recovery) | `docs/error_handling.md` |
| Test data organization | `test/README.md` |
| CI workflows | `.github/workflows/README.md` |

## Dependencies (fetched via CMake FetchContent)

- Google Highway 1.3.0 - Portable SIMD
- Google Test 1.14.0 - Unit testing
- Google Benchmark 1.8.3 - Performance benchmarking

## Issue Labels

Use `gh issue create --label "label"` with the following labels:

| Label | Description |
|-------|-------------|
| `bug` | an unexpected problem or unintended behavior |
| `feature` | a feature request or enhancement |
| `documentation` | improvements or additions to documentation |
| `performance 🚀` | performance improvement |
| `testing 🧪` | test coverage or infrastructure |
| `cleanup 🧹` | code cleanup or refactoring |
| `api 🔌` | public API changes or additions |
| `c-api 🔧` | C API wrapper |
| `cli ⌨️` | vroom command line tool |
| `simd ⚡` | SIMD implementation or optimization |
| `arrow 🏹` | Apache Arrow integration |
| `security 🔒` | security vulnerability or hardening |
| `critical ☠️` | must fix - security or correctness issue |
| `up next 📌` | next items to address from code review |
| `good first issue ❤️` | good issue for first-time contributors |
| `help wanted ❤️` | we'd love your help! |
| `duplicate` | this issue or pull request already exists |
| `wontfix ❌` | this will not be worked on |
