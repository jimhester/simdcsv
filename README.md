# simdcsv

![CI](https://github.com/jimhester/simdcsv/workflows/CI/badge.svg)
[![codecov](https://codecov.io/gh/jimhester/simdcsv/branch/main/graph/badge.svg)](https://codecov.io/gh/jimhester/simdcsv)

High-performance CSV parser using SIMD instructions, designed for integration with [vroom](https://github.com/tidyverse/vroom).

Implementations based on speculative multi-threaded parsing from Chang et al. and SIMD approaches adapted from Langdale & Lemire (simdjson).

## Status

Active development focused on production readiness:
- ✅ **Test Suite**: 79 tests (42 well-formed + 37 error handling)
- ✅ **Error Handling**: Comprehensive error detection and reporting
- ✅ **CI/CD**: Automated testing on Linux and macOS
- 🚧 **SIMD Parser**: AVX2 implementation in progress
- 📋 **Planned**: vroom integration, Highway/SIMDe evaluation

## Building

```bash
# Configure
cmake -B build -DCMAKE_BUILD_TYPE=Release

# Build
cmake --build build

# Run tests
cd build && ctest --output-on-failure
```

## Documentation

- [Production Readiness Plan](PRODUCTION_READINESS_PLAN.md) - Complete roadmap
- [Literature Review](docs/literature_review.md) - Research analysis (2017-2026)
- [Error Handling Guide](docs/error_handling.md) - Error detection and reporting
- [Test Suite](test/README.md) - Test organization and coverage

# References

Ge, Chang and Li, Yinan and Eilebrecht, Eric and Chandramouli, Badrish and Kossmann, Donald, [Speculative Distributed CSV Data Parsing for Big Data Analytics](https://www.microsoft.com/en-us/research/uploads/prod/2019/04/chunker-sigmod19.pdf), SIGMOD 2019.

Geoff Langdale, Daniel Lemire, [Parsing Gigabytes of JSON per Second](https://arxiv.org/abs/1902.08318), VLDB Journal 28 (6), 2019.
