# Code Coverage in libvroom

This document explains how code coverage is measured in libvroom and important caveats about header file coverage reporting.

## Table of Contents

- [Coverage Tools](#coverage-tools)
- [Running Coverage Locally](#running-coverage-locally)
- [Header File Coverage Limitation](#header-file-coverage-limitation)
- [Affected Headers](#affected-headers)
- [Interpreting Coverage Reports](#interpreting-coverage-reports)

## Coverage Tools

libvroom uses the following tools for code coverage:

- **gcov/lcov**: GNU coverage tools for generating coverage data
- **Codecov**: Cloud service for tracking coverage over time and displaying reports on pull requests

## Running Coverage Locally

```bash
# Build with coverage enabled
cmake -B build -DCMAKE_BUILD_TYPE=Debug -DENABLE_COVERAGE=ON
cmake --build build -j$(nproc)

# Run tests to generate coverage data
cd build && ctest --output-on-failure -j$(nproc)

# Generate coverage report
lcov --capture --directory . --output-file coverage.info
lcov --remove coverage.info '*/test/*' '*/benchmark/*' '*/build/_deps/*' '/usr/*' --output-file coverage.info
lcov --list coverage.info
```

## Header File Coverage Limitation

**Important**: Codecov significantly under-reports coverage for header-only and template-heavy code. This is a known limitation of gcov-based coverage tools, not a reflection of actual test coverage.

### Why This Happens

When code is defined in header files (templates, inline functions, constexpr), the compiler generates the actual machine code in each `.cpp` file that includes the header. The coverage tools attribute execution to the `.cpp` translation unit where the code was instantiated, not to the original header file.

This means:

1. **Template code** in headers gets attributed to whichever `.cpp` file first instantiates it
2. **Inline functions** may show as uncovered in the header but are actually executed when called from `.cpp` files
3. **Header-only libraries** appear to have very low coverage even when fully tested

### Coverage Discrepancy

The reported coverage can differ dramatically between local analysis and Codecov:

| Measurement | Coverage |
|-------------|----------|
| Codecov (headers only) | ~6% |
| Local gcov analysis | 72-94% |

The local analysis correctly attributes coverage to where the code is defined, while Codecov's aggregation can miss coverage that was attributed to `.cpp` files.

## Affected Headers

The following header files contain significant template/inline code and are affected by this limitation:

| Header | Description | Why Affected |
|--------|-------------|--------------|
| `include/two_pass.h` | Core two-pass parsing algorithm | Heavy template use for SIMD dispatch |
| `include/simd_highway.h` | Portable SIMD operations | All SIMD operations are inline/template |
| `include/type_detector.h` | Type detection and inference | Template-based type detection |
| `include/value_extraction.h` | Value extraction from CSV fields | Inline parsing functions |
| `include/branchless_state_machine.h` | Quote state machine | Branchless inline operations |

## Interpreting Coverage Reports

When reviewing coverage:

1. **Focus on `.cpp` file coverage**: These files accurately reflect test execution
2. **Don't be alarmed by low header coverage**: The code is tested, just attributed elsewhere
3. **Use patch coverage for PRs**: Codecov's patch coverage for new code in `.cpp` files is accurate
4. **Run local coverage for accurate header analysis**: Use `gcov` directly on header files to see true coverage

### Verifying Header Coverage Locally

To get accurate coverage for a specific header:

```bash
# After running tests with coverage enabled
cd build

# Check coverage for a specific header
gcov -o CMakeFiles/libvroom.dir/src/ ../include/two_pass.h

# Or use lcov to generate an HTML report
genhtml coverage.info --output-directory coverage_html
# Then open coverage_html/index.html in a browser
```

## Future Improvements

Potential solutions being considered:

1. **Source-based coverage** (Clang): Uses `-fprofile-instr-generate -fcoverage-mapping` which tracks coverage at the source level rather than object level
2. **llvm-cov**: May provide better header attribution than gcov
3. **Coverage annotations**: Adding explicit instantiation in test files to improve header attribution

## See Also

- [CI Workflows README](../.github/workflows/README.md) - CI pipeline including coverage job
- [Test Suite README](../test/README.md) - Test organization and running tests
- [codecov.yml](../codecov.yml) - Codecov configuration
