# GitHub Actions CI

This directory contains GitHub Actions workflows for libvroom continuous integration.

## Workflows

### ci.yml - Main CI Pipeline

Runs on every push and pull request to main/master branches and all `claude/**` branches.

**Build Matrix:**
- **Platforms**: Ubuntu (latest), macOS (latest)
- **Build Types**: Release, Debug
- **Total Jobs**: 4 build combinations

**Build Steps:**
1. Checkout code
2. Install dependencies (CMake, build tools)
3. Configure CMake with specified build type
4. Build all targets (libvroom, libvroom_test, error_handling_test)
5. Run well-formed CSV tests (42 tests)
6. Run error handling tests (37 tests)
7. Run full CTest suite (79 tests)

**Code Quality Checks:**
1. Verify required files exist
2. Count and validate test files
3. Ensure all 16 malformed CSV test files present

## CI Badge

Add to README.md:
```markdown
![CI](https://github.com/jimhester/libvroom/workflows/CI/badge.svg)
```

## Local Testing

To test locally before pushing:

```bash
# Run the same commands as CI
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
./build/libvroom_test
./build/error_handling_test
cd build && ctest --output-on-failure
```

## Adding New Tests

When adding new test files:

1. Add test file to appropriate `test/data/` subdirectory
2. Update test harness (`csv_parser_test.cpp` or `error_handling_test.cpp`)
3. Update file count validation in `ci.yml` if needed
4. CI will automatically run new tests on next push

## Platform-Specific Notes

### Linux (Ubuntu)
- Uses GCC 13.3.0
- Installs: cmake, build-essential
- AVX2 support via `-march=native`

### macOS
- Uses Apple Clang
- Installs: cmake (via Homebrew)
- ARM64 (M1/M2) and x86_64 support

### Future Platforms
- **Windows**: MSVC support planned (requires adjustments for AVX2 intrinsics)
- **ARM Linux**: For testing ARM NEON/SVE implementations

## Debugging CI Failures

### Build Failures
1. Check CMake configuration output
2. Verify all source files compile
3. Check for missing dependencies

### Test Failures
1. Review test output (shown via `--output-on-failure`)
2. Check if test data files are present
3. Verify file permissions and line endings

### Code Quality Failures
1. Ensure all required files committed
2. Check file counts match expected values
3. Verify directory structure

## Performance Considerations

- **Caching**: Could add CMake/build caching for faster builds
- **Parallel builds**: CMake uses multiple cores by default
- **Test parallelization**: CTest can run tests in parallel

### fuzz.yml - Fuzz Testing

Continuous fuzz testing using libFuzzer with sanitizers.

**Triggers:**
- **Scheduled**: Weekly on Sundays at 2:00 UTC
- **Manual**: On-demand via workflow_dispatch

**Fuzz Targets:**
- `fuzz_csv_parser` - Core two-pass CSV parser
- `fuzz_dialect_detection` - Dialect detection
- `fuzz_parse_auto` - Integrated parse_auto()

**Configuration:**
- Default duration: 10 minutes per target (scheduled runs)
- Manual runs: Configurable 1-30 minutes
- Sanitizers: AddressSanitizer, UndefinedBehaviorSanitizer

**On Crash Detection:**
1. Workflow fails with error message
2. Crash artifacts uploaded (30-day retention)
3. Details in workflow logs

See `fuzz/README.md` for local fuzzing instructions.

## Future Enhancements

Potential workflow additions:

1. **Coverage reporting**: Add code coverage with lcov/gcov
2. **Benchmarking**: Performance regression testing
3. **Static analysis**: clang-tidy, cppcheck
4. **Format checking**: clang-format validation
5. **Windows builds**: MSVC support
6. **ARM64 builds**: Native ARM testing
7. **Release automation**: Automatic tagging and releases
