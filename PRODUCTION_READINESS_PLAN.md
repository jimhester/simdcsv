# simdcsv Production Readiness Plan

## Executive Summary

This document outlines the roadmap for transforming simdcsv from an experimental research project (~1,000 LOC) into a production-ready, high-performance CSV parsing library. The plan addresses architecture support, testing, error handling, benchmarking, and incorporates insights from recent literature.

**Current State**: Experimental AVX2-based CSV parser with multi-threading support
**Target State**: Production-ready library supporting Intel, AMD, and ARM architectures with comprehensive testing and error handling

---

## 1. Multi-Architecture SIMD Support

### 1.1 Architecture Detection & Runtime Dispatch

**Goal**: Support Intel, AMD, and ARM processors with optimal SIMD instruction selection

#### Tasks:
- [ ] **CPU Feature Detection**
  - Implement runtime CPU capability detection
  - Support x86/x64: SSE2, SSE4.2, AVX2, AVX-512
  - Support ARM: NEON, SVE (Scalable Vector Extension)
  - Use CPUID on x86, AT_HWCAP on ARM
  - Create `cpu_features.h` with detection utilities

- [ ] **SIMD Abstraction Layer**
  - Create portable SIMD wrapper API (`simd_abstraction.h`)
  - Abstract common operations: load, store, compare, movemask
  - Implement for each architecture:
    - `simd_x86_sse2.h` - Baseline x86 (128-bit)
    - `simd_x86_avx2.h` - Current implementation (256-bit)
    - `simd_x86_avx512.h` - Latest Intel/AMD (512-bit)
    - `simd_arm_neon.h` - ARM 64-bit (128-bit)
    - `simd_arm_sve.h` - ARM Scalable Vectors (128-2048 bit)
  - Fallback scalar implementation for unsupported CPUs

- [ ] **Runtime Function Dispatch**
  - Implement function pointer selection at startup
  - Select optimal SIMD path based on detected features
  - Consider Intel's Multi-Versioning approach (`__attribute__((target(...)))`)
  - Minimize dispatch overhead (one-time at initialization)

- [ ] **Build System Updates**
  - Add architecture-specific compilation flags
  - Support cross-compilation for ARM
  - Create CMake build option for multi-architecture builds
  - Add `-march=` options: `native`, `x86-64`, `armv8-a`, etc.

#### Success Criteria:
- Single binary works optimally on Intel, AMD, and ARM
- <5% performance overhead from dispatch mechanism
- Graceful degradation on older CPUs

---

## 2. Comprehensive Test Suite

### 2.1 Test Infrastructure

**Goal**: Achieve >95% code coverage with automated testing

#### Tasks:
- [ ] **Test Framework Selection**
  - Evaluate: Google Test, Catch2, doctest
  - Recommendation: **Google Test** (industry standard, good C++ support)
  - Set up test directory structure:
    ```
    test/
    ├── unit/           # Unit tests for individual components
    ├── integration/    # End-to-end parsing tests
    ├── fuzzing/        # Fuzz testing harness
    ├── fixtures/       # Test data files
    └── CMakeLists.txt
    ```

- [ ] **Test Data Generation**
  - Create representative CSV test files:
    - Small (1KB), Medium (1MB), Large (1GB), XL (10GB+)
    - Various field counts: 2, 10, 100, 1000 columns
    - Different patterns: numeric, text, mixed, UTF-8
  - Generate edge case files (see section 2.2)
  - Script for automated test data generation

- [ ] **CI/CD Integration**
  - GitHub Actions workflows for:
    - Build on Ubuntu, macOS, Windows
    - Test on x86 and ARM (via QEMU or native runners)
    - Code coverage reporting (codecov.io)
    - Sanitizer builds (ASan, UBSan, MSan)
  - Pre-commit hooks for local testing

- [ ] **Code Coverage**
  - Integrate gcov/lcov for coverage reports
  - Target: >95% line coverage, >90% branch coverage
  - Identify and test uncovered paths

### 2.2 Test Cases by Category

#### Unit Tests
- [ ] **SIMD Operations** (`test/unit/test_simd.cpp`)
  - `fill_input()` - verify correct 64-byte loading
  - `cmp_mask_against_input()` - test comparison masks
  - `find_quote_mask()` - quote detection accuracy
  - Bit manipulation helpers (clear_lowest_bit, count_ones, etc.)

- [ ] **Index Management** (`test/unit/test_index.cpp`)
  - Index creation and storage
  - Serialization/deserialization round-trip
  - Multi-threaded index merging
  - Memory allocation and bounds checking

- [ ] **Parsing Components** (`test/unit/test_parsing.cpp`)
  - First pass: quote and newline detection
  - Second pass: field separator detection
  - FSM state transitions
  - Speculative parsing edge cases

#### Integration Tests
- [ ] **Well-Formed CSV Files** (`test/integration/test_valid.cpp`)
  - Simple CSV (no quotes, no special chars)
  - Quoted fields with commas
  - Quoted fields with newlines
  - Empty fields
  - Single column, single row
  - Large files (>4GB for 32-bit safety)
  - Unicode/UTF-8 content

- [ ] **Malformed CSV Files** (see Section 3)

- [ ] **Performance Regression Tests**
  - Track parsing speed on reference datasets
  - Alert on >10% performance degradation
  - Compare SIMD vs scalar implementations

#### Property-Based Testing
- [ ] **QuickCheck-style tests** using RapidCheck
  - Generate random CSV structures
  - Verify: `parse(serialize(data)) == data`
  - Invariants: field count consistency, index ordering

### 2.3 Correctness Validation

#### Tasks:
- [ ] **Reference Implementation Comparison**
  - Compare against established parsers:
    - Python `csv` module
    - Apache Arrow CSV reader
    - Pandas `read_csv()`
  - Verify identical field extraction on test corpus

- [ ] **Differential Testing**
  - Run SIMD and scalar parsers on same input
  - Assert identical output indices
  - Catch SIMD implementation bugs

- [ ] **Fuzz Testing** (see Section 3.3)

---

## 3. Malformed CSV Handling

### 3.1 Error Detection Strategy

**Goal**: Graceful handling of invalid CSV with clear error reporting

#### Design Decisions:
- [ ] **Error Handling Philosophy**
  - **Option A**: Strict - reject any malformed CSV (recommended for correctness)
  - **Option B**: Lenient - best-effort parsing with warnings (compatibility)
  - **Recommendation**: Strict mode by default, lenient mode opt-in via flag

- [ ] **Error Types to Detect**
  1. Unclosed quoted fields (quote opened, never closed)
  2. Stray quotes (quote in middle of unquoted field)
  3. Inconsistent column counts (ragged rows)
  4. Invalid escape sequences
  5. Binary/non-text data in input
  6. Truncated files (unexpected EOF)

#### Tasks:
- [ ] **Error Code Enumeration** (`error_codes.h`)
  ```cpp
  enum class CSVError {
    OK,
    UNCLOSED_QUOTE,
    STRAY_QUOTE,
    INCONSISTENT_COLUMNS,
    INVALID_ESCAPE,
    UNEXPECTED_EOF,
    BINARY_DATA_DETECTED
  };
  ```

- [ ] **Error Context Reporting**
  - Track line number and column position of errors
  - Provide snippet of problematic data
  - Example: `Error at line 1024, col 7: unclosed quote`

- [ ] **Validation Pass**
  - Optional pre-parsing validation phase
  - Quick scan for structural errors before full parse
  - Trade-off: 2x pass overhead vs early error detection

### 3.2 Recovery Strategies

#### Tasks:
- [ ] **Unclosed Quote Handling**
  - Detection: EOF reached while inside quoted field
  - Recovery options:
    1. Error and abort (strict)
    2. Auto-close at EOF (lenient)
    3. Auto-close at next newline (heuristic)

- [ ] **Inconsistent Column Count**
  - Detection: Row has different field count than header/previous rows
  - Recovery options:
    1. Error on first mismatch (strict)
    2. Pad short rows with empty fields (lenient)
    3. Truncate long rows (data loss, not recommended)
    4. Callback for user-defined handling

- [ ] **Stray Quote Handling**
  - Detection: Quote appears in unquoted field middle
  - Per RFC 4180: this is technically invalid
  - Recovery: treat as literal character (lenient mode)

- [ ] **Configuration API**
  ```cpp
  struct CSVParseConfig {
    bool strict_mode = true;
    bool auto_close_quotes = false;
    bool enforce_column_count = true;
    std::function<void(CSVError, size_t line, size_t col)> error_callback;
  };
  ```

### 3.3 Fuzz Testing for Edge Cases

#### Tasks:
- [ ] **Fuzzing Infrastructure**
  - Integrate AFL++ or libFuzzer
  - Continuous fuzzing on OSS-Fuzz (Google's service)
  - Target: 24/7 fuzzing for 1M+ executions

- [ ] **Fuzzing Targets**
  - Random byte sequences
  - Mutated valid CSV files
  - Pathological cases (all quotes, no quotes, etc.)
  - Boundary conditions (empty file, 1-byte file, etc.)

- [ ] **Crash/Hang Detection**
  - Sanitizer integration (ASan for memory errors)
  - Timeout detection (hang prevention)
  - Corpus minimization (reduce failing inputs)

### 3.4 Test Files for Malformed CSV

#### Tasks:
- [ ] **Create Test Fixtures** (`test/fixtures/malformed/`)
  - `unclosed_quote.csv` - Quote opened, never closed
  - `stray_quote.csv` - Quote in middle of field
  - `inconsistent_columns.csv` - Varying field counts
  - `nested_quotes.csv` - Quote inside quoted field
  - `multiline_unclosed.csv` - Newline inside unclosed quote
  - `binary_data.csv` - Non-UTF8 bytes
  - `truncated.csv` - File cuts off mid-field
  - `empty.csv` - Zero-byte file
  - `only_quotes.csv` - Pathological case (all quotes)
  - `mixed_line_endings.csv` - \r\n, \n, \r mixed

- [ ] **Expected Behavior Documentation**
  - Document expected parse result for each malformed file
  - Specify error codes, messages, and recovery behavior
  - Use as acceptance tests

---

## 4. Enhanced Benchmark Suite

### 4.1 Benchmark Infrastructure Improvements

**Goal**: Comprehensive, reproducible performance measurement

#### Tasks:
- [ ] **Google Benchmark Integration**
  - Replace custom timing code with Google Benchmark library
  - Benefits: statistical analysis, comparison mode, JSON output
  - Keep existing `TimingAccumulator` for detailed perf counters

- [ ] **Benchmark Dimensions**
  - File sizes: 1KB, 10KB, 100KB, 1MB, 10MB, 100MB, 1GB, 10GB
  - Column counts: 2, 5, 10, 50, 100, 500 columns
  - Data types: integers, floats, strings, mixed
  - Thread counts: 1, 2, 4, 8, 16 threads
  - SIMD levels: scalar, SSE2, AVX2, AVX-512, NEON

- [ ] **Comparison Benchmarks**
  - Baseline: stdlib (no SIMD)
  - Competition:
    - simdjson CSV mode (if exists)
    - Apache Arrow CSV reader
    - Pandas read_csv() via Python
    - csv-parser (Vince Bartle's library)
    - fast-cpp-csv-parser
  - Metric: GB/s throughput

- [ ] **Real-World Datasets**
  - NYC Taxi Trip Data (1.5GB, 19 columns)
  - Common Crawl data samples
  - Financial tick data (high-frequency, many rows)
  - Genomics data (wide tables, many columns)
  - Log file formats (syslog CSV exports)

### 4.2 Performance Metrics

#### Tasks:
- [ ] **Throughput Metrics**
  - MB/s and GB/s (primary metric)
  - Records per second
  - Fields per second
  - Normalize by CPU frequency for comparability

- [ ] **Efficiency Metrics**
  - Instructions per byte
  - Cycles per byte (already tracked)
  - Branch misses per 1K instructions
  - Cache miss rate (L1, L2, L3)
  - IPC (instructions per cycle) - already tracked

- [ ] **Scalability Metrics**
  - Thread scaling efficiency (parallel speedup)
  - NUMA effects (multi-socket systems)
  - Large file behavior (>RAM datasets with mmap)

- [ ] **Energy Efficiency** (optional)
  - Joules per GB parsed (RAPL counters on Linux)
  - Performance per watt

### 4.3 Benchmark Reporting

#### Tasks:
- [ ] **Automated Reports**
  - Generate markdown tables with results
  - Create graphs: throughput vs file size, scaling charts
  - Historical tracking: performance over git commits

- [ ] **Public Benchmark Dashboard**
  - Host results on GitHub Pages
  - Update on each release
  - Comparison tables vs competitors

- [ ] **Regression Detection**
  - CI fails if benchmark degrades >10%
  - Bisect performance regressions automatically

---

## 5. Literature Review & Novel Techniques

### 5.1 Recent Research (2020-2026)

**Goal**: Incorporate state-of-the-art techniques and validate assumptions

#### Tasks:
- [ ] **Paper Review**
  - **simdjson (Langdale & Lemire, 2019)** - Already referenced
    - Review for updates/improvements since original paper
    - Check simdjson GitHub for CSV-specific optimizations

  - **Mison (Li et al., 2017)** - Speculative parsing for JSON
    - Techniques applicable to CSV

  - **Zebra (Palkar et al., 2018)** - Vectorized parsing
    - LLVM-based JIT for format parsing

  - **SIMDscan (Willhalm et al., 2009)** - SIMD for database ops
    - String comparison techniques

  - **AVX-512 parsing papers** (2020-2023)
    - Newer instruction set optimizations
    - Gather/scatter, mask operations for CSV

  - **ARM SVE papers** (2019-2024)
    - Scalable vector approaches
    - Length-agnostic SIMD algorithms

  - **Parallel CSV parsing** (Ge et al., 2021)
    - Hadoop-scale CSV parsing
    - Better load balancing techniques

  - **Error handling in high-perf parsers** (recent)
    - Error reporting without sacrificing speed

- [ ] **Technique Evaluation**
  - For each paper:
    - Summarize key technique
    - Assess applicability to simdcsv
    - Estimate implementation effort
    - Prototype high-impact ideas

- [ ] **Create Literature Summary**
  - Document in `docs/literature_review.md`
  - Track: technique, source, status (implemented/considered/rejected)
  - Cite relevant papers in code comments

### 5.2 Known Limitations Analysis

#### Tasks:
- [ ] **Current Limitation Assessment**
  - **Speculative parsing failures**:
    - Problem: Quote state speculation wrong with adversarial input
    - Mitigation: Fallback to FSM parser, or reduce speculation window

  - **Index-only output**:
    - Problem: Doesn't extract actual field values
    - Mitigation: Add value extraction phase with type detection

  - **Memory overhead**:
    - Problem: Full file load into padded buffer
    - Mitigation: Streaming parser (chunked input)

  - **Quote escaping**:
    - Problem: Only handles `""` inside quotes (RFC 4180)
    - Mitigation: Support `\"` (Excel-style) as config option

  - **Line ending detection**:
    - Problem: Assumes `\n`, may fail on `\r\n` or `\r`
    - Mitigation: Auto-detect line endings, configurable override

- [ ] **Benchmark Against Limitations**
  - Create pathological test cases for each limitation
  - Measure performance degradation
  - Document: "Performs poorly when..."

### 5.3 Novel Optimizations to Explore

#### Tasks:
- [ ] **AVX-512 Masked Operations**
  - Use mask registers for conditional processing
  - Eliminate branches in quote handling

- [ ] **Carryless Multiply Improvements**
  - Current quote tracking uses CLMUL
  - Explore: parallel prefix computation, alternate algorithms

- [ ] **Prefetching**
  - Software prefetch next 64-byte chunk
  - Reduce memory stalls

- [ ] **Huge Pages**
  - Use 2MB pages for large files
  - Reduce TLB misses

- [ ] **SIMD String Comparison**
  - Fast strcmp for field value matching
  - Useful for field filtering

- [ ] **Adaptive Parsing**
  - Profile first N rows to detect patterns
  - Switch parsing strategy based on data characteristics
  - Examples: mostly unquoted → simplify, many quotes → careful handling

---

## 6. API Design & Usability

### 6.1 Public API Definition

**Goal**: Intuitive, safe, and flexible API for library users

#### Tasks:
- [ ] **Core API Design** (`include/simdcsv.h`)
  ```cpp
  namespace simdcsv {
    // Simple API: parse file to 2D vector
    std::vector<std::vector<std::string>> parse_file(
      const std::string& path,
      const CSVParseConfig& config = {}
    );

    // Streaming API: callback per row
    void parse_file_streaming(
      const std::string& path,
      std::function<void(const std::vector<std::string>&)> row_callback,
      const CSVParseConfig& config = {}
    );

    // Low-level API: index-based (current implementation)
    index parse_file_indexed(
      const std::string& path,
      const CSVParseConfig& config = {}
    );

    // In-memory parsing
    index parse_buffer(
      const char* data,
      size_t size,
      const CSVParseConfig& config = {}
    );
  }
  ```

- [ ] **Configuration Options**
  ```cpp
  struct CSVParseConfig {
    char delimiter = ',';          // Field separator
    char quote_char = '"';         // Quote character
    bool has_header = true;        // First row is header
    bool strict_mode = true;       // Error on malformed CSV
    size_t num_threads = 0;        // 0 = auto-detect
    SIMDLevel simd_level = AUTO;   // Force SIMD level
    // ... (from Section 3.2)
  };
  ```

- [ ] **Type-Aware Parsing** (future enhancement)
  ```cpp
  // Schema-based parsing with type inference
  struct ColumnSchema {
    std::string name;
    enum { INT, FLOAT, STRING, DATE } type;
  };

  DataFrame parse_file_typed(
    const std::string& path,
    const std::vector<ColumnSchema>& schema = {} // auto-infer if empty
  );
  ```

### 6.2 C API for Language Bindings

#### Tasks:
- [ ] **C Wrapper** (`include/simdcsv_c.h`)
  - Expose core functionality as C ABI
  - Enable bindings for Python, R, Julia, Rust, etc.
  - Handle: memory management, error codes, opaque handles

- [ ] **Python Bindings**
  - Use pybind11 for C++ → Python
  - Integrate with pandas: `simdcsv.read_csv()` → DataFrame
  - Benchmark vs pandas native implementation

### 6.3 Documentation

#### Tasks:
- [ ] **API Documentation**
  - Use Doxygen for C++ API docs
  - Generate HTML docs, host on GitHub Pages
  - Document: parameters, return values, exceptions, examples

- [ ] **User Guide** (`docs/user_guide.md`)
  - Installation instructions
  - Quick start examples
  - Configuration guide
  - Performance tuning tips

- [ ] **Developer Guide** (`docs/developer_guide.md`)
  - Architecture overview
  - SIMD implementation details
  - Adding new SIMD backends
  - Contributing guidelines

- [ ] **Benchmarking Guide** (`docs/benchmarking.md`)
  - How to run benchmarks
  - Interpreting results
  - Platform-specific notes

---

## 7. Build System & Packaging

### 7.1 Modern Build System

**Goal**: CMake-based build with easy cross-platform support

#### Tasks:
- [ ] **CMake Migration**
  - Replace Makefile with CMakeLists.txt
  - Support: Linux, macOS, Windows (MSVC, MinGW)
  - Features:
    - Library target (static, shared)
    - Executable target (CLI tool)
    - Test target
    - Benchmark target
    - Install target

- [ ] **Compiler Support**
  - GCC 9+ (current: ✓)
  - Clang 10+
  - MSVC 2019+ (for Windows)
  - Intel ICC (optional)

- [ ] **Dependency Management**
  - Option 1: Git submodules for Google Test, Benchmark
  - Option 2: CMake FetchContent
  - Option 3: vcpkg, Conan for system-wide deps

### 7.2 Packaging & Distribution

#### Tasks:
- [ ] **Package Formats**
  - **Linux**: .deb (Debian/Ubuntu), .rpm (Fedora/RHEL)
  - **macOS**: Homebrew formula
  - **Windows**: NuGet package, vcpkg port
  - **Source**: GitHub releases with tarball

- [ ] **Install Targets**
  - Headers → `/usr/local/include/simdcsv/`
  - Library → `/usr/local/lib/libsimdcsv.{a,so}`
  - Binary → `/usr/local/bin/simdcsv`
  - Documentation → `/usr/local/share/doc/simdcsv/`

- [ ] **Version Management**
  - Semantic versioning: MAJOR.MINOR.PATCH
  - ABI compatibility tracking
  - Changelog maintenance

---

## 8. Performance Optimization

### 8.1 Profiling & Hotspot Analysis

#### Tasks:
- [ ] **Profiling Infrastructure**
  - Linux: perf, Valgrind/Callgrind
  - macOS: Instruments
  - Windows: VTune, Windows Performance Analyzer
  - Identify: CPU hotspots, cache misses, branch mispredictions

- [ ] **Memory Profiling**
  - Heap allocation patterns (minimize allocations)
  - Memory bandwidth utilization
  - NUMA effects on multi-socket

- [ ] **Micro-benchmarks**
  - Isolate individual functions (e.g., find_quote_mask)
  - A/B test optimization variants
  - Use Google Benchmark for precision

### 8.2 Optimization Targets

#### Tasks:
- [ ] **Branch Reduction**
  - Current TODO in code: "remove branches if possible"
  - Use branchless techniques: CMOV, masks
  - Example: replace `if (mask) { ... }` with masked operations

- [ ] **Loop Unrolling**
  - Manually unroll critical loops
  - Process 128 or 256 bytes per iteration (2-4x current)

- [ ] **Memory Access Patterns**
  - Align data to cache line boundaries (64 bytes)
  - Minimize random access, maximize sequential

- [ ] **Prefetching**
  - Insert `_mm_prefetch()` for next chunk
  - Tune prefetch distance (empirical testing)

- [ ] **SIMD Width Utilization**
  - AVX-512: process 512 bits (64 bytes) per op
  - Ensure all 512 bits are used effectively

- [ ] **Multi-threading Efficiency**
  - Reduce thread synchronization overhead
  - Optimize work distribution (current chunk-based approach)
  - Consider work-stealing for load balancing

---

## 9. Production Hardening

### 9.1 Error Handling & Robustness

#### Tasks:
- [ ] **Exception Safety**
  - Use RAII for resource management
  - Mark noexcept where appropriate
  - Handle allocation failures gracefully

- [ ] **Input Validation**
  - Check: file exists, readable, size > 0
  - Detect: binary files, non-CSV formats
  - Fail fast with clear error messages

- [ ] **Resource Limits**
  - Max file size handling (prevent OOM)
  - Configurable memory limits
  - Graceful degradation on resource exhaustion

### 9.2 Security

#### Tasks:
- [ ] **Sanitizer Testing**
  - AddressSanitizer (ASan): memory errors
  - UndefinedBehaviorSanitizer (UBSan): undefined behavior
  - MemorySanitizer (MSan): uninitialized reads
  - ThreadSanitizer (TSan): data races

- [ ] **Fuzz Testing** (see Section 3.3)
  - Continuous fuzzing for security issues
  - Prevent: buffer overflows, integer overflows, crashes

- [ ] **Dependency Audit**
  - Minimal dependencies (currently none!)
  - Track CVEs if dependencies added

### 9.3 Logging & Diagnostics

#### Tasks:
- [ ] **Logging Framework**
  - Optional: spdlog, or custom lightweight logger
  - Log levels: ERROR, WARN, INFO, DEBUG, TRACE
  - Controlled via environment variable or config

- [ ] **Debug Mode**
  - Verbose output: show parsing decisions, SIMD path selection
  - Dump intermediate results (quote masks, field indices)
  - Performance trace (timing per phase)

---

## 10. Additional Production Features

### 10.1 Streaming & Memory Efficiency

#### Tasks:
- [ ] **Streaming Parser**
  - Don't require full file in memory
  - Process in fixed-size chunks (e.g., 64MB)
  - Handle record boundaries spanning chunks

- [ ] **Memory Mapping**
  - Use mmap() for large files
  - On-demand paging (reduce upfront load time)
  - Benchmark vs read() approach

### 10.2 Advanced CSV Features

#### Tasks:
- [ ] **Header Handling**
  - Parse header row separately
  - Return field names with data
  - Column name → index mapping

- [ ] **Field Type Detection**
  - Auto-detect: int, float, string, date, bool
  - Provide type hints to user
  - Optional: convert to native types (not just strings)

- [ ] **Column Selection**
  - Parse only specified columns (skip others)
  - Useful for wide CSVs with many unused fields

- [ ] **Row Filtering**
  - Predicate-based row filtering during parse
  - Avoid allocating memory for filtered rows

- [ ] **Custom Delimiters**
  - Support tab-separated (TSV), pipe-separated, etc.
  - Multi-character delimiters (rare, but requested)

- [ ] **Line Ending Auto-detection**
  - Detect: `\n`, `\r\n`, `\r`
  - Handle mixed line endings (rare but exists)

### 10.3 Output Formats

#### Tasks:
- [ ] **Value Extraction**
  - Convert index-based output to string vectors
  - Zero-copy where possible (string_view)

- [ ] **Apache Arrow Integration**
  - Output directly to Arrow RecordBatch
  - Zero-copy columnar format
  - Integrate with Arrow ecosystem (Parquet, etc.)

- [ ] **JSON Output**
  - Convert CSV → JSON array of objects
  - Use field names as keys

---

## 11. Community & Ecosystem

### 11.1 Open Source Best Practices

#### Tasks:
- [ ] **Licensing**
  - Choose license: MIT, Apache 2.0, or BSD (permissive recommended)
  - Add LICENSE file
  - Update README with license badge

- [ ] **Contributing Guidelines**
  - Create CONTRIBUTING.md
  - Code style guide (ClangFormat config)
  - PR template

- [ ] **Code of Conduct**
  - Adopt Contributor Covenant
  - Create CODE_OF_CONDUCT.md

- [ ] **Issue Templates**
  - Bug report template
  - Feature request template
  - Question template

### 11.2 Documentation & Examples

#### Tasks:
- [ ] **README Enhancement**
  - Installation instructions
  - Quick start example
  - Performance comparison table
  - Link to full documentation

- [ ] **Example Programs** (`examples/`)
  - `simple_parse.cpp` - Basic usage
  - `streaming_parse.cpp` - Large file streaming
  - `type_detection.cpp` - Auto-type inference
  - `error_handling.cpp` - Handling malformed CSV
  - `benchmark.cpp` - Custom benchmarking

- [ ] **Tutorials**
  - Blog post: "Fast CSV Parsing with SIMD"
  - Video: Architecture walkthrough

### 11.3 Integrations

#### Tasks:
- [ ] **Language Bindings**
  - Python (pybind11) - high priority
  - R (Rcpp)
  - Julia (CxxWrap.jl)
  - Rust (cxx or bindgen)

- [ ] **Framework Integrations**
  - Pandas drop-in replacement
  - DuckDB CSV reader
  - Polars CSV reader

---

## 12. Milestones & Prioritization

### Phase 1: Foundation (Months 1-2)
**Priority: Critical**

1. Multi-architecture SIMD support (Intel, AMD, ARM)
   - CPU detection
   - SIMD abstraction layer
   - AVX2, AVX-512, NEON implementations

2. Test infrastructure
   - Google Test setup
   - CI/CD pipeline (GitHub Actions)
   - Test data generation

3. CMake build system
   - Replace Makefile
   - Cross-platform support

4. Basic malformed CSV handling
   - Error detection
   - Strict mode implementation

**Deliverable**: Library compiles and runs on x86 and ARM with basic tests

---

### Phase 2: Correctness & Robustness (Months 3-4)
**Priority: High**

1. Comprehensive test suite
   - 100+ unit tests
   - 50+ integration tests
   - >90% code coverage

2. Malformed CSV handling completion
   - All error types detected
   - Lenient mode option
   - Fuzz testing setup

3. Sanitizer builds
   - ASan, UBSan, MSan, TSan clean

4. Public API design
   - C++ API finalized
   - C wrapper for bindings

**Deliverable**: Production-quality correctness with comprehensive error handling

---

### Phase 3: Performance & Benchmarking (Months 5-6)
**Priority: High**

1. Enhanced benchmark suite
   - Google Benchmark integration
   - Competitor comparisons
   - Real-world datasets

2. Performance optimization
   - Profiling-guided optimizations
   - Branch reduction
   - Prefetching

3. Literature review implementation
   - Evaluate 5-10 recent papers
   - Prototype top 3 techniques

4. Benchmark documentation
   - Public dashboard
   - Performance claims validation

**Deliverable**: Documented performance leadership with scientific backing

---

### Phase 4: Usability & Ecosystem (Months 7-8)
**Priority: Medium**

1. API enhancements
   - Streaming parser
   - Type detection
   - Column selection

2. Documentation
   - API docs (Doxygen)
   - User guide
   - Developer guide

3. Python bindings
   - pybind11 wrapper
   - Pandas integration
   - PyPI package

4. Example programs
   - 5+ complete examples

**Deliverable**: Developer-friendly library with excellent documentation

---

### Phase 5: Distribution & Community (Months 9-10)
**Priority: Medium**

1. Packaging
   - Debian, Homebrew, vcpkg
   - GitHub releases

2. Community setup
   - Contributing guidelines
   - Issue templates
   - Code of conduct

3. Integrations
   - Additional language bindings
   - Framework integrations

4. Marketing
   - Blog posts
   - Conference talks (optional)
   - Hacker News post

**Deliverable**: Widely available, community-ready library

---

### Phase 6: Advanced Features (Months 11-12)
**Priority: Low (nice-to-have)

1. Apache Arrow output
2. Advanced CSV features (custom delimiters, filtering)
3. Additional SIMD architectures (RISC-V, older SSE variants)
4. Performance monitoring (energy efficiency)

**Deliverable**: Feature-complete, industry-leading CSV parser

---

## 13. Success Metrics

### Performance Targets
- [ ] **Throughput**: >5 GB/s on modern hardware (AVX-512 or ARM SVE)
- [ ] **Competitive**: 2-5x faster than pandas, Apache Arrow on benchmarks
- [ ] **Scaling**: >80% parallel efficiency up to 16 threads
- [ ] **Memory**: <10% overhead vs file size (excluding output)

### Quality Targets
- [ ] **Code coverage**: >95% line coverage, >90% branch coverage
- [ ] **Fuzz stability**: 1M+ executions without crash
- [ ] **Sanitizer clean**: 0 errors on ASan, UBSan, MSan, TSan
- [ ] **Platform support**: Linux, macOS, Windows (x86/ARM)

### Ecosystem Targets
- [ ] **Documentation**: 100% of public API documented
- [ ] **Examples**: 5+ working example programs
- [ ] **Bindings**: Python bindings with PyPI package
- [ ] **Adoption**: 100+ GitHub stars, 3+ external contributors

---

## 14. Open Questions & Decisions Needed

### Technical Decisions
- [ ] **Strict vs lenient default**: Should default mode be strict (error on malformed) or lenient (best-effort)?
- [ ] **Streaming vs full-load**: Primary API streaming or in-memory? Both?
- [ ] **Type detection**: Auto-detect types or require schema?
- [ ] **Thread pool**: Built-in thread pool or user-managed?

### Strategic Decisions
- [ ] **Target use case**: Focus on big data (Hadoop-scale) or interactive (pandas-replacement)?
- [ ] **Scope creep**: How many CSV variants to support (Excel, etc.)?
- [ ] **Maintenance commitment**: Long-term maintenance plan post-launch?

---

## 15. References & Resources

### Papers
1. Chang et al., "Speculative Distributed CSV Data Parsing for Big Data Analytics", SIGMOD 2019
2. Langdale & Lemire, "Parsing Gigabytes of JSON per Second", VLDB Journal 2019
3. RFC 4180 - Common Format and MIME Type for CSV Files

### Related Projects
- simdjson: https://github.com/simdjson/simdjson
- Apache Arrow CSV: https://arrow.apache.org/docs/cpp/csv.html
- csv-parser: https://github.com/vincentlaucsb/csv-parser
- fast-cpp-csv-parser: https://github.com/ben-strasser/fast-cpp-csv-parser

### Tools
- Google Test: https://github.com/google/googletest
- Google Benchmark: https://github.com/google/benchmark
- AFL++: https://github.com/AFLplusplus/AFLplusplus
- pybind11: https://github.com/pybind/pybind11

---

## Conclusion

This plan transforms simdcsv from a research prototype into a production-ready, high-performance CSV parsing library. The phased approach prioritizes correctness and robustness first, then performance, and finally ecosystem features. With an estimated 10-12 month timeline, the result will be a competitive, well-tested, and widely usable library suitable for both industry and research applications.

**Next Steps**:
1. Review and approve this plan
2. Prioritize phases based on project goals
3. Begin Phase 1: Foundation work
