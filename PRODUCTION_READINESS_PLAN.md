# simdcsv Production Readiness Plan

## Executive Summary

This document outlines the roadmap for transforming simdcsv from an experimental research project (~1,000 LOC) into a production-ready, high-performance CSV parsing library. The plan addresses architecture support, testing, error handling, benchmarking, and incorporates insights from recent literature.

**Current State**: Experimental AVX2-based CSV parser with multi-threading support and index-based output

**Primary Goal**: Replace the multithreaded parser in [vroom](https://github.com/tidyverse/vroom), a fast CSV reader for R that uses lazy evaluation via Altrep

**Target State**: Production-ready library supporting Intel, AMD, and ARM architectures with comprehensive testing, error handling, and seamless vroom integration

**Key Alignment**: simdcsv's index-based approach is perfectly suited for vroom's architecture, which indexes file positions and lazily materializes values on-demand rather than eagerly parsing all data.

---

## 1. vroom Integration & R Ecosystem

### 1.1 Understanding vroom's Architecture

**Goal**: Design simdcsv to serve as a drop-in replacement for vroom's current multithreaded parser

#### Background: How vroom Works

[vroom](https://vroom.r-lib.org/) is a high-performance CSV reader for R that achieves speed through:

1. **Index-based parsing**: Records file positions of fields without materializing values
2. **Altrep lazy evaluation**: Uses R's Alternative Representation framework (R 3.5+) to delay value parsing until accessed
3. **Memory mapping**: Maps files into memory for zero-copy reads
4. **Multithreading**: Parallelizes indexing and materialization of non-character columns

**Perfect Alignment**: simdcsv already produces index-based output, making it an ideal backend for vroom's architecture.

#### Tasks:
- [ ] **Study vroom's Current Parser** ([vroom issue #105](https://github.com/tidyverse/vroom/issues/105))
  - Analyze current indexing implementation in vroom's C++ code
  - Understand performance bottlenecks in parallel parsing
  - Identify integration points for simdcsv
  - Review memory mapping strategy

- [ ] **Design simdcsv-vroom Interface**
  - Define C++ API that vroom can consume
  - Index format compatibility (match vroom's internal representation)
  - Thread coordination between simdcsv and vroom's Altrep vectors
  - Error reporting mechanism compatible with R

- [x] **CleverCSV-Style Dialect Detection**
  - Implement improved delimiter detection (addresses [vroom #105](https://github.com/tidyverse/vroom/issues/105))
  - Auto-detect quote characters and escape sequences
  - Based on CleverCSV algorithm (see [arXiv:1811.11242](https://arxiv.org/pdf/1811.11242.pdf))
  - Optimize to work with minimal data (e.g., first 10 records)
  - Fallback to current heuristics if detection uncertain

- [ ] **Benchmark Against vroom**
  - Compare simdcsv indexing speed vs vroom's current implementation
  - Test on vroom's benchmark datasets
  - Measure: indexing throughput, memory usage, thread scaling
  - Target: 2-3x faster indexing than current vroom

### 1.2 cpp11 Integration

**Goal**: Use [cpp11](https://github.com/r-lib/cpp11) for R bindings instead of Rcpp

#### Background: Why cpp11

cpp11 is a modern R/C++ interface library authored by Jim Hester (simdcsv author):
- **Header-only**: No shared library dependencies, eliminates version mismatches
- **Faster compilation**: Reduced memory and build time vs Rcpp
- **Modern C++**: Leverages C++11 features
- **Better safety**: Improved protection for R API interactions
- **Vendoring**: Can bundle headers directly in R packages

#### Tasks:
- [ ] **cpp11 Wrapper Implementation**
  - Create `src/cpp11.cpp` with `[[cpp11::register]]` functions
  - Expose core parsing API to R
  - Handle memory management between C++ and R
  - Use `cpp11::strings`, `cpp11::integers` for zero-copy where possible

- [ ] **Integration with vroom's Altrep**
  - Design callbacks for Altrep materialization
  - Support vroom's lazy character vectors (`VROOM_USE_ALTREP_CHR`)
  - Optimize for best-case scenario (character data, lazy access)
  - Handle worst-case scenario (numeric data, eager parsing)

- [ ] **R Package Structure** (optional standalone package)
  - Create `simdcsv` R package skeleton
  - Add cpp11 to DESCRIPTION LinkingTo
  - Provide `simdcsv::read_csv()` function for testing
  - Compare performance vs `vroom::vroom()` and `data.table::fread()`

### 1.3 vroom-Specific Features

#### Tasks:
- [x] **Line Ending Handling**
  - Support `\n`, `\r\n`, `\r` (Windows/Unix/old Mac)
  - Mixed line endings in single file (rare but exists)
  - Fast SIMD-based detection

- [x] **Delimiter Detection**
  - Current vroom issue: suboptimal guessing results
  - CleverCSV approach: analyze first N rows for delimiter patterns
  - Score candidates: `,`, `\t`, `;`, `|`, ` ` (space)
  - Detect quote character: `"`, `'`

- [ ] **Column Type Inference**
  - Collaborate with vroom's existing type detection
  - Provide hints from parsed data (all numeric, all text, etc.)
  - Support vroom's `col_types` specification

- [ ] **Progress Reporting**
  - Integrate with R's progress API
  - Report indexing progress for large files
  - Enable vroom's progress bars

---

## 2. Literature Review & Research Foundation

**PRIORITY: This comes FIRST before implementation**

### 2.1 Recent Research (2020-2026)

**Goal**: Ground all design decisions in current research and validate assumptions before coding

#### Tasks:
- [ ] **Core Papers Review**
  - **Chang et al., "Speculative Distributed CSV Data Parsing"** (SIGMOD 2019) - Already referenced
    - Deep dive into speculative parsing trade-offs
    - Understand when speculation fails
    - Optimal speculation window size

  - **Langdale & Lemire, "Parsing Gigabytes of JSON per Second"** (VLDB Journal 2019) - Already referenced
    - SIMD techniques for structural indexing
    - Two-stage parsing approach
    - Bit manipulation tricks

  - **CleverCSV** (van den Burgh & Nazabal, 2019) - [arXiv:1811.11242](https://arxiv.org/pdf/1811.11242.pdf)
    - Dialect detection algorithm (for vroom #105)
    - Pattern consistency scoring
    - Minimal sample size requirements

- [ ] **SIMD & Performance Papers**
  - **Mison** (Li et al., 2017) - Speculative parsing for JSON
    - Level-based speculation
    - Applicable to CSV with quotes

  - **Zebra** (Palkar et al., 2018) - Vectorized parsing
    - LLVM-based JIT approaches
    - Runtime code generation trade-offs

  - **AVX-512 optimization papers** (2020-2024)
    - Masked operations for conditional processing
    - New instructions useful for CSV
    - Performance pitfalls (frequency throttling)

  - **ARM SVE/SVE2 papers** (2019-2024)
    - Scalable vector length algorithms
    - Predication vs masking
    - Performance portability

- [ ] **Parallel Parsing Research**
  - **Parallel CSV parsing** (Ge et al., 2021)
    - Improved load balancing
    - Reducing synchronization overhead

  - **High-performance text processing** (recent)
    - Memory bandwidth optimization
    - Cache-conscious algorithms

- [ ] **Create Literature Summary**
  - Document in `docs/literature_review.md`
  - For each paper: technique, applicability, implementation priority
  - Identify conflicting recommendations and resolve
  - Cite papers in code comments where techniques are applied

### 2.2 SIMD Abstraction Library Research

**Goal**: Use existing, well-maintained SIMD libraries instead of custom per-architecture code

#### Research Findings:

Two leading candidates identified:

1. **Google Highway** - [github.com/google/highway](https://github.com/google/highway)
   - Performance-portable SIMD with runtime dispatch
   - Supports SSE4, AVX2, AVX-512, NEON, SVE, WASM, PowerPC
   - Used in: JPEG XL, NumPy (proposed in [NEP 54](https://numpy.org/neps/nep-0054-simd-cpp-highway.html))
   - Dual-licensed: Apache 2 / BSD-3
   - **Pros**: High-level API, well-tested, active development
   - **Cons**: Learning curve, potential overhead from abstraction

2. **SIMDe (SIMD Everywhere)** - [github.com/simd-everywhere/simde](https://github.com/simd-everywhere/simde)
   - Header-only library for cross-platform SIMD
   - Emulates specific instruction sets (e.g., AVX2 on ARM via NEON)
   - Provides low-level intrinsics, closer to existing simdcsv code
   - **Pros**: Drop-in replacement for intrinsics, minimal refactoring
   - **Cons**: Emulation overhead on some platforms

#### Tasks:
- [x] **Evaluate Highway for simdcsv**
  - Prototype: port existing AVX2 code to Highway abstractions
  - Measure: performance overhead, code complexity
  - Test: automatic dispatch to best available SIMD level
  - Decision criteria: <5% overhead acceptable

- [x] **Evaluate SIMDe for simdcsv**
  - Test: compile existing AVX2 code with SIMDe on ARM
  - Measure: emulation performance vs native NEON
  - Check: compatibility with existing bit manipulation tricks

- [x] **Make Library Selection Decision**
  - **Recommendation**: Start with **Highway** for long-term maintainability
  - Fallback: SIMDe if Highway overhead too high
  - Worst case: Keep custom code for AVX2, use library for ARM

- [x] **Integration Plan**
  - Add selected library as dependency (git submodule or CMake FetchContent)
  - Create abstraction wrapper if needed
  - Maintain performance parity with current AVX2 code

### 2.3 Known Limitations Analysis

#### Tasks:
- [ ] **Document Current Limitations**
  - Speculative parsing failures
  - Index-only output (already addressed by vroom integration)
  - Memory overhead (full file load)
  - Quote escaping variants
  - Line ending detection

- [ ] **Prioritize Based on vroom Needs**
  - Which limitations affect vroom integration?
  - Which can be deferred to post-launch?

- [ ] **Create Pathological Test Cases**
  - For each limitation, create worst-case input
  - Measure performance degradation
  - Document: "Known to perform poorly when..."

---

## 3. Multi-Architecture SIMD Support

**STRATEGY CHANGE: Depth-first, not breadth-first**

**Priority**: Get AVX2 (x86-64) perfect FIRST, then expand to ARM

### 3.1 Phase 1: Optimize AVX2 Implementation (Current Architecture)

**Goal**: Achieve best-in-class performance on x86-64 before porting to other architectures

#### Rationale:
- Most development/CI systems are x86-64
- Faster iteration on single platform
- Establish performance baseline before porting
- Validate correctness thoroughly on one architecture
- **Then** use SIMD abstraction library (Highway/SIMDe) to port optimized design

#### Tasks:
- [ ] **Profile Current AVX2 Implementation**
  - Use perf, VTune, or similar profiler
  - Identify hotspots: quote detection, field parsing, index storage
  - Measure: cache misses, branch mispredictions, IPC

- [ ] **Optimize AVX2 Code Paths**
  - Apply findings from literature review (Section 2.1)
  - Reduce branches using mask operations
  - Optimize carryless multiply for quote tracking
  - Experiment with different loop unrolling factors
  - Target: >5 GB/s on modern x86-64 CPU

- [ ] **Test Thoroughly on x86-64**
  - Comprehensive test suite (see Section 4)
  - Fuzz testing for edge cases
  - Validate on Intel and AMD processors
  - Test on various microarchitectures (Skylake, Zen 3, etc.)

### 3.2 Phase 2: Add AVX-512 Support (x86-64 Only)

**Goal**: Leverage 512-bit SIMD on newer Intel/AMD processors

#### Tasks:
- [ ] **AVX-512 Implementation**
  - Port optimized AVX2 code to AVX-512
  - Use mask registers for conditional processing
  - Process 64-byte chunks with single instruction
  - Beware: frequency throttling on some CPUs

- [ ] **Runtime Dispatch Between AVX2 and AVX-512**
  - Detect AVX-512 support at runtime
  - Fallback to AVX2 if unavailable
  - Consider: AVX-512 may be slower on some workloads (due to downclocking)

### 3.3 Phase 3: Port to ARM via SIMD Library

**Goal**: Use Highway or SIMDe to support ARM NEON/SVE

**IMPORTANT**: Only start this phase after AVX2/AVX-512 is production-ready

#### Tasks:
- [x] **Choose SIMD Abstraction Library** (based on Section 2.2 evaluation)
  - If Highway: Port AVX2 code to Highway abstractions
  - If SIMDe: Use SIMDe headers for ARM cross-compilation

- [x] **Implement ARM NEON Backend**
  - 128-bit SIMD, process 16 bytes per iteration
  - Adapt algorithms for narrower vectors
  - Test on ARM64 hardware (Raspberry Pi, Apple Silicon, AWS Graviton)

- [ ] **Optional: ARM SVE Support**
  - Scalable vector length (128-2048 bits)
  - Length-agnostic algorithms
  - Only if target hardware available (e.g., Fujitsu A64FX, AWS Graviton 3)

#### Success Criteria (for multi-architecture):
- Single binary works on x86-64 (AVX2, AVX-512) and ARM64 (NEON)
- <5% performance overhead from abstraction layer
- Graceful degradation to scalar code if no SIMD available

---

## 4. Comprehensive Test Suite

### 4.1 Test Infrastructure

**Goal**: Achieve >95% code coverage with automated testing

#### Tasks:
- [x] **Test Framework Selection**
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

- [x] **CI/CD Integration**
  - GitHub Actions workflows for:
    - Build on Ubuntu, macOS, Windows
    - Test on x86 and ARM (via QEMU or native runners)
    - Code coverage reporting (codecov.io)
    - Sanitizer builds (ASan, UBSan, MSan)
  - Pre-commit hooks for local testing

- [x] **Code Coverage**
  - Integrate gcov/lcov for coverage reports
  - Target: >95% line coverage, >90% branch coverage
  - Identify and test uncovered paths

### 4.2 Test Cases by Category

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

### 4.3 Correctness Validation

#### Additional Task for vroom Integration:
- [ ] **vroom Compatibility Testing**
  - Test index format matches vroom's expectations
  - Verify integration with Altrep vectors
  - Compare results with vroom's current parser
  - Test on vroom's existing test suite

### 4.4 Reference Implementation Comparison

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

## 5. Malformed CSV Handling

### 5.1 Error Detection Strategy

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
- [x] **Error Code Enumeration** (`error_codes.h`)
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

- [x] **Error Context Reporting**
  - Track line number and column position of errors
  - Provide snippet of problematic data
  - Example: `Error at line 1024, col 7: unclosed quote`

- [ ] **Validation Pass**
  - Optional pre-parsing validation phase
  - Quick scan for structural errors before full parse
  - Trade-off: 2x pass overhead vs early error detection

### 5.2 Recovery Strategies

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

### 5.3 Fuzz Testing for Edge Cases

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

### 5.4 Test Files for Malformed CSV

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

## 6. Enhanced Benchmark Suite

### 6.1 Benchmark Infrastructure Improvements

**Goal**: Comprehensive, reproducible performance measurement

#### Tasks:
- [x] **Google Benchmark Integration**
  - Replace custom timing code with Google Benchmark library
  - Benefits: statistical analysis, comparison mode, JSON output
  - Keep existing `TimingAccumulator` for detailed perf counters

- [ ] **Benchmark Dimensions**
  - File sizes: 1KB, 10KB, 100KB, 1MB, 10MB, 100MB, 1GB, 10GB
  - Column counts: 2, 5, 10, 50, 100, 500 columns
  - Data types: integers, floats, strings, mixed
  - Thread counts: 1, 2, 4, 8, 16 threads
  - SIMD levels: scalar, SSE2, AVX2, AVX-512, NEON

- [x] **Comparison Benchmarks**
  - **Primary**: vroom (current R parser) - this is the key comparison!
  - **R ecosystem**: data.table::fread(), readr::read_csv()
  - **Other languages**:
    - Python: pandas read_csv(), Apache Arrow CSV reader
    - C++: csv-parser (Vince Bartle), fast-cpp-csv-parser
    - DuckDB CSV reader
    - zsv (SIMD CSV parser)
  - Metric: GB/s throughput, indexing time

- [ ] **Real-World Datasets**
  - NYC Taxi Trip Data (1.5GB, 19 columns)
  - Common Crawl data samples
  - Financial tick data (high-frequency, many rows)
  - Genomics data (wide tables, many columns)
  - Log file formats (syslog CSV exports)

### 6.2 Performance Metrics

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

### 6.3 Benchmark Reporting

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

## 7. API Design & Usability

### 7.1 Public API Definition

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

## 7. API Design & Usability (DUPLICATE - TO BE REMOVED)

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
- [x] **CMake Migration**
  - Replace Makefile with CMakeLists.txt
  - Support: Linux, macOS, Windows (MSVC, MinGW)
  - Features:
    - Library target (static, shared)
    - Executable target (CLI tool)
    - Test target
    - Benchmark target
    - Install target

- [x] **Compiler Support**
  - GCC 9+ (current: ✓)
  - Clang 10+
  - MSVC 2019+ (for Windows)
  - Intel ICC (optional)

- [x] **Dependency Management**
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

## 12. Revised Milestones & Prioritization

**PRIMARY GOAL**: vroom integration with 2-3x faster indexing than current implementation

**STRATEGY**: Literature review → AVX2 optimization → vroom integration → ARM support

### Phase 1: Research & Foundation (Months 1-2)
**Priority: CRITICAL - Do this FIRST**

1. **Literature Review** (Section 2)
   - Review 8-10 key papers (Chang, Langdale/Lemire, CleverCSV, etc.)
   - Evaluate Highway vs SIMDe for SIMD abstraction
   - Document techniques in `docs/literature_review.md`
   - Identify optimization priorities

2. **vroom Architecture Study** (Section 1.1)
   - Deep dive into vroom's current parser implementation
   - Understand index format and Altrep integration
   - Analyze vroom issue #105 (dialect detection)
   - Identify integration points

3. **Test Infrastructure** (Section 4.1)
   - Google Test setup
   - CI/CD pipeline (GitHub Actions)
   - Test data generation scripts
   - Code coverage infrastructure

4. **CMake Build System** (Section 7)
   - Replace Makefile with CMake
   - Support Linux, macOS (initial platforms)
   - Dependency management (Highway/SIMDe as git submodule)

**Deliverable**: Research foundation documented, build system modern, tests running

---

### Phase 2: AVX2 Optimization & Correctness (Months 3-4)
**Priority: HIGH - Perfect x86-64 before other architectures**

1. **Optimize AVX2 Implementation** (Section 3.1)
   - Profile current code (perf, VTune)
   - Apply literature review findings
   - Branch reduction, prefetching, loop unrolling
   - Target: >5 GB/s indexing throughput on modern x86-64

2. **Comprehensive Testing** (Section 4)
   - 100+ unit tests (SIMD operations, parsing, indexing)
   - 50+ integration tests (well-formed and malformed CSV)
   - Property-based testing with RapidCheck
   - >90% code coverage

3. **Error Handling** (Section 5)
   - Implement all error types (unclosed quotes, inconsistent columns, etc.)
   - Strict and lenient modes
   - Clear error messages with line/column positions

4. **Sanitizer Builds**
   - ASan, UBSan, MSan, TSan clean
   - Fuzz testing setup (AFL++)
   - 1M+ executions without crash

**Deliverable**: Production-quality AVX2 implementation, thoroughly tested

---

### Phase 3: vroom Integration (Months 5-6)
**Priority: HIGH - Primary integration target**

1. **cpp11 Bindings** (Section 1.2)
   - Implement C++ → R interface using cpp11
   - Memory management between C++ and R
   - Zero-copy optimization where possible

2. **vroom-Compatible API** (Section 1.1)
   - Index format matching vroom's expectations
   - Altrep integration callbacks
   - Thread coordination with vroom

3. **vroom-Specific Features** (Section 1.3)
   - CleverCSV-style dialect detection (addresses vroom #105)
   - Line ending auto-detection (\n, \r\n, \r)
   - Column type hints for vroom
   - Progress reporting integration

4. **Benchmark vs vroom** (Section 6.1)
   - Test on vroom's benchmark datasets
   - Compare: simdcsv vs current vroom vs data.table::fread()
   - Target: 2-3x faster indexing than vroom
   - Validate: memory usage, thread scaling

**Deliverable**: simdcsv integrated with vroom, measurably faster

---

### Phase 4: AVX-512 & Enhanced Performance (Months 7-8)
**Priority: MEDIUM - Optimize further on x86-64**

1. **AVX-512 Implementation** (Section 3.2)
   - Port AVX2 code to AVX-512
   - Runtime dispatch based on CPU support
   - Beware downclocking, benchmark carefully

2. **Performance Optimization** (Section 8)
   - Profiling-guided optimization
   - Micro-benchmarks for hotspots
   - Cache-conscious data structures
   - NUMA awareness (multi-socket systems)

3. **Enhanced Benchmarks** (Section 6)
   - Google Benchmark integration
   - Real-world datasets (NYC Taxi, genomics, financial)
   - Energy efficiency metrics (RAPL)
   - Public benchmark dashboard

**Deliverable**: Peak x86-64 performance, comprehensive benchmarks

---

### Phase 5: ARM Support via SIMD Library (Months 9-10)
**Priority: MEDIUM - Multi-architecture support**

1. **SIMD Library Integration** (Section 2.2 & 3.3)
   - Choose Highway or SIMDe based on Phase 1 evaluation
   - Port AVX2/AVX-512 code to library abstractions
   - Maintain <5% performance overhead

2. **ARM NEON Implementation**
   - 128-bit SIMD for ARM64
   - Test on: Apple Silicon, AWS Graviton, Raspberry Pi
   - Adapt algorithms for narrower vectors

3. **Multi-Architecture CI**
   - GitHub Actions with ARM runners (or QEMU)
   - Test on x86-64 (AVX2, AVX-512) and ARM64 (NEON)
   - Cross-compilation support

**Deliverable**: Single binary supporting x86-64 and ARM64

---

### Phase 6: Ecosystem & Distribution (Months 11-12)
**Priority: LOW - Nice-to-have**

1. **R Package** (optional standalone)
   - Create simdcsv R package
   - CRAN submission preparation
   - Documentation and vignettes

2. **Packaging & Distribution**
   - Debian/Ubuntu .deb
   - Homebrew formula (macOS)
   - vcpkg port (cross-platform)

3. **Documentation**
   - API docs (Doxygen)
   - User guide and tutorials
   - Developer guide
   - Example programs

4. **Community Setup**
   - Contributing guidelines
   - Issue templates
   - Code of conduct

**Deliverable**: Widely distributable, well-documented library

---

### Optional Future Phases

**Phase 7: Advanced Features** (as needed)
- Apache Arrow output format
- Streaming parser (don't load full file)
- Advanced CSV features (custom delimiters, column filtering)
- ARM SVE support (scalable vectors)
- Windows native support (MSVC)

**Phase 8: Additional Integrations**
- Python bindings (pybind11) for pandas
- Julia bindings (CxxWrap.jl)
- Rust bindings (cxx crate)
- DuckDB/Polars integration

---

## 13. Success Metrics

### Performance Targets (vroom Integration)
- [ ] **Primary**: 2-3x faster indexing than current vroom implementation
- [ ] **Throughput**: >5 GB/s on modern x86-64 hardware (AVX2)
- [ ] **AVX-512**: >8 GB/s on systems with AVX-512 support
- [ ] **R Ecosystem**: Faster than data.table::fread() for indexing phase
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
3. van den Burgh & Nazabal, "Wrangling Messy CSV Files by Detecting Row and Type Patterns", arXiv:1811.11242, 2019 (CleverCSV)
4. RFC 4180 - Common Format and MIME Type for CSV Files

### Related Projects (Integration Targets)
- **vroom**: https://github.com/tidyverse/vroom - Primary integration target
- **cpp11**: https://github.com/r-lib/cpp11 - R/C++ interface (by Jim Hester)
- vroom issue #105 (dialect detection): https://github.com/tidyverse/vroom/issues/105

### SIMD Libraries
- **Google Highway**: https://github.com/google/highway - Performance-portable SIMD
  - NumPy NEP 54: https://numpy.org/neps/nep-0054-simd-cpp-highway.html
- **SIMDe**: https://github.com/simd-everywhere/simde - SIMD Everywhere (header-only)

### Other CSV Parsers (for benchmarking)
- simdjson: https://github.com/simdjson/simdjson
- Apache Arrow CSV: https://arrow.apache.org/docs/cpp/csv.html
- csv-parser: https://github.com/vincentlaucsb/csv-parser
- fast-cpp-csv-parser: https://github.com/ben-strasser/fast-cpp-csv-parser
- data.table (R): https://github.com/Rdatatable/data.table

### Tools
- Google Test: https://github.com/google/googletest
- Google Benchmark: https://github.com/google/benchmark
- AFL++: https://github.com/AFLplusplus/AFLplusplus
- RapidCheck: https://github.com/emil-e/rapidcheck (property-based testing)

---

## Conclusion

This plan transforms simdcsv from a research prototype into a production-ready, high-performance CSV parser designed specifically for **vroom integration**. The revised approach prioritizes:

1. **Research first**: Literature review and SIMD library evaluation before coding
2. **Depth over breadth**: Perfect AVX2 on x86-64 before expanding to ARM
3. **Primary goal**: 2-3x faster indexing for vroom via simdcsv backend
4. **Modern tooling**: cpp11 for R bindings, Highway/SIMDe for portability

**Key Advantages for vroom**:
- simdcsv's index-based output perfectly matches vroom's Altrep architecture
- SIMD acceleration dramatically speeds up the indexing phase
- Multi-threaded design scales well for large files
- CleverCSV dialect detection addresses vroom issue #105

With an estimated **10-12 month timeline**, the result will be a production-ready parser that significantly accelerates vroom's CSV reading capabilities while maintaining correctness and robustness.

**Next Steps**:
1. **Phase 1 (Months 1-2)**: Literature review, vroom architecture study, test infrastructure
2. Evaluate Highway vs SIMDe for SIMD abstraction
3. Profile and optimize AVX2 implementation
4. Begin vroom integration work

---

## Current Progress (Updated: 2026-01-03)

### ✅ Completed Milestones

**Literature Review & Research Foundation (Phase 1)** - COMPLETED
- ✅ Reviewed Google Highway vs SIMDe for SIMD abstraction
- ✅ Evaluated Highway's performance-portable approach
- ✅ Decision made: Use Highway for long-term maintainability
- ✅ Integration completed with Highway 1.3.0

**Phase 5: ARM Support via SIMD Library** - COMPLETED
- ✅ Google Highway 1.3.0 integrated for portable SIMD
- ✅ ARM NEON support (128-bit SIMD) working on macOS ARM
- ✅ x86 SSE/AVX2 support via Highway
- ✅ Fixed critical unsigned integer underflow bug in `get_quotation_state()`
- ✅ All 295 tests passing on both x86-64 and ARM64

**Test Infrastructure** - COMPLETED
- ✅ Google Test framework integrated
- ✅ CI/CD pipeline with GitHub Actions (Ubuntu, macOS, x86, ARM)
- ✅ Code coverage reporting with Codecov
- ✅ 295 comprehensive tests (unit, integration, error handling, CSV parsing, dialect detection)
- ✅ Coverage tracking excluding test files and benchmarks

**Error Handling Framework** - COMPLETED
- ✅ Error codes enumeration (16 error types)
- ✅ Error severity levels (WARNING, ERROR, FATAL)
- ✅ Error context with file, line, column tracking
- ✅ Comprehensive error handling tests (100% coverage on error.cpp)
- ✅ Malformed CSV test files (16+ test cases)

**CMake Build System** - COMPLETED
- ✅ Modern CMake build system
- ✅ Dependency management via FetchContent (Highway, GoogleTest, Google Benchmark)
- ✅ Multi-platform support (Linux, macOS)
- ✅ Sanitizer support configuration

**CSV Dialect Auto-Detection** - COMPLETED (PR #23)
- ✅ CleverCSV-inspired detection algorithm (consistency-based scoring)
- ✅ Pattern score (row length consistency) × Type score (cell type inference)
- ✅ Delimiter detection: comma, semicolon, tab, pipe, colon
- ✅ Quote character detection: double-quote, single-quote
- ✅ Line ending detection: LF, CRLF, CR, MIXED
- ✅ Header detection heuristics
- ✅ Escape sequence detection: backslash (`\"`) and double-quote (`""`) escaping
- ✅ 41 dialect detection tests

**Benchmark Suite** - COMPLETED (PR #13)
- ✅ Google Benchmark integration
- ✅ External parser comparison: DuckDB, zsv, Arrow
- ✅ Performance profiling infrastructure

**Documentation Site** - COMPLETED (PR #20)
- ✅ Quarto documentation site with doxybook2 integration
- ✅ API reference generation from Doxygen

**CLI Utility** - COMPLETED
- ✅ scsv command-line utility for CSV parsing
- ✅ Row count command with SIMD optimization (PR #16)

### 🚧 In Progress

**Dialect-Aware Parser Integration** (PR #24)
- Parser now supports configurable delimiters and quote characters
- Integration with dialect detection for auto-parsing

### 📋 Remaining Steps

1. **Performance Optimization (Phase 2 & 4)**
   - Profile Highway SIMD implementation
   - Optimize for >5 GB/s on x86-64
   - Add AVX-512 support

2. **vroom Integration (Phase 3)**
   - cpp11 bindings for R
   - Integration with vroom's Altrep architecture
   - Column type inference collaboration

3. **Advanced Features**
   - Streaming parser
   - Apache Arrow output
   - Additional language bindings
