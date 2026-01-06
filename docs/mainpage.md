# simdcsv API Reference {#mainpage}

High-performance CSV parser using SIMD instructions.

## Quick Start

The simplest way to use simdcsv is with the `Parser` class and `FileBuffer`:

```cpp
#include "simdcsv.h"

// Load CSV file (RAII - automatic cleanup)
simdcsv::FileBuffer buffer = simdcsv::load_file("data.csv");

// Create parser with 4 threads
simdcsv::Parser parser(4);

// Parse with auto-detection (default behavior)
auto result = parser.parse(buffer.data(), buffer.size());

if (result.success()) {
    std::cout << "Detected: " << result.dialect.to_string() << "\n";
    std::cout << "Columns: " << result.num_columns() << "\n";
}
// Memory automatically freed when buffer goes out of scope
```

### Common Patterns

```cpp
// 1. Auto-detect dialect, throw on errors (default - fastest)
auto result = parser.parse(buf, len);

// 2. Auto-detect dialect, collect errors
ErrorCollector errors(ErrorMode::PERMISSIVE);
auto result = parser.parse(buf, len, {.errors = &errors});

// 3. Explicit dialect, throw on errors
auto result = parser.parse(buf, len, {.dialect = Dialect::csv()});

// 4. Explicit dialect with error collection
auto result = parser.parse(buf, len, {
    .dialect = Dialect::tsv(),
    .errors = &errors
});

// 5. Maximum performance with branchless algorithm
auto result = parser.parse(buf, len, ParseOptions::branchless());
```

---

## Core API

### Primary Classes

| Class | Description |
|-------|-------------|
| @ref simdcsv::Parser | **Main parser class** - unified API for all parsing needs. |
| @ref simdcsv::FileBuffer | RAII wrapper for file buffers with automatic cleanup. |
| @ref simdcsv::ParseOptions | Configuration for parsing (dialect, errors, algorithm). |

### Supporting Classes

| Class | Description |
|-------|-------------|
| @ref simdcsv::index | Result structure containing parsed field positions. |
| @ref simdcsv::ErrorCollector | Collects and manages parse errors. |

### Internal Classes (Deprecated for Direct Use)

| Class | Description |
|-------|-------------|
| @ref simdcsv::two_pass | Low-level parser implementation. Use `Parser` instead. |

### Dialect Detection

| Class/Function | Description |
|----------------|-------------|
| @ref simdcsv::Dialect | CSV dialect configuration (delimiter, quote char, etc.). |
| @ref simdcsv::DialectDetector | Automatic dialect detection engine. |
| @ref simdcsv::detect_dialect() | Convenience function to detect dialect from buffer. |
| @ref simdcsv::detect_dialect_file() | Convenience function to detect dialect from file. |

### Key Methods

| Method | Description |
|--------|-------------|
| @ref simdcsv::Parser::parse "Parser::parse()" | **Unified parse method** - handles all use cases via ParseOptions. |

The `Parser::parse()` method replaces multiple legacy methods:
- Auto-detects dialect by default, or use explicit `{.dialect = ...}`
- Collects errors with `{.errors = &collector}`, or throws by default
- Choose algorithm with `{.algorithm = ParseAlgorithm::BRANCHLESS}` for optimization

### File I/O

| Function | Description |
|----------|-------------|
| @ref simdcsv::load_file() | Load a file into a FileBuffer (recommended). |
| @ref get_corpus() | Load a file into a SIMD-aligned buffer (legacy). |
| @ref allocate_padded_buffer() | Allocate a padded buffer for SIMD operations. |

---

## Dialect Support

simdcsv supports multiple CSV dialects beyond standard comma-separated:

```cpp
// Standard CSV (comma, double-quote)
auto result = parser.parse(data, len, simdcsv::Dialect::csv());

// Tab-separated values
auto result = parser.parse(data, len, simdcsv::Dialect::tsv());

// Semicolon-separated (European style)
auto result = parser.parse(data, len, simdcsv::Dialect::semicolon());

// Pipe-separated
auto result = parser.parse(data, len, simdcsv::Dialect::pipe());

// Custom dialect
simdcsv::Dialect custom;
custom.delimiter = ':';
custom.quote_char = '\'';
auto result = parser.parse(data, len, custom);
```

### Automatic Detection

```cpp
// Detect dialect from file (standalone detection)
auto detected = simdcsv::detect_dialect_file("mystery.csv");
if (detected.success()) {
    std::cout << "Delimiter: '" << detected.dialect.delimiter << "'\n";
    std::cout << "Confidence: " << detected.confidence << "\n";
}

// Parse with auto-detection (default behavior)
simdcsv::Parser parser(4);
auto result = parser.parse(data, len);  // Auto-detects dialect
std::cout << "Detected: " << result.dialect.to_string() << "\n";
```

---

## Error Handling

simdcsv supports three error handling modes:

| Mode | Behavior |
|------|----------|
| @ref simdcsv::ErrorMode::STRICT "STRICT" | Stop on first error |
| @ref simdcsv::ErrorMode::PERMISSIVE "PERMISSIVE" | Collect all errors, try to recover |
| @ref simdcsv::ErrorMode::BEST_EFFORT "BEST_EFFORT" | Ignore errors, parse what's possible |

### Example with Error Handling

```cpp
#include "simdcsv.h"

simdcsv::FileBuffer buffer = simdcsv::load_file("data.csv");
simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
simdcsv::Parser parser(4);

// Use unified parse() with error collection via ParseOptions
auto result = parser.parse(buffer.data(), buffer.size(), {.errors = &errors});

if (errors.has_errors()) {
    std::cout << errors.summary() << std::endl;
    for (const auto& err : errors.errors()) {
        std::cerr << err.to_string() << std::endl;
    }
}
```

---

## Header Files

| Header | Purpose |
|--------|---------|
| @ref simdcsv.h | **Main public header** - includes Parser, FileBuffer, and everything you need |
| @ref error.h | Error handling (ErrorCollector, ErrorCode, ParseError) |
| @ref dialect.h | Dialect configuration and detection |
| @ref io_util.h | File loading with SIMD alignment |
| @ref mem_util.h | Aligned memory allocation |
| @ref two_pass.h | Internal implementation (use simdcsv.h instead) |

---

## See Also

- [Error Handling Guide](error_handling.md) - Detailed error handling documentation
