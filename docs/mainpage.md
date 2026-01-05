# simdcsv API Reference {#mainpage}

High-performance CSV parser using SIMD instructions.

## Quick Start

The simplest way to use simdcsv is with the high-level `Parser` and `FileBuffer` classes:

```cpp
#include "simdcsv.h"

// Load CSV file (RAII - automatic cleanup)
simdcsv::FileBuffer buffer = simdcsv::load_file("data.csv");

// Create parser with 4 threads
simdcsv::Parser parser(4);

// Parse with auto-detection of dialect
simdcsv::ErrorCollector errors;
auto result = parser.parse_auto(buffer.data(), buffer.size(), errors);

if (result.success()) {
    std::cout << "Detected: " << result.dialect.to_string() << "\n";
    std::cout << "Columns: " << result.num_columns() << "\n";
}
// Memory automatically freed when buffer goes out of scope
```

### Low-Level API

For maximum performance and control, use the `two_pass` class directly:

```cpp
#include "two_pass.h"
#include "io_util.h"
#include "mem_util.h"

// Load CSV file with SIMD-aligned padding
auto corpus = get_corpus("data.csv", 64);

// Create parser and parse
simdcsv::two_pass parser;
auto idx = parser.init(corpus.size(), 4);  // 4 threads
parser.parse(corpus.data(), idx, corpus.size());

// Field positions are now in idx.indexes
// Don't forget to free the buffer
aligned_free((void*)corpus.data());
```

---

## Core API

### High-Level Classes (Recommended)

| Class | Description |
|-------|-------------|
| @ref simdcsv::Parser | Simplified parser with automatic index management. |
| @ref simdcsv::FileBuffer | RAII wrapper for file buffers with automatic cleanup. |

### Low-Level Classes

| Class | Description |
|-------|-------------|
| @ref simdcsv::two_pass | Core CSV parser with full control over parsing. |
| @ref simdcsv::index | Result structure containing parsed field positions. |
| @ref simdcsv::ErrorCollector | Collects and manages parse errors. |

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
| @ref simdcsv::Parser::parse "Parser::parse()" | Parse with specified dialect. |
| @ref simdcsv::Parser::parse_auto "Parser::parse_auto()" | Parse with auto-detected dialect. |
| @ref simdcsv::Parser::parse_with_errors "Parser::parse_with_errors()" | Parse with error collection. |
| @ref simdcsv::two_pass::init "two_pass::init()" | Initialize an index for parsing. |
| @ref simdcsv::two_pass::parse "two_pass::parse()" | Parse CSV data (fast, throws on error). |
| @ref simdcsv::two_pass::parse_with_errors "two_pass::parse_with_errors()" | Parse with detailed error collection. |

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
// Detect dialect from file
auto detected = simdcsv::detect_dialect_file("mystery.csv");
if (detected.success()) {
    std::cout << "Delimiter: '" << detected.dialect.delimiter << "'\n";
    std::cout << "Confidence: " << detected.confidence << "\n";
}

// Parse with auto-detection
simdcsv::Parser parser(4);
simdcsv::ErrorCollector errors;
auto result = parser.parse_auto(data, len, errors);
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

auto result = parser.parse_with_errors(buffer.data(), buffer.size(), errors);

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
| @ref simdcsv.h | Main public header - includes everything you need |
| @ref two_pass.h | Core parser (two_pass, index classes) |
| @ref error.h | Error handling (ErrorCollector, ErrorCode, ParseError) |
| @ref dialect.h | Dialect configuration and detection |
| @ref io_util.h | File loading with SIMD alignment |
| @ref mem_util.h | Aligned memory allocation |

---

## See Also

- [Error Handling Guide](error_handling.md) - Detailed error handling documentation
