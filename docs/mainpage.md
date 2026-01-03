# simdcsv API Reference {#mainpage}

High-performance CSV parser using SIMD instructions.

## Quick Start

```cpp
#include "two_pass.h"
#include "io_util.h"
#include "mem_util.h"

// Load CSV file with SIMD-aligned padding
auto corpus = get_corpus("data.csv", 64);

// Create parser and parse
simdcsv::two_pass parser;
auto idx = parser.init(corpus.size(), 1);  // single-threaded
parser.parse(corpus.data(), idx, corpus.size());

// Field positions are now in idx.indexes
// Don't forget to free the buffer
aligned_free((void*)corpus.data());
```

---

## Core API

### Primary Classes

| Class | Description |
|-------|-------------|
| @ref simdcsv::two_pass | The main CSV parser class. Use this to parse CSV data. |
| @ref simdcsv::index | Result structure containing parsed field positions. |
| @ref simdcsv::ErrorCollector | Collects and manages parse errors. |

### Key Methods

| Method | Description |
|--------|-------------|
| @ref simdcsv::two_pass::init "two_pass::init()" | Initialize an index for parsing. Call this first. |
| @ref simdcsv::two_pass::parse "two_pass::parse()" | Parse CSV data (fast, throws on error). |
| @ref simdcsv::two_pass::parse_with_errors "two_pass::parse_with_errors()" | Parse with detailed error collection. |

### File I/O

| Function | Description |
|----------|-------------|
| @ref get_corpus() | Load a file into a SIMD-aligned buffer. |
| @ref allocate_padded_buffer() | Allocate a padded buffer for SIMD operations. |

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
#include "two_pass.h"
#include "error.h"

simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
simdcsv::two_pass parser;
auto idx = parser.init(len, 1);

bool success = parser.parse_with_errors(data, idx, len, errors);

if (errors.has_errors()) {
    for (const auto& err : errors.errors()) {
        std::cerr << err.to_string() << std::endl;
    }
}
```

---

## Header Files

| Header | Purpose |
|--------|---------|
| @ref two_pass.h | Core parser (two_pass, index classes) |
| @ref error.h | Error handling (ErrorCollector, ErrorCode, ParseError) |
| @ref io_util.h | File loading with SIMD alignment |
| @ref mem_util.h | Aligned memory allocation |

---

## See Also

- [Getting Started Guide](../getting-started.html) - Tutorial for new users
- [Architecture](../architecture.html) - How the parser works
- [Error Handling Guide](../error-handling.html) - Detailed error handling docs
- [Benchmarks](../benchmarks.html) - Performance data
