# Error Handling in simdcsv

This document describes the error handling strategy for simdcsv, including error types, reporting mechanisms, and usage patterns.

## Overview

simdcsv provides comprehensive error detection and reporting for malformed CSV files. The error handling system is designed to:

1. **Detect common CSV errors** - Quote escaping, field count mismatches, invalid characters
2. **Provide precise location information** - Line, column, and byte offset
3. **Support multiple error modes** - Strict, permissive, and best-effort parsing
4. **Enable both exception and non-exception workflows** - Choose based on your needs
5. **Collect multiple errors** - See all problems in one pass (permissive mode)

## Error Types

All error types are defined in `include/error.h` as the `ErrorCode` enum:

### Quote-Related Errors

- **`UNCLOSED_QUOTE`** (FATAL)
  - Quoted field not closed before EOF or newline
  - Example: `"unclosed quote,field2`

- **`INVALID_QUOTE_ESCAPE`** (ERROR)
  - Invalid quote escape sequence (quotes must be doubled: `""`)
  - Example: `"bad"escape"` (should be `"bad""escape"`)

- **`QUOTE_IN_UNQUOTED_FIELD`** (ERROR)
  - Quote appears in the middle of an unquoted field
  - Example: `field"with"quotes` (should be quoted or escaped)

### Field Structure Errors

- **`INCONSISTENT_FIELD_COUNT`** (ERROR)
  - Row has different number of fields than header
  - Example: Header has 3 fields, data row has 2

- **`FIELD_TOO_LARGE`** (ERROR)
  - Field exceeds maximum size limit
  - Configurable limit (default: 32KB per field)

### Line Ending Errors

- **`MIXED_LINE_ENDINGS`** (WARNING)
  - File uses inconsistent line endings (CRLF, LF, CR)
  - Example: Some lines use `\r\n`, others use `\n`

- **`INVALID_LINE_ENDING`** (ERROR)
  - Invalid line ending sequence

### Character Encoding Errors

- **`INVALID_UTF8`** (ERROR)
  - Invalid UTF-8 byte sequence
  - simdcsv expects valid UTF-8 encoding

- **`NULL_BYTE`** (ERROR)
  - Unexpected null byte (`\0`) in data
  - CSV files should not contain null bytes

### Structure Errors

- **`EMPTY_HEADER`** (ERROR)
  - Header row is empty or missing

- **`DUPLICATE_COLUMN_NAMES`** (WARNING)
  - Header contains duplicate column names
  - Example: `A,B,A,C`

### Other Errors

- **`AMBIGUOUS_SEPARATOR`** (ERROR)
  - Cannot reliably determine field separator

- **`FILE_TOO_LARGE`** (FATAL)
  - File exceeds maximum size limit

- **`IO_ERROR`** (FATAL)
  - File I/O error (read failed, file not found, etc.)

- **`INTERNAL_ERROR`** (FATAL)
  - Internal parser error (bug in simdcsv)

## Error Severity Levels

Errors are classified by severity:

- **`WARNING`**: Non-fatal issues that don't prevent parsing
  - Example: `MIXED_LINE_ENDINGS`, `DUPLICATE_COLUMN_NAMES`
  - Parser can continue normally

- **`ERROR`**: Recoverable errors
  - Example: `INCONSISTENT_FIELD_COUNT`, `QUOTE_IN_UNQUOTED_FIELD`
  - Parser can skip problematic row and continue (in permissive mode)

- **`FATAL`**: Unrecoverable errors
  - Example: `UNCLOSED_QUOTE` at EOF, `IO_ERROR`
  - Parser must stop

## Error Reporting

### ParseError Structure

Each error is represented by a `ParseError` struct:

```cpp
struct ParseError {
    ErrorCode code;          // Error type
    ErrorSeverity severity;  // WARNING, ERROR, or FATAL
    size_t line;            // Line number (1-indexed)
    size_t column;          // Column number (1-indexed)
    size_t byte_offset;     // Byte offset in file
    std::string message;    // Human-readable message
    std::string context;    // Snippet of problematic data
};
```

Example error:

```cpp
ParseError {
    code: UNCLOSED_QUOTE,
    severity: FATAL,
    line: 5,
    column: 10,
    byte_offset: 123,
    message: "Quote not closed before end of line",
    context: "\"unclosed"
}
```

### Error String Representation

Errors can be formatted as strings:

```cpp
ParseError error(...);
std::cout << error.to_string() << std::endl;

// Output:
// [FATAL] UNCLOSED_QUOTE at line 5, column 10 (byte 123): Quote not closed before end of line
//   Context: "unclosed
```

## Error Handling Modes

simdcsv supports three error handling modes via the `ErrorMode` enum:

### STRICT Mode (Default)

```cpp
ErrorCollector collector(ErrorMode::STRICT);
```

- **Stop on first error** (except warnings)
- Best for: Production data where any error is unacceptable
- Use when: CSV must be perfectly formatted

### PERMISSIVE Mode

```cpp
ErrorCollector collector(ErrorMode::PERMISSIVE);
```

- **Collect all errors**, stop only on FATAL
- Best for: Data validation, debugging malformed CSVs
- Use when: You want to see all problems in one pass

### BEST_EFFORT Mode

```cpp
ErrorCollector collector(ErrorMode::BEST_EFFORT);
```

- **Try to parse regardless of errors**
- Best for: Importing messy real-world data
- Use when: You need to extract whatever data you can

## Using the ErrorCollector

### Basic Usage

```cpp
#include "error.h"

using namespace simdcsv;

// Create error collector
ErrorCollector errors(ErrorMode::PERMISSIVE);

// Add errors during parsing
errors.add_error(
    ErrorCode::INCONSISTENT_FIELD_COUNT,
    ErrorSeverity::ERROR,
    5,      // line
    1,      // column
    120,    // byte offset
    "Expected 3 fields but found 2",
    "1,2"   // context
);

// Check if parsing should stop
if (errors.should_stop()) {
    std::cerr << "Fatal error encountered" << std::endl;
    return;
}

// After parsing, check for errors
if (errors.has_errors()) {
    std::cout << errors.summary() << std::endl;

    // Iterate through individual errors
    for (const auto& error : errors.errors()) {
        std::cout << error.to_string() << std::endl;
    }
}
```

### Error Summary

```cpp
ErrorCollector errors;
// ... add errors ...

std::cout << errors.summary() << std::endl;

// Output:
// Total errors: 3 (Warnings: 1, Errors: 2)
//
// Details:
// [WARNING] MIXED_LINE_ENDINGS at line 1, column 1 (byte 10): ...
// [ERROR] INCONSISTENT_FIELD_COUNT at line 3, column 1 (byte 45): ...
// [ERROR] QUOTE_IN_UNQUOTED_FIELD at line 7, column 5 (byte 123): ...
```

### Clearing Errors

```cpp
errors.clear();  // Reset error collector
```

## Exception-Based Error Handling

For traditional exception-based workflows:

```cpp
#include "error.h"

using namespace simdcsv;

try {
    // Parse CSV
    auto result = parse_csv(data);
} catch (const ParseException& e) {
    // Single error
    std::cerr << "Parse error: " << e.what() << std::endl;

    // Access detailed error info
    const ParseError& error = e.error();
    std::cerr << "At line " << error.line
              << ", column " << error.column << std::endl;

    // Multiple errors (if thrown with error vector)
    for (const auto& err : e.errors()) {
        std::cerr << err.to_string() << std::endl;
    }
}
```

### Creating ParseException

```cpp
// Single error
ParseError error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL, ...);
throw ParseException(error);

// Multiple errors
std::vector<ParseError> errors;
// ... collect errors ...
throw ParseException(errors);
```

## Integration with Parser

The error handling system is designed to integrate seamlessly with the CSV parser:

```cpp
class CSVParser {
private:
    ErrorCollector errors_;

public:
    CSVParser(ErrorMode mode = ErrorMode::STRICT)
        : errors_(mode) {}

    ParseResult parse(const std::string& data) {
        // During parsing, add errors
        if (/* unclosed quote detected */) {
            errors_.add_error(
                ErrorCode::UNCLOSED_QUOTE,
                ErrorSeverity::FATAL,
                current_line,
                current_column,
                current_offset,
                "Quote not closed",
                get_context()
            );

            // Check if we should stop
            if (errors_.should_stop()) {
                return handle_error();
            }
        }

        // ... continue parsing ...
    }

    const ErrorCollector& errors() const { return errors_; }
};
```

## Test Files

The test suite includes 16 malformed CSV test files in `test/data/malformed/`:

1. **unclosed_quote.csv** - Quote not closed before newline
2. **unclosed_quote_eof.csv** - Quote not closed at end of file
3. **quote_in_unquoted_field.csv** - Quote in middle of unquoted field
4. **inconsistent_columns.csv** - Rows with varying field counts
5. **inconsistent_columns_all_rows.csv** - Every row has different count
6. **invalid_quote_escape.csv** - Malformed quote escape sequence
7. **empty_header.csv** - Missing or empty header row
8. **duplicate_column_names.csv** - Duplicate column names in header
9. **trailing_quote.csv** - Quote after unquoted field data
10. **quote_not_at_start.csv** - Quote appears mid-field
11. **multiple_errors.csv** - Multiple error types in one file
12. **mixed_line_endings.csv** - Inconsistent line ending styles
13. **null_byte.csv** - Contains null byte character
14. **triple_quote.csv** - Triple quote sequence (ambiguous)
15. **unescaped_quote_in_quoted.csv** - Unescaped quote inside quoted field
16. **quote_after_data.csv** - Quote appears after field data

All test files are validated by `test/error_handling_test.cpp`.

## Best Practices

### For Production Use

1. **Use STRICT mode** for production parsing where data integrity is critical
2. **Log errors** with full context for debugging
3. **Provide clear error messages** to users
4. **Validate data sources** to catch errors early

```cpp
ErrorCollector errors(ErrorMode::STRICT);
auto result = parse_csv(data, errors);

if (errors.has_errors()) {
    logger.error("CSV parsing failed: " + errors.summary());
    return Error::INVALID_CSV;
}
```

### For Data Import/Validation

1. **Use PERMISSIVE mode** to collect all errors
2. **Report all issues** to help users fix data
3. **Provide line numbers** for easy correction

```cpp
ErrorCollector errors(ErrorMode::PERMISSIVE);
auto result = parse_csv(data, errors);

if (errors.has_errors()) {
    std::cout << "Found " << errors.error_count() << " issues:\n";
    std::cout << errors.summary() << std::endl;
}
```

### For Messy Real-World Data

1. **Use BEST_EFFORT mode** to extract what you can
2. **Log warnings** for data quality monitoring
3. **Validate extracted data** after parsing

```cpp
ErrorCollector errors(ErrorMode::BEST_EFFORT);
auto result = parse_csv(messy_data, errors);

// Got some data, but maybe with issues
std::cout << "Parsed " << result.row_count() << " rows\n";
if (errors.has_errors()) {
    std::cout << "With " << errors.error_count() << " warnings\n";
}
```

## Future Enhancements

Potential future additions to error handling:

1. **Error recovery suggestions** - "Did you mean to escape this quote?"
2. **Custom error handlers** - User-defined callbacks for specific error types
3. **Error filtering** - Ignore specific error types
4. **Detailed statistics** - Count of each error type
5. **Error grouping** - Group similar errors together
6. **Auto-correction hints** - Suggest fixes for common issues
7. **Performance metrics** - Track parsing speed despite errors

## See Also

- `include/error.h` - Error handling API
- `src/error.cpp` - Error handling implementation
- `test/error_handling_test.cpp` - Error handling tests
- `test/data/malformed/` - Malformed CSV test files
- `docs/literature_review.md` - CSV parsing research
