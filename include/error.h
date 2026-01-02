#ifndef SIMDCSV_ERROR_H
#define SIMDCSV_ERROR_H

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>
#include <stdexcept>

namespace simdcsv {

// CSV Error Types
// Note: Some error codes are reserved for future implementation (marked [RESERVED])
enum class ErrorCode {
    NONE = 0,

    // Quote-related errors (all implemented)
    UNCLOSED_QUOTE,              // Quoted field not closed before EOF
    INVALID_QUOTE_ESCAPE,        // Invalid quote escape sequence
    QUOTE_IN_UNQUOTED_FIELD,     // Quote appears in middle of unquoted field

    // Field structure errors
    INCONSISTENT_FIELD_COUNT,    // Row has different number of fields than header
    FIELD_TOO_LARGE,             // [RESERVED] Field exceeds maximum size limit

    // Line ending errors
    MIXED_LINE_ENDINGS,          // File uses inconsistent line endings (warning)
    INVALID_LINE_ENDING,         // [RESERVED] Invalid line ending sequence

    // Character encoding errors
    INVALID_UTF8,                // [RESERVED] Invalid UTF-8 sequence
    NULL_BYTE,                   // Unexpected null byte in data

    // Structure errors (all implemented)
    EMPTY_HEADER,                // Header row is empty
    DUPLICATE_COLUMN_NAMES,      // Header contains duplicate column names

    // Separator errors
    AMBIGUOUS_SEPARATOR,         // [RESERVED] Cannot determine separator reliably

    // General errors
    FILE_TOO_LARGE,              // [RESERVED] File exceeds maximum size
    IO_ERROR,                    // [RESERVED] File I/O error
    INTERNAL_ERROR               // Internal parser error
};

// Error severity levels
enum class ErrorSeverity {
    WARNING,    // Non-fatal, parser can continue (e.g., mixed line endings)
    ERROR,      // Recoverable error (e.g., inconsistent field count - can skip row)
    FATAL       // Unrecoverable error (e.g., unclosed quote at EOF)
};

// Detailed error information
struct ParseError {
    ErrorCode code;
    ErrorSeverity severity;

    // Location information
    size_t line;          // Line number (1-indexed)
    size_t column;        // Column number (1-indexed)
    size_t byte_offset;   // Byte offset in file

    // Context
    std::string message;  // Human-readable error message
    std::string context;  // Snippet of problematic data

    ParseError(ErrorCode c, ErrorSeverity s, size_t l, size_t col,
               size_t offset, const std::string& msg, const std::string& ctx = "")
        : code(c), severity(s), line(l), column(col),
          byte_offset(offset), message(msg), context(ctx) {}

    // Convert error to string
    std::string to_string() const;
};

// Error handling modes
enum class ErrorMode {
    STRICT,      // Stop on first error
    PERMISSIVE,  // Try to recover from errors, report all
    BEST_EFFORT  // Ignore errors, parse what we can
};

// Error collector - accumulates errors during parsing
// SECURITY: Maximum error limit prevents OOM from malicious inputs with many errors
class ErrorCollector {
public:
    static constexpr size_t DEFAULT_MAX_ERRORS = 10000;

    explicit ErrorCollector(ErrorMode mode = ErrorMode::STRICT,
                           size_t max_errors = DEFAULT_MAX_ERRORS)
        : mode_(mode), max_errors_(max_errors), has_fatal_(false) {}

    // Add an error (respects max_errors_ limit to prevent OOM)
    void add_error(const ParseError& error) {
        if (errors_.size() >= max_errors_) return;
        errors_.push_back(error);
        if (error.severity == ErrorSeverity::FATAL) {
            has_fatal_ = true;
        }
    }

    // Check if error limit has been reached
    bool at_error_limit() const { return errors_.size() >= max_errors_; }

    // Convenience methods
    void add_error(ErrorCode code, ErrorSeverity severity, size_t line,
                   size_t column, size_t offset, const std::string& message,
                   const std::string& context = "") {
        add_error(ParseError(code, severity, line, column, offset, message, context));
    }

    // Check if we should stop parsing
    bool should_stop() const {
        if (mode_ == ErrorMode::STRICT && !errors_.empty()) return true;
        if (has_fatal_) return true;
        return false;
    }

    // Query errors
    bool has_errors() const { return !errors_.empty(); }
    bool has_fatal_errors() const { return has_fatal_; }
    size_t error_count() const { return errors_.size(); }
    const std::vector<ParseError>& errors() const { return errors_; }

    // Get summary
    std::string summary() const;

    // Clear errors
    void clear() {
        errors_.clear();
        has_fatal_ = false;
    }

    ErrorMode mode() const { return mode_; }
    void set_mode(ErrorMode mode) { mode_ = mode; }

    // Merge errors from another collector (for multi-threaded parsing)
    // Errors are sorted by byte_offset to maintain logical order
    void merge_from(const ErrorCollector& other) {
        if (other.errors_.empty()) return;

        errors_.reserve(errors_.size() + other.errors_.size());
        for (const auto& err : other.errors_) {
            errors_.push_back(err);
        }
        if (other.has_fatal_) {
            has_fatal_ = true;
        }
    }

    // Sort errors by byte offset (call after merging from multiple threads)
    void sort_by_offset() {
        std::sort(errors_.begin(), errors_.end(),
            [](const ParseError& a, const ParseError& b) {
                return a.byte_offset < b.byte_offset;
            });
    }

    // Merge multiple collectors into this one, sorted by offset
    void merge_sorted(std::vector<ErrorCollector>& collectors) {
        for (auto& collector : collectors) {
            merge_from(collector);
        }
        sort_by_offset();
    }

private:
    ErrorMode mode_;
    size_t max_errors_;
    std::vector<ParseError> errors_;
    bool has_fatal_;
};

// Exception thrown for fatal parse errors (when using exceptions)
class ParseException : public std::runtime_error {
public:
    explicit ParseException(const ParseError& error)
        : std::runtime_error(error.message), errors_{error} {}

    explicit ParseException(const std::vector<ParseError>& errors)
        : std::runtime_error(format_errors(errors)), errors_(errors) {}

    const ParseError& error() const {
        if (errors_.empty()) throw std::logic_error("No errors in ParseException");
        return errors_[0];
    }

    const std::vector<ParseError>& errors() const { return errors_; }

private:
    std::vector<ParseError> errors_;

    static std::string format_errors(const std::vector<ParseError>& errors);
};

// Helper functions
const char* error_code_to_string(ErrorCode code);
const char* error_severity_to_string(ErrorSeverity severity);

} // namespace simdcsv

#endif // SIMDCSV_ERROR_H
