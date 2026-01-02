#ifndef SIMDCSV_CSV_PARSER_H
#define SIMDCSV_CSV_PARSER_H

#include <cstdint>
#include <string>
#include <vector>
#include <unordered_set>
#include "error.h"

namespace simdcsv {

// Parse result containing both parsed data and any errors encountered
struct ParseResult {
    bool success;
    size_t rows_parsed;
    size_t fields_parsed;
    std::vector<std::vector<std::string>> data;  // Optional: materialized data
    ErrorCollector errors;

    ParseResult() : success(true), rows_parsed(0), fields_parsed(0), errors(ErrorMode::PERMISSIVE) {}
};

// Configuration for the parser
struct ParserConfig {
    char delimiter = ',';
    char quote = '"';
    bool has_header = true;
    ErrorMode error_mode = ErrorMode::PERMISSIVE;
    size_t max_field_size = 1024 * 1024;  // 1MB default max field size

    ParserConfig() = default;
};

// Line ending types for detection
enum class LineEnding {
    UNKNOWN,
    LF,      // Unix: \n
    CRLF,    // Windows: \r\n
    CR       // Old Mac: \r
};

// CSV Parser with integrated error handling
class CSVParser {
public:
    explicit CSVParser(const ParserConfig& config = ParserConfig())
        : config_(config), errors_(config.error_mode) {}

    // Parse a buffer and populate the error collector
    ParseResult parse(const uint8_t* buf, size_t len);

    // Parse from string (convenience method)
    ParseResult parse(const std::string& content) {
        return parse(reinterpret_cast<const uint8_t*>(content.data()), content.size());
    }

    // Get the error collector (for inspection after parsing)
    const ErrorCollector& errors() const { return errors_; }

    // Reset parser state
    void reset() {
        errors_.clear();
        header_fields_.clear();
        expected_field_count_ = 0;
    }

private:
    ParserConfig config_;
    ErrorCollector errors_;
    std::vector<std::string> header_fields_;
    size_t expected_field_count_ = 0;

    // Internal parsing state
    enum class State {
        RECORD_START,
        FIELD_START,
        UNQUOTED_FIELD,
        QUOTED_FIELD,
        QUOTED_END  // After closing quote, waiting for delimiter or newline
    };

    // Parse and validate the header row
    bool parseHeader(const uint8_t* buf, size_t len, size_t& pos);

    // Check for duplicate column names
    void checkDuplicateColumns();

    // Detect line ending type
    LineEnding detectLineEnding(const uint8_t* buf, size_t len, size_t pos);

    // Get context string around an error position
    std::string getContext(const uint8_t* buf, size_t len, size_t pos, size_t context_size = 20);

    // Calculate line and column from byte offset
    void getLineColumn(const uint8_t* buf, size_t offset, size_t& line, size_t& column);
};

} // namespace simdcsv

#endif // SIMDCSV_CSV_PARSER_H
