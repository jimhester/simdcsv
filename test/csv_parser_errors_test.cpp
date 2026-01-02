#include <gtest/gtest.h>
#include "csv_parser.h"
#include "io_util.h"
#include <fstream>
#include <filesystem>
#include <sstream>

namespace fs = std::filesystem;
using namespace simdcsv;

class CSVParserErrorTest : public ::testing::Test {
protected:
    std::string getTestDataPath(const std::string& filename) {
        return "test/data/malformed/" + filename;
    }

    std::string readFile(const std::string& path) {
        std::ifstream file(path, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Failed to open file: " + path);
        }
        std::ostringstream ss;
        ss << file.rdbuf();
        return ss.str();
    }

    bool hasErrorCode(const ParseResult& result, ErrorCode code) {
        for (const auto& err : result.errors.errors()) {
            if (err.code == code) return true;
        }
        return false;
    }

    size_t countErrorCode(const ParseResult& result, ErrorCode code) {
        size_t count = 0;
        for (const auto& err : result.errors.errors()) {
            if (err.code == code) ++count;
        }
        return count;
    }

    void printErrors(const ParseResult& result) {
        for (const auto& err : result.errors.errors()) {
            std::cout << err.to_string() << std::endl;
        }
    }
};

// ============================================================================
// UNCLOSED QUOTE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, UnclosedQuote) {
    std::string content = readFile(getTestDataPath("unclosed_quote.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::UNCLOSED_QUOTE))
        << "Should detect unclosed quote";

    // Find the error and check it's on the right line
    for (const auto& err : result.errors.errors()) {
        if (err.code == ErrorCode::UNCLOSED_QUOTE) {
            EXPECT_GE(err.line, 3) << "Unclosed quote should be on line 3 or later";
            EXPECT_EQ(err.severity, ErrorSeverity::FATAL);
        }
    }
}

TEST_F(CSVParserErrorTest, UnclosedQuoteEOF) {
    std::string content = readFile(getTestDataPath("unclosed_quote_eof.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::UNCLOSED_QUOTE))
        << "Should detect unclosed quote at EOF";
    EXPECT_FALSE(result.success) << "Parsing should fail with unclosed quote";
}

// ============================================================================
// QUOTE IN UNQUOTED FIELD TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, QuoteInUnquotedField) {
    std::string content = readFile(getTestDataPath("quote_in_unquoted_field.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
        << "Should detect quote in unquoted field";

    for (const auto& err : result.errors.errors()) {
        if (err.code == ErrorCode::QUOTE_IN_UNQUOTED_FIELD) {
            EXPECT_EQ(err.line, 3) << "Quote error should be on line 3";
        }
    }
}

TEST_F(CSVParserErrorTest, QuoteNotAtStart) {
    std::string content = readFile(getTestDataPath("quote_not_at_start.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
        << "Should detect quote not at start of field";
}

TEST_F(CSVParserErrorTest, QuoteAfterData) {
    std::string content = readFile(getTestDataPath("quote_after_data.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
        << "Should detect quote after data in unquoted field";
}

TEST_F(CSVParserErrorTest, TrailingQuote) {
    std::string content = readFile(getTestDataPath("trailing_quote.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
        << "Should detect trailing quote in unquoted field";
}

// ============================================================================
// INVALID QUOTE ESCAPE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, InvalidQuoteEscape) {
    std::string content = readFile(getTestDataPath("invalid_quote_escape.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::INVALID_QUOTE_ESCAPE))
        << "Should detect invalid quote escape sequence";
}

TEST_F(CSVParserErrorTest, UnescapedQuoteInQuoted) {
    std::string content = readFile(getTestDataPath("unescaped_quote_in_quoted.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    // This should detect an error - either invalid quote escape or quote in unquoted field
    // depending on how the parser recovers
    EXPECT_TRUE(hasErrorCode(result, ErrorCode::INVALID_QUOTE_ESCAPE) ||
                hasErrorCode(result, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
        << "Should detect unescaped quote in quoted field";
}

TEST_F(CSVParserErrorTest, TripleQuote) {
    std::string content = readFile(getTestDataPath("triple_quote.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    // Triple quote """ in the context of """bad""" is actually valid RFC 4180:
    // The outer quotes are field delimiters, "" is an escaped quote,
    // so """bad""" represents the value "bad" (with quotes in the value).
    // This file is NOT malformed, so we expect no errors.
    EXPECT_FALSE(result.errors.has_errors())
        << "Triple quote sequence \"\"\"bad\"\"\" is valid RFC 4180 CSV";
}

// ============================================================================
// INCONSISTENT FIELD COUNT TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, InconsistentColumns) {
    std::string content = readFile(getTestDataPath("inconsistent_columns.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::INCONSISTENT_FIELD_COUNT))
        << "Should detect inconsistent column count";

    // Count number of field count errors
    size_t count = countErrorCode(result, ErrorCode::INCONSISTENT_FIELD_COUNT);
    EXPECT_GE(count, 1) << "Should have at least one field count error";
}

TEST_F(CSVParserErrorTest, InconsistentColumnsAllRows) {
    std::string content = readFile(getTestDataPath("inconsistent_columns_all_rows.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::INCONSISTENT_FIELD_COUNT))
        << "Should detect inconsistent column counts across all rows";

    // Multiple rows have wrong field count
    size_t count = countErrorCode(result, ErrorCode::INCONSISTENT_FIELD_COUNT);
    EXPECT_GE(count, 2) << "Should have multiple field count errors";
}

// ============================================================================
// EMPTY HEADER TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, EmptyHeader) {
    std::string content = readFile(getTestDataPath("empty_header.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::EMPTY_HEADER))
        << "Should detect empty header row";

    for (const auto& err : result.errors.errors()) {
        if (err.code == ErrorCode::EMPTY_HEADER) {
            EXPECT_EQ(err.line, 1) << "Empty header error should be on line 1";
        }
    }
}

// ============================================================================
// DUPLICATE COLUMN NAMES TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, DuplicateColumnNames) {
    std::string content = readFile(getTestDataPath("duplicate_column_names.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::DUPLICATE_COLUMN_NAMES))
        << "Should detect duplicate column names";

    // Count duplicates - A and B both appear twice
    size_t count = countErrorCode(result, ErrorCode::DUPLICATE_COLUMN_NAMES);
    EXPECT_GE(count, 2) << "Should detect at least 2 duplicate column names (A and B)";
}

// ============================================================================
// NULL BYTE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, NullByte) {
    std::string content = readFile(getTestDataPath("null_byte.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::NULL_BYTE))
        << "Should detect null byte in data";
}

// ============================================================================
// MIXED LINE ENDINGS TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, MixedLineEndings) {
    std::string content = readFile(getTestDataPath("mixed_line_endings.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    EXPECT_TRUE(hasErrorCode(result, ErrorCode::MIXED_LINE_ENDINGS))
        << "Should detect mixed line endings";

    // Should be a warning, not an error
    for (const auto& err : result.errors.errors()) {
        if (err.code == ErrorCode::MIXED_LINE_ENDINGS) {
            EXPECT_EQ(err.severity, ErrorSeverity::WARNING);
        }
    }
}

// ============================================================================
// MULTIPLE ERRORS TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, MultipleErrors) {
    std::string content = readFile(getTestDataPath("multiple_errors.csv"));
    CSVParser parser;
    ParseResult result = parser.parse(content);

    // This file should have multiple types of errors
    EXPECT_TRUE(result.errors.has_errors()) << "Should have errors";

    // Should detect duplicate column names (A appears twice)
    EXPECT_TRUE(hasErrorCode(result, ErrorCode::DUPLICATE_COLUMN_NAMES))
        << "Should detect duplicate column names";

    // Total error count should be >= 2
    EXPECT_GE(result.errors.error_count(), 2)
        << "Should have at least 2 errors";
}

// ============================================================================
// ERROR MODE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, StrictModeStopsOnFirstError) {
    ParserConfig config;
    config.error_mode = ErrorMode::STRICT;
    CSVParser parser(config);

    std::string content = readFile(getTestDataPath("inconsistent_columns_all_rows.csv"));
    ParseResult result = parser.parse(content);

    // In strict mode, should stop after first error
    EXPECT_EQ(result.errors.error_count(), 1)
        << "Strict mode should stop after first error";
}

TEST_F(CSVParserErrorTest, PermissiveModeCollectsAllErrors) {
    ParserConfig config;
    config.error_mode = ErrorMode::PERMISSIVE;
    CSVParser parser(config);

    std::string content = readFile(getTestDataPath("inconsistent_columns_all_rows.csv"));
    ParseResult result = parser.parse(content);

    // In permissive mode, should collect all errors
    EXPECT_GE(result.errors.error_count(), 2)
        << "Permissive mode should collect multiple errors";
}

// ============================================================================
// PARSER CONFIGURATION TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, ParserReset) {
    CSVParser parser;

    std::string content = readFile(getTestDataPath("unclosed_quote.csv"));
    ParseResult result1 = parser.parse(content);
    EXPECT_TRUE(result1.errors.has_errors());

    parser.reset();

    // Parse a valid file after reset
    std::string valid_content = "A,B,C\n1,2,3\n";
    ParseResult result2 = parser.parse(valid_content);
    EXPECT_FALSE(result2.errors.has_errors())
        << "After reset, parser should not have errors from previous parse";
}

TEST_F(CSVParserErrorTest, NoHeaderMode) {
    ParserConfig config;
    config.has_header = false;
    CSVParser parser(config);

    // File with "header" that has duplicates - but since no header mode, shouldn't error
    std::string content = "A,A,A\n1,2,3\n";
    ParseResult result = parser.parse(content);

    EXPECT_FALSE(hasErrorCode(result, ErrorCode::DUPLICATE_COLUMN_NAMES))
        << "No header mode should not check for duplicate column names";
}

// ============================================================================
// EDGE CASES
// ============================================================================

TEST_F(CSVParserErrorTest, EmptyFile) {
    CSVParser parser;
    ParseResult result = parser.parse("");

    EXPECT_FALSE(result.errors.has_errors())
        << "Empty file should not generate errors";
    EXPECT_EQ(result.rows_parsed, 0);
}

TEST_F(CSVParserErrorTest, SingleLineNoNewline) {
    CSVParser parser;
    ParseResult result = parser.parse("A,B,C");

    EXPECT_FALSE(result.errors.has_errors())
        << "Single line without newline should parse without errors";
}

TEST_F(CSVParserErrorTest, ValidCSVNoErrors) {
    CSVParser parser;
    std::string content = "A,B,C\n1,2,3\n4,5,6\n";
    ParseResult result = parser.parse(content);

    EXPECT_FALSE(result.errors.has_errors())
        << "Valid CSV should not generate errors";
    EXPECT_EQ(result.rows_parsed, 2);
}

// ============================================================================
// COMPREHENSIVE MALFORMED FILE TEST
// ============================================================================

TEST_F(CSVParserErrorTest, AllMalformedFilesGenerateErrors) {
    std::vector<std::pair<std::string, ErrorCode>> test_cases = {
        {"unclosed_quote.csv", ErrorCode::UNCLOSED_QUOTE},
        {"unclosed_quote_eof.csv", ErrorCode::UNCLOSED_QUOTE},
        {"quote_in_unquoted_field.csv", ErrorCode::QUOTE_IN_UNQUOTED_FIELD},
        {"quote_not_at_start.csv", ErrorCode::QUOTE_IN_UNQUOTED_FIELD},
        {"quote_after_data.csv", ErrorCode::QUOTE_IN_UNQUOTED_FIELD},
        {"trailing_quote.csv", ErrorCode::QUOTE_IN_UNQUOTED_FIELD},
        {"invalid_quote_escape.csv", ErrorCode::INVALID_QUOTE_ESCAPE},
        {"inconsistent_columns.csv", ErrorCode::INCONSISTENT_FIELD_COUNT},
        {"inconsistent_columns_all_rows.csv", ErrorCode::INCONSISTENT_FIELD_COUNT},
        {"empty_header.csv", ErrorCode::EMPTY_HEADER},
        {"duplicate_column_names.csv", ErrorCode::DUPLICATE_COLUMN_NAMES},
        {"null_byte.csv", ErrorCode::NULL_BYTE},
        {"mixed_line_endings.csv", ErrorCode::MIXED_LINE_ENDINGS},
    };

    int failures = 0;
    for (const auto& [filename, expected_error] : test_cases) {
        std::string path = getTestDataPath(filename);
        if (!fs::exists(path)) {
            std::cout << "Skipping missing file: " << filename << std::endl;
            continue;
        }

        std::string content = readFile(path);
        CSVParser parser;
        ParseResult result = parser.parse(content);

        if (!hasErrorCode(result, expected_error)) {
            std::cout << "FAIL: " << filename << " - expected "
                      << error_code_to_string(expected_error) << " but got:" << std::endl;
            if (result.errors.has_errors()) {
                printErrors(result);
            } else {
                std::cout << "  (no errors)" << std::endl;
            }
            failures++;
        }
    }

    EXPECT_EQ(failures, 0) << failures << " malformed files did not generate expected errors";
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
