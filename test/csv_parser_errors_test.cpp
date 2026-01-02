#include <gtest/gtest.h>
#include "two_pass.h"
#include "error.h"
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

    bool hasErrorCode(const ErrorCollector& errors, ErrorCode code) {
        for (const auto& err : errors.errors()) {
            if (err.code == code) return true;
        }
        return false;
    }

    size_t countErrorCode(const ErrorCollector& errors, ErrorCode code) {
        size_t count = 0;
        for (const auto& err : errors.errors()) {
            if (err.code == code) ++count;
        }
        return count;
    }

    void printErrors(const ErrorCollector& errors) {
        for (const auto& err : errors.errors()) {
            std::cout << err.to_string() << std::endl;
        }
    }

    // Helper to parse with error collection
    bool parseWithErrors(const std::string& content, ErrorCollector& errors) {
        two_pass parser;
        auto idx = parser.init(content.size(), 1);
        const uint8_t* buf = reinterpret_cast<const uint8_t*>(content.data());
        return parser.parse_validate(buf, idx, content.size(), errors);
    }
};

// ============================================================================
// UNCLOSED QUOTE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, UnclosedQuote) {
    std::string content = readFile(getTestDataPath("unclosed_quote.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::UNCLOSED_QUOTE))
        << "Should detect unclosed quote";

    // Find the error and check severity
    for (const auto& err : errors.errors()) {
        if (err.code == ErrorCode::UNCLOSED_QUOTE) {
            EXPECT_EQ(err.severity, ErrorSeverity::FATAL);
        }
    }
}

TEST_F(CSVParserErrorTest, UnclosedQuoteEOF) {
    std::string content = readFile(getTestDataPath("unclosed_quote_eof.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    bool success = parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::UNCLOSED_QUOTE))
        << "Should detect unclosed quote at EOF";
    EXPECT_FALSE(success) << "Parsing should fail with unclosed quote";
}

// ============================================================================
// QUOTE IN UNQUOTED FIELD TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, QuoteInUnquotedField) {
    std::string content = readFile(getTestDataPath("quote_in_unquoted_field.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
        << "Should detect quote in unquoted field";
}

TEST_F(CSVParserErrorTest, QuoteNotAtStart) {
    std::string content = readFile(getTestDataPath("quote_not_at_start.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
        << "Should detect quote not at start of field";
}

TEST_F(CSVParserErrorTest, QuoteAfterData) {
    std::string content = readFile(getTestDataPath("quote_after_data.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
        << "Should detect quote after data in unquoted field";
}

TEST_F(CSVParserErrorTest, TrailingQuote) {
    std::string content = readFile(getTestDataPath("trailing_quote.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
        << "Should detect trailing quote in unquoted field";
}

// ============================================================================
// INVALID QUOTE ESCAPE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, InvalidQuoteEscape) {
    std::string content = readFile(getTestDataPath("invalid_quote_escape.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::INVALID_QUOTE_ESCAPE))
        << "Should detect invalid quote escape sequence";
}

TEST_F(CSVParserErrorTest, UnescapedQuoteInQuoted) {
    std::string content = readFile(getTestDataPath("unescaped_quote_in_quoted.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    // This should detect an error - either invalid quote escape or quote in unquoted field
    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::INVALID_QUOTE_ESCAPE) ||
                hasErrorCode(errors, ErrorCode::QUOTE_IN_UNQUOTED_FIELD))
        << "Should detect unescaped quote in quoted field";
}

TEST_F(CSVParserErrorTest, TripleQuote) {
    std::string content = readFile(getTestDataPath("triple_quote.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    // Triple quote """ in the context of """bad""" is actually valid RFC 4180:
    // The outer quotes are field delimiters, "" is an escaped quote,
    // so """bad""" represents the value "bad" (with quotes in the value).
    // This file is NOT malformed, so we expect no errors.
    EXPECT_FALSE(errors.has_errors())
        << "Triple quote sequence \"\"\"bad\"\"\" is valid RFC 4180 CSV";
}

// ============================================================================
// INCONSISTENT FIELD COUNT TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, InconsistentColumns) {
    std::string content = readFile(getTestDataPath("inconsistent_columns.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::INCONSISTENT_FIELD_COUNT))
        << "Should detect inconsistent column count";
}

TEST_F(CSVParserErrorTest, InconsistentColumnsAllRows) {
    std::string content = readFile(getTestDataPath("inconsistent_columns_all_rows.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::INCONSISTENT_FIELD_COUNT))
        << "Should detect inconsistent column counts across all rows";

    // Multiple rows have wrong field count
    size_t count = countErrorCode(errors, ErrorCode::INCONSISTENT_FIELD_COUNT);
    EXPECT_GE(count, 2) << "Should have multiple field count errors";
}

// ============================================================================
// EMPTY HEADER TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, EmptyHeader) {
    std::string content = readFile(getTestDataPath("empty_header.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::EMPTY_HEADER))
        << "Should detect empty header row";
}

// ============================================================================
// DUPLICATE COLUMN NAMES TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, DuplicateColumnNames) {
    std::string content = readFile(getTestDataPath("duplicate_column_names.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::DUPLICATE_COLUMN_NAMES))
        << "Should detect duplicate column names";

    // Count duplicates - A and B both appear twice
    size_t count = countErrorCode(errors, ErrorCode::DUPLICATE_COLUMN_NAMES);
    EXPECT_GE(count, 2) << "Should detect at least 2 duplicate column names (A and B)";
}

// ============================================================================
// NULL BYTE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, NullByte) {
    std::string content = readFile(getTestDataPath("null_byte.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::NULL_BYTE))
        << "Should detect null byte in data";
}

// ============================================================================
// MIXED LINE ENDINGS TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, MixedLineEndings) {
    std::string content = readFile(getTestDataPath("mixed_line_endings.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::MIXED_LINE_ENDINGS))
        << "Should detect mixed line endings";

    // Should be a warning, not an error
    for (const auto& err : errors.errors()) {
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
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    // This file should have multiple types of errors
    EXPECT_TRUE(errors.has_errors()) << "Should have errors";

    // Should detect duplicate column names (A appears twice)
    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::DUPLICATE_COLUMN_NAMES))
        << "Should detect duplicate column names";

    // Total error count should be >= 2
    EXPECT_GE(errors.error_count(), 2)
        << "Should have at least 2 errors";
}

// ============================================================================
// ERROR MODE TESTS
// ============================================================================

TEST_F(CSVParserErrorTest, StrictModeStopsOnFirstError) {
    std::string content = readFile(getTestDataPath("inconsistent_columns_all_rows.csv"));
    ErrorCollector errors(ErrorMode::STRICT);
    parseWithErrors(content, errors);

    // In strict mode, should stop after first error
    EXPECT_EQ(errors.error_count(), 1)
        << "Strict mode should stop after first error";
}

TEST_F(CSVParserErrorTest, PermissiveModeCollectsAllErrors) {
    std::string content = readFile(getTestDataPath("inconsistent_columns_all_rows.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors);

    // In permissive mode, should collect all errors
    EXPECT_GE(errors.error_count(), 2)
        << "Permissive mode should collect multiple errors";
}

// ============================================================================
// EDGE CASES
// ============================================================================

TEST_F(CSVParserErrorTest, EmptyFile) {
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors("", errors);

    EXPECT_FALSE(errors.has_errors())
        << "Empty file should not generate errors";
}

TEST_F(CSVParserErrorTest, SingleLineNoNewline) {
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors("A,B,C", errors);

    EXPECT_FALSE(errors.has_errors())
        << "Single line without newline should parse without errors";
}

TEST_F(CSVParserErrorTest, ValidCSVNoErrors) {
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithErrors("A,B,C\n1,2,3\n4,5,6\n", errors);

    EXPECT_FALSE(errors.has_errors())
        << "Valid CSV should not generate errors";
}

// ============================================================================
// SIMD-BASED ERROR DETECTION TESTS (APPROACH 2)
// ============================================================================

// Helper to parse with SIMD error detection
bool parseWithSIMDErrors(const std::string& content, ErrorCollector& errors) {
    two_pass parser;
    auto idx = parser.init(content.size(), 1);
    const uint8_t* buf = reinterpret_cast<const uint8_t*>(content.data());
    return parser.parse_with_simd_errors(buf, idx, content.size(), errors);
}

TEST_F(CSVParserErrorTest, SIMDErrorLocationsBasic) {
    // Test SIMDErrorLocations class
    SIMDErrorLocations locations;

    locations.add_location(100, ErrorCode::NULL_BYTE);
    locations.add_location(50, ErrorCode::QUOTE_IN_UNQUOTED_FIELD);
    locations.add_location(200, ErrorCode::NULL_BYTE);

    EXPECT_EQ(locations.size(), 3);

    locations.sort_by_offset();

    const auto& locs = locations.locations();
    EXPECT_EQ(locs[0].byte_offset, 50);
    EXPECT_EQ(locs[1].byte_offset, 100);
    EXPECT_EQ(locs[2].byte_offset, 200);
}

TEST_F(CSVParserErrorTest, SIMDErrorLocationsMerge) {
    SIMDErrorLocations loc1;
    SIMDErrorLocations loc2;

    loc1.add_location(100, ErrorCode::NULL_BYTE);
    loc2.add_location(50, ErrorCode::NULL_BYTE);
    loc1.add_location(200, ErrorCode::NULL_BYTE);

    std::vector<SIMDErrorLocations> others = {loc1, loc2};
    SIMDErrorLocations merged;
    merged.merge_sorted(others);

    EXPECT_EQ(merged.size(), 3);

    // Should be sorted
    const auto& locs = merged.locations();
    EXPECT_EQ(locs[0].byte_offset, 50);
    EXPECT_EQ(locs[1].byte_offset, 100);
    EXPECT_EQ(locs[2].byte_offset, 200);
}

TEST_F(CSVParserErrorTest, SIMDDetectsNullBytes) {
    std::string content = "A,B,C\n1,2,3\n4,";
    content += '\0';  // NULL byte
    content += ",6\n";

    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithSIMDErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::NULL_BYTE))
        << "SIMD should detect NULL byte";
}

TEST_F(CSVParserErrorTest, SIMDDetectsMultipleNullBytes) {
    std::string content = "A,B,C\n";

    // Add rows with NULL bytes at various positions
    for (int i = 0; i < 100; ++i) {
        content += "1,2,3\n";
    }
    content += "1,";
    content += '\0';  // First NULL byte
    content += ",3\n";

    for (int i = 0; i < 100; ++i) {
        content += "4,5,6\n";
    }
    content += "7,8,";
    content += '\0';  // Second NULL byte
    content += "\n";

    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithSIMDErrors(content, errors);

    size_t null_count = countErrorCode(errors, ErrorCode::NULL_BYTE);
    EXPECT_GE(null_count, 2) << "SIMD should detect multiple NULL bytes";
}

TEST_F(CSVParserErrorTest, SIMDNoErrorsOnValidCSV) {
    std::string content = "A,B,C\n1,2,3\n4,5,6\n7,8,9\n";

    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithSIMDErrors(content, errors);

    // No NULL bytes, no quote errors
    EXPECT_FALSE(hasErrorCode(errors, ErrorCode::NULL_BYTE));
}

TEST_F(CSVParserErrorTest, SIMDDetectsNullByteFile) {
    std::string content = readFile(getTestDataPath("null_byte.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithSIMDErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::NULL_BYTE))
        << "SIMD should detect NULL byte from file";
}

TEST_F(CSVParserErrorTest, SIMDDetectsFieldCountErrors) {
    std::string content = "A,B,C\n1,2,3\n1,2\n4,5,6\n";

    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithSIMDErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::INCONSISTENT_FIELD_COUNT))
        << "SIMD approach should still detect field count errors";
}

TEST_F(CSVParserErrorTest, SIMDDetectsDuplicateColumns) {
    std::string content = readFile(getTestDataPath("duplicate_column_names.csv"));
    ErrorCollector errors(ErrorMode::PERMISSIVE);
    parseWithSIMDErrors(content, errors);

    EXPECT_TRUE(hasErrorCode(errors, ErrorCode::DUPLICATE_COLUMN_NAMES))
        << "SIMD approach should still detect duplicate columns";
}

TEST_F(CSVParserErrorTest, SIMDVsRegularConsistency) {
    // Compare SIMD and regular error detection for NULL bytes
    std::string content = "A,B,C\n1,2,3\n4,";
    content += '\0';
    content += ",6\n7,8,9\n";

    ErrorCollector errors1(ErrorMode::PERMISSIVE);
    parseWithErrors(content, errors1);

    ErrorCollector errors2(ErrorMode::PERMISSIVE);
    parseWithSIMDErrors(content, errors2);

    // Both should detect NULL byte
    EXPECT_EQ(hasErrorCode(errors1, ErrorCode::NULL_BYTE),
              hasErrorCode(errors2, ErrorCode::NULL_BYTE))
        << "SIMD and regular parsing should detect same NULL byte errors";
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
        ErrorCollector errors(ErrorMode::PERMISSIVE);
        parseWithErrors(content, errors);

        if (!hasErrorCode(errors, expected_error)) {
            std::cout << "FAIL: " << filename << " - expected "
                      << error_code_to_string(expected_error) << " but got:" << std::endl;
            if (errors.has_errors()) {
                printErrors(errors);
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
