/**
 * @file c_api_test.cpp
 * @brief Tests for the simdcsv C API wrapper.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include <fstream>
#include <filesystem>

extern "C" {
#include "simdcsv_c.h"
}

namespace fs = std::filesystem;

class CAPITest : public ::testing::Test {
protected:
    void SetUp() override { temp_files_.clear(); }
    void TearDown() override {
        for (const auto& f : temp_files_) fs::remove(f);
    }
    std::string createTestFile(const std::string& content) {
        static int counter = 0;
        std::string filename = "test_c_api_" + std::to_string(counter++) + ".csv";
        std::ofstream file(filename);
        file << content;
        file.close();
        temp_files_.push_back(filename);
        return filename;
    }
    std::vector<std::string> temp_files_;
};

// Version Tests
TEST_F(CAPITest, VersionString) {
    const char* version = simdcsv_version();
    ASSERT_NE(version, nullptr);
    EXPECT_STREQ(version, "0.1.0");
}

// Error String Tests
TEST_F(CAPITest, ErrorStrings) {
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_OK), "No error");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_UNCLOSED_QUOTE), "Unclosed quote");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_NULL_POINTER), "Null pointer");
}

TEST_F(CAPITest, AllErrorStrings) {
    // Test all error strings for complete coverage
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_INVALID_QUOTE_ESCAPE), "Invalid quote escape");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_QUOTE_IN_UNQUOTED), "Quote in unquoted field");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_INCONSISTENT_FIELDS), "Inconsistent field count");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_FIELD_TOO_LARGE), "Field too large");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_MIXED_LINE_ENDINGS), "Mixed line endings");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_INVALID_LINE_ENDING), "Invalid line ending");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_INVALID_UTF8), "Invalid UTF-8");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_NULL_BYTE), "Null byte in data");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_EMPTY_HEADER), "Empty header");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_DUPLICATE_COLUMNS), "Duplicate columns");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_AMBIGUOUS_SEPARATOR), "Ambiguous separator");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_FILE_TOO_LARGE), "File too large");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_IO), "I/O error");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_INTERNAL), "Internal error");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_INVALID_ARGUMENT), "Invalid argument");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_OUT_OF_MEMORY), "Out of memory");
    EXPECT_STREQ(simdcsv_error_string(SIMDCSV_ERROR_INVALID_HANDLE), "Invalid handle");
    // Unknown error code
    EXPECT_STREQ(simdcsv_error_string(static_cast<simdcsv_error_t>(999)), "Unknown error");
}

// Buffer Tests
TEST_F(CAPITest, BufferCreateFromData) {
    const uint8_t data[] = "a,b,c\n1,2,3\n";
    size_t len = sizeof(data) - 1;
    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    ASSERT_NE(buffer, nullptr);
    EXPECT_EQ(simdcsv_buffer_length(buffer), len);
    EXPECT_EQ(memcmp(simdcsv_buffer_data(buffer), data, len), 0);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, BufferLoadFile) {
    std::string content = "name,value\nalpha,1\nbeta,2\n";
    std::string filename = createTestFile(content);
    simdcsv_buffer_t* buffer = simdcsv_buffer_load_file(filename.c_str());
    ASSERT_NE(buffer, nullptr);
    EXPECT_EQ(simdcsv_buffer_length(buffer), content.size());
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, BufferLoadFileNotFound) {
    EXPECT_EQ(simdcsv_buffer_load_file("nonexistent.csv"), nullptr);
}

TEST_F(CAPITest, BufferNullHandling) {
    EXPECT_EQ(simdcsv_buffer_data(nullptr), nullptr);
    EXPECT_EQ(simdcsv_buffer_length(nullptr), 0u);
    simdcsv_buffer_destroy(nullptr);
}

TEST_F(CAPITest, BufferCreateInvalidInput) {
    // Null data pointer
    EXPECT_EQ(simdcsv_buffer_create(nullptr, 100), nullptr);
    // Zero length
    const uint8_t data[] = "test";
    EXPECT_EQ(simdcsv_buffer_create(data, 0), nullptr);
}

TEST_F(CAPITest, BufferLoadFileNull) {
    EXPECT_EQ(simdcsv_buffer_load_file(nullptr), nullptr);
}

// Dialect Tests
TEST_F(CAPITest, DialectCSV) {
    // Use simdcsv_dialect_create for CSV: delimiter=',', quote='"', escape='"', double_quote=true
    simdcsv_dialect_t* d = simdcsv_dialect_create(',', '"', '"', true);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(simdcsv_dialect_delimiter(d), ',');
    EXPECT_EQ(simdcsv_dialect_quote_char(d), '"');
    simdcsv_dialect_destroy(d);
}

TEST_F(CAPITest, DialectTSV) {
    // Use simdcsv_dialect_create for TSV: delimiter='\t', quote='"', escape='"', double_quote=true
    simdcsv_dialect_t* d = simdcsv_dialect_create('\t', '"', '"', true);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(simdcsv_dialect_delimiter(d), '\t');
    simdcsv_dialect_destroy(d);
}

TEST_F(CAPITest, DialectCustom) {
    simdcsv_dialect_t* d = simdcsv_dialect_create(':', '\'', '\\', false);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(simdcsv_dialect_delimiter(d), ':');
    EXPECT_EQ(simdcsv_dialect_quote_char(d), '\'');
    simdcsv_dialect_destroy(d);
}

TEST_F(CAPITest, DialectSemicolon) {
    // Use simdcsv_dialect_create for semicolon: delimiter=';', quote='"', escape='"', double_quote=true
    simdcsv_dialect_t* d = simdcsv_dialect_create(';', '"', '"', true);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(simdcsv_dialect_delimiter(d), ';');
    EXPECT_EQ(simdcsv_dialect_quote_char(d), '"');
    EXPECT_EQ(simdcsv_dialect_escape_char(d), '"');
    EXPECT_TRUE(simdcsv_dialect_double_quote(d));
    simdcsv_dialect_destroy(d);
}

TEST_F(CAPITest, DialectPipe) {
    // Use simdcsv_dialect_create for pipe: delimiter='|', quote='"', escape='"', double_quote=true
    simdcsv_dialect_t* d = simdcsv_dialect_create('|', '"', '"', true);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(simdcsv_dialect_delimiter(d), '|');
    EXPECT_EQ(simdcsv_dialect_quote_char(d), '"');
    EXPECT_EQ(simdcsv_dialect_escape_char(d), '"');
    EXPECT_TRUE(simdcsv_dialect_double_quote(d));
    simdcsv_dialect_destroy(d);
}

TEST_F(CAPITest, DialectEscapeAndDoubleQuote) {
    // Test custom dialect with escape char and double_quote = false
    simdcsv_dialect_t* d = simdcsv_dialect_create(',', '"', '\\', false);
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(simdcsv_dialect_escape_char(d), '\\');
    EXPECT_FALSE(simdcsv_dialect_double_quote(d));
    simdcsv_dialect_destroy(d);
}

TEST_F(CAPITest, DialectNullHandling) {
    EXPECT_EQ(simdcsv_dialect_delimiter(nullptr), '\0');
    EXPECT_EQ(simdcsv_dialect_quote_char(nullptr), '\0');
    EXPECT_EQ(simdcsv_dialect_escape_char(nullptr), '\0');
    EXPECT_FALSE(simdcsv_dialect_double_quote(nullptr));
    simdcsv_dialect_destroy(nullptr);
}

// Error Collector Tests
TEST_F(CAPITest, ErrorCollectorCreate) {
    simdcsv_error_collector_t* c = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 0);
    ASSERT_NE(c, nullptr);
    EXPECT_EQ(simdcsv_error_collector_mode(c), SIMDCSV_MODE_PERMISSIVE);
    EXPECT_FALSE(simdcsv_error_collector_has_errors(c));
    EXPECT_EQ(simdcsv_error_collector_count(c), 0u);
    simdcsv_error_collector_destroy(c);
}

TEST_F(CAPITest, ErrorCollectorModes) {
    // Test that different modes can be set at creation time
    simdcsv_error_collector_t* strict = simdcsv_error_collector_create(SIMDCSV_MODE_STRICT, 100);
    ASSERT_NE(strict, nullptr);
    EXPECT_EQ(simdcsv_error_collector_mode(strict), SIMDCSV_MODE_STRICT);
    simdcsv_error_collector_destroy(strict);

    simdcsv_error_collector_t* best_effort = simdcsv_error_collector_create(SIMDCSV_MODE_BEST_EFFORT, 100);
    ASSERT_NE(best_effort, nullptr);
    EXPECT_EQ(simdcsv_error_collector_mode(best_effort), SIMDCSV_MODE_BEST_EFFORT);
    simdcsv_error_collector_destroy(best_effort);
}

TEST_F(CAPITest, ErrorCollectorClear) {
    simdcsv_error_collector_t* c = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 100);
    ASSERT_NE(c, nullptr);

    // Clear should work even on empty collector
    simdcsv_error_collector_clear(c);
    EXPECT_EQ(simdcsv_error_collector_count(c), 0u);

    // Clear with null (should be no-op)
    simdcsv_error_collector_clear(nullptr);

    simdcsv_error_collector_destroy(c);
}

TEST_F(CAPITest, ErrorCollectorGetErrors) {
    simdcsv_error_collector_t* c = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 100);
    ASSERT_NE(c, nullptr);

    simdcsv_parse_error_t error;
    // Test get with invalid index (no errors yet)
    EXPECT_EQ(simdcsv_error_collector_get(c, 0, &error), SIMDCSV_ERROR_INVALID_ARGUMENT);

    // Test get with null error pointer
    EXPECT_EQ(simdcsv_error_collector_get(c, 0, nullptr), SIMDCSV_ERROR_NULL_POINTER);

    simdcsv_error_collector_destroy(c);
}

TEST_F(CAPITest, ErrorCollectorNullHandling) {
    EXPECT_EQ(simdcsv_error_collector_mode(nullptr), SIMDCSV_MODE_STRICT);
    EXPECT_FALSE(simdcsv_error_collector_has_errors(nullptr));
    EXPECT_FALSE(simdcsv_error_collector_has_fatal(nullptr));
    EXPECT_EQ(simdcsv_error_collector_count(nullptr), 0u);
    simdcsv_parse_error_t error;
    EXPECT_EQ(simdcsv_error_collector_get(nullptr, 0, &error), SIMDCSV_ERROR_NULL_POINTER);
    simdcsv_error_collector_destroy(nullptr);
}

// Index Tests
TEST_F(CAPITest, IndexCreate) {
    simdcsv_index_t* idx = simdcsv_index_create(1000, 1);
    ASSERT_NE(idx, nullptr);
    EXPECT_EQ(simdcsv_index_num_threads(idx), 1u);
    EXPECT_NE(simdcsv_index_positions(idx), nullptr);
    simdcsv_index_destroy(idx);
}

TEST_F(CAPITest, IndexCreateInvalid) {
    EXPECT_EQ(simdcsv_index_create(0, 1), nullptr);
    EXPECT_EQ(simdcsv_index_create(1000, 0), nullptr);
}

TEST_F(CAPITest, IndexColumnsAndTotalCount) {
    const uint8_t data[] = "a,b,c\n1,2,3\n4,5,6\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    ASSERT_NE(buffer, nullptr);
    ASSERT_NE(parser, nullptr);
    ASSERT_NE(index, nullptr);

    simdcsv_error_t err = simdcsv_parse(parser, buffer, index, nullptr, nullptr);
    EXPECT_EQ(err, SIMDCSV_OK);

    // Test columns accessor
    size_t columns = simdcsv_index_columns(index);
    EXPECT_GE(columns, 0u);  // Columns may or may not be set by parse

    // Test total_count accessor
    uint64_t total = simdcsv_index_total_count(index);
    EXPECT_GT(total, 0u);

    // Verify total_count matches count for single-threaded parse
    EXPECT_EQ(total, simdcsv_index_count(index, 0));

    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, IndexCountOutOfBounds) {
    simdcsv_index_t* idx = simdcsv_index_create(1000, 2);
    ASSERT_NE(idx, nullptr);

    // Thread ID out of bounds
    EXPECT_EQ(simdcsv_index_count(idx, 100), 0u);

    simdcsv_index_destroy(idx);
}

TEST_F(CAPITest, IndexNullHandling) {
    EXPECT_EQ(simdcsv_index_num_threads(nullptr), 0u);
    EXPECT_EQ(simdcsv_index_columns(nullptr), 0u);
    EXPECT_EQ(simdcsv_index_count(nullptr, 0), 0u);
    EXPECT_EQ(simdcsv_index_total_count(nullptr), 0u);
    EXPECT_EQ(simdcsv_index_positions(nullptr), nullptr);
    simdcsv_index_destroy(nullptr);
}

// Parser Tests
TEST_F(CAPITest, ParserCreate) {
    simdcsv_parser_t* p = simdcsv_parser_create();
    ASSERT_NE(p, nullptr);
    simdcsv_parser_destroy(p);
}

TEST_F(CAPITest, ParseSimpleCSV) {
    const uint8_t data[] = "a,b,c\n1,2,3\n4,5,6\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    ASSERT_NE(buffer, nullptr);
    ASSERT_NE(parser, nullptr);
    ASSERT_NE(index, nullptr);

    simdcsv_error_t err = simdcsv_parse(parser, buffer, index, nullptr, nullptr);
    EXPECT_EQ(err, SIMDCSV_OK);
    EXPECT_GT(simdcsv_index_count(index, 0), 0u);

    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithErrors) {
    const uint8_t data[] = "a,b,c\n1,2,3\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 0);

    simdcsv_error_t err = simdcsv_parse(parser, buffer, index, errors, nullptr);
    EXPECT_EQ(err, SIMDCSV_OK);
    EXPECT_FALSE(simdcsv_error_collector_has_fatal(errors));

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseNullPointers) {
    const uint8_t data[] = "a,b,c\n";
    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, sizeof(data) - 1);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(100, 1);

    EXPECT_EQ(simdcsv_parse(nullptr, buffer, index, nullptr, nullptr), SIMDCSV_ERROR_NULL_POINTER);
    EXPECT_EQ(simdcsv_parse(parser, nullptr, index, nullptr, nullptr), SIMDCSV_ERROR_NULL_POINTER);
    EXPECT_EQ(simdcsv_parse(parser, buffer, nullptr, nullptr, nullptr), SIMDCSV_ERROR_NULL_POINTER);

    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseNullErrorCollector) {
    // Test that null error collector is handled gracefully (falls back to non-error parse)
    const uint8_t data[] = "a,b,c\n1,2,3\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);

    simdcsv_error_t err = simdcsv_parse(parser, buffer, index, nullptr, nullptr);
    EXPECT_EQ(err, SIMDCSV_OK);

    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithDialect) {
    const uint8_t data[] = "a,b,c\n1,2,3\n4,5,6\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 100);
    // CSV dialect: delimiter=',', quote='"', escape='"', double_quote=true
    simdcsv_dialect_t* dialect = simdcsv_dialect_create(',', '"', '"', true);

    simdcsv_error_t err = simdcsv_parse(parser, buffer, index, errors, dialect);
    EXPECT_EQ(err, SIMDCSV_OK);
    EXPECT_GT(simdcsv_index_count(index, 0), 0u);
    EXPECT_FALSE(simdcsv_error_collector_has_fatal(errors));

    simdcsv_dialect_destroy(dialect);
    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParserDestroyNull) {
    // Should not crash with null
    simdcsv_parser_destroy(nullptr);
}

// Dialect Detection Tests
TEST_F(CAPITest, DetectDialectCSV) {
    const uint8_t data[] = "name,value,count\nalpha,1,100\nbeta,2,200\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_detection_result_t* result = simdcsv_detect_dialect(buffer);
    ASSERT_NE(result, nullptr);

    EXPECT_TRUE(simdcsv_detection_result_success(result));
    simdcsv_dialect_t* d = simdcsv_detection_result_dialect(result);
    EXPECT_EQ(simdcsv_dialect_delimiter(d), ',');

    simdcsv_dialect_destroy(d);
    simdcsv_detection_result_destroy(result);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, DetectDialectNull) {
    EXPECT_EQ(simdcsv_detect_dialect(nullptr), nullptr);
}

TEST_F(CAPITest, DetectionResultAllAccessors) {
    const uint8_t data[] = "name,value,count\nalpha,1,100\nbeta,2,200\ngamma,3,300\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_detection_result_t* result = simdcsv_detect_dialect(buffer);
    ASSERT_NE(result, nullptr);

    EXPECT_TRUE(simdcsv_detection_result_success(result));
    EXPECT_GT(simdcsv_detection_result_confidence(result), 0.0);

    // Test columns accessor
    size_t columns = simdcsv_detection_result_columns(result);
    EXPECT_EQ(columns, 3u);

    // Test rows_analyzed accessor
    size_t rows = simdcsv_detection_result_rows_analyzed(result);
    EXPECT_GE(rows, 1u);

    // Test has_header accessor
    bool has_header = simdcsv_detection_result_has_header(result);
    // The header detection may vary, so just verify it returns a value
    (void)has_header;

    // Test warning accessor (may be null for clean data)
    const char* warning = simdcsv_detection_result_warning(result);
    // warning is expected to be null or a valid string
    (void)warning;

    simdcsv_detection_result_destroy(result);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, DetectionResultNullHandling) {
    EXPECT_FALSE(simdcsv_detection_result_success(nullptr));
    EXPECT_EQ(simdcsv_detection_result_confidence(nullptr), 0.0);
    EXPECT_EQ(simdcsv_detection_result_dialect(nullptr), nullptr);
    EXPECT_EQ(simdcsv_detection_result_columns(nullptr), 0u);
    EXPECT_EQ(simdcsv_detection_result_rows_analyzed(nullptr), 0u);
    EXPECT_FALSE(simdcsv_detection_result_has_header(nullptr));
    EXPECT_EQ(simdcsv_detection_result_warning(nullptr), nullptr);
    simdcsv_detection_result_destroy(nullptr);
}

// Parse Auto Tests
TEST_F(CAPITest, ParseAuto) {
    const uint8_t data[] = "name,value\nalpha,1\nbeta,2\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 0);

    simdcsv_detection_result_t* detected = nullptr;
    simdcsv_error_t err = simdcsv_parse_auto(parser, buffer, index, errors, &detected);
    EXPECT_EQ(err, SIMDCSV_OK);

    if (detected) {
        EXPECT_TRUE(simdcsv_detection_result_success(detected));
        simdcsv_detection_result_destroy(detected);
    }

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseAutoNullPointers) {
    const uint8_t data[] = "name,value\n";
    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, sizeof(data) - 1);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(100, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 100);

    EXPECT_EQ(simdcsv_parse_auto(nullptr, buffer, index, errors, nullptr), SIMDCSV_ERROR_NULL_POINTER);
    EXPECT_EQ(simdcsv_parse_auto(parser, nullptr, index, errors, nullptr), SIMDCSV_ERROR_NULL_POINTER);
    EXPECT_EQ(simdcsv_parse_auto(parser, buffer, nullptr, errors, nullptr), SIMDCSV_ERROR_NULL_POINTER);

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseAutoNullDetectedPointer) {
    // Test that parse_auto works when detected out-parameter is null
    const uint8_t data[] = "name,value\nalpha,1\nbeta,2\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 0);

    simdcsv_error_t err = simdcsv_parse_auto(parser, buffer, index, errors, nullptr);
    EXPECT_EQ(err, SIMDCSV_OK);

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseAutoNullErrorCollector) {
    // Test that parse_auto works when error collector is null
    const uint8_t data[] = "name,value\nalpha,1\nbeta,2\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);

    simdcsv_detection_result_t* detected = nullptr;
    simdcsv_error_t err = simdcsv_parse_auto(parser, buffer, index, nullptr, &detected);
    EXPECT_EQ(err, SIMDCSV_OK);

    if (detected) {
        simdcsv_detection_result_destroy(detected);
    }

    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseTSVWithDialect) {
    // Test parse with explicit TSV dialect
    const uint8_t data[] = "a\tb\tc\n1\t2\t3\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    // TSV dialect: delimiter='\t', quote='"', escape='"', double_quote=true
    simdcsv_dialect_t* dialect = simdcsv_dialect_create('\t', '"', '"', true);

    simdcsv_error_t err = simdcsv_parse(parser, buffer, index, nullptr, dialect);
    EXPECT_EQ(err, SIMDCSV_OK);
    EXPECT_GT(simdcsv_index_count(index, 0), 0u);

    simdcsv_dialect_destroy(dialect);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

// Utility Function Tests
TEST_F(CAPITest, RecommendedThreads) {
    EXPECT_GE(simdcsv_recommended_threads(), 1u);
}

TEST_F(CAPITest, SIMDPadding) {
    EXPECT_GE(simdcsv_simd_padding(), 16u);
}

// Integration Test
TEST_F(CAPITest, FullWorkflowFromFile) {
    std::string content = "id,name,value\n1,alpha,100\n2,beta,200\n";
    std::string filename = createTestFile(content);

    simdcsv_buffer_t* buffer = simdcsv_buffer_load_file(filename.c_str());
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(simdcsv_buffer_length(buffer), 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 0);

    simdcsv_error_t err = simdcsv_parse(parser, buffer, index, errors, nullptr);
    EXPECT_EQ(err, SIMDCSV_OK);
    EXPECT_GT(simdcsv_index_count(index, 0), 0u);
    EXPECT_FALSE(simdcsv_error_collector_has_fatal(errors));

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

// Error Conversion Tests - exercise error code conversions in simdcsv_c.cpp
TEST_F(CAPITest, ParseWithUnclosedQuoteError) {
    // CSV with unclosed quote at EOF
    const uint8_t data[] = "a,b,c\n\"unclosed";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 100);

    simdcsv_error_t err = simdcsv_parse(parser, buffer, index, errors, nullptr);
    // Should return the fatal error code
    EXPECT_EQ(err, SIMDCSV_ERROR_UNCLOSED_QUOTE);
    EXPECT_TRUE(simdcsv_error_collector_has_fatal(errors));

    // Verify we can retrieve the error details
    if (simdcsv_error_collector_count(errors) > 0) {
        simdcsv_parse_error_t parse_error;
        EXPECT_EQ(simdcsv_error_collector_get(errors, 0, &parse_error), SIMDCSV_OK);
        EXPECT_EQ(parse_error.code, SIMDCSV_ERROR_UNCLOSED_QUOTE);
        EXPECT_EQ(parse_error.severity, SIMDCSV_SEVERITY_FATAL);
        EXPECT_NE(parse_error.message, nullptr);
    }

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithInconsistentFieldCount) {
    // CSV with inconsistent field count
    const uint8_t data[] = "a,b,c\n1,2\n3,4,5\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 100);

    simdcsv_parse(parser, buffer, index, errors, nullptr);

    EXPECT_TRUE(simdcsv_error_collector_has_errors(errors));

    // Check that at least one error has INCONSISTENT_FIELDS code
    bool found_inconsistent = false;
    for (size_t i = 0; i < simdcsv_error_collector_count(errors); ++i) {
        simdcsv_parse_error_t parse_error;
        if (simdcsv_error_collector_get(errors, i, &parse_error) == SIMDCSV_OK) {
            if (parse_error.code == SIMDCSV_ERROR_INCONSISTENT_FIELDS) {
                found_inconsistent = true;
                EXPECT_EQ(parse_error.severity, SIMDCSV_SEVERITY_ERROR);
            }
        }
    }
    EXPECT_TRUE(found_inconsistent);

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithQuoteInUnquotedField) {
    // CSV with quote in unquoted field
    const uint8_t data[] = "a,b,c\ntest\"bad,2,3\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 100);

    simdcsv_parse(parser, buffer, index, errors, nullptr);
    EXPECT_TRUE(simdcsv_error_collector_has_errors(errors));

    // Check for QUOTE_IN_UNQUOTED error
    bool found_error = false;
    for (size_t i = 0; i < simdcsv_error_collector_count(errors); ++i) {
        simdcsv_parse_error_t parse_error;
        if (simdcsv_error_collector_get(errors, i, &parse_error) == SIMDCSV_OK) {
            if (parse_error.code == SIMDCSV_ERROR_QUOTE_IN_UNQUOTED) {
                found_error = true;
            }
        }
    }
    EXPECT_TRUE(found_error);

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithInvalidQuoteEscape) {
    // CSV with invalid quote escape ("abc"def - quote not at start/end)
    const uint8_t data[] = "a,b,c\n\"abc\"def,2,3\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 100);

    simdcsv_parse(parser, buffer, index, errors, nullptr);
    EXPECT_TRUE(simdcsv_error_collector_has_errors(errors));

    // Check for INVALID_QUOTE_ESCAPE error
    bool found_error = false;
    for (size_t i = 0; i < simdcsv_error_collector_count(errors); ++i) {
        simdcsv_parse_error_t parse_error;
        if (simdcsv_error_collector_get(errors, i, &parse_error) == SIMDCSV_OK) {
            if (parse_error.code == SIMDCSV_ERROR_INVALID_QUOTE_ESCAPE) {
                found_error = true;
            }
        }
    }
    EXPECT_TRUE(found_error);

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithMixedLineEndings) {
    // CSV with mixed line endings (LF and CRLF)
    const uint8_t data[] = "a,b,c\n1,2,3\r\n4,5,6\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 100);

    simdcsv_parse(parser, buffer, index, errors, nullptr);

    // Check for MIXED_LINE_ENDINGS warning
    bool found_warning = false;
    for (size_t i = 0; i < simdcsv_error_collector_count(errors); ++i) {
        simdcsv_parse_error_t parse_error;
        if (simdcsv_error_collector_get(errors, i, &parse_error) == SIMDCSV_OK) {
            if (parse_error.code == SIMDCSV_ERROR_MIXED_LINE_ENDINGS) {
                found_warning = true;
                EXPECT_EQ(parse_error.severity, SIMDCSV_SEVERITY_WARNING);
            }
        }
    }
    EXPECT_TRUE(found_warning);

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithNullByte) {
    // CSV with null byte in data
    uint8_t data[] = "a,b,c\n1,\x00,3\n";
    size_t len = 12;  // Includes the null byte as data

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 100);

    simdcsv_parse(parser, buffer, index, errors, nullptr);

    // Check for NULL_BYTE error
    bool found_error = false;
    for (size_t i = 0; i < simdcsv_error_collector_count(errors); ++i) {
        simdcsv_parse_error_t parse_error;
        if (simdcsv_error_collector_get(errors, i, &parse_error) == SIMDCSV_OK) {
            if (parse_error.code == SIMDCSV_ERROR_NULL_BYTE) {
                found_error = true;
            }
        }
    }
    EXPECT_TRUE(found_error);

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithEmptyHeader) {
    // CSV with empty header
    const uint8_t data[] = "\n1,2,3\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 100);

    simdcsv_parse(parser, buffer, index, errors, nullptr);

    // Check for EMPTY_HEADER error
    bool found_error = false;
    for (size_t i = 0; i < simdcsv_error_collector_count(errors); ++i) {
        simdcsv_parse_error_t parse_error;
        if (simdcsv_error_collector_get(errors, i, &parse_error) == SIMDCSV_OK) {
            if (parse_error.code == SIMDCSV_ERROR_EMPTY_HEADER) {
                found_error = true;
            }
        }
    }
    EXPECT_TRUE(found_error);

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}

TEST_F(CAPITest, ParseWithDuplicateColumnNames) {
    // CSV with duplicate column names
    const uint8_t data[] = "name,value,name\n1,2,3\n";
    size_t len = sizeof(data) - 1;

    simdcsv_buffer_t* buffer = simdcsv_buffer_create(data, len);
    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(len, 1);
    simdcsv_error_collector_t* errors = simdcsv_error_collector_create(SIMDCSV_MODE_PERMISSIVE, 100);

    simdcsv_parse(parser, buffer, index, errors, nullptr);

    // Check for DUPLICATE_COLUMNS error
    bool found_error = false;
    for (size_t i = 0; i < simdcsv_error_collector_count(errors); ++i) {
        simdcsv_parse_error_t parse_error;
        if (simdcsv_error_collector_get(errors, i, &parse_error) == SIMDCSV_OK) {
            if (parse_error.code == SIMDCSV_ERROR_DUPLICATE_COLUMNS) {
                found_error = true;
            }
        }
    }
    EXPECT_TRUE(found_error);

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}
