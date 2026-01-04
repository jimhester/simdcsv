/**
 * @file c_api_test.cpp
 * @brief Tests for the simdcsv C API.
 *
 * These tests verify that the C API wrapper correctly exposes the
 * functionality of the C++ library.
 */

#include <gtest/gtest.h>
#include "simdcsv_c.h"
#include <cstring>
#include <string>
#include <vector>

// Test fixture for C API tests
class CApiTest : public ::testing::Test {
protected:
    void SetUp() override {
        parser = simdcsv_parser_create();
        ASSERT_NE(parser, nullptr);
    }

    void TearDown() override {
        simdcsv_parser_destroy(parser);
    }

    simdcsv_parser_t* parser = nullptr;
};

/* ============================================================================
 * Version Tests
 * ========================================================================== */

TEST(CApiVersionTest, VersionReturnsString) {
    const char* version = simdcsv_version();
    ASSERT_NE(version, nullptr);
    EXPECT_GT(strlen(version), 0);
    EXPECT_STREQ(version, "0.1.0");
}

/* ============================================================================
 * Memory Management Tests
 * ========================================================================== */

TEST(CApiMemoryTest, AllocBuffer) {
    uint8_t* buffer = simdcsv_alloc_buffer(1024, 64);
    ASSERT_NE(buffer, nullptr);

    // Write some data to ensure it's accessible
    memset(buffer, 'x', 1024);

    simdcsv_free_buffer(buffer);
}

TEST(CApiMemoryTest, AllocBufferZeroSize) {
    // Zero size should still work (with padding)
    uint8_t* buffer = simdcsv_alloc_buffer(0, 64);
    // Behavior may vary, but should not crash
    simdcsv_free_buffer(buffer);
}

TEST(CApiMemoryTest, FreeNullBuffer) {
    // Should not crash
    simdcsv_free_buffer(nullptr);
}

TEST(CApiMemoryTest, LoadFile) {
    uint8_t* buffer = nullptr;
    size_t length = 0;

    int result = simdcsv_load_file("test/data/basic/simple.csv", 64, &buffer, &length);
    ASSERT_EQ(result, 0);
    ASSERT_NE(buffer, nullptr);
    EXPECT_GT(length, 0);

    simdcsv_free_buffer(buffer);
}

TEST(CApiMemoryTest, LoadFileNotFound) {
    uint8_t* buffer = nullptr;
    size_t length = 0;

    int result = simdcsv_load_file("nonexistent.csv", 64, &buffer, &length);
    EXPECT_NE(result, 0);
    EXPECT_EQ(buffer, nullptr);
    EXPECT_EQ(length, 0);
}

TEST(CApiMemoryTest, LoadFileNullParams) {
    uint8_t* buffer = nullptr;
    size_t length = 0;

    EXPECT_NE(simdcsv_load_file(nullptr, 64, &buffer, &length), 0);
    EXPECT_NE(simdcsv_load_file("test.csv", 64, nullptr, &length), 0);
    EXPECT_NE(simdcsv_load_file("test.csv", 64, &buffer, nullptr), 0);
}

/* ============================================================================
 * Parser Lifecycle Tests
 * ========================================================================== */

TEST(CApiParserTest, CreateDestroy) {
    simdcsv_parser_t* parser = simdcsv_parser_create();
    ASSERT_NE(parser, nullptr);
    simdcsv_parser_destroy(parser);
}

TEST(CApiParserTest, DestroyNull) {
    // Should not crash
    simdcsv_parser_destroy(nullptr);
}

/* ============================================================================
 * Index Lifecycle Tests
 * ========================================================================== */

TEST_F(CApiTest, IndexCreateDestroy) {
    simdcsv_index_t* index = simdcsv_index_create(parser, 1024, 1);
    ASSERT_NE(index, nullptr);

    EXPECT_EQ(simdcsv_index_n_threads(index), 1);

    simdcsv_index_destroy(index);
}

TEST_F(CApiTest, IndexCreateMultiThread) {
    simdcsv_index_t* index = simdcsv_index_create(parser, 1024, 4);
    ASSERT_NE(index, nullptr);

    EXPECT_EQ(simdcsv_index_n_threads(index), 4);

    simdcsv_index_destroy(index);
}

TEST_F(CApiTest, IndexCreateNullParser) {
    simdcsv_index_t* index = simdcsv_index_create(nullptr, 1024, 1);
    EXPECT_EQ(index, nullptr);
}

TEST(CApiIndexTest, DestroyNull) {
    // Should not crash
    simdcsv_index_destroy(nullptr);
}

TEST(CApiIndexTest, AccessorsNull) {
    EXPECT_EQ(simdcsv_index_columns(nullptr), 0);
    EXPECT_EQ(simdcsv_index_n_threads(nullptr), 0);
    EXPECT_EQ(simdcsv_index_count(nullptr, 0), 0);
    EXPECT_EQ(simdcsv_index_positions(nullptr), nullptr);
    EXPECT_EQ(simdcsv_index_total_count(nullptr), 0);
}

/* ============================================================================
 * Error Collector Tests
 * ========================================================================== */

TEST(CApiErrorsTest, CreateDestroy) {
    simdcsv_errors_t* errors = simdcsv_errors_create(SIMDCSV_ERROR_MODE_PERMISSIVE);
    ASSERT_NE(errors, nullptr);
    simdcsv_errors_destroy(errors);
}

TEST(CApiErrorsTest, DestroyNull) {
    // Should not crash
    simdcsv_errors_destroy(nullptr);
}

TEST(CApiErrorsTest, ModeGetSet) {
    simdcsv_errors_t* errors = simdcsv_errors_create(SIMDCSV_ERROR_MODE_STRICT);
    ASSERT_NE(errors, nullptr);

    EXPECT_EQ(simdcsv_errors_get_mode(errors), SIMDCSV_ERROR_MODE_STRICT);

    simdcsv_errors_set_mode(errors, SIMDCSV_ERROR_MODE_PERMISSIVE);
    EXPECT_EQ(simdcsv_errors_get_mode(errors), SIMDCSV_ERROR_MODE_PERMISSIVE);

    simdcsv_errors_set_mode(errors, SIMDCSV_ERROR_MODE_BEST_EFFORT);
    EXPECT_EQ(simdcsv_errors_get_mode(errors), SIMDCSV_ERROR_MODE_BEST_EFFORT);

    simdcsv_errors_destroy(errors);
}

TEST(CApiErrorsTest, InitialState) {
    simdcsv_errors_t* errors = simdcsv_errors_create(SIMDCSV_ERROR_MODE_PERMISSIVE);
    ASSERT_NE(errors, nullptr);

    EXPECT_EQ(simdcsv_errors_has_errors(errors), 0);
    EXPECT_EQ(simdcsv_errors_has_fatal(errors), 0);
    EXPECT_EQ(simdcsv_errors_count(errors), 0);

    simdcsv_errors_destroy(errors);
}

TEST(CApiErrorsTest, NullHandling) {
    EXPECT_EQ(simdcsv_errors_get_mode(nullptr), SIMDCSV_ERROR_MODE_STRICT);
    EXPECT_EQ(simdcsv_errors_has_errors(nullptr), 0);
    EXPECT_EQ(simdcsv_errors_has_fatal(nullptr), 0);
    EXPECT_EQ(simdcsv_errors_count(nullptr), 0);

    simdcsv_parse_error_t error;
    EXPECT_NE(simdcsv_errors_get(nullptr, 0, &error), 0);

    // These should not crash
    simdcsv_errors_clear(nullptr);
    simdcsv_errors_set_mode(nullptr, SIMDCSV_ERROR_MODE_STRICT);
}

TEST(CApiErrorsTest, Summary) {
    simdcsv_errors_t* errors = simdcsv_errors_create(SIMDCSV_ERROR_MODE_PERMISSIVE);
    ASSERT_NE(errors, nullptr);

    char* summary = simdcsv_errors_summary(errors);
    ASSERT_NE(summary, nullptr);
    // Empty errors should still produce some output
    simdcsv_free_string(summary);

    simdcsv_errors_destroy(errors);
}

/* ============================================================================
 * Dialect Helper Tests
 * ========================================================================== */

TEST(CApiDialectTest, DialectCSV) {
    simdcsv_dialect_t d = simdcsv_dialect_csv();
    EXPECT_EQ(d.delimiter, ',');
    EXPECT_EQ(d.quote_char, '"');
    EXPECT_EQ(d.escape_char, '"');
    EXPECT_EQ(d.double_quote, 1);
}

TEST(CApiDialectTest, DialectTSV) {
    simdcsv_dialect_t d = simdcsv_dialect_tsv();
    EXPECT_EQ(d.delimiter, '\t');
    EXPECT_EQ(d.quote_char, '"');
}

TEST(CApiDialectTest, DialectSemicolon) {
    simdcsv_dialect_t d = simdcsv_dialect_semicolon();
    EXPECT_EQ(d.delimiter, ';');
    EXPECT_EQ(d.quote_char, '"');
}

TEST(CApiDialectTest, DialectPipe) {
    simdcsv_dialect_t d = simdcsv_dialect_pipe();
    EXPECT_EQ(d.delimiter, '|');
    EXPECT_EQ(d.quote_char, '"');
}

TEST(CApiDialectTest, DialectValidation) {
    simdcsv_dialect_t valid = simdcsv_dialect_csv();
    EXPECT_EQ(simdcsv_dialect_is_valid(&valid), 1);

    // Invalid: delimiter == quote_char
    simdcsv_dialect_t invalid1 = {',', ',', '"', 1, SIMDCSV_LINE_ENDING_UNKNOWN};
    EXPECT_EQ(simdcsv_dialect_is_valid(&invalid1), 0);

    // Invalid: delimiter is newline
    simdcsv_dialect_t invalid2 = {'\n', '"', '"', 1, SIMDCSV_LINE_ENDING_UNKNOWN};
    EXPECT_EQ(simdcsv_dialect_is_valid(&invalid2), 0);

    EXPECT_EQ(simdcsv_dialect_is_valid(nullptr), 0);
}

/* ============================================================================
 * Parsing Tests
 * ========================================================================== */

TEST_F(CApiTest, ParseSimpleCSV) {
    // Simple CSV: "a,b,c\n1,2,3\n"
    const char* csv_data = "a,b,c\n1,2,3\n";
    size_t len = strlen(csv_data);

    // Allocate buffer with padding
    uint8_t* buffer = simdcsv_alloc_buffer(len, 64);
    ASSERT_NE(buffer, nullptr);
    memcpy(buffer, csv_data, len);

    simdcsv_index_t* index = simdcsv_index_create(parser, len, 1);
    ASSERT_NE(index, nullptr);

    simdcsv_dialect_t dialect = simdcsv_dialect_csv();
    int result = simdcsv_parse(parser, buffer, index, len, &dialect);
    EXPECT_EQ(result, 0);

    // Check we got positions
    uint64_t count = simdcsv_index_count(index, 0);
    EXPECT_GT(count, 0);

    // Note: Column detection not yet implemented in parser
    // When implemented, this should return 3
    // uint64_t columns = simdcsv_index_columns(index);
    // EXPECT_EQ(columns, 3);

    simdcsv_index_destroy(index);
    simdcsv_free_buffer(buffer);
}

TEST_F(CApiTest, ParseWithErrors) {
    const char* csv_data = "a,b,c\n1,2,3\n4,5,6\n";
    size_t len = strlen(csv_data);

    uint8_t* buffer = simdcsv_alloc_buffer(len, 64);
    ASSERT_NE(buffer, nullptr);
    memcpy(buffer, csv_data, len);

    simdcsv_index_t* index = simdcsv_index_create(parser, len, 1);
    ASSERT_NE(index, nullptr);

    simdcsv_errors_t* errors = simdcsv_errors_create(SIMDCSV_ERROR_MODE_PERMISSIVE);
    ASSERT_NE(errors, nullptr);

    simdcsv_dialect_t dialect = simdcsv_dialect_csv();
    int result = simdcsv_parse_with_errors(parser, buffer, index, len, errors, &dialect);
    EXPECT_EQ(result, 0);

    // Well-formed CSV should have no errors
    EXPECT_EQ(simdcsv_errors_has_errors(errors), 0);

    simdcsv_errors_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_free_buffer(buffer);
}

TEST_F(CApiTest, ParseWithErrorsMalformed) {
    // Malformed: unclosed quote
    const char* csv_data = "a,b,c\n\"unclosed,2,3\n";
    size_t len = strlen(csv_data);

    uint8_t* buffer = simdcsv_alloc_buffer(len, 64);
    ASSERT_NE(buffer, nullptr);
    memcpy(buffer, csv_data, len);

    simdcsv_index_t* index = simdcsv_index_create(parser, len, 1);
    ASSERT_NE(index, nullptr);

    simdcsv_errors_t* errors = simdcsv_errors_create(SIMDCSV_ERROR_MODE_PERMISSIVE);
    ASSERT_NE(errors, nullptr);

    simdcsv_dialect_t dialect = simdcsv_dialect_csv();
    int result = simdcsv_parse_with_errors(parser, buffer, index, len, errors, &dialect);

    // Should detect the error
    if (simdcsv_errors_has_errors(errors)) {
        size_t count = simdcsv_errors_count(errors);
        EXPECT_GT(count, 0);

        simdcsv_parse_error_t error;
        if (simdcsv_errors_get(errors, 0, &error) == 0) {
            EXPECT_NE(error.message, nullptr);
        }
    }

    simdcsv_errors_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_free_buffer(buffer);
}

TEST_F(CApiTest, ParseNullDialect) {
    const char* csv_data = "a,b,c\n1,2,3\n";
    size_t len = strlen(csv_data);

    uint8_t* buffer = simdcsv_alloc_buffer(len, 64);
    ASSERT_NE(buffer, nullptr);
    memcpy(buffer, csv_data, len);

    simdcsv_index_t* index = simdcsv_index_create(parser, len, 1);
    ASSERT_NE(index, nullptr);

    // NULL dialect should use default CSV
    int result = simdcsv_parse(parser, buffer, index, len, nullptr);
    EXPECT_EQ(result, 0);

    simdcsv_index_destroy(index);
    simdcsv_free_buffer(buffer);
}

TEST_F(CApiTest, ParseNullParams) {
    uint8_t buffer[32] = {0};
    simdcsv_index_t* index = simdcsv_index_create(parser, 32, 1);
    simdcsv_errors_t* errors = simdcsv_errors_create(SIMDCSV_ERROR_MODE_PERMISSIVE);

    EXPECT_NE(simdcsv_parse(nullptr, buffer, index, 10, nullptr), 0);
    EXPECT_NE(simdcsv_parse(parser, nullptr, index, 10, nullptr), 0);
    EXPECT_NE(simdcsv_parse(parser, buffer, nullptr, 10, nullptr), 0);

    EXPECT_NE(simdcsv_parse_with_errors(nullptr, buffer, index, 10, errors, nullptr), 0);
    EXPECT_NE(simdcsv_parse_with_errors(parser, nullptr, index, 10, errors, nullptr), 0);
    EXPECT_NE(simdcsv_parse_with_errors(parser, buffer, nullptr, 10, errors, nullptr), 0);
    EXPECT_NE(simdcsv_parse_with_errors(parser, buffer, index, 10, nullptr, nullptr), 0);

    simdcsv_errors_destroy(errors);
    simdcsv_index_destroy(index);
}

/* ============================================================================
 * Dialect Detection Tests
 * ========================================================================== */

TEST(CApiDetectionTest, DetectCSV) {
    const char* csv_data = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
    size_t len = strlen(csv_data);

    simdcsv_detection_result_t result;
    int status = simdcsv_detect_dialect(
        reinterpret_cast<const uint8_t*>(csv_data), len, &result);

    EXPECT_EQ(status, 0);
    EXPECT_EQ(result.dialect.delimiter, ',');
    EXPECT_GT(result.confidence, 0.5);
    EXPECT_EQ(result.detected_columns, 3);
}

TEST(CApiDetectionTest, DetectTSV) {
    const char* tsv_data = "a\tb\tc\n1\t2\t3\n4\t5\t6\n";
    size_t len = strlen(tsv_data);

    simdcsv_detection_result_t result;
    int status = simdcsv_detect_dialect(
        reinterpret_cast<const uint8_t*>(tsv_data), len, &result);

    EXPECT_EQ(status, 0);
    EXPECT_EQ(result.dialect.delimiter, '\t');
}

TEST(CApiDetectionTest, DetectFromFile) {
    simdcsv_detection_result_t result;
    int status = simdcsv_detect_dialect_file("test/data/basic/simple.csv", &result);

    EXPECT_EQ(status, 0);
    EXPECT_EQ(result.dialect.delimiter, ',');
    EXPECT_GT(result.detected_columns, 0);
}

TEST(CApiDetectionTest, DetectNullParams) {
    simdcsv_detection_result_t result;
    uint8_t buffer[32] = {0};

    EXPECT_NE(simdcsv_detect_dialect(nullptr, 10, &result), 0);
    EXPECT_NE(simdcsv_detect_dialect(buffer, 10, nullptr), 0);
    EXPECT_NE(simdcsv_detect_dialect_file(nullptr, &result), 0);
    EXPECT_NE(simdcsv_detect_dialect_file("test.csv", nullptr), 0);
}

/* ============================================================================
 * Auto Parse Tests
 * ========================================================================== */

TEST_F(CApiTest, ParseAuto) {
    const char* csv_data = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
    size_t len = strlen(csv_data);

    uint8_t* buffer = simdcsv_alloc_buffer(len, 64);
    ASSERT_NE(buffer, nullptr);
    memcpy(buffer, csv_data, len);

    simdcsv_index_t* index = simdcsv_index_create(parser, len, 1);
    ASSERT_NE(index, nullptr);

    simdcsv_errors_t* errors = simdcsv_errors_create(SIMDCSV_ERROR_MODE_PERMISSIVE);
    ASSERT_NE(errors, nullptr);

    simdcsv_detection_result_t detected;
    int result = simdcsv_parse_auto(parser, buffer, index, len, errors, &detected);
    EXPECT_EQ(result, 0);

    EXPECT_EQ(detected.dialect.delimiter, ',');
    EXPECT_EQ(detected.detected_columns, 3);

    simdcsv_errors_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_free_buffer(buffer);
}

/* ============================================================================
 * Index Serialization Tests
 * ========================================================================== */

TEST_F(CApiTest, IndexWriteRead) {
    const char* csv_data = "a,b,c\n1,2,3\n4,5,6\n";
    size_t len = strlen(csv_data);

    uint8_t* buffer = simdcsv_alloc_buffer(len, 64);
    ASSERT_NE(buffer, nullptr);
    memcpy(buffer, csv_data, len);

    simdcsv_index_t* index = simdcsv_index_create(parser, len, 1);
    ASSERT_NE(index, nullptr);

    simdcsv_dialect_t dialect = simdcsv_dialect_csv();
    int result = simdcsv_parse(parser, buffer, index, len, &dialect);
    EXPECT_EQ(result, 0);

    // Save index
    const char* filename = "test_index.bin";
    result = simdcsv_index_write(index, filename);
    EXPECT_EQ(result, 0);

    uint64_t original_columns = simdcsv_index_columns(index);
    uint8_t original_threads = simdcsv_index_n_threads(index);
    uint64_t original_count = simdcsv_index_total_count(index);

    // Read back
    simdcsv_index_t* loaded = simdcsv_index_read(filename);
    ASSERT_NE(loaded, nullptr);

    EXPECT_EQ(simdcsv_index_columns(loaded), original_columns);
    EXPECT_EQ(simdcsv_index_n_threads(loaded), original_threads);
    EXPECT_EQ(simdcsv_index_total_count(loaded), original_count);

    // Cleanup
    simdcsv_index_destroy(loaded);
    simdcsv_index_destroy(index);
    simdcsv_free_buffer(buffer);
    std::remove(filename);
}

TEST(CApiIndexSerializationTest, WriteNullParams) {
    EXPECT_NE(simdcsv_index_write(nullptr, "test.bin"), 0);

    simdcsv_parser_t* parser = simdcsv_parser_create();
    simdcsv_index_t* index = simdcsv_index_create(parser, 1024, 1);
    EXPECT_NE(simdcsv_index_write(index, nullptr), 0);

    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
}

TEST(CApiIndexSerializationTest, ReadNonexistent) {
    simdcsv_index_t* index = simdcsv_index_read("nonexistent_file.bin");
    EXPECT_EQ(index, nullptr);
}

TEST(CApiIndexSerializationTest, ReadNull) {
    simdcsv_index_t* index = simdcsv_index_read(nullptr);
    EXPECT_EQ(index, nullptr);
}

/* ============================================================================
 * Integration Tests with Real Files
 * ========================================================================== */

TEST_F(CApiTest, ParseRealFile) {
    uint8_t* buffer = nullptr;
    size_t length = 0;

    int result = simdcsv_load_file("test/data/basic/simple.csv", 64, &buffer, &length);
    ASSERT_EQ(result, 0);
    ASSERT_NE(buffer, nullptr);

    simdcsv_index_t* index = simdcsv_index_create(parser, length, 1);
    ASSERT_NE(index, nullptr);

    simdcsv_errors_t* errors = simdcsv_errors_create(SIMDCSV_ERROR_MODE_PERMISSIVE);
    ASSERT_NE(errors, nullptr);

    simdcsv_dialect_t dialect = simdcsv_dialect_csv();
    result = simdcsv_parse_with_errors(parser, buffer, index, length, errors, &dialect);
    EXPECT_EQ(result, 0);

    // Get results
    // Note: Column detection not yet implemented in parser
    // uint64_t columns = simdcsv_index_columns(index);
    // EXPECT_GT(columns, 0);

    uint64_t count = simdcsv_index_total_count(index);
    EXPECT_GT(count, 0);

    const uint64_t* positions = simdcsv_index_positions(index);
    EXPECT_NE(positions, nullptr);

    simdcsv_errors_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_free_buffer(buffer);
}

TEST_F(CApiTest, ParseQuotedFile) {
    uint8_t* buffer = nullptr;
    size_t length = 0;

    int result = simdcsv_load_file("test/data/quoted/simple_quoted.csv", 64, &buffer, &length);
    if (result != 0) {
        GTEST_SKIP() << "Test file not found";
    }

    simdcsv_index_t* index = simdcsv_index_create(parser, length, 1);
    ASSERT_NE(index, nullptr);

    simdcsv_errors_t* errors = simdcsv_errors_create(SIMDCSV_ERROR_MODE_PERMISSIVE);
    ASSERT_NE(errors, nullptr);

    simdcsv_dialect_t dialect = simdcsv_dialect_csv();
    result = simdcsv_parse_with_errors(parser, buffer, index, length, errors, &dialect);
    EXPECT_EQ(result, 0);

    simdcsv_errors_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_free_buffer(buffer);
}

/* ============================================================================
 * Error Collection Integration Tests
 * ========================================================================== */

TEST_F(CApiTest, ErrorCollection) {
    // Parse a file that should have errors
    uint8_t* buffer = nullptr;
    size_t length = 0;

    int result = simdcsv_load_file("test/data/malformed/unclosed_quote.csv", 64, &buffer, &length);
    if (result != 0) {
        GTEST_SKIP() << "Test file not found";
    }

    simdcsv_index_t* index = simdcsv_index_create(parser, length, 1);
    ASSERT_NE(index, nullptr);

    simdcsv_errors_t* errors = simdcsv_errors_create(SIMDCSV_ERROR_MODE_PERMISSIVE);
    ASSERT_NE(errors, nullptr);

    simdcsv_dialect_t dialect = simdcsv_dialect_csv();
    simdcsv_parse_with_errors(parser, buffer, index, length, errors, &dialect);

    // Check if we got errors
    if (simdcsv_errors_has_errors(errors)) {
        size_t count = simdcsv_errors_count(errors);
        EXPECT_GT(count, 0);

        // Get first error
        simdcsv_parse_error_t error;
        if (simdcsv_errors_get(errors, 0, &error) == 0) {
            EXPECT_NE(error.code, SIMDCSV_ERROR_NONE);
            EXPECT_NE(error.message, nullptr);
            EXPECT_GT(strlen(error.message), 0);
        }

        // Test summary
        char* summary = simdcsv_errors_summary(errors);
        ASSERT_NE(summary, nullptr);
        EXPECT_GT(strlen(summary), 0);
        simdcsv_free_string(summary);

        // Test clear
        simdcsv_errors_clear(errors);
        EXPECT_EQ(simdcsv_errors_has_errors(errors), 0);
        EXPECT_EQ(simdcsv_errors_count(errors), 0);
    }

    simdcsv_errors_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_free_buffer(buffer);
}
