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

// Dialect Tests
TEST_F(CAPITest, DialectCSV) {
    simdcsv_dialect_t* d = simdcsv_dialect_csv();
    ASSERT_NE(d, nullptr);
    EXPECT_EQ(simdcsv_dialect_delimiter(d), ',');
    EXPECT_EQ(simdcsv_dialect_quote_char(d), '"');
    EXPECT_EQ(simdcsv_dialect_validate(d), SIMDCSV_OK);
    simdcsv_dialect_destroy(d);
}

TEST_F(CAPITest, DialectTSV) {
    simdcsv_dialect_t* d = simdcsv_dialect_tsv();
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

TEST_F(CAPITest, DialectNullHandling) {
    EXPECT_EQ(simdcsv_dialect_delimiter(nullptr), '\0');
    EXPECT_EQ(simdcsv_dialect_validate(nullptr), SIMDCSV_ERROR_NULL_POINTER);
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

TEST_F(CAPITest, ErrorCollectorNullHandling) {
    EXPECT_EQ(simdcsv_error_collector_mode(nullptr), SIMDCSV_MODE_STRICT);
    EXPECT_FALSE(simdcsv_error_collector_has_errors(nullptr));
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

TEST_F(CAPITest, IndexNullHandling) {
    EXPECT_EQ(simdcsv_index_num_threads(nullptr), 0u);
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

    simdcsv_error_t err = simdcsv_parse(parser, buffer, index, nullptr);
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

    simdcsv_error_t err = simdcsv_parse_with_errors(parser, buffer, index, errors, nullptr);
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

    EXPECT_EQ(simdcsv_parse(nullptr, buffer, index, nullptr), SIMDCSV_ERROR_NULL_POINTER);
    EXPECT_EQ(simdcsv_parse(parser, nullptr, index, nullptr), SIMDCSV_ERROR_NULL_POINTER);
    EXPECT_EQ(simdcsv_parse(parser, buffer, nullptr, nullptr), SIMDCSV_ERROR_NULL_POINTER);

    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
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

TEST_F(CAPITest, DetectionResultNullHandling) {
    EXPECT_FALSE(simdcsv_detection_result_success(nullptr));
    EXPECT_EQ(simdcsv_detection_result_confidence(nullptr), 0.0);
    EXPECT_EQ(simdcsv_detection_result_dialect(nullptr), nullptr);
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

    simdcsv_error_t err = simdcsv_parse_with_errors(parser, buffer, index, errors, nullptr);
    EXPECT_EQ(err, SIMDCSV_OK);
    EXPECT_GT(simdcsv_index_count(index, 0), 0u);
    EXPECT_FALSE(simdcsv_error_collector_has_fatal(errors));

    simdcsv_error_collector_destroy(errors);
    simdcsv_index_destroy(index);
    simdcsv_parser_destroy(parser);
    simdcsv_buffer_destroy(buffer);
}
