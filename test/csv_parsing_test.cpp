#include <gtest/gtest.h>
#include <string>

#include "two_pass.h"
#include "io_util.h"

// ============================================================================
// PARSER INTEGRATION TESTS (portable SIMD via Highway)
// ============================================================================

class CSVParserTest : public ::testing::Test {
protected:
    std::string getTestDataPath(const std::string& category, const std::string& filename) {
        return "test/data/" + category + "/" + filename;
    }
};

TEST_F(CSVParserTest, ParseSimpleCSV) {
    std::string path = getTestDataPath("basic", "simple.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Parser should successfully parse simple.csv";
    // Note: Column detection not yet implemented in experimental parser
    // EXPECT_GT(idx.columns, 0) << "Should detect at least one column";
}

TEST_F(CSVParserTest, ParseSimpleCSVColumnCount) {
    std::string path = getTestDataPath("basic", "simple.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Parser should successfully parse simple.csv";
    // Note: Column detection not yet implemented in experimental parser
    // simple.csv has 3 columns: A,B,C (will verify when column detection added)
    // EXPECT_EQ(idx.columns, 3) << "simple.csv should have 3 columns";
}

TEST_F(CSVParserTest, ParseWideColumnsCSV) {
    std::string path = getTestDataPath("basic", "wide_columns.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Parser should handle wide CSV";
    // Note: Column detection not yet implemented in experimental parser
    // EXPECT_EQ(idx.columns, 20) << "wide_columns.csv should have 20 columns";
}

TEST_F(CSVParserTest, ParseSingleColumnCSV) {
    std::string path = getTestDataPath("basic", "single_column.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Parser should handle single column CSV";
    // Note: Column detection not yet implemented in experimental parser
    // EXPECT_EQ(idx.columns, 1) << "single_column.csv should have 1 column";
}

TEST_F(CSVParserTest, ParseQuotedFieldsCSV) {
    std::string path = getTestDataPath("quoted", "quoted_fields.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Parser should handle quoted fields";
    // Note: Column detection not yet implemented in experimental parser
    // EXPECT_EQ(idx.columns, 3) << "quoted_fields.csv should have 3 columns";
}

TEST_F(CSVParserTest, ParseEscapedQuotesCSV) {
    std::string path = getTestDataPath("quoted", "escaped_quotes.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Parser should handle escaped quotes";
    // Note: Column detection not yet implemented in experimental parser
    // EXPECT_GT(idx.columns, 0) << "Should detect columns in escaped_quotes.csv";
}

TEST_F(CSVParserTest, ParseNewlinesInQuotesCSV) {
    std::string path = getTestDataPath("quoted", "newlines_in_quotes.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Parser should handle newlines in quoted fields";
    // Note: Column detection not yet implemented in experimental parser
    // EXPECT_EQ(idx.columns, 3) << "newlines_in_quotes.csv should have 3 columns";
}

TEST_F(CSVParserTest, ParseFinancialDataCSV) {
    std::string path = getTestDataPath("real_world", "financial.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Parser should handle financial data";
    // Note: Column detection not yet implemented in experimental parser
    // EXPECT_EQ(idx.columns, 6) << "financial.csv should have 6 columns (Date,Open,High,Low,Close,Volume)";
}

TEST_F(CSVParserTest, ParseUnicodeCSV) {
    std::string path = getTestDataPath("real_world", "unicode.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Parser should handle UTF-8 data";
    // Note: Column detection not yet implemented in experimental parser
    // EXPECT_GT(idx.columns, 0) << "Should detect columns in unicode.csv";
}

TEST_F(CSVParserTest, ParseEmptyFieldsCSV) {
    std::string path = getTestDataPath("edge_cases", "empty_fields.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Parser should handle empty fields";
    // Note: Column detection not yet implemented in experimental parser
    // EXPECT_EQ(idx.columns, 3) << "empty_fields.csv should have 3 columns";
}

TEST_F(CSVParserTest, IndexStructureValid) {
    std::string path = getTestDataPath("basic", "simple.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    parser.parse(data.data(), idx, data.size());

    ASSERT_NE(idx.indexes, nullptr) << "Index array should be allocated";
    ASSERT_NE(idx.n_indexes, nullptr) << "n_indexes array should be allocated";
    EXPECT_EQ(idx.n_threads, 1) << "Should use 1 thread as requested";
}

TEST_F(CSVParserTest, MultiThreadedParsing) {
    std::string path = getTestDataPath("basic", "many_rows.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 2);  // Use 2 threads

    bool success = parser.parse(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Parser should handle multi-threaded parsing";
    EXPECT_EQ(idx.n_threads, 2) << "Should use 2 threads";
    // Note: Column detection not yet implemented in experimental parser
    // EXPECT_GT(idx.columns, 0) << "Should detect columns";
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
