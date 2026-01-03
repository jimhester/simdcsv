/**
 * Extended CSV Parser Tests
 *
 * Tests for additional coverage identified from zsv and duckdb test suites:
 * - Encoding (BOM, Latin-1)
 * - Whitespace handling (blank rows, trimming)
 * - Large files and buffer boundaries
 * - Comment lines
 * - Ragged CSVs (variable column counts)
 * - Fuzz-discovered edge cases
 */

#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <cstring>

#include "two_pass.h"
#include "io_util.h"
#include "mem_util.h"

class CSVExtendedTest : public ::testing::Test {
protected:
    std::string getTestDataPath(const std::string& category, const std::string& filename) {
        return "test/data/" + category + "/" + filename;
    }

    bool fileExists(const std::string& path) {
        std::ifstream f(path);
        return f.good();
    }

    size_t countNewlines(const uint8_t* data, size_t len) {
        size_t count = 0;
        bool in_quote = false;
        for (size_t i = 0; i < len; ++i) {
            if (data[i] == '"') {
                // Handle escaped quotes
                if (i + 1 < len && data[i + 1] == '"') {
                    ++i;
                } else {
                    in_quote = !in_quote;
                }
            } else if (data[i] == '\n' && !in_quote) {
                ++count;
            }
        }
        return count;
    }
};

// ============================================================================
// ENCODING TESTS
// ============================================================================

TEST_F(CSVExtendedTest, UTF8BOMFileExists) {
    std::string path = getTestDataPath("encoding", "utf8_bom.csv");
    ASSERT_TRUE(fileExists(path)) << "utf8_bom.csv should exist";
}

TEST_F(CSVExtendedTest, UTF8BOMDetection) {
    std::string path = getTestDataPath("encoding", "utf8_bom.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    // Check that file starts with BOM (EF BB BF)
    ASSERT_GE(data.size(), 3) << "File should be at least 3 bytes";
    EXPECT_EQ(data[0], 0xEF) << "First byte should be 0xEF";
    EXPECT_EQ(data[1], 0xBB) << "Second byte should be 0xBB";
    EXPECT_EQ(data[2], 0xBF) << "Third byte should be 0xBF";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, UTF8BOMParsing) {
    std::string path = getTestDataPath("encoding", "utf8_bom.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should handle BOM (may or may not skip it)
    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle UTF-8 BOM file";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, Latin1FileExists) {
    std::string path = getTestDataPath("encoding", "latin1.csv");
    ASSERT_TRUE(fileExists(path)) << "latin1.csv should exist";
}

TEST_F(CSVExtendedTest, Latin1Detection) {
    std::string path = getTestDataPath("encoding", "latin1.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    // Check for Latin-1 specific bytes (0xE9 = é in Latin-1)
    bool has_latin1_char = false;
    for (size_t i = 0; i < data.size(); ++i) {
        if (data[i] == 0xE9) {  // é in Latin-1
            has_latin1_char = true;
            break;
        }
    }
    EXPECT_TRUE(has_latin1_char) << "File should contain Latin-1 character 0xE9";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, Latin1Parsing) {
    std::string path = getTestDataPath("encoding", "latin1.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should parse Latin-1 file (treating bytes as-is)
    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle Latin-1 file";

    aligned_free((void*)data.data());
}

// ============================================================================
// WHITESPACE TESTS
// ============================================================================

TEST_F(CSVExtendedTest, BlankLeadingRowsFileExists) {
    std::string path = getTestDataPath("whitespace", "blank_leading_rows.csv");
    ASSERT_TRUE(fileExists(path)) << "blank_leading_rows.csv should exist";
}

TEST_F(CSVExtendedTest, BlankLeadingRowsParsing) {
    std::string path = getTestDataPath("whitespace", "blank_leading_rows.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle blank leading rows";

    // File has 5 blank lines + header + 3 data rows = 9 lines
    size_t newlines = countNewlines(data.data(), data.size());
    EXPECT_EQ(newlines, 9) << "File should have 9 lines (5 blank + header + 3 data)";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, WhitespaceOnlyRowsFileExists) {
    std::string path = getTestDataPath("whitespace", "whitespace_only_rows.csv");
    ASSERT_TRUE(fileExists(path)) << "whitespace_only_rows.csv should exist";
}

TEST_F(CSVExtendedTest, WhitespaceOnlyRowsParsing) {
    std::string path = getTestDataPath("whitespace", "whitespace_only_rows.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle whitespace-only rows";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, TrimFieldsFileExists) {
    std::string path = getTestDataPath("whitespace", "trim_fields.csv");
    ASSERT_TRUE(fileExists(path)) << "trim_fields.csv should exist";
}

TEST_F(CSVExtendedTest, BlankRowsMixedFileExists) {
    std::string path = getTestDataPath("whitespace", "blank_rows_mixed.csv");
    ASSERT_TRUE(fileExists(path)) << "blank_rows_mixed.csv should exist";
}

TEST_F(CSVExtendedTest, BlankRowsMixedParsing) {
    std::string path = getTestDataPath("whitespace", "blank_rows_mixed.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle blank rows mixed throughout";

    aligned_free((void*)data.data());
}

// ============================================================================
// LARGE FILE / BUFFER BOUNDARY TESTS
// ============================================================================

TEST_F(CSVExtendedTest, LongLineFileExists) {
    std::string path = getTestDataPath("large", "long_line.csv");
    ASSERT_TRUE(fileExists(path)) << "long_line.csv should exist";
}

TEST_F(CSVExtendedTest, LongLineParsing) {
    std::string path = getTestDataPath("large", "long_line.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    // File should be >10KB
    EXPECT_GT(data.size(), 10000) << "long_line.csv should be >10KB";

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle very long lines";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, LargeFieldFileExists) {
    std::string path = getTestDataPath("large", "large_field.csv");
    ASSERT_TRUE(fileExists(path)) << "large_field.csv should exist";
}

TEST_F(CSVExtendedTest, LargeFieldParsing) {
    std::string path = getTestDataPath("large", "large_field.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    // File should be >64KB (larger than typical SIMD buffer)
    EXPECT_GT(data.size(), 64000) << "large_field.csv should be >64KB";

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle very large fields";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, BufferBoundaryFileExists) {
    std::string path = getTestDataPath("large", "buffer_boundary.csv");
    ASSERT_TRUE(fileExists(path)) << "buffer_boundary.csv should exist";
}

TEST_F(CSVExtendedTest, BufferBoundaryParsing) {
    std::string path = getTestDataPath("large", "buffer_boundary.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle quoted newlines at buffer boundaries";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, ParallelChunkBoundaryFileExists) {
    std::string path = getTestDataPath("large", "parallel_chunk_boundary.csv");
    ASSERT_TRUE(fileExists(path)) << "parallel_chunk_boundary.csv should exist";
}

TEST_F(CSVExtendedTest, ParallelChunkBoundaryParsing) {
    std::string path = getTestDataPath("large", "parallel_chunk_boundary.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    // File should be ~2MB
    EXPECT_GT(data.size(), 1500000) << "parallel_chunk_boundary.csv should be >1.5MB";

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle parallel chunk boundary test file";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, ParallelChunkBoundaryMultiThreaded) {
    std::string path = getTestDataPath("large", "parallel_chunk_boundary.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    // Parse with multiple threads to stress test chunk boundaries
    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 4);  // 4 threads

    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Multi-threaded parsing should handle chunk boundaries";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, ParallelChunkBoundary8Threads) {
    std::string path = getTestDataPath("large", "parallel_chunk_boundary.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 8);  // 8 threads

    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "8-thread parsing should handle chunk boundaries";

    aligned_free((void*)data.data());
}

// ============================================================================
// COMMENT LINE TESTS
// ============================================================================

TEST_F(CSVExtendedTest, HashCommentsFileExists) {
    std::string path = getTestDataPath("comments", "hash_comments.csv");
    ASSERT_TRUE(fileExists(path)) << "hash_comments.csv should exist";
}

TEST_F(CSVExtendedTest, HashCommentsParsing) {
    std::string path = getTestDataPath("comments", "hash_comments.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser currently doesn't skip comments, but should parse without crashing
    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle files with comment-like lines";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, QuotedHashFileExists) {
    std::string path = getTestDataPath("comments", "quoted_hash.csv");
    ASSERT_TRUE(fileExists(path)) << "quoted_hash.csv should exist";
}

TEST_F(CSVExtendedTest, QuotedHashParsing) {
    std::string path = getTestDataPath("comments", "quoted_hash.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Hash inside quoted field should NOT be treated as comment
    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle # inside quoted fields";

    aligned_free((void*)data.data());
}

// ============================================================================
// RAGGED CSV TESTS (variable column counts)
// ============================================================================

TEST_F(CSVExtendedTest, FewerColumnsFileExists) {
    std::string path = getTestDataPath("ragged", "fewer_columns.csv");
    ASSERT_TRUE(fileExists(path)) << "fewer_columns.csv should exist";
}

TEST_F(CSVExtendedTest, FewerColumnsParsing) {
    std::string path = getTestDataPath("ragged", "fewer_columns.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should handle rows with fewer columns than header
    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle rows with fewer columns";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, MoreColumnsFileExists) {
    std::string path = getTestDataPath("ragged", "more_columns.csv");
    ASSERT_TRUE(fileExists(path)) << "more_columns.csv should exist";
}

TEST_F(CSVExtendedTest, MoreColumnsParsing) {
    std::string path = getTestDataPath("ragged", "more_columns.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should handle rows with more columns than header
    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle rows with more columns";

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, MixedColumnsFileExists) {
    std::string path = getTestDataPath("ragged", "mixed_columns.csv");
    ASSERT_TRUE(fileExists(path)) << "mixed_columns.csv should exist";
}

TEST_F(CSVExtendedTest, MixedColumnsParsing) {
    std::string path = getTestDataPath("ragged", "mixed_columns.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should handle mixed column counts
    bool success = parser.parse(data.data(), idx, data.size());
    EXPECT_TRUE(success) << "Parser should handle mixed column counts";

    aligned_free((void*)data.data());
}

// ============================================================================
// FUZZ TEST CASES
// ============================================================================

TEST_F(CSVExtendedTest, BadEscapeFileExists) {
    std::string path = getTestDataPath("fuzz", "bad_escape.csv");
    ASSERT_TRUE(fileExists(path)) << "bad_escape.csv should exist";
}

TEST_F(CSVExtendedTest, BadEscapeParsing) {
    std::string path = getTestDataPath("fuzz", "bad_escape.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should handle backslash escapes (non-RFC 4180)
    bool success = parser.parse(data.data(), idx, data.size());
    // May or may not succeed depending on strictness
    (void)success;  // Just ensure no crash

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, InvalidUTF8FileExists) {
    std::string path = getTestDataPath("fuzz", "invalid_utf8.csv");
    ASSERT_TRUE(fileExists(path)) << "invalid_utf8.csv should exist";
}

TEST_F(CSVExtendedTest, InvalidUTF8Parsing) {
    std::string path = getTestDataPath("fuzz", "invalid_utf8.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should not crash on invalid UTF-8
    bool success = parser.parse(data.data(), idx, data.size());
    (void)success;  // Just ensure no crash

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, ScatteredNullsFileExists) {
    std::string path = getTestDataPath("fuzz", "scattered_nulls.csv");
    ASSERT_TRUE(fileExists(path)) << "scattered_nulls.csv should exist";
}

TEST_F(CSVExtendedTest, ScatteredNullsParsing) {
    std::string path = getTestDataPath("fuzz", "scattered_nulls.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should handle null bytes without crashing
    bool success = parser.parse(data.data(), idx, data.size());
    (void)success;

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, DeepQuotesFileExists) {
    std::string path = getTestDataPath("fuzz", "deep_quotes.csv");
    ASSERT_TRUE(fileExists(path)) << "deep_quotes.csv should exist";
}

TEST_F(CSVExtendedTest, DeepQuotesParsing) {
    std::string path = getTestDataPath("fuzz", "deep_quotes.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should handle many consecutive quotes
    bool success = parser.parse(data.data(), idx, data.size());
    (void)success;

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, QuoteDelimiterAltFileExists) {
    std::string path = getTestDataPath("fuzz", "quote_delimiter_alt.csv");
    ASSERT_TRUE(fileExists(path)) << "quote_delimiter_alt.csv should exist";
}

TEST_F(CSVExtendedTest, QuoteDelimiterAltParsing) {
    std::string path = getTestDataPath("fuzz", "quote_delimiter_alt.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should handle alternating quotes and delimiters
    bool success = parser.parse(data.data(), idx, data.size());
    (void)success;

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, JustQuotesFileExists) {
    std::string path = getTestDataPath("fuzz", "just_quotes.csv");
    ASSERT_TRUE(fileExists(path)) << "just_quotes.csv should exist";
}

TEST_F(CSVExtendedTest, JustQuotesParsing) {
    std::string path = getTestDataPath("fuzz", "just_quotes.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should handle file with only quotes
    bool success = parser.parse(data.data(), idx, data.size());
    (void)success;

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, QuoteEOFFileExists) {
    std::string path = getTestDataPath("fuzz", "quote_eof.csv");
    ASSERT_TRUE(fileExists(path)) << "quote_eof.csv should exist";
}

TEST_F(CSVExtendedTest, QuoteEOFParsing) {
    std::string path = getTestDataPath("fuzz", "quote_eof.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should handle unclosed quote at EOF
    bool success = parser.parse(data.data(), idx, data.size());
    (void)success;

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, MixedCRFileExists) {
    std::string path = getTestDataPath("fuzz", "mixed_cr.csv");
    ASSERT_TRUE(fileExists(path)) << "mixed_cr.csv should exist";
}

TEST_F(CSVExtendedTest, MixedCRParsing) {
    std::string path = getTestDataPath("fuzz", "mixed_cr.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should handle mixed CR and CRLF
    bool success = parser.parse(data.data(), idx, data.size());
    (void)success;

    aligned_free((void*)data.data());
}

TEST_F(CSVExtendedTest, AFLBinaryFileExists) {
    std::string path = getTestDataPath("fuzz", "afl_binary.csv");
    ASSERT_TRUE(fileExists(path)) << "afl_binary.csv should exist";
}

TEST_F(CSVExtendedTest, AFLBinaryParsing) {
    std::string path = getTestDataPath("fuzz", "afl_binary.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Parser should not crash on binary garbage
    bool success = parser.parse(data.data(), idx, data.size());
    (void)success;

    aligned_free((void*)data.data());
}

// ============================================================================
// ALL FILES PRESENT TEST
// ============================================================================

TEST_F(CSVExtendedTest, AllExtendedTestFilesPresent) {
    // Encoding
    EXPECT_TRUE(fileExists(getTestDataPath("encoding", "utf8_bom.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("encoding", "latin1.csv")));

    // Whitespace
    EXPECT_TRUE(fileExists(getTestDataPath("whitespace", "blank_leading_rows.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("whitespace", "whitespace_only_rows.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("whitespace", "trim_fields.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("whitespace", "blank_rows_mixed.csv")));

    // Large
    EXPECT_TRUE(fileExists(getTestDataPath("large", "long_line.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("large", "large_field.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("large", "buffer_boundary.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("large", "parallel_chunk_boundary.csv")));

    // Comments
    EXPECT_TRUE(fileExists(getTestDataPath("comments", "hash_comments.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("comments", "quoted_hash.csv")));

    // Ragged
    EXPECT_TRUE(fileExists(getTestDataPath("ragged", "fewer_columns.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("ragged", "more_columns.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("ragged", "mixed_columns.csv")));

    // Fuzz
    EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "bad_escape.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "invalid_utf8.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "scattered_nulls.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "deep_quotes.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "quote_delimiter_alt.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "just_quotes.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "quote_eof.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "mixed_cr.csv")));
    EXPECT_TRUE(fileExists(getTestDataPath("fuzz", "afl_binary.csv")));
}
